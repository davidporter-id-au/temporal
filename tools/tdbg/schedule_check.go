package tdbg

import (
	"bufio"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/urfave/cli/v2"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	fqnGeneratorTask     = "scheduler.generate"
	fqnSchedulerIdleTask = "scheduler.idle"

	scheduleCheckPageSize           = 100
	defaultScheduleCheckParallelism = 10
	defaultScheduleCheckNsParallel  = 10
)

var chasmScheduleBaseQuery = fmt.Sprintf(
	"TemporalNamespaceDivision = '%d' AND ExecutionStatus = 'Running' AND TemporalSchedulePaused = false",
	chasm.SchedulerArchetypeID,
)

type scheduleCheckResult struct {
	Namespace           string   `json:"namespace"`
	ScheduleID          string   `json:"scheduleId"`
	HasGenerator        bool     `json:"hasGenerator"`
	HasIdle             bool     `json:"hasIdle"`
	Error               string   `json:"error,omitempty"`
	TaskFQNs            []string `json:"taskFQNs,omitempty"`
	HighWatermark       string   `json:"highWatermark,omitempty"`
	MissedTotal         int      `json:"missedTotal"`
	MissedWithinCatchup int      `json:"missedWithinCatchup"`
	MissedCapped        bool     `json:"missedCapped,omitempty"`
}

func isMissingTasks(r scheduleCheckResult) bool {
	return (!r.HasGenerator && !r.HasIdle) || r.Error != ""
}

func intFromEnv(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return fallback
}

type threadSafeEncoder struct {
	mu  sync.Mutex
	enc *json.Encoder
}

func (e *threadSafeEncoder) Encode(v any) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.enc.Encode(v)
}

type threadSafeWriter struct {
	mu sync.Mutex
	f  *os.File
}

func (w *threadSafeWriter) WriteString(s string) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	_, err := w.f.WriteString(s)
	return err
}

func AdminCheckSchedules(c *cli.Context, clientFactory ClientFactory) error {
	adminClient := clientFactory.AdminClient(c)
	wfClient := clientFactory.WorkflowClient(c)

	nsInputs := getNamespaceInputs(c)
	if len(nsInputs) == 0 {
		return fmt.Errorf("no namespaces provided; pass via -n flag or piped stdin")
	}

	parallelism := c.Int("parallelism")
	if parallelism <= 0 {
		parallelism = intFromEnv("TDBG_CHECK_PARALLELISM", defaultScheduleCheckParallelism)
	}
	nsParallelism := c.Int("ns-parallelism")
	if nsParallelism <= 0 {
		nsParallelism = intFromEnv("TDBG_CHECK_NS_PARALLELISM", defaultScheduleCheckNsParallel)
	}
	query := chasmScheduleBaseQuery
	outputDir := c.String("output-dir")
	ts := time.Now().UTC().Format("20060102T150405Z")

	logger := log.NewNoopLogger()
	registry, err := newChasmRegistry(logger)
	if err != nil {
		return fmt.Errorf("failed to create CHASM registry: %w", err)
	}

	// If --schedule-id is provided, check just that one (stdout only, no filter).
	if sid := c.String(FlagScheduleID); sid != "" {
		enc := &threadSafeEncoder{enc: json.NewEncoder(c.App.Writer)}
		emit := func(r scheduleCheckResult) error {
			return enc.Encode(r)
		}
		fmt.Fprintf(c.App.ErrWriter, "Checking schedule %s in %s...\n", sid, nsInputs[0].Namespace)
		return processPage(c, adminClient, registry, nsInputs[0].Namespace, []string{sid}, parallelism, emit)
	}

	// Set up output dir if requested.
	var summaryWriter *threadSafeWriter
	if outputDir != "" {
		if err := os.MkdirAll(outputDir, 0o755); err != nil {
			return fmt.Errorf("creating output dir: %w", err)
		}
		summaryPath := filepath.Join(outputDir, fmt.Sprintf("summary_%s.txt", ts))
		sf, err := os.Create(summaryPath)
		if err != nil {
			return fmt.Errorf("creating summary file: %w", err)
		}
		defer sf.Close()
		summaryWriter = &threadSafeWriter{f: sf}
		fmt.Fprintf(c.App.ErrWriter, "Writing output to %s\n", outputDir)
	}

	// Fan out across namespaces.
	nsSem := make(chan struct{}, nsParallelism)
	var wg sync.WaitGroup
	errCh := make(chan error, len(nsInputs))

	for _, nsInput := range nsInputs {
		wg.Add(1)
		go func(input namespaceInput) {
			defer wg.Done()
			nsSem <- struct{}{}
			defer func() { <-nsSem }()

			nsResult, err := runNamespaceCheck(c, wfClient, adminClient, registry, input, query, parallelism, outputDir, ts)
			if err != nil {
				errCh <- fmt.Errorf("%s: %w", input.Namespace, err)
				if summaryWriter != nil {
					_ = summaryWriter.WriteString(fmt.Sprintf("[%s] ERROR: %v\n", input.Namespace, err))
				}
				return
			}

			line := fmt.Sprintf("[%s] Done. %d checked, %d missing, %d errors\n",
				input.Namespace, nsResult.total, nsResult.missing, nsResult.errors)
			fmt.Fprint(c.App.ErrWriter, line)
			if summaryWriter != nil {
				_ = summaryWriter.WriteString(line)
			}
		}(nsInput)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		fmt.Fprintf(c.App.ErrWriter, "ERROR: %v\n", err)
	}

	return nil
}

type namespaceSummary struct {
	total   int
	missing int
	errors  int
}

func runNamespaceCheck(
	c *cli.Context,
	wfClient workflowservice.WorkflowServiceClient,
	adminClient adminservice.AdminServiceClient,
	registry *chasm.Registry,
	input namespaceInput,
	query string,
	parallelism int,
	outputDir string,
	ts string,
) (namespaceSummary, error) {
	var summary namespaceSummary
	var enc *threadSafeEncoder
	var nsFile *os.File
	namespace := input.Namespace

	if outputDir != "" {
		path := filepath.Join(outputDir, fmt.Sprintf("%s_%s.jsonl", namespace, ts))
		f, err := os.Create(path)
		if err != nil {
			return summary, fmt.Errorf("creating output file: %w", err)
		}
		defer f.Close()
		nsFile = f
		enc = &threadSafeEncoder{enc: json.NewEncoder(f)}
	} else {
		enc = &threadSafeEncoder{enc: json.NewEncoder(c.App.Writer)}
	}

	var mu sync.Mutex
	emit := func(r scheduleCheckResult) error {
		mu.Lock()
		summary.total++
		if isMissingTasks(r) {
			summary.missing++
		}
		if r.Error != "" {
			summary.errors++
		}
		mu.Unlock()

		if !isMissingTasks(r) {
			return nil
		}
		return enc.Encode(r)
	}

	if err := checkNamespace(c, wfClient, adminClient, registry, namespace, query, parallelism, input.pageTokenBytes(), emit); err != nil {
		return summary, err
	}

	if nsFile != nil {
		_ = nsFile.Sync()
	}

	return summary, nil
}

type namespaceInput struct {
	Namespace string `json:"namespace"`
	PageToken string `json:"pageToken,omitempty"`
}

func (n namespaceInput) pageTokenBytes() []byte {
	if n.PageToken == "" {
		return nil
	}
	b, err := hex.DecodeString(n.PageToken)
	if err != nil {
		return nil
	}
	return b
}

func getNamespaceInputs(c *cli.Context) []namespaceInput {
	// Piped stdin first.
	if stat, _ := os.Stdin.Stat(); (stat.Mode() & os.ModeCharDevice) == 0 {
		var inputs []namespaceInput
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if line == "" {
				continue
			}
			// Try jsonl first.
			if strings.HasPrefix(line, "{") {
				var input namespaceInput
				if json.Unmarshal([]byte(line), &input) == nil && input.Namespace != "" {
					inputs = append(inputs, input)
					continue
				}
			}
			// Plain string.
			inputs = append(inputs, namespaceInput{Namespace: line})
		}
		if len(inputs) > 0 {
			return inputs
		}
	}

	// Fall back to -n flag.
	if ns := c.String(FlagNamespace); ns != "" && ns != "default" {
		return []namespaceInput{{Namespace: ns}}
	}

	return nil
}

func checkNamespace(
	c *cli.Context,
	wfClient workflowservice.WorkflowServiceClient,
	adminClient adminservice.AdminServiceClient,
	registry *chasm.Registry,
	namespace string,
	query string,
	parallelism int,
	startPageToken []byte,
	emit func(scheduleCheckResult) error,
) error {
	if len(startPageToken) > 0 {
		fmt.Fprintf(c.App.ErrWriter, "[%s] Resuming from pageToken=%x\n", namespace, startPageToken)
	} else {
		fmt.Fprintf(c.App.ErrWriter, "[%s] Listing unpaused CHASM schedules...\n", namespace)
	}

	nextPageToken := startPageToken
	total := 0
	pageNum := 0

	for {
		ctx, cancel := newContext(c)
		resp, err := wfClient.ListWorkflowExecutions(ctx, &workflowservice.ListWorkflowExecutionsRequest{
			Namespace:     namespace,
			PageSize:      scheduleCheckPageSize,
			Query:         query,
			NextPageToken: nextPageToken,
		})
		cancel()
		if err != nil {
			if retryAfter, ok := retryableRateLimit(err); ok {
				fmt.Fprintf(c.App.ErrWriter, "[%s] Rate limited, retrying in %v...\n", namespace, retryAfter)
				time.Sleep(retryAfter * time.Duration(rand.Intn(5)))
				continue
			}
			return fmt.Errorf("listing workflows: %w", err)
		}

		var ids []string
		for _, exec := range resp.Executions {
			ids = append(ids, exec.Execution.WorkflowId)
		}

		if len(ids) > 0 {
			pageNum++
			total += len(ids)
			tokenStr := ""
			if len(nextPageToken) > 0 {
				tokenStr = fmt.Sprintf(" pageToken=%x", nextPageToken)
			}
			fmt.Fprintf(c.App.ErrWriter, "[%s] Page %d: checking %d schedules (%d total)%s\n", namespace, pageNum, len(ids), total, tokenStr)

			if err := processPage(c, adminClient, registry, namespace, ids, parallelism, emit); err != nil {
				return err
			}
		}

		nextPageToken = resp.NextPageToken
		if len(nextPageToken) == 0 {
			break
		}
	}

	if total == 0 {
		fmt.Fprintf(c.App.ErrWriter, "[%s] No unpaused CHASM schedules found.\n", namespace)
	}

	return nil
}

func processPage(
	c *cli.Context,
	adminClient adminservice.AdminServiceClient,
	registry *chasm.Registry,
	namespace string,
	ids []string,
	parallelism int,
	emit func(scheduleCheckResult) error,
) error {
	results := make(chan scheduleCheckResult, len(ids))
	sem := make(chan struct{}, parallelism)
	var wg sync.WaitGroup

	for _, id := range ids {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			results <- checkScheduleTasks(c, adminClient, registry, namespace, id)
		}(id)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	for r := range results {
		if err := emit(r); err != nil {
			return err
		}
	}

	return nil
}

func checkScheduleTasks(
	c *cli.Context,
	adminClient adminservice.AdminServiceClient,
	registry *chasm.Registry,
	namespace string,
	scheduleID string,
) scheduleCheckResult {
	result := scheduleCheckResult{
		Namespace:  namespace,
		ScheduleID: scheduleID,
	}

	ctx, cancel := newContext(c)
	defer cancel()

	var resp *adminservice.DescribeMutableStateResponse
	err := retryOnResourceExhausted(c, namespace, scheduleID, "describe-mutable-state", func() error {
		rsp, err := adminClient.DescribeMutableState(ctx, &adminservice.DescribeMutableStateRequest{
			Namespace: namespace,
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: scheduleID,
			},
			Archetype: chasm.SchedulerArchetype,
		})
		if err != nil {
			return err
		}
		resp = rsp
		return nil
	})
	if err != nil {
		result.Error = err.Error()
		return result
	}

	resp, err = adminClient.DescribeMutableState(ctx, &adminservice.DescribeMutableStateRequest{
		Namespace: namespace,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: scheduleID,
		},
		Archetype: chasm.SchedulerArchetype,
	})
	if err != nil {
		result.Error = err.Error()
		return result
	}

	parsed, err := parseSchedulerNodes(resp.GetDatabaseMutableState().GetChasmNodes(), registry)
	if err != nil {
		result.Error = err.Error()
		return result
	}

	result.HasGenerator = parsed.hasGenerator
	result.HasIdle = parsed.hasIdle
	result.TaskFQNs = parsed.taskFQNs

	if parsed.highWatermark == nil || parsed.spec == nil {
		return result
	}

	hwmTime := parsed.highWatermark.AsTime()
	result.HighWatermark = hwmTime.UTC().Format(time.RFC3339)

	total, withinCatchup, capped, err := countMissedRuns(parsed.spec, hwmTime, time.Now().UTC(), parsed.catchupWindow)
	if err != nil {
		// Don't overwrite a primary error; report spec problems via the result fields.
		result.Error = fmt.Sprintf("count missed runs: %v", err)
		return result
	}
	result.MissedTotal = total
	result.MissedWithinCatchup = withinCatchup
	result.MissedCapped = capped

	return result
}

func retryableRateLimit(err error) (time.Duration, bool) {
	if s, ok := status.FromError(err); ok && s.Code() == codes.ResourceExhausted {
		return time.Second, true
	}
	return 0, false
}
