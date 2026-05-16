package tdbg

import (
	"bufio"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/urfave/cli/v2"
	commonpb "go.temporal.io/api/common/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/codec"
	"go.temporal.io/server/common/log"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	fqnGeneratorComponent = "scheduler.generator"
	fqnSchedulerComponent = "scheduler.scheduler"
	defaultFixParallelism = 5
)

type scheduleFixResult struct {
	Namespace     string          `json:"namespace"`
	ScheduleID    string          `json:"scheduleId"`
	Action        string          `json:"action"`
	Reason        string          `json:"reason"`
	Error         string          `json:"error"`
	Paused        bool            `json:"paused"`
	Notes         string          `json:"notes"`
	HighWatermark string          `json:"highWatermark"`
	BackfillStart string          `json:"backfillStart"`
	BackfillEnd   string          `json:"backfillEnd"`
	Backfilled    bool            `json:"backfilled"`
	WithinCatchup bool            `json:"withinCatchup"`
	CatchupWindow string          `json:"catchupWindow"`
	OverlapPolicy string          `json:"overlapPolicy"`
	Spec          json.RawMessage `json:"spec"`
	UnpauseTime   string          `json:"unpauseTime"`
	HasGenerator  bool            `json:"hasGenerator"`
	HasIdle       bool            `json:"hasIdle"`
	TaskFQNs      []string        `json:"taskFQNs"`
}

type fixInput struct {
	Namespace  string `json:"namespace"`
	ScheduleID string `json:"scheduleId"`
}

type fixSummary struct {
	mu      sync.Mutex
	fixed   int
	skipped int
	errors  int
}

func (s *fixSummary) record(action string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch {
	case strings.HasPrefix(action, "fixed"):
		s.fixed++
	case strings.HasPrefix(action, "skipped"):
		s.skipped++
	default:
		s.errors++
	}
}

func AdminFixSchedule(c *cli.Context, clientFactory ClientFactory) error {
	adminClient := clientFactory.AdminClient(c)
	wfClient := clientFactory.WorkflowClient(c)

	logger := log.NewNoopLogger()
	registry, err := newChasmRegistry(logger)
	if err != nil {
		return fmt.Errorf("failed to create CHASM registry: %w", err)
	}

	parallelism := c.Int("parallelism")
	if parallelism <= 0 {
		parallelism = intFromEnv("TDBG_FIX_PARALLELISM", defaultFixParallelism)
	}

	enc := &threadSafeEncoder{enc: json.NewEncoder(c.App.Writer)}

	// Single schedule via flags.
	if stat, _ := os.Stdin.Stat(); (stat.Mode() & os.ModeCharDevice) != 0 {
		ns := c.String(FlagNamespace)
		sid := c.String(FlagScheduleID)
		if ns == "" || sid == "" {
			return fmt.Errorf("provide -n and --schedule-id, or pipe JSONL with namespace and scheduleId fields")
		}
		result := fixSchedule(c, wfClient, adminClient, registry, ns, sid)
		return enc.Encode(result)
	}

	// Streaming piped input with a fixed worker pool.
	summary := &fixSummary{}
	work := make(chan fixInput)

	var wg sync.WaitGroup
	for range parallelism {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for input := range work {
				result := fixSchedule(c, wfClient, adminClient, registry, input.Namespace, input.ScheduleID)
				_ = enc.Encode(result)
				summary.record(result.Action)
			}
		}()
	}

	scanner := bufio.NewScanner(os.Stdin)
	total := 0
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var input fixInput
		if json.Unmarshal([]byte(line), &input) != nil || input.Namespace == "" || input.ScheduleID == "" {
			continue
		}
		total++
		work <- input
	}
	close(work)

	wg.Wait()

	fmt.Fprintf(c.App.ErrWriter, "Done. %d total, %d fixed, %d skipped, %d errors\n",
		total, summary.fixed, summary.skipped, summary.errors)
	return nil
}

func fixSchedule(
	c *cli.Context,
	wfClient workflowservice.WorkflowServiceClient,
	adminClient adminservice.AdminServiceClient,
	registry *chasm.Registry,
	namespace string,
	scheduleID string,
) scheduleFixResult {
	result := scheduleFixResult{
		Namespace:  namespace,
		ScheduleID: scheduleID,
	}

	// Single DescribeMutableState call to get everything: pause status, notes,
	// policies, task presence, and generator high watermark.
	var parsed *parsedScheduler
	err := retryOnResourceExhausted(c, namespace, scheduleID, "inspect", func() error {
		var inspectErr error
		parsed, inspectErr = inspectSchedule(c, adminClient, registry, namespace, scheduleID)
		return inspectErr
	})
	if err != nil {
		result.Action = "error-inspect"
		result.Error = err.Error()
		return result
	}

	hwmStr := "<nil>"
	if parsed.highWatermark != nil {
		hwmStr = parsed.highWatermark.AsTime().UTC().Format(time.RFC3339)
	}
	specJSON, _ := codec.NewJSONPBEncoder().Encode(parsed.spec)
	fmt.Fprintf(c.App.ErrWriter, "[%s/%s] paused=%v notes=%q catchupWindow=%s overlapPolicy=%s hwm=%s hasGenerator=%v hasIdle=%v taskFQNs=%v spec=%s\n",
		namespace, scheduleID,
		parsed.isPaused,
		parsed.notes,
		parsed.catchupWindow,
		parsed.overlapPolicy,
		hwmStr,
		parsed.hasGenerator,
		parsed.hasIdle,
		parsed.taskFQNs,
		specJSON,
	)

	result.Paused = parsed.isPaused
	result.Notes = parsed.notes
	result.HasGenerator = parsed.hasGenerator
	result.HasIdle = parsed.hasIdle
	result.TaskFQNs = parsed.taskFQNs
	result.Spec = json.RawMessage(specJSON)

	// If paused by someone else, skip.
	if parsed.isPaused {
		result.Action = "skipped-paused"
		result.Reason = fmt.Sprintf("already paused (notes: %q)", parsed.notes)
		return result
	}

	// Check task presence (unless --force).
	if !c.Bool("force") {
		if parsed.hasGenerator || parsed.hasIdle {
			result.Action = "skipped-not-missing"
			return result
		}
	}

	now := time.Now().UTC()
	catchupWindow := parsed.catchupWindow
	overlapPolicy := parsed.overlapPolicy
	highWatermark := parsed.highWatermark

	if highWatermark != nil {
		result.HighWatermark = highWatermark.AsTime().UTC().Format(time.RFC3339)
	}
	result.BackfillEnd = now.Format(time.RFC3339)
	result.CatchupWindow = catchupWindow.String()
	result.OverlapPolicy = overlapPolicy.String()

	// Send unpause patch with backfill. The unpause triggers Generator.Generate()
	// regardless of current pause state, which regenerates missing tasks.
	// By default, backfill from max(HWM, now-catchupWindow) to now.
	// With --skip-catchup-window, backfill from HWM to now regardless.
	// Note: this will overwrite the schedule's notes.
	unpausePatch := &schedulepb.SchedulePatch{
		Unpause: "Unpaused via tdbg schedule fix",
	}

	if highWatermark != nil && catchupWindow > 0 {
		result.WithinCatchup = now.Sub(highWatermark.AsTime()) <= catchupWindow
	}

	if !c.Bool("no-backfill") && highWatermark != nil {
		hwmTime := highWatermark.AsTime()
		backfillStart := hwmTime

		if !c.Bool("skip-catchup-window") && catchupWindow > 0 {
			catchupBoundary := now.Add(-catchupWindow)
			if catchupBoundary.After(backfillStart) {
				backfillStart = catchupBoundary
			}
		}

		result.BackfillStart = backfillStart.UTC().Format(time.RFC3339)
		result.Backfilled = true
		unpausePatch.BackfillRequest = []*schedulepb.BackfillRequest{
			{
				StartTime: timestamppb.New(backfillStart),
				EndTime:   timestamppb.New(now),
			},
		}
	}

	unpauseTime := time.Now().UTC()
	err = retryOnResourceExhausted(c, namespace, scheduleID, "patch", func() error {
		ctx, cancel := newContext(c)
		defer cancel()
		_, patchErr := wfClient.PatchSchedule(ctx, &workflowservice.PatchScheduleRequest{
			Namespace:  namespace,
			ScheduleId: scheduleID,
			Patch:      unpausePatch,
		})
		return patchErr
	})
	if err != nil {
		result.Action = "error-patch"
		result.Error = fmt.Sprintf("patch: %v", err)
		return result
	}
	result.UnpauseTime = unpauseTime.Format(time.RFC3339)
	fmt.Fprintf(c.App.ErrWriter, "[%s/%s] unpaused at %s backfilled=%v\n",
		namespace, scheduleID, result.UnpauseTime, result.Backfilled)

	if result.Backfilled {
		result.Action = "fixed-with-backfill"
	} else {
		result.Action = "fixed-no-backfill"
	}
	return result
}

const (
	fixMaxRetries     = 3
	fixInitialBackoff = 2 * time.Second
)

func retryOnResourceExhausted(c *cli.Context, namespace, scheduleID, op string, fn func() error) error {
	backoff := fixInitialBackoff
	for attempt := 1; attempt <= fixMaxRetries; attempt++ {
		err := fn()
		if err == nil {
			return nil
		}
		if _, ok := retryableRateLimit(err); !ok || attempt == fixMaxRetries {
			return err
		}
		fmt.Fprintf(c.App.ErrWriter, "[%s/%s] %s: rate limited, retrying in %v (attempt %d/%d)...\n",
			namespace, scheduleID, op, backoff, attempt, fixMaxRetries)
		time.Sleep(backoff + time.Duration(rand.Int31n(100)*int32(time.Millisecond)))
		backoff *= 2
	}
	return nil
}

// inspectSchedule uses a single DescribeMutableState call to extract pause
// status, notes, policies, task presence, and generator high watermark.
func inspectSchedule(
	c *cli.Context,
	adminClient adminservice.AdminServiceClient,
	registry *chasm.Registry,
	namespace string,
	scheduleID string,
) (*parsedScheduler, error) {
	ctx, cancel := newContext(c)
	defer cancel()

	resp, err := adminClient.DescribeMutableState(ctx, &adminservice.DescribeMutableStateRequest{
		Namespace: namespace,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: scheduleID,
		},
		Archetype: chasm.SchedulerArchetype,
	})
	if err != nil {
		return nil, fmt.Errorf("describe mutable state: %w", err)
	}

	return parseSchedulerNodes(resp.GetDatabaseMutableState().GetChasmNodes(), registry)
}
