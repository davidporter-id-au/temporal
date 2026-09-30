package scheduleaudit

import (
	"bytes"
	"encoding/json"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
)

func TestRowWriter(t *testing.T) {
	t.Run("emits one self-contained row per flagged schedule", func(t *testing.T) {
		missTime := mustParseTime("2026-05-19T19:00:00Z")
		skipTime := mustParseTime("2026-05-19T20:00:00Z")
		closed := mustParseTime("2026-05-19T18:05:00Z")
		results := []Result{{
			Namespace:      "ns1",
			ScheduleID:     "s1",
			WorkflowType:   "Foo,Bar",
			WindowStart:    mustParseTime("2026-05-19T18:00:00Z"),
			WindowEnd:      mustParseTime("2026-05-19T22:00:00Z"),
			AsOf:           mustParseTime("2026-05-19T22:05:00Z"),
			OverlapPolicy:  enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
			CatchupWindow:  10 * time.Minute,
			DelayThreshold: time.Minute,
			Expected:       5, Actual: 4, Matched: 3,
			Scheduled: []ScheduledTime{
				{Nominal: missTime, Jittered: missTime},
				{Nominal: skipTime, Jittered: skipTime},
			},
			Observed: []Execution{{
				WorkflowID: "w1", RunID: "r1",
				StartTime:   mustParseTime("2026-05-19T18:00:00Z"),
				CloseTime:   &closed,
				Status:      enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
				NominalTime: mustParseTime("2026-05-19T18:00:00Z"),
			}},
			Missed: map[time.Time]string{
				missTime: categoryRealMiss,
				skipTime: categorySkipOverlap,
			},
		}}
		var buf bytes.Buffer
		require.NoError(t, writeResults(&buf, results))
		lines := strings.Split(strings.TrimRight(buf.String(), "\n"), "\n")
		require.Len(t, lines, 1)

		var got row
		require.NoError(t, json.Unmarshal([]byte(lines[0]), &got))
		require.Equal(t, "ns1", got.Namespace)
		require.Equal(t, "Foo,Bar", got.WorkflowType) // comma preserved (no CSV escaping)
		require.Equal(t, mustParseTime("2026-05-19T18:00:00Z"), got.Window.Start)
		require.Equal(t, mustParseTime("2026-05-19T22:00:00Z"), got.Window.End)
		require.Equal(t, mustParseTime("2026-05-19T22:05:00Z"), got.AsOf)
		require.Equal(t, auditStatusComplete, got.AuditStatus)
		require.Equal(t, "1m0s", got.DelayThreshold)
		require.Equal(t, "Skip", got.OverlapPolicy)
		require.Equal(t, "drops", got.OverlapClass)
		require.Equal(t, "10m0s", got.CatchupWindow)
		require.Equal(t, 2, got.Missed)
		require.Equal(t, 1, got.Counts.RealMiss)
		require.Equal(t, 1, got.Counts.SkipOverlap)
		require.Zero(t, got.Counts.AwaitingStart)
		require.Equal(t, []missRow{
			{Nominal: missTime, Category: categoryRealMiss},
			{Nominal: skipTime, Category: categorySkipOverlap},
		}, got.Misses)
		require.Equal(t, []scheduledTimeRow{
			{Nominal: missTime, Jittered: missTime},
			{Nominal: skipTime, Jittered: skipTime},
		}, got.ScheduledTimes)
		require.Len(t, got.Observed, 1)
		require.Equal(t, "Completed", got.Observed[0].Status)
		require.Equal(t, "w1", got.Observed[0].WorkflowID)
		require.NotNil(t, got.Observed[0].Close)
	})

	t.Run("emits schedule-level inconclusive result without misses", func(t *testing.T) {
		result := Result{Namespace: "ns1", ScheduleID: "changed", AuditStatus: auditStatusInconclusiveScheduleChanged}
		var buf bytes.Buffer
		wrote, err := NewRowWriter(&buf, 0, false).Write(result)
		require.NoError(t, err)
		require.True(t, wrote)
		require.Contains(t, buf.String(), `"audit_status":"inconclusive_schedule_changed"`)
	})

	t.Run("skips results with no missed times", func(t *testing.T) {
		results := []Result{{Namespace: "ns1", ScheduleID: "perfect", Expected: 5, Matched: 5}}
		var buf bytes.Buffer
		require.NoError(t, writeResults(&buf, results))
		require.Empty(t, buf.String())
	})

	t.Run("expected-only misses are suppressed unless included", func(t *testing.T) {
		t1 := mustParseTime("2026-05-19T19:00:00Z")
		t2 := mustParseTime("2026-05-19T20:00:00Z")
		tests := []struct {
			name            string
			missed          map[time.Time]string
			status          string
			expected        int
			matched         int
			includeExpected bool
			wantWrote       bool
			wantSuppressed  int
		}{
			{name: "skip_overlap only", missed: map[time.Time]string{t1: categorySkipOverlap}, wantSuppressed: 1},
			{name: "pending only", missed: map[time.Time]string{t1: categoryPending}, wantSuppressed: 1},
			{name: "awaiting_start only", missed: map[time.Time]string{t1: categoryAwaitingStart}, wantSuppressed: 1},
			{name: "included expected", missed: map[time.Time]string{t1: categorySkipOverlap}, includeExpected: true, wantWrote: true},
			{name: "skip_late_blocker flags", missed: map[time.Time]string{t1: categorySkipLateBlocker}, wantWrote: true},
			{name: "paused flags", missed: map[time.Time]string{t1: categoryPaused}, wantWrote: true},
			{name: "inconclusive_overlap flags", missed: map[time.Time]string{t1: categoryInconclusiveOverlap}, wantWrote: true},
			{name: "mixed flags", missed: map[time.Time]string{t1: categoryRealMiss, t2: categorySkipOverlap}, wantWrote: true},
			{name: "no misses is not counted as suppressed", missed: map[time.Time]string{}},
			{name: "jitter seed fully matched", status: auditStatusInconclusiveJitterSeed, expected: 2, matched: 2, wantSuppressed: 1},
			{name: "jitter seed fully matched, included", status: auditStatusInconclusiveJitterSeed, expected: 2, matched: 2, includeExpected: true, wantWrote: true},
			{name: "jitter seed with unmatched time flags", status: auditStatusInconclusiveJitterSeed, expected: 2, matched: 1, wantWrote: true},
			{name: "schedule changed flags", status: auditStatusInconclusiveScheduleChanged, expected: 2, matched: 2, wantWrote: true},
		}
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				var buf bytes.Buffer
				rw := NewRowWriter(&buf, 0, tc.includeExpected)
				wrote, err := rw.Write(Result{
					Namespace: "ns1", ScheduleID: "s1", Missed: tc.missed,
					AuditStatus: tc.status, Expected: tc.expected, Matched: tc.matched,
				})
				require.NoError(t, err)
				require.Equal(t, tc.wantWrote, wrote)
				require.Equal(t, tc.wantWrote, buf.Len() > 0)
				require.Equal(t, tc.wantSuppressed, rw.Suppressed())
			})
		}
	})

	t.Run("mixed row carries expected misses as context", func(t *testing.T) {
		t1 := mustParseTime("2026-05-19T19:00:00Z")
		t2 := mustParseTime("2026-05-19T20:00:00Z")
		var buf bytes.Buffer
		_, err := NewRowWriter(&buf, 0, false).Write(Result{
			Namespace: "ns1", ScheduleID: "s1",
			Missed: map[time.Time]string{t1: categorySkipLateBlocker, t2: categorySkipOverlap},
		})
		require.NoError(t, err)
		var got row
		require.NoError(t, json.Unmarshal(buf.Bytes(), &got))
		require.Equal(t, 1, got.Counts.SkipLateBlocker)
		require.Equal(t, 1, got.Counts.SkipOverlap)
		require.Equal(t, []missRow{
			{Nominal: t1, Category: categorySkipLateBlocker},
			{Nominal: t2, Category: categorySkipOverlap},
		}, got.Misses)
	})

	t.Run("delay threshold flags a slow schedule with no missed times", func(t *testing.T) {
		slow := Result{
			Namespace: "ns1", ScheduleID: "slow",
			Delays: []ActionDelay{{WorkflowID: "w1", DispatchDelay: 10 * time.Minute}},
		}
		emit := func(threshold time.Duration) string {
			var buf bytes.Buffer
			rw := NewRowWriter(&buf, threshold, false)
			_, err := rw.Write(slow)
			require.NoError(t, err)
			return buf.String()
		}
		require.Empty(t, emit(0), "threshold 0 flags on missed times only")
		require.Empty(t, emit(30*time.Minute), "dispatch delay below threshold is not flagged")
		require.NotEmpty(t, emit(5*time.Minute), "dispatch delay at/over threshold is flagged with no misses")
	})

	t.Run("delays are emitted on the row", func(t *testing.T) {
		miss := mustParseTime("2026-05-19T19:00:00Z")
		results := []Result{{
			Namespace: "ns1", ScheduleID: "s1",
			Delays: []ActionDelay{{
				WorkflowID:    "w1",
				Nominal:       mustParseTime("2026-05-19T19:00:00Z"),
				Actual:        mustParseTime("2026-05-19T19:00:00Z"),
				Desired:       mustParseTime("2026-05-19T19:00:00Z"),
				Start:         mustParseTime("2026-05-19T19:10:00Z"),
				DispatchDelay: 10 * time.Minute,
				E2EDelay:      10 * time.Minute,
				Category:      categoryLate,
			}},
			Missed: map[time.Time]string{miss: categoryRealMiss},
		}}
		var buf bytes.Buffer
		require.NoError(t, writeResults(&buf, results))
		var got row
		require.NoError(t, json.Unmarshal([]byte(strings.TrimSpace(buf.String())), &got))
		require.Len(t, got.Delays, 1)
		require.Equal(t, "w1", got.Delays[0].WorkflowID)
		require.Equal(t, categoryLate, got.Delays[0].Category)
		require.Equal(t, "10m0s", got.Delays[0].DispatchDelay)
		require.Equal(t, 1, got.Counts.Late)
	})

	t.Run("running execution renders close as null", func(t *testing.T) {
		miss := mustParseTime("2026-05-19T19:00:00Z")
		results := []Result{{
			Namespace: "ns1", ScheduleID: "s1",
			Observed: []Execution{{
				WorkflowID:  "w1",
				StartTime:   mustParseTime("2026-05-19T18:00:00Z"),
				Status:      enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
				NominalTime: mustParseTime("2026-05-19T18:00:00Z"),
			}},
			Missed: map[time.Time]string{miss: categoryRealMiss},
		}}
		var buf bytes.Buffer
		require.NoError(t, writeResults(&buf, results))
		require.Contains(t, buf.String(), `"close":null`)
		require.Contains(t, buf.String(), `"status":"Running"`)
	})
}

func writeResults(w io.Writer, results []Result) error {
	rw := NewRowWriter(w, 0, false)
	for _, result := range results {
		if _, err := rw.Write(result); err != nil {
			return err
		}
	}
	return nil
}
