package scheduler_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler"
	"go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Repro for an orphaned idle task: a schedule can lose its armed SchedulerIdleTask
// with nothing left to close it, leaving the entity open indefinitely.
//
// The idle deadline is getLastEventTime() + IdleTime, and getLastEventTime()
// includes the StartTime of every started action still in the Invoker's buffer.
// Invoker.recordExecuteResult stamps StartTime on a start that just launched, so
// it raises the deadline - but it only calls addTasks(), never Generate(). So the
// deadline moves without the Generator arming a task at the new time.
//
// The already-armed task is then invalidated by
// SchedulerIdleTaskHandler.Validate ("expiration_shift"), and CHASM's
// closeTransactionCleanupInvalidTasks deletes it on the very next close. Nothing
// arms a replacement. Validate's comment - "the old task is premature, the
// Generator will arm a fresh task at the new time" - is not guaranteed by any
// code path.
//
// Reachable whenever a start is recorded and no completion callback follows to
// drive HandleNexusCompletion -> Generate(): a workflow that outlives the idle
// window, a dropped/failed completion callback, or a manual-only schedule whose
// action never reports back.

// idleTaskCount counts logical SchedulerIdleTask entries on the root component.
// Physical timer count can't be used: CHASM materializes only the first pure task
// of each (component, scheduled time) group.
func idleTaskCount(t *testing.T, env *testEnv) int {
	t.Helper()
	idleTaskID, ok := env.Registry.TaskIDFor(&schedulerpb.SchedulerIdleTask{})
	require.True(t, ok, "idle task must be registered")

	rootPath, err := chasm.DefaultPathEncoder.Encode(nil, []string{})
	require.NoError(t, err)
	root, ok := env.Node.Snapshot(nil).Nodes[rootPath]
	require.True(t, ok)

	count := 0
	for _, task := range root.GetMetadata().GetComponentAttributes().GetPureTasks() {
		if task.GetTypeId() == idleTaskID {
			count++
		}
	}
	return count
}

// inTransaction runs fn against the root Scheduler and commits. The component is
// re-resolved every time: CloseTransaction clears the tree's valueToNode map, so
// a pointer held across a close still mutates but its ctx.AddTask calls are
// silently dropped.
func inTransaction(t *testing.T, env *testEnv, fn func(*scheduler.Scheduler, chasm.MutableContext)) {
	t.Helper()
	ctx := env.MutableContext()
	component, err := env.Node.Component(ctx, chasm.ComponentRef{})
	require.NoError(t, err)
	sched, ok := component.(*scheduler.Scheduler)
	require.True(t, ok)
	fn(sched, ctx)
	require.NoError(t, env.CloseTransaction())
}

// makeManualOnly swaps in an empty spec. The conflict token must be bumped with
// it: getCompiledSpec caches the compiled spec keyed on ConflictToken, so a spec
// swap alone leaves the Generator still computing wakeups from the old interval.
func makeManualOnly(t *testing.T, env *testEnv) {
	t.Helper()
	inTransaction(t, env, func(sched *scheduler.Scheduler, _ chasm.MutableContext) {
		sched.Schedule.Spec = &schedulepb.ScheduleSpec{}
		sched.ConflictToken++
	})
}

func TestIdleTask_OrphanedWhenRecordedStartMovesDeadline(t *testing.T) {
	env := newTestEnv(t)
	handler := newGeneratorHandler(env)

	// A manual-only schedule (empty spec) has no next wakeup, so the Generator
	// takes its idle branch. Same shape as an exhausted spec, without needing to
	// construct one - see TestIdleTask_Validate_ManualOnlyClosesFromIdle.
	makeManualOnly(t, env)

	// One buffered start, queued but not yet launched. Note this does not hold the
	// schedule open: getIdleExpiration never looks at the buffer, only at
	// idleTime, isHeldOpen (paused / pending backfill) and the spec's next wakeup.
	const requestID = "req-in-flight"
	inTransaction(t, env, func(sched *scheduler.Scheduler, ctx chasm.MutableContext) {
		sched.Invoker.Get(ctx).BufferedStarts = []*schedulespb.BufferedStart{{
			NominalTime: timestamppb.New(env.TimeSource.Now()),
			ActualTime:  timestamppb.New(env.TimeSource.Now()),
			DesiredTime: timestamppb.New(env.TimeSource.Now()),
			RequestId:   requestID,
			WorkflowId:  "wf-in-flight",
			Manual:      true,
			// recordProcessBufferResult stamps Attempt=1 when it readies a start
			// for execution. It matters here: processBuffer only re-processes
			// starts with Attempt==0, and the default SKIP overlap policy would
			// drop this one back out of the buffer once a workflow is running.
			Attempt: 1,
		}}
	})

	// Generator tick: the schedule is idle, so an idle task is armed.
	inTransaction(t, env, func(sched *scheduler.Scheduler, ctx chasm.MutableContext) {
		require.NoError(t, handler.Execute(
			ctx, sched.Generator.Get(ctx), chasm.TaskAttributes{}, &schedulerpb.GeneratorTask{}))
	})

	var armedAt time.Time
	inTransaction(t, env, func(sched *scheduler.Scheduler, _ chasm.MutableContext) {
		require.NotNil(t, sched.IdleCloseTime, "schedule should be armed to close")
		armedAt = sched.IdleCloseTime.AsTime()
	})
	require.Equal(t, 1, idleTaskCount(t, env), "exactly one idle task should be armed")

	// The ExecuteTask launches the workflow and records the result. This stamps
	// StartTime, which raises getLastEventTime and therefore the idle deadline.
	// recordExecuteResult calls addTasks() but not Generate(), so no idle task is
	// armed at the new deadline.
	startedAt := env.TimeSource.Now().Add(30 * time.Minute)
	inTransaction(t, env, func(sched *scheduler.Scheduler, ctx chasm.MutableContext) {
		newlyStarted, _ := sched.Invoker.Get(ctx).RecordExecuteResult(ctx, []*schedulespb.BufferedStart{{
			RequestId: requestID,
			RunId:     "run-in-flight",
			StartTime: timestamppb.New(startedAt),
		}}, nil)
		require.Equal(t, 1, newlyStarted)
	})

	// The armed task is now stale, and CHASM's cleanup pass already deleted it on
	// the close above - eagerly, without waiting for the timer to fire.
	inTransaction(t, env, func(sched *scheduler.Scheduler, ctx chasm.MutableContext) {
		require.True(t, sched.IdleDeadlineForTest(ctx, scheduler.DefaultTweakables.IdleTime).After(armedAt),
			"recorded start must have moved the deadline past the armed time")
		require.False(t, sched.Closed, "schedule is still open")
		require.NotNil(t, sched.IdleCloseTime,
			"IdleCloseTime still advertises the deleted task's deadline")
		require.Equal(t, armedAt, sched.IdleCloseTime.AsTime())
	})

	// Characterization: this asserts the CURRENT (buggy) behaviour so the branch
	// demonstrates the defect on demand. When the hole is fixed, invert it to
	// require.Equal(t, 1, ...) - the schedule must keep exactly one idle task,
	// armed at the new deadline.
	require.Equal(t, 0, idleTaskCount(t, env),
		"BUG: the schedule has no armed idle task and nothing will arm one, so it can never close")
}

// Control: the same sequence with a Generate() after recording the start does arm
// a fresh task. This is the path HandleNexusCompletion takes, and it is why the
// hole is normally invisible - a completion callback rescues the schedule.
func TestIdleTask_NotOrphanedWhenGenerateFollowsRecordedStart(t *testing.T) {
	env := newTestEnv(t)
	handler := newGeneratorHandler(env)

	makeManualOnly(t, env)

	const requestID = "req-in-flight"
	inTransaction(t, env, func(sched *scheduler.Scheduler, ctx chasm.MutableContext) {
		sched.Invoker.Get(ctx).BufferedStarts = []*schedulespb.BufferedStart{{
			NominalTime: timestamppb.New(env.TimeSource.Now()),
			ActualTime:  timestamppb.New(env.TimeSource.Now()),
			DesiredTime: timestamppb.New(env.TimeSource.Now()),
			RequestId:   requestID,
			WorkflowId:  "wf-in-flight",
			Manual:      true,
			// recordProcessBufferResult stamps Attempt=1 when it readies a start
			// for execution. It matters here: processBuffer only re-processes
			// starts with Attempt==0, and the default SKIP overlap policy would
			// drop this one back out of the buffer once a workflow is running.
			Attempt: 1,
		}}
	})
	inTransaction(t, env, func(sched *scheduler.Scheduler, ctx chasm.MutableContext) {
		require.NoError(t, handler.Execute(
			ctx, sched.Generator.Get(ctx), chasm.TaskAttributes{}, &schedulerpb.GeneratorTask{}))
	})
	require.Equal(t, 1, idleTaskCount(t, env))

	startedAt := env.TimeSource.Now().Add(30 * time.Minute)
	inTransaction(t, env, func(sched *scheduler.Scheduler, ctx chasm.MutableContext) {
		sched.Invoker.Get(ctx).RecordExecuteResult(ctx, []*schedulespb.BufferedStart{{
			RequestId: requestID,
			RunId:     "run-in-flight",
			StartTime: timestamppb.New(startedAt),
		}}, nil)
		// The rescue: what HandleNexusCompletion does after recording a completion.
		require.NoError(t, handler.Execute(
			ctx, sched.Generator.Get(ctx), chasm.TaskAttributes{}, &schedulerpb.GeneratorTask{}))
	})

	require.Equal(t, 1, idleTaskCount(t, env),
		"a Generate() after the recorded start re-arms, so the schedule stays closable")
	inTransaction(t, env, func(sched *scheduler.Scheduler, _ chasm.MutableContext) {
		require.Equal(t, startedAt.Add(scheduler.DefaultTweakables.IdleTime).UTC(),
			sched.IdleCloseTime.AsTime().UTC(),
			"the re-armed deadline should be anchored on the recorded start")
	})
}

// Completion is the other rescue path, and it is the one the production incident
// exercised 43 times. Pinned so a refactor that drops the Generate() call from
// HandleNexusCompletion surfaces here.
func TestIdleTask_CompletionRecordingReArmsIdleTask(t *testing.T) {
	env := newTestEnv(t)
	handler := newGeneratorHandler(env)

	makeManualOnly(t, env)

	const requestID = "req-done"
	startedAt := env.TimeSource.Now().Add(30 * time.Minute)
	inTransaction(t, env, func(sched *scheduler.Scheduler, ctx chasm.MutableContext) {
		sched.Invoker.Get(ctx).BufferedStarts = []*schedulespb.BufferedStart{{
			NominalTime: timestamppb.New(env.TimeSource.Now()),
			ActualTime:  timestamppb.New(env.TimeSource.Now()),
			DesiredTime: timestamppb.New(env.TimeSource.Now()),
			RequestId:   requestID,
			WorkflowId:  "wf-done",
			RunId:       "run-done",
			StartTime:   timestamppb.New(startedAt),
			HasCallback: true,
			Manual:      true,
		}}
		require.NoError(t, handler.Execute(
			ctx, sched.Generator.Get(ctx), chasm.TaskAttributes{}, &schedulerpb.GeneratorTask{}))
	})
	require.Equal(t, 1, idleTaskCount(t, env))

	inTransaction(t, env, func(sched *scheduler.Scheduler, ctx chasm.MutableContext) {
		sched.RecordCompletedAction(ctx, &schedulespb.CompletedResult{
			Status:    enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
			CloseTime: timestamppb.New(env.TimeSource.Now().Add(time.Hour)),
		}, requestID)
		require.NoError(t, handler.Execute(
			ctx, sched.Generator.Get(ctx), chasm.TaskAttributes{}, &schedulerpb.GeneratorTask{}))
	})

	// At least one, not exactly one: recording a completion does not change any
	// StartTime, so the deadline is unchanged, the already-armed task stays valid,
	// and the freshly armed one lands beside it. That duplicate is a separate
	// defect (idle tasks accumulate whenever the deadline does not advance) and is
	// deliberately not in scope here.
	require.GreaterOrEqual(t, idleTaskCount(t, env), 1,
		"a completion must leave the schedule with an armed idle task")
	inTransaction(t, env, func(sched *scheduler.Scheduler, _ chasm.MutableContext) {
		require.NotNil(t, sched.IdleCloseTime)
	})
}
