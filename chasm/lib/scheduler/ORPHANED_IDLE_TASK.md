# Draft issue: orphaned idle task leaves a V2 schedule permanently open

Not filed yet. Body below is ready to paste into an issue; it is deliberately
free of customer identifiers, since `temporalio/temporal` is public.

---

## Summary

A CHASM (V2) schedule can lose its armed `SchedulerIdleTask` with nothing left to arm a replacement, leaving the entity open indefinitely. `ScheduleIdleCloseTime` keeps advertising a close time that will never arrive.

## Mechanism

The idle deadline is `getLastEventTime() + IdleTime`, and `getLastEventTime()` (`chasm/lib/scheduler/scheduler.go`) maxes over `Invoker.recentActions()` — which includes the `StartTime` of every started action still in the buffer.

`Invoker.recordExecuteResult` (`chasm/lib/scheduler/invoker.go`) stamps `StartTime` on a start that just launched, so it **raises the deadline**. But it only calls `addTasks()` — never `Generate()`. The deadline moves without the Generator arming a task at the new time.

`SchedulerIdleTaskHandler.Validate` then invalidates the already-armed task as `expiration_shift`, and `Node.closeTransactionCleanupInvalidTasks` deletes it on the **very next `CloseTransaction`** — eagerly, without waiting for the timer to fire. Nothing arms a replacement.

`Validate`'s own comment is the assumption that doesn't hold:

```go
// If lastEventTime advanced since arm (e.g., a workflow start appended to
// recentActions), the recomputed deadline is later than ScheduledTime - the
// old task is premature, the Generator will arm a fresh task at the new time.
```

Nothing guarantees a Generator tick follows a recorded start.

## Why it is normally invisible

`HandleNexusCompletion` calls `Generate()` after recording a completion, which re-arms. So the hole only bites when a start is recorded and **no completion callback follows**:

- a workflow that outlives the idle window (default `IdleTime` is 7 days, so this needs a long-running action, but the window is configurable);
- a completion callback that is dropped or never delivered;
- a manual-only schedule (empty spec) whose action never reports back.

Note that pending buffered starts do **not** hold the schedule open — `getIdleExpiration` only consults `idleTime`, `isHeldOpen()` (paused / pending backfill) and the spec's next wakeup. It never looks at the buffer. So a schedule with an exhausted or empty spec and in-flight starts is armed to idle-close, which is what sets up the race.

## Repro

Branch `scheduler-orphaned-idle-task-repro` (this branch), test
`TestIdleTask_OrphanedWhenRecordedStartMovesDeadline` in
`chasm/lib/scheduler/scheduler_idle_orphan_test.go`.

```
go test ./chasm/lib/scheduler/ -run TestIdleTask_Orphaned -v
```

Observed:

| | |
|---|---|
| armed at | `createTime + 7d` |
| after recording the start | deadline moves **+30m** |
| idle task count | **1 → 0** |
| `Closed` | `false` |
| `IdleCloseTime` | still the deleted task's stale deadline |

It is written as a characterization test — it asserts the current buggy behaviour so the branch demonstrates the defect on demand, with an inline note to invert the assertion when fixed. Two control tests pin the paths that *do* rescue the schedule: an explicit `Generate()` after the recorded start, and a recorded completion.

## Possible fixes

1. Call `Generate()` from the path that records started actions, so any move in the deadline re-arms. Simplest and closes the hole at the source, but adds a Generator tick per execute batch.
2. Stop anchoring the idle deadline on action `StartTime` at all, so recording a start cannot move it.
3. Have `Validate` re-arm rather than only invalidate — not currently possible, it holds a read-only context.

## Related

The idle deadline has a second, independent defect: `getLastEventTime()` is not monotonic, because `applyCompletedRetention` truncates `BufferedStarts` and can evict the start holding the largest `StartTime`. That regresses the deadline (closing schedules early) and causes idle tasks to accumulate, one per action. Fix in progress on `scheduler-idle-deadline-monotonic`; it is orthogonal to this issue and does not close this hole.
