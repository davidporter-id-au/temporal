package tdbg

import (
	"fmt"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	schedulerpb "go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/common/persistence/serialization"
	wscheduler "go.temporal.io/server/service/worker/scheduler"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// maxMissedCount caps the missed-runs walk so a pathological spec (e.g., 1s
// interval over a year) can't hang the tool.
const maxMissedCount = 10000

// parsedScheduler is the decoded view of a scheduler's CHASM nodes that both
// check and fix need: pause status, policies, spec, the generator's high
// watermark, and which scheduler tasks are present.
type parsedScheduler struct {
	isPaused      bool
	notes         string
	catchupWindow time.Duration
	overlapPolicy enumspb.ScheduleOverlapPolicy
	spec          *schedulepb.ScheduleSpec
	highWatermark *timestamppb.Timestamp
	taskFQNs      []string
	hasGenerator  bool
	hasIdle       bool
}

// parseSchedulerNodes walks the CHASM nodes from a DescribeMutableState
// response and extracts the scheduler/generator state plus task FQNs.
func parseSchedulerNodes(
	nodes map[string]*persistencespb.ChasmNode,
	registry *chasm.Registry,
) (*parsedScheduler, error) {
	if len(nodes) == 0 {
		return nil, fmt.Errorf("no CHASM nodes found")
	}

	parsed := &parsedScheduler{}

	for _, node := range nodes {
		componentAttr := node.GetMetadata().GetComponentAttributes()
		if componentAttr == nil {
			continue
		}

		componentFQN, _ := registry.ComponentFqnByID(componentAttr.GetTypeId())

		switch componentFQN {
		case fqnSchedulerComponent:
			dataBlob := node.GetData()
			if dataBlob != nil && len(dataBlob.GetData()) > 0 {
				var schedState schedulerpb.SchedulerState
				if err := serialization.Decode(dataBlob, &schedState); err == nil {
					scheduleState := schedState.GetSchedule().GetState()
					parsed.isPaused = scheduleState.GetPaused()
					parsed.notes = scheduleState.GetNotes()

					policies := schedState.GetSchedule().GetPolicies()
					parsed.catchupWindow = policies.GetCatchupWindow().AsDuration()
					parsed.overlapPolicy = policies.GetOverlapPolicy()
					parsed.spec = schedState.GetSchedule().GetSpec()
				}
			}

		case fqnGeneratorComponent:
			dataBlob := node.GetData()
			if dataBlob != nil && len(dataBlob.GetData()) > 0 {
				var genState schedulerpb.GeneratorState
				if err := serialization.Decode(dataBlob, &genState); err == nil {
					parsed.highWatermark = genState.LastProcessedTime
				}
			}
		}

		for _, task := range componentAttr.GetSideEffectTasks() {
			fqn, _ := registry.TaskFqnByID(task.GetTypeId())
			if fqn != "" {
				parsed.taskFQNs = append(parsed.taskFQNs, fqn)
			}
		}
		for _, task := range componentAttr.GetPureTasks() {
			fqn, _ := registry.TaskFqnByID(task.GetTypeId())
			if fqn != "" {
				parsed.taskFQNs = append(parsed.taskFQNs, fqn)
			}
		}
	}

	for _, fqn := range parsed.taskFQNs {
		switch fqn {
		case fqnGeneratorTask:
			parsed.hasGenerator = true
		case fqnSchedulerIdleTask:
			parsed.hasIdle = true
		}
	}

	return parsed, nil
}

// countMissedRuns compiles the spec and walks GetNextTime from hwm to end,
// counting every nominal firing time in the window. withinCatchup is the
// subset that falls inside the catchupWindow relative to end (i.e., the runs
// the scheduler would have actually attempted to catch up on, rather than
// dropping). If catchupWindow is 0, withinCatchup equals total.
//
// jitterSeed is intentionally empty: jitter only perturbs the result time
// within a bounded window, it does not change whether a nominal time matches.
func countMissedRuns(
	spec *schedulepb.ScheduleSpec,
	hwm, end time.Time,
	catchupWindow time.Duration,
) (total, withinCatchup int, capped bool, err error) {
	cs, err := wscheduler.NewSpecBuilder().NewCompiledSpec(spec)
	if err != nil {
		return 0, 0, false, fmt.Errorf("compile spec: %w", err)
	}
	cutoff := end.Add(-catchupWindow)
	t := hwm
	for total < maxMissedCount {
		res := cs.GetNextTime("", t)
		if res.Next.IsZero() || res.Next.After(end) {
			return total, withinCatchup, false, nil
		}
		total++
		if catchupWindow == 0 || !res.Next.Before(cutoff) {
			withinCatchup++
		}
		t = res.Next
	}
	return total, withinCatchup, true, nil
}
