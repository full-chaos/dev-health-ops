package syncreconciler

import (
	"fmt"
	"sort"
	"time"
)

// StageName identifies one stage of the MutationPipeline for budget lookup,
// structured logging, and metrics labeling. It is a bounded, compile-time
// enum -- never derived from operator input -- so it is always safe as a
// Prometheus label value and a log attribute.
type StageName string

const (
	StageLeaseRepair            StageName = "lease_repair"
	StageUnreclaimableSweep     StageName = "unreclaimable_sweep"
	StageTerminalDeliveryRepair StageName = "terminal_delivery_repair"
	// StageTerminalOutboxClose is CHAOS-4583's closer: a bounded, read-adjacent
	// safety net over sync_dispatch_outbox, sibling to StageUnreclaimableSweep
	// and StageTerminalDeliveryRepair (see terminal_outbox_close.go's package
	// doc comment).
	StageTerminalOutboxClose StageName = "terminal_outbox_close"
	StageMaterializer        StageName = "materializer"
	StageKernel              StageName = "kernel"
	StageObserver            StageName = "observer"
)

// orderedStages is the exhaustive, ordered list of stages MutationPipeline.Step
// runs per tick. It is the single source of truth both stage_budget_test.go
// (which parses pipeline.go's Step method and asserts every runStage call
// names one of these) and StageBudgets.validate below check against. Adding a
// stage to the pipeline without adding it here is exactly the CHAOS-4035
// defect shape this ticket exists to prevent from recurring one level down
// the stack: a component wired into a call graph a reviewed comment does not
// mention.
var orderedStages = []StageName{
	StageLeaseRepair,
	StageUnreclaimableSweep,
	StageTerminalDeliveryRepair,
	StageTerminalOutboxClose,
	StageMaterializer,
	StageKernel,
	StageObserver,
}

const (
	minimumStageBudget = time.Millisecond
	maximumStageBudget = 10 * time.Second
)

// StageBudgets gives each MutationPipeline stage its own bounded sub-context
// instead of every stage racing one flat envelope for the whole 8-stage/3-pool
// pipeline (CHAOS-4239). A stage that exceeds its own budget fails only
// itself; it can no longer starve its seven siblings of the time they need.
type StageBudgets map[StageName]time.Duration

// DefaultStageBudgets sizes each stage from what it actually does, not from
// one shared guess:
//
//   - LeaseRepair: one domain-pool indexed update. Cheap and first in line.
//   - UnreclaimableSweep: spans all three pools (CHAOS-4005/CHAOS-4097) --
//     the durable route read alone crosses a role boundary before the domain
//     transaction even opens.
//   - TerminalDeliveryRepair: the CHAOS-4092 join, now index-friendly and
//     measured at ~467ms warm even before the fix; this budget carries slack
//     for the cold-cache case CHAOS-4092 also measured, and for the cold-start
//     window CHAOS-4239 observed failures clustering in.
//   - TerminalOutboxClose (CHAOS-4583): four index-driven CTE updates against
//     sync_dispatch_outbox, the same table and pool Materializer already
//     budgets 600ms for; sized identically since it is the same shape of
//     work (bounded per-kind candidate scan + UPDATE ... RETURNING).
//   - Materializer: coordinator-exclusive wakeup materialization.
//   - Kernel: the heaviest stage -- claims on the domain pool, delivers on the
//     queue pool, and runs the publish closure (a River insert plus a domain
//     reference read) inline.
//   - Observer: proven 0.079ms against a live database (CHAOS-4239 EXPLAIN);
//     this budget is almost entirely cold-start headroom.
//
// The sum (StageBudgets.Sum) replaces the single hardcoded
// defaultObservationTimeout as the mutation loop's outer envelope --
// dependencies.go wires that composition explicitly rather than leaving the
// two to drift. See stage_budget_test.go for the pin that keeps this map's
// key set exhaustive against Step's actual stage list.
func DefaultStageBudgets() StageBudgets {
	return StageBudgets{
		StageLeaseRepair:            400 * time.Millisecond,
		StageUnreclaimableSweep:     600 * time.Millisecond,
		StageTerminalDeliveryRepair: 750 * time.Millisecond,
		StageTerminalOutboxClose:    600 * time.Millisecond,
		StageMaterializer:           600 * time.Millisecond,
		StageKernel:                 1000 * time.Millisecond,
		StageObserver:               400 * time.Millisecond,
	}
}

// Sum is the documented composition: the mutation loop's outer envelope is
// derived from this, never hardcoded independently of it.
func (budgets StageBudgets) Sum() time.Duration {
	var total time.Duration
	for _, budget := range budgets {
		total += budget
	}
	return total
}

// validate requires EXACTLY the stage set orderedStages names -- no fewer
// (an unbudgeted stage would silently inherit a zero context.WithTimeout,
// failing every tick) and no more (a stale entry for a stage that no longer
// exists is exactly the kind of comment-vs-code drift CHAOS-4035 already cost
// a production incident over).
func (budgets StageBudgets) validate() error {
	if len(budgets) != len(orderedStages) {
		return fmt.Errorf("%w: stage budgets has %d entries, want exactly %d (%s)",
			ErrInvalidConfiguration, len(budgets), len(orderedStages), stageNameList())
	}
	for _, stage := range orderedStages {
		budget, ok := budgets[stage]
		if !ok {
			return fmt.Errorf("%w: stage budgets is missing %q", ErrInvalidConfiguration, stage)
		}
		if budget < minimumStageBudget || budget > maximumStageBudget {
			return fmt.Errorf("%w: stage %q budget %s outside [%s, %s]",
				ErrInvalidConfiguration, stage, budget, minimumStageBudget, maximumStageBudget)
		}
	}
	return nil
}

func stageNameList() string {
	names := make([]string, 0, len(orderedStages))
	for _, stage := range orderedStages {
		names = append(names, string(stage))
	}
	sort.Strings(names)
	result := ""
	for index, name := range names {
		if index > 0 {
			result += ", "
		}
		result += name
	}
	return result
}
