package syncreconciler

import (
	"context"
	"errors"
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
	// StageOrphanedUnitRepair is CHAOS-5453's provider-unit backstop: a
	// 'dispatching' unit whose worker_job_outbox delivery is terminal and whose
	// River job COMPLETED or was reaped. It sits beside
	// StageTerminalDeliveryRepair rather than inside it because the two are
	// separated by grant, not by taste -- this one's write is a domain-role
	// INSERT into worker_job_outbox and its evidence is a queue-role read of
	// river_job (see orphaned_unit_repair.go's two-pools section).
	StageOrphanedUnitRepair StageName = "orphaned_unit_repair"
	StageMaterializer       StageName = "materializer"
	StageKernel             StageName = "kernel"
	// StageChunkSidecarReclaim (CHAOS-14243) deletes the stored chunk rows of
	// terminal units in bounded batches (chunk_sidecar_reclaim.go).
	StageChunkSidecarReclaim StageName = "chunk_sidecar_reclaim"
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
	StageOrphanedUnitRepair,
	StageMaterializer,
	StageKernel,
	StageChunkSidecarReclaim,
	StageObserver,
}

const (
	minimumStageBudget = time.Millisecond
	maximumStageBudget = 10 * time.Second
)

// StageBudgets gives each MutationPipeline stage its own bounded sub-context
// instead of every stage racing one flat envelope for the whole 9-stage/3-pool
// pipeline (CHAOS-4239). A stage that exceeds its own budget fails only
// itself; it can no longer starve its eight siblings of the time they need.
type StageBudgets map[StageName]time.Duration

// DefaultStageBudgets sizes each stage from what it actually does, not from
// one shared guess:
//
//   - LeaseRepair: one domain-pool indexed update. Cheap and first in line.
//   - UnreclaimableSweep: spans all three pools (CHAOS-4005/CHAOS-4097) --
//     the durable route read alone crosses a role boundary before the domain
//     transaction even opens.
//   - TerminalDeliveryRepair: 2 x its measured prod p99, capped at 1200ms
//     (CHAOS-14386). Measured p99 0.719s against the old 750ms budget: the max
//     of 24 hourly p99s of dev_health_reconciler_stage_duration_seconds to
//     2026-11-20. That p99 sat under 750ms only because the budget cut the
//     tail. The slow statement is the coordinator readiness read
//     (ready_finalize_repair.go), which scans scheduled_sync_occurrences once
//     per scheduled candidate run.
//   - TerminalOutboxClose (CHAOS-4583): four index-driven CTE updates against
//     sync_dispatch_outbox, the same table and pool Materializer already
//     budgets 600ms for; sized identically since it is the same shape of
//     work (bounded per-kind candidate scan + UPDATE ... RETURNING).
//   - OrphanedUnitRepair (CHAOS-5453): one bounded LEFT JOIN survey of
//     worker_job_outbox against river_job on the queue pool, then one domain
//     transaction whose per-candidate work is two indexed reads and one
//     INSERT. Measured prod p99 0.147s (same source as TerminalDeliveryRepair)
//     against 600ms: kept; its failures are rare stalls, not its own
//     statements.
//   - Materializer: coordinator-exclusive wakeup materialization.
//   - Kernel: the heaviest stage -- claims on the domain pool, delivers on the
//     queue pool, and runs the publish closure (a River insert plus a domain
//     reference read) inline.
//   - ChunkSidecarReclaim (CHAOS-14243): up to 4 PK-range DELETEs of at most
//     4 MiB of TOASTed payload each, one transaction per statement; it stops
//     early when the deadline would not fit another statement, and it yields
//     the tick when an earlier stage used 3/4 of its budget (CHAOS-14380).
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
		StageTerminalDeliveryRepair: 1200 * time.Millisecond,
		StageTerminalOutboxClose:    600 * time.Millisecond,
		StageOrphanedUnitRepair:     600 * time.Millisecond,
		StageMaterializer:           600 * time.Millisecond,
		StageKernel:                 1000 * time.Millisecond,
		StageChunkSidecarReclaim:    1500 * time.Millisecond,
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

// ErrStageBudgetSpent marks a stage failure whose cause is the stage's OWN
// budget deadline, not the database (CHAOS-13743). Before it, such a failure
// read "... database unavailable" although the database was healthy and only
// the budget had run out.
var ErrStageBudgetSpent = errors.New("syncreconciler: stage budget spent")

const stageBudgetSpentReason = "stage budget spent"

// sqlStateQueryCanceled is what Postgres reports for a statement our own
// context cancelled (57014). It is only a budget signal when the stage
// context has expired; a server statement_timeout while the stage context is
// alive stays a database error.
const sqlStateQueryCanceled = "57014"

// stageBudgetSpentError carries no cause text, so the log line names the
// budget, never "database unavailable". Unwrap keeps the cause in the chain:
// errors.Is(err, ErrUnavailable) and errors.As to a step error still hold.
type stageBudgetSpentError struct {
	stage  StageName
	budget time.Duration
	cause  error
}

func (spent stageBudgetSpentError) Error() string {
	return "syncreconciler: stage " + string(spent.stage) + " budget spent (" + spent.budget.String() + ")"
}

func (spent stageBudgetSpentError) Unwrap() []error {
	return []error{ErrStageBudgetSpent, spent.cause}
}

// stageDeadlineSpent decides budget-spent from the STAGE context, not from the
// error: stages carry the deadline differently (the materializer keeps only a
// label, lease repair and the kernel return a bare ErrUnavailable, and a
// commit after a cancelled statement fails with no SQLSTATE at all), but the
// stage context is the same fact for every stage. A real server SQLSTATE
// always wins. The caller has already returned on a parent cancellation.
func stageDeadlineSpent(stageCtx context.Context, sqlstate string) bool {
	return errors.Is(stageCtx.Err(), context.DeadlineExceeded) && !isServerSQLState(sqlstate)
}

func isServerSQLState(sqlstate string) bool {
	return sqlstate != "" && sqlstate != stageContextDeadlineLabel && sqlstate != sqlStateQueryCanceled
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
