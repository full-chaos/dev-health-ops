package syncreconciler

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/syncrunrollup"
	"github.com/jackc/pgx/v5"
)

// The route-unavailable branch of the unreclaimable sweep.
//
// The Go dispatcher never reads public.worker_job_routes: the domain role it
// runs under holds no grant on that table, so it claims a run's units and
// stages them into public.worker_job_outbox unconditionally, and the outbox
// relay resolves the route once, at drain, on the coordinator role
// (internal/joboutbox/relay.go's Step). That split means a route the relay
// cannot resolve does not fail a dispatch -- it parks a run:
//
//   - a route row that is absent, duplicated, paused or drifted makes
//     jobroute.Controller.DeferredKinds fail, and Relay.Step returns before it
//     claims ANY outbox row. Nothing increments attempt_count, so the relay's
//     own MaxRelayAttempts bound is never approached;
//   - a route rolled back to celery excludes the kind from the relay's claim
//     entirely, with the same effect on attempt_count and no consumer left to
//     take the work.
//
// Either way the run's units hold 'planned' or 'dispatching' forever,
// finalize_sync_run requires every unit terminal and so never completes the
// run, and the run's dispatch wakeup re-arms on its own schedule indefinitely.
// The ordinary sweep cannot reach this population: it declines outright while
// River does not own provider units, and its never-published branch demands
// that the capability matrix decline the pair, which is false for a unit the
// dispatcher legitimately planned.
//
// This branch is the bound. Once a parked run passes RouteUnavailableWindow or
// RouteUnavailableAttempts, its parked units terminalize as 'failed' carrying
// routeUnavailableErrorCategory and a reason naming the fault, the run's
// rollup is recomputed and its finalize wakeup re-armed, so the run reaches a
// terminal status an operator and every downstream report can read.
//
// It deliberately does NOT dispatch anything: a control-plane fault must never
// be answered by a producer picking a transport for itself, which is the
// concurrent-owner hazard the fail-closed resolve exists to prevent. Giving up
// loudly is the other half of failing closed, not a relaxation of it.
//
// A route store this pass cannot READ is not a fault it may act on: Step
// returns that as a step error before reaching here, because "the row says the
// route is broken" and "nobody could ask" are opposite findings and only the
// first authorizes destroying work.

const (
	// RouteUnavailableWindow bounds how long a run planned before a
	// sync.provider_unit route fault may hold parked units. A run planned at
	// or before now-RouteUnavailableWindow, while the durable route is one no
	// River runtime can serve, has its parked units terminalized.
	//
	// It is far longer than any ordinary dispatch wait and shorter than the
	// hour the never-attempted strand gets, because the two answer different
	// questions: that one waits to be sure nothing is coming, this one already
	// knows from the route row that nothing is.
	RouteUnavailableWindow = 30 * time.Minute

	// RouteUnavailableAttempts bounds how many dispatch wakeup attempts a run
	// records against the same fault before its parked units are released,
	// whichever bound the run reaches first. It covers the run planned well
	// inside the window that is already re-driving dispatch against a route
	// that cannot serve it, so a busy run is not held for the full window
	// merely because it is young.
	RouteUnavailableAttempts = 5

	// routeUnavailableErrorCategory is the durable denial token an operator
	// reads off the unit. It is a SEPARATE category from
	// unreclaimableErrorCategory and unreclaimableTerminalDeliveryCategory for
	// the same reason those two are separate from each other: a control-plane
	// route fault is neither a capability the matrix declines nor a delivery
	// that died, and filing it as either hides the one action -- restore the
	// route -- that resolves it.
	routeUnavailableErrorCategory = "route_unavailable"

	routeUnavailableStepCandidateQuery = "route-unavailable candidate read of public.sync_run_units"
	routeUnavailableStepCandidateScan  = "route-unavailable candidate scan"
	routeUnavailableStepCandidateRows  = "route-unavailable candidate iteration"
	routeUnavailableStepPayload        = "route-unavailable terminalize payload encode"
	routeUnavailableStepExec           = "route-unavailable terminalize write to public.sync_run_units"
	routeUnavailableStepRows           = "route-unavailable terminalize affected-row count"
)

// routeUnavailableCandidate is one parked unit plus the two facts the bound is
// measured on. Both live on the run, not the unit: a unit's own timestamps are
// re-stamped by the dispatcher's stale reclaim on every pass, so a bound hung
// off them would reset forever and never fire.
type routeUnavailableCandidate struct {
	unreclaimableCandidate
	runPlannedAt   time.Time
	wakeupAttempts int64
}

// providerUnitRouteFault names the durable state for the reason string and the
// operator log line. It is only ever called once the caller has established
// that the route is not one River owns, so it has no "healthy" arm.
func providerUnitRouteFault(fence routeFence, usable bool) string {
	switch {
	case !usable:
		return "absent or duplicated"
	case fence.paused:
		return "paused"
	case fence.transport == "celery":
		return "rolled back to celery"
	case fence.transport == "":
		return "empty"
	default:
		return "drifted to " + fence.transport
	}
}

// The candidate read.
//
// Paging is a single bounded query rather than the keyset loop the ordinary
// sweep needs, because every predicate here is expressed IN the statement:
// there is no post-SQL filter behind which a prefix of ineligible rows could
// hide a genuine candidate, which is the only thing that made the cursor
// load-bearing there.
//
// status IN ('planned','dispatching') is the whole parked population. A
// 'running' unit is excluded by the status list and a unit holding any lease
// by the two lease predicates: something took it, and lease repair owns what
// happens next.
//
// The wakeup join is LEFT: uq_sync_dispatch_outbox_run_kind makes it at most
// one row, and a run whose dispatch wakeup has already been closed still has
// its parked units measured against the window.
const selectRouteUnavailableCandidatesSQL = `
SELECT unit.id::text, unit.sync_run_id::text, unit.org_id,
	unit.provider, unit.dataset_key, unit.cost_class,
	unit.created_at, unit.updated_at,
	run.created_at, COALESCE(wakeup.attempts, 0)
FROM public.sync_run_units AS unit
JOIN public.sync_runs AS run
	ON run.id = unit.sync_run_id AND run.org_id = unit.org_id
LEFT JOIN public.sync_dispatch_outbox AS wakeup
	ON wakeup.sync_run_id = run.id AND wakeup.kind = 'dispatch_sync_run'
WHERE unit.status IN ('planned', 'dispatching')
	AND unit.lease_owner IS NULL
	AND unit.lease_expires_at IS NULL
	AND run.status NOT IN ('success', 'partial_failed', 'failed')
	AND (run.created_at <= $1 OR COALESCE(wakeup.attempts, 0) >= $2)
ORDER BY unit.created_at, unit.id
LIMIT $3
`

// The terminal write, with the same optimistic CAS the sibling terminalize
// statements carry: updated_at pins the row this pass decided about, so a
// runtime that touched the unit between the read and the commit wins and this
// pass writes nothing for it.
//
// There is no attempts predicate. A unit that already ran, failed and was
// re-planned is parked by the same fault as one that never ran, and the bound
// has passed for both.
const terminalizeRouteUnavailableSQL = `
UPDATE public.sync_run_units
SET status = 'failed',
	available_at = NULL,
	error = $2,
	last_retry_reason = $3,
	result = $4::jsonb,
	lease_owner = NULL,
	lease_expires_at = NULL,
	updated_at = $5
WHERE id = $1::uuid
	AND status IN ('planned', 'dispatching')
	AND lease_owner IS NULL
	AND updated_at = $6
`

// routeUnavailableReason is the durable sentence an operator reads off the
// row. It names the fault, both bounds and which of them the run passed, so
// the row alone answers "why did this fail" and "what do I fix" without a log
// search.
func routeUnavailableReason(candidate routeUnavailableCandidate, fault string, now time.Time) string {
	return fmt.Sprintf(
		"route unavailable for %s: the durable sync.provider_unit route is %s, "+
			"so no runtime can execute this unit; the run has been parked for %s "+
			"across %d dispatch attempts, past the %s / %d-attempt bound",
		candidate.pair(), fault,
		now.Sub(candidate.runPlannedAt).Round(time.Second),
		candidate.wakeupAttempts, RouteUnavailableWindow, RouteUnavailableAttempts,
	)
}

// terminalizeRouteUnavailable runs one bounded route-unavailable pass. It owns
// its own domain transaction and takes the same route fence the ordinary sweep
// takes before committing, so an operator restoring the route mid-pass wins.
func (sweep *UnreclaimableSweep) terminalizeRouteUnavailable(
	ctx context.Context,
	now time.Time,
	limit int,
	opening routeFence,
	openingUsable bool,
	result *UnreclaimableSweepResult,
) error {
	result.RouteUnavailableFault = providerUnitRouteFault(opening, openingUsable)

	tx, err := sweep.begin(ctx)
	if err != nil || tx == nil {
		return sweepUnavailable(sweepStepBegin, err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	candidates, err := sweep.selectRouteUnavailable(ctx, tx, now, limit)
	if err != nil {
		return err
	}
	result.RouteUnavailableCandidates = len(candidates)
	seenRuns := make(map[string]struct{}, len(candidates))
	locked := make([]unreclaimableCandidate, 0, len(candidates))
	for _, candidate := range candidates {
		result.RouteUnavailableUnitIDs = append(result.RouteUnavailableUnitIDs, candidate.id)
		locked = append(locked, candidate.unreclaimableCandidate)
		if _, ok := seenRuns[candidate.syncRunID]; !ok {
			seenRuns[candidate.syncRunID] = struct{}{}
			result.RouteUnavailableRunIDs = append(result.RouteUnavailableRunIDs, candidate.syncRunID)
		}
	}

	// Shadow reports selection and writes nothing, exactly as it does on the
	// ordinary branch. The deferred rollback discards the transaction.
	if sweep.config.Mode != SweepModeActive || len(candidates) == 0 {
		return nil
	}

	// The same two pre-locks the ordinary branch takes, in the same order and
	// for the same reason: the per-bucket advisory lock dispatch's own claim
	// takes, then every candidate's unit row ascending, both before this
	// transaction locks a run row through syncrunrollup.Bump.
	if err := acquireUnreclaimableBucketLocks(ctx, tx, locked); err != nil {
		return sweepUnavailable(sweepStepBucketLock, err)
	}
	if err := lockSyncRunUnitsAscending(ctx, tx, result.RouteUnavailableUnitIDs); err != nil {
		return sweepUnavailable(sweepStepUnitLock, err)
	}
	// Ascending sync_run_id, matching LeaseRepair.Step and the ordinary sweep,
	// so two reconciler paths can never walk an overlapping run set in
	// opposite order. Stable, so candidates within one run keep selection
	// order and the reported ids above still describe this pass.
	sort.SliceStable(candidates, func(left, right int) bool {
		return candidates[left].syncRunID < candidates[right].syncRunID
	})

	for _, candidate := range candidates {
		affected, err := sweep.terminalizeOneRouteUnavailable(
			ctx, tx, candidate, result.RouteUnavailableFault, now,
		)
		if err != nil {
			return err
		}
		if affected < 0 || affected > 1 {
			return sweepUnavailable(routeUnavailableStepRows, nil)
		}
		result.RouteUnavailableTerminalized += int(affected)
	}

	// The same fence the ordinary branch takes, and it carries the same
	// limit: a FOR SHARE read locks a ROW, so when the fault is an absent
	// route row there is nothing to lock and the fence degrades to a re-read.
	// An operator inserting the row between that re-read and this commit
	// therefore wins the race and these units fail anyway. That is accepted:
	// every candidate here has been parked past the bound, so the run it
	// belongs to was already dead for at least that long, and its outbox rows
	// survive to be delivered once -- where the unit's terminal status
	// declines the claim -- rather than being executed against a run nobody
	// is waiting on.
	held, err := sweep.holdRouteFence(ctx, opening, openingUsable)
	if err != nil {
		return err
	}
	if !held.ok {
		// The route moved under this pass. An operator restoring it is the
		// outcome this branch wants, so the staged writes are abandoned rather
		// than committed against a route that can now serve the work.
		result.RouteUnavailableTerminalized = 0
		result.DeclinedRouteChange = true
		return nil
	}
	commitErr := tx.Commit(ctx)
	held.release()
	if commitErr != nil {
		return sweepUnavailable(sweepStepCommit, commitErr)
	}
	return nil
}

func (sweep *UnreclaimableSweep) selectRouteUnavailable(
	ctx context.Context,
	tx pgx.Tx,
	now time.Time,
	limit int,
) ([]routeUnavailableCandidate, error) {
	rows, err := tx.Query(
		ctx, selectRouteUnavailableCandidatesSQL,
		now.Add(-RouteUnavailableWindow), int64(RouteUnavailableAttempts), limit,
	)
	if err != nil {
		return nil, sweepUnavailable(routeUnavailableStepCandidateQuery, err)
	}
	defer rows.Close()
	page := make([]routeUnavailableCandidate, 0, limit)
	for rows.Next() {
		var candidate routeUnavailableCandidate
		if err := rows.Scan(
			&candidate.id, &candidate.syncRunID, &candidate.orgID,
			&candidate.provider, &candidate.datasetKey, &candidate.costClass,
			&candidate.createdAt, &candidate.updatedAt,
			&candidate.runPlannedAt, &candidate.wakeupAttempts,
		); err != nil {
			return nil, sweepUnavailable(routeUnavailableStepCandidateScan, err)
		}
		page = append(page, candidate)
	}
	if err := rows.Err(); err != nil {
		return nil, sweepUnavailable(routeUnavailableStepCandidateRows, err)
	}
	return page, nil
}

func (sweep *UnreclaimableSweep) terminalizeOneRouteUnavailable(
	ctx context.Context,
	tx pgx.Tx,
	candidate routeUnavailableCandidate,
	fault string,
	now time.Time,
) (int64, error) {
	reason := routeUnavailableReason(candidate, fault, now)
	payload, err := json.Marshal(map[string]string{
		"error_category": routeUnavailableErrorCategory,
		"reason":         reason,
		"provider":       candidate.provider,
		"dataset_key":    candidate.datasetKey,
		"route_kind":     unreclaimableProviderUnitID,
		"route_fault":    fault,
	})
	if err != nil {
		return 0, sweepUnavailable(routeUnavailableStepPayload, err)
	}
	command, err := tx.Exec(
		ctx, terminalizeRouteUnavailableSQL,
		candidate.id, routeUnavailableErrorCategory, reason, string(payload),
		now, candidate.updatedAt,
	)
	if err != nil {
		return 0, sweepUnavailable(routeUnavailableStepExec, err)
	}
	rowsAffected := command.RowsAffected()
	if rowsAffected == 0 {
		// A lost CAS wrote nothing, so there is no rollup to recompute.
		return 0, nil
	}
	// Terminalize, then Bump, then ArmFinalize, in the same transaction and in
	// the same order every other single-run terminal writer uses. Bump keeps
	// the run's counters live; ArmFinalize is what gives finalize_sync_run a
	// reason to reconsider the run, without which every unit would be terminal
	// and the run still permanently open.
	if _, _, _, err := syncrunrollup.Bump(ctx, tx, candidate.syncRunID); err != nil {
		return 0, sweepUnavailable(sweepStepRollupBump, err)
	}
	if err := syncrunrollup.ArmFinalize(
		ctx, tx, candidate.syncRunID, candidate.orgID, now,
	); err != nil {
		return 0, sweepUnavailable(sweepStepArmFinalize, err)
	}
	return rowsAffected, nil
}
