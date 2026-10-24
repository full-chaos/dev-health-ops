package syncdispatchruntime

import (
	"context"
	"fmt"
	"github.com/full-chaos/dev-health-ops/internal/syncdispatchruntime/synclog"
	"time"

	"github.com/jackc/pgx/v5"
)

// budgetUnitSelectColumns is the full sync_run_units projection every
// BudgetGuard candidate query needs -- every column budgetUnit carries
// (CHAOS-4175 map, item 16), minus sync_run_id: the caller already knows it
// (it is the query's own WHERE clause), so it is filled in from the
// argument rather than round-tripped through Postgres.
const budgetUnitSelectColumns = `
id::text, org_id, integration_id::text, source_id::text, provider, dataset_key, cost_class,
since_at, before_at, status, available_at, updated_at,
lease_owner, lease_expires_at, last_heartbeat_at,
rate_limit_deferrals, rate_limit_first_seen_at,
budget_deferrals, budget_first_deferred_at, first_blocked_at, result, processor_flags`

// scanBudgetUnit reads one budgetUnitSelectColumns row. syncRunID is not a
// SELECT column (see budgetUnitSelectColumns) so it is stamped onto every
// row by the caller, who already knows it from its own WHERE clause.
func scanBudgetUnit(rows pgx.Rows, syncRunID string) (budgetUnit, error) {
	var unit budgetUnit
	var resultRaw, processorFlagsRaw []byte
	if err := rows.Scan(
		&unit.id, &unit.orgID, &unit.integrationID, &unit.sourceID, &unit.provider, &unit.datasetKey, &unit.costClass,
		&unit.sinceAt, &unit.beforeAt, &unit.status, &unit.availableAt, &unit.updatedAt,
		&unit.leaseOwner, &unit.leaseExpiresAt, &unit.lastHeartbeat,
		&unit.rateLimitDeferrals, &unit.rateLimitFirstSeenAt,
		&unit.budgetDeferrals, &unit.budgetFirstDeferredAt, &unit.firstBlockedAt, &resultRaw, &processorFlagsRaw,
	); err != nil {
		return budgetUnit{}, err
	}
	unit.syncRunID = syncRunID
	unit.result = decodeUnitResult(resultRaw)
	unit.processorFlags = decodeProcessorFlags(processorFlagsRaw)
	return unit, nil
}

// loadDispatchCandidatePage reads one keyset page of the dispatch candidates of a run: PLANNED, due RETRYING, or
// stale DISPATCHING (a claim that never completed and aged out), in id order, at most limit rows with id after afterID.
func loadDispatchCandidatePage(
	ctx context.Context, tx pgx.Tx, syncRunID string, now time.Time,
) func(afterID string, limit int) ([]budgetUnit, error) {
	return func(afterID string, limit int) ([]budgetUnit, error) {
		const eligible = `
SELECT` + budgetUnitSelectColumns + `
FROM public.sync_run_units
WHERE sync_run_id = $1::uuid
  AND (
        status = $2
     OR (status = $3 AND available_at IS NOT NULL AND available_at <= $4)
     OR (status = $5 AND updated_at <= $6)
      )`
		args := []any{syncRunID, syncRunUnitStatusPlanned, syncRunUnitStatusRetrying, now, syncRunUnitStatusDispatching, staleDispatchCutoff(now)}
		query := eligible + `
ORDER BY id
LIMIT $7`
		if afterID == "" {
			args = append(args, limit)
		} else {
			query = eligible + `
  AND id > $8::uuid
ORDER BY id
LIMIT $7`
			args = append(args, limit, afterID)
		}
		rows, err := tx.Query(ctx, query, args...)
		if err != nil {
			return nil, fmt.Errorf("load dispatch candidate units: %w", err)
		}
		defer rows.Close()
		var units []budgetUnit
		for rows.Next() {
			unit, err := scanBudgetUnit(rows, syncRunID)
			if err != nil {
				return nil, fmt.Errorf("scan dispatch candidate unit: %w", err)
			}
			units = append(units, unit)
		}
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("load dispatch candidate units: %w", err)
		}
		return units, nil
	}
}

// pageDispatchCandidates walks the candidates a page at a time. ignoredUnitIDs is applied to a page after it is
// read, so a page of ignored ids does not end the walk.
func pageDispatchCandidates(
	load func(afterID string, limit int) ([]budgetUnit, error), ignoredUnitIDs map[string]bool, visit func([]budgetUnit) error,
) error {
	return forEachUnitPage(unitPageSize, load, func(unit budgetUnit) string { return unit.id }, func(page []budgetUnit) error {
		kept := make([]budgetUnit, 0, len(page))
		for _, unit := range page {
			if !ignoredUnitIDs[unit.id] {
				kept = append(kept, unit)
			}
		}
		if len(kept) == 0 {
			return nil
		}
		return visit(kept)
	})
}

// forEachDispatchCandidatePage ports _dispatch_candidate_units verbatim, a keyset page at a time: every unit in this
// run eligible for THIS pass -- PLANNED, due RETRYING, or stale DISPATCHING (a claim that never completed and aged out)
// -- minus whatever the caller has already decided on this pass (ignoredUnitIDs; Python filters this in Python for the
// same reason the surplus query does, see surplusRetryCandidates). Each statement returns at most unitPageSize rows.
func forEachDispatchCandidatePage(
	ctx context.Context, tx pgx.Tx, syncRunID string, ignoredUnitIDs map[string]bool, now time.Time, visit func([]budgetUnit) error,
) error {
	return pageDispatchCandidates(loadDispatchCandidatePage(ctx, tx, syncRunID, now), ignoredUnitIDs, visit)
}

// dispatchCandidateUnits is every dispatch candidate as one slice, for the caller that needs the whole set at once
// (enforceRun's admission). It reads through forEachDispatchCandidatePage, so no statement returns more than a page.
func dispatchCandidateUnits(
	ctx context.Context, tx pgx.Tx, syncRunID string, ignoredUnitIDs map[string]bool, now time.Time,
) ([]budgetUnit, error) {
	var units []budgetUnit
	err := forEachDispatchCandidatePage(ctx, tx, syncRunID, ignoredUnitIDs, now, func(page []budgetUnit) error {
		units = append(units, page...)
		return nil
	})
	return units, err
}

// budgetSurplusMaxCandidatesDefault ports BUDGET_SURPLUS_MAX_CANDIDATES_DEFAULT
// verbatim (CHAOS-3465).
const budgetSurplusMaxCandidatesDefault = 16

// budgetSurplusMaxCandidates ports _budget_surplus_max_candidates verbatim.
func budgetSurplusMaxCandidates() int {
	return budgetEnvInt("SYNC_BUDGET_SURPLUS_MAX_CANDIDATES", budgetSurplusMaxCandidatesDefault)
}

// farFutureSurplusOrder is surplusRetryOrder's stand-in for Python's
// datetime.max(tzinfo=utc): a nil ordering column sorts LAST, never first,
// so a unit that somehow reached this population without a deferral
// timestamp does not jump the longest-deferred-first queue.
var farFutureSurplusOrder = time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC)

// surplusRetryOrder ports _surplus_retry_order verbatim: longest-deferred
// first (budget_first_deferred_at ASC), first_blocked_at ASC as the
// tiebreak, unit id as the final, total tiebreak.
func surplusRetryOrder(unit budgetUnit) (time.Time, time.Time, string) {
	firstDeferred := farFutureSurplusOrder
	if unit.budgetFirstDeferredAt != nil {
		firstDeferred = *unit.budgetFirstDeferredAt
	}
	firstBlocked := farFutureSurplusOrder
	if unit.firstBlockedAt != nil {
		firstBlocked = *unit.firstBlockedAt
	}
	return firstDeferred, firstBlocked, unit.id
}

// surplusBefore is surplusRetryOrder as a total order: longest-deferred first, first_blocked_at as the tiebreak, unit
// id as the final one.
func surplusBefore(left, right budgetUnit) bool {
	firstDeferredLeft, firstBlockedLeft, idLeft := surplusRetryOrder(left)
	firstDeferredRight, firstBlockedRight, idRight := surplusRetryOrder(right)
	if !firstDeferredLeft.Equal(firstDeferredRight) {
		return firstDeferredLeft.Before(firstDeferredRight)
	}
	if !firstBlockedLeft.Equal(firstBlockedRight) {
		return firstBlockedLeft.Before(firstBlockedRight)
	}
	return idLeft < idRight
}

// surplusPick keeps the first limit units of the surplus order of everything it is offered, and counts the offers.
// It never holds more than limit units.
type surplusPick struct {
	limit   int
	offered int
	best    []budgetUnit
}

func (pick *surplusPick) offer(unit budgetUnit) {
	pick.offered++
	if pick.limit <= 0 {
		return
	}
	at := len(pick.best)
	for at > 0 && surplusBefore(unit, pick.best[at-1]) {
		at--
	}
	if at >= pick.limit {
		return
	}
	pick.best = append(pick.best, budgetUnit{})
	copy(pick.best[at+1:], pick.best[at:])
	pick.best[at] = unit
	if len(pick.best) > pick.limit {
		pick.best = pick.best[:pick.limit]
	}
}

// loadSurplusPage reads one keyset page of the run's future-RETRYING units, in id order.
func loadSurplusPage(
	ctx context.Context, tx pgx.Tx, syncRunID string, now time.Time,
) func(afterID string, limit int) ([]budgetUnit, error) {
	return func(afterID string, limit int) ([]budgetUnit, error) {
		const future = `
SELECT` + budgetUnitSelectColumns + `
FROM public.sync_run_units
WHERE sync_run_id = $1::uuid AND status = $2 AND available_at IS NOT NULL AND available_at > $3`
		args := []any{syncRunID, syncRunUnitStatusRetrying, now}
		query := future + `
ORDER BY id
LIMIT $4`
		if afterID == "" {
			args = append(args, limit)
		} else {
			query = future + `
  AND id > $5::uuid
ORDER BY id
LIMIT $4`
			args = append(args, limit, afterID)
		}
		rows, err := tx.Query(ctx, query, args...)
		if err != nil {
			return nil, fmt.Errorf("load surplus retry candidates: %w", err)
		}
		defer rows.Close()
		var units []budgetUnit
		for rows.Next() {
			unit, err := scanBudgetUnit(rows, syncRunID)
			if err != nil {
				return nil, fmt.Errorf("scan surplus retry candidate: %w", err)
			}
			units = append(units, unit)
		}
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("load surplus retry candidates: %w", err)
		}
		return units, nil
	}
}

// surplusRetryCandidates ports _surplus_retry_candidates verbatim: the
// units a surplus may be spent on. See that function's docstring for the
// three deliberate exclusions (empty slotHeadroom -> no candidates at all;
// FAILED units are unreachable by construction -- this query only ever
// selects RETRYING; a unit whose own last cause is not budget_deferred is
// skipped, since a cooldown-deferred unit is waiting on the provider, not
// the budget). The future-RETRYING units are read a keyset page at a time and
// only the first budgetSurplusMaxCandidates of the surplus order are kept, so
// neither a statement nor a slice grows with the number of deferred units.
func surplusRetryCandidates(
	ctx context.Context, tx pgx.Tx, logger *synclog.Logger, syncRunID string, ignoredUnitIDs map[string]bool,
	slotHeadroom map[dispatchBucket]int, now time.Time,
) ([]budgetUnit, error) {
	if len(slotHeadroom) == 0 {
		return nil, nil
	}
	return pickSurplusCandidates(ctx, logger, syncRunID, loadSurplusPage(ctx, tx, syncRunID, now), ignoredUnitIDs)
}

func pickSurplusCandidates(
	ctx context.Context, logger *synclog.Logger, syncRunID string,
	load func(afterID string, limit int) ([]budgetUnit, error), ignoredUnitIDs map[string]bool,
) ([]budgetUnit, error) {
	considered := budgetSurplusMaxCandidates()
	pick := &surplusPick{limit: considered}
	err := forEachUnitPage(unitPageSize, load, func(unit budgetUnit) string { return unit.id }, func(page []budgetUnit) error {
		for _, unit := range page {
			if ignoredUnitIDs[unit.id] {
				continue
			}
			if unit.lastErrorCategory() != budgetDeferredCategory {
				continue
			}
			pick.offer(unit)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if pick.offered > considered {
		// A silent cap reads as "surplus considered everything and nothing
		// else fitted", which is a different fact entirely.
		logger.Info(ctx, synclog.MsgDispatchSyncRunBudgetSurplusCandidatesTruncated, synclog.Run(synclog.ParseID(syncRunID)), synclog.Count(synclog.KeyDeferredUnits, pick.offered), synclog.Count(synclog.KeyConsideredUnits, considered))
	}
	return pick.best, nil
}
