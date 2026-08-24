package syncdispatchruntime

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
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
budget_deferrals, budget_first_deferred_at, first_blocked_at, result`

// scanBudgetUnit reads one budgetUnitSelectColumns row. syncRunID is not a
// SELECT column (see budgetUnitSelectColumns) so it is stamped onto every
// row by the caller, who already knows it from its own WHERE clause.
func scanBudgetUnit(rows pgx.Rows, syncRunID string) (budgetUnit, error) {
	var unit budgetUnit
	var resultRaw []byte
	if err := rows.Scan(
		&unit.id, &unit.orgID, &unit.integrationID, &unit.sourceID, &unit.provider, &unit.datasetKey, &unit.costClass,
		&unit.sinceAt, &unit.beforeAt, &unit.status, &unit.availableAt, &unit.updatedAt,
		&unit.leaseOwner, &unit.leaseExpiresAt, &unit.lastHeartbeat,
		&unit.rateLimitDeferrals, &unit.rateLimitFirstSeenAt,
		&unit.budgetDeferrals, &unit.budgetFirstDeferredAt, &unit.firstBlockedAt, &resultRaw,
	); err != nil {
		return budgetUnit{}, err
	}
	unit.syncRunID = syncRunID
	unit.result = decodeUnitResult(resultRaw)
	return unit, nil
}

// dispatchCandidateUnits ports _dispatch_candidate_units verbatim: every
// unit in this run eligible for THIS pass -- PLANNED, due RETRYING, or
// stale DISPATCHING (a claim that never completed and aged out) -- minus
// whatever the caller has already decided on this pass (ignoredUnitIDs;
// Python filters this in Python for the same reason the surplus query
// does, see surplusRetryCandidates).
func dispatchCandidateUnits(
	ctx context.Context, tx pgx.Tx, syncRunID string, ignoredUnitIDs map[string]bool, now time.Time,
) ([]budgetUnit, error) {
	rows, err := tx.Query(ctx, `
SELECT`+budgetUnitSelectColumns+`
FROM public.sync_run_units
WHERE sync_run_id = $1::uuid
  AND (
        status = $2
     OR (status = $3 AND available_at IS NOT NULL AND available_at <= $4)
     OR (status = $5 AND updated_at <= $6)
      )
ORDER BY id`,
		syncRunID, syncRunUnitStatusPlanned, syncRunUnitStatusRetrying, now, syncRunUnitStatusDispatching, staleDispatchCutoff(now))
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
		if ignoredUnitIDs[unit.id] {
			continue
		}
		units = append(units, unit)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("load dispatch candidate units: %w", err)
	}
	return units, nil
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

// surplusRetryCandidates ports _surplus_retry_candidates verbatim: the
// units a surplus may be spent on. See that function's docstring for the
// three deliberate exclusions (empty slotHeadroom -> no candidates at all;
// FAILED units are unreachable by construction -- this query only ever
// selects RETRYING; a unit whose own last cause is not budget_deferred is
// skipped, since a cooldown-deferred unit is waiting on the provider, not
// the budget).
func surplusRetryCandidates(
	ctx context.Context, tx pgx.Tx, logger *slog.Logger, syncRunID string, ignoredUnitIDs map[string]bool,
	slotHeadroom map[dispatchBucket]int, now time.Time,
) ([]budgetUnit, error) {
	if logger == nil {
		logger = slog.Default()
	}
	if len(slotHeadroom) == 0 {
		return nil, nil
	}
	rows, err := tx.Query(ctx, `
SELECT`+budgetUnitSelectColumns+`
FROM public.sync_run_units
WHERE sync_run_id = $1::uuid AND status = $2 AND available_at IS NOT NULL AND available_at > $3
ORDER BY id`,
		syncRunID, syncRunUnitStatusRetrying, now)
	if err != nil {
		return nil, fmt.Errorf("load surplus retry candidates: %w", err)
	}
	defer rows.Close()

	var deferred []budgetUnit
	for rows.Next() {
		unit, err := scanBudgetUnit(rows, syncRunID)
		if err != nil {
			return nil, fmt.Errorf("scan surplus retry candidate: %w", err)
		}
		if ignoredUnitIDs[unit.id] {
			continue
		}
		if unit.lastErrorCategory() != budgetDeferredCategory {
			continue
		}
		deferred = append(deferred, unit)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("load surplus retry candidates: %w", err)
	}

	sort.SliceStable(deferred, func(i, j int) bool {
		firstDeferredI, firstBlockedI, idI := surplusRetryOrder(deferred[i])
		firstDeferredJ, firstBlockedJ, idJ := surplusRetryOrder(deferred[j])
		if !firstDeferredI.Equal(firstDeferredJ) {
			return firstDeferredI.Before(firstDeferredJ)
		}
		if !firstBlockedI.Equal(firstBlockedJ) {
			return firstBlockedI.Before(firstBlockedJ)
		}
		return idI < idJ
	})

	considered := budgetSurplusMaxCandidates()
	if len(deferred) > considered {
		// A silent cap reads as "surplus considered everything and nothing
		// else fitted", which is a different fact entirely.
		logger.InfoContext(ctx, "dispatch_sync_run.budget_surplus_candidates_truncated",
			slog.String("sync_run_id", syncRunID), slog.Int("deferred_units", len(deferred)), slog.Int("considered_units", considered))
		deferred = deferred[:considered]
	}
	return deferred, nil
}
