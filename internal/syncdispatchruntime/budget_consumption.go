package syncdispatchruntime

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
)

// activeBudgetConsumptionSelectColumns is budgetUnitSelectColumns plus
// sync_run_id -- unlike every other query in this family, this one is NOT
// scoped to one sync_run_id (see activeBudgetConsumption's doc comment), so
// sync_run_id cannot be stamped from a caller argument the way
// scanBudgetUnit does; it must come back as its own column.
const activeBudgetConsumptionSelectColumns = `
id::text, sync_run_id::text, org_id, integration_id::text, source_id::text, provider, dataset_key, cost_class,
since_at, before_at, status, available_at, updated_at,
lease_owner, lease_expires_at, last_heartbeat_at,
rate_limit_deferrals, rate_limit_first_seen_at,
budget_deferrals, budget_first_deferred_at, first_blocked_at, result`

func scanActiveBudgetConsumptionUnit(rows pgx.Rows) (budgetUnit, error) {
	var unit budgetUnit
	var resultRaw []byte
	if err := rows.Scan(
		&unit.id, &unit.syncRunID, &unit.orgID, &unit.integrationID, &unit.sourceID, &unit.provider, &unit.datasetKey, &unit.costClass,
		&unit.sinceAt, &unit.beforeAt, &unit.status, &unit.availableAt, &unit.updatedAt,
		&unit.leaseOwner, &unit.leaseExpiresAt, &unit.lastHeartbeat,
		&unit.rateLimitDeferrals, &unit.rateLimitFirstSeenAt,
		&unit.budgetDeferrals, &unit.budgetFirstDeferredAt, &unit.firstBlockedAt, &resultRaw,
	); err != nil {
		return budgetUnit{}, err
	}
	unit.result = decodeUnitResult(resultRaw)
	return unit, nil
}

// budgetEstimator is the credential-bound estimation call activeBudgetConsumption
// needs -- satisfied by *HTTPBridge.DispatchBudgetEstimate, narrowed to an
// interface here so unit tests can supply a fake instead of a real bridge.
type budgetEstimator interface {
	DispatchBudgetEstimate(ctx context.Context, orgID, runID string, unitIDs []string) (map[string][]budgetEstimate, error)
}

// activeBudgetConsumption ports _active_budget_consumption verbatim,
// including the one property that makes it different from every other
// query in this file: it is NOT scoped to sync_run_id. A budget bucket is
// keyed by (provider, org_id, host, credential_fingerprint, dimension,
// route_family), not by run -- another sync run hitting the same provider
// consumes the SAME bucket concurrently, so "is there room" has to be
// answered against every dispatching/running unit system-wide, not just
// this run's own.
//
// Python calls SyncTaskBootstrap.load/estimate_provider_budget per unit,
// each independently try/excepted, so one unit's estimation failure never
// affects any other. The estimate-only bridge this Go port uses
// (DispatchBudgetEstimate) is scoped to ONE (org_id, sync_run_id) per call
// by design (CHAOS-4175 estimate-bridge ruling, tenant-fenced), so active
// units -- which can span many different runs and orgs at once -- are
// grouped by (org_id, sync_run_id) here and the bridge is called once per
// group. This is a pure Go-side batching optimization over Python's
// unit-by-unit calls, not a behavior change: the per-unit degrade-to-empty
// semantics on failure is preserved by degrading every unit in a group
// whose OWN bridge call fails (network/decode error, not a per-unit
// estimator exception -- those are already degraded server-side by
// batch_estimate_provider_budget_for_units), matching what each of that
// group's units would have individually logged and gotten had Python
// called each in its own try/except and hit the same failure.
func activeBudgetConsumption(
	ctx context.Context, tx pgx.Tx, bridge budgetEstimator, logger *slog.Logger, now time.Time, budgetKeys map[string]bool,
) (map[string]int, error) {
	if logger == nil {
		logger = slog.Default()
	}
	consumedByBucket := map[string]int{}
	if len(budgetKeys) == 0 {
		return consumedByBucket, nil
	}

	rows, err := tx.Query(ctx, `
SELECT`+activeBudgetConsumptionSelectColumns+`
FROM public.sync_run_units
WHERE (status = $1 AND updated_at > $2)
   OR (status = $3 AND (lease_expires_at IS NULL OR lease_expires_at > $4))
ORDER BY id`,
		syncRunUnitStatusDispatching, staleDispatchCutoff(now), syncRunUnitStatusRunning, now)
	if err != nil {
		return nil, fmt.Errorf("load active budget consumption units: %w", err)
	}
	defer rows.Close()

	type runGroupKey struct{ orgID, syncRunID string }
	unitsByGroup := map[runGroupKey][]budgetUnit{}
	for rows.Next() {
		unit, err := scanActiveBudgetConsumptionUnit(rows)
		if err != nil {
			return nil, fmt.Errorf("scan active budget consumption unit: %w", err)
		}
		key := runGroupKey{orgID: unit.orgID, syncRunID: unit.syncRunID}
		unitsByGroup[key] = append(unitsByGroup[key], unit)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("load active budget consumption units: %w", err)
	}

	// Sorted so this function's group iteration order (and therefore its
	// bridge-call order) is deterministic run-to-run -- helpful for
	// reproducing a slow pass, and costs nothing since there is no
	// cross-group ordering contract to preserve either way.
	groupKeys := make([]runGroupKey, 0, len(unitsByGroup))
	for key := range unitsByGroup {
		groupKeys = append(groupKeys, key)
	}
	sort.Slice(groupKeys, func(i, j int) bool {
		if groupKeys[i].orgID != groupKeys[j].orgID {
			return groupKeys[i].orgID < groupKeys[j].orgID
		}
		return groupKeys[i].syncRunID < groupKeys[j].syncRunID
	})

	// This grouping-into-per-run bridge calls is a Go-side cost Python
	// never pays (it calls SyncTaskBootstrap.load per unit, in-process, no
	// HTTP hop). If system-wide active consumption ever fans out to dozens
	// of (org, run) groups in one pass, that latency should be visible in
	// the logs, not discovered by reading a trace after the fact -- and it
	// self-documents as a cost CHAOS-4198's later native estimator port
	// removes entirely (no bridge, no grouping, no fanout).
	if len(groupKeys) > 0 {
		var totalUnits int
		for _, units := range unitsByGroup {
			totalUnits += len(units)
		}
		logger.InfoContext(ctx, "dispatch_sync_run.budget_guard_active_consumption_fanout",
			slog.Int("group_count", len(groupKeys)), slog.Int("total_units", totalUnits))
	}

	for _, key := range groupKeys {
		units := unitsByGroup[key]
		unitIDs := make([]string, len(units))
		for i, unit := range units {
			unitIDs[i] = unit.id
		}

		estimatesByUnit, err := bridge.DispatchBudgetEstimate(ctx, key.orgID, key.syncRunID, unitIDs)
		if err != nil {
			for _, unit := range units {
				logger.WarnContext(ctx, "dispatch_sync_run.budget_guard_active_estimate_failed",
					attrsToAny(append(unitLogAttrs(key.syncRunID, unit), slog.String("error", err.Error())))...)
			}
			continue
		}

		for _, estimates := range estimatesByUnit {
			for _, estimate := range estimates {
				budgetKey := budgetKeyFor(estimate.Bucket, estimate.RouteFamily)
				if budgetKeys[budgetKey] {
					consumedByBucket[budgetKey] += estimate.EstimatedUnits
				}
			}
		}
	}

	return consumedByBucket, nil
}
