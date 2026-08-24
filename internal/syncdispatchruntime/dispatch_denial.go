package syncdispatchruntime

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/providerfamilycontract"
)

// runHasDispatchingOrRunningUnits ports _run_has_dispatching_or_running_units
// verbatim: true iff this run has at least one unit currently DISPATCHING or
// RUNNING. Dispatch()'s total-cap hard-deny branch reads this to choose
// between two different failure shapes -- active units present means the
// run cannot be terminalized outright (something is still in flight), so
// only the never-going-to-dispatch-again units are failed; no active units
// means the WHOLE run can be terminalized now.
func runHasDispatchingOrRunningUnits(ctx context.Context, tx pgx.Tx, syncRunID string) (bool, error) {
	var exists bool
	err := tx.QueryRow(ctx, `
SELECT EXISTS (
  SELECT 1 FROM public.sync_run_units
  WHERE sync_run_id = $1::uuid AND status IN ($2, $3)
)`, syncRunID, syncRunUnitStatusDispatching, syncRunUnitStatusRunning).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("check for dispatching/running units: %w", err)
	}
	return exists, nil
}

// failPlannedUnits ports _fail_planned_units verbatim: fail every unit of
// the run that is not dispatched and never will be. Covers PLANNED and
// RETRYING -- on a total-cap hard deny the guard re-denies every future
// redispatch, so a deferred RETRYING unit is just as stranded as a PLANNED
// one, and a lingering RETRYING unit blocks finalize_sync_run (it requires
// all units terminal) forever. Returns the number of rows updated.
func failPlannedUnits(ctx context.Context, tx pgx.Tx, syncRunID, errorText string, now time.Time) (int, error) {
	tag, err := tx.Exec(ctx, `
UPDATE public.sync_run_units
SET status = $2, error = $3, updated_at = $4
WHERE sync_run_id = $1::uuid AND status IN ($5, $6)`,
		syncRunID, syncRunUnitStatusFailed, errorText, now, syncRunUnitStatusPlanned, syncRunUnitStatusRetrying)
	if err != nil {
		return 0, fmt.Errorf("fail planned/retrying units: %w", err)
	}
	return int(tag.RowsAffected()), nil
}

// failStaleDispatchingUnits ports _fail_stale_dispatching_units verbatim.
// Write-time CAS (NOT load-and-mutate): the status='dispatching' predicate
// is evaluated by Postgres at UPDATE time, so a stale row that a delayed
// unit execution concurrently claimed to RUNNING (DISPATCHING -> RUNNING +
// live lease) between the caller's read and this write is EXCLUDED by
// construction -- a live worker's claim is never overwritten with FAILED.
func failStaleDispatchingUnits(ctx context.Context, tx pgx.Tx, syncRunID, errorText string, now time.Time) (int, error) {
	tag, err := tx.Exec(ctx, `
UPDATE public.sync_run_units
SET status = $2, error = $3, result = $4::json, updated_at = $5
WHERE sync_run_id = $1::uuid AND status = $6 AND updated_at <= $7`,
		syncRunID, syncRunUnitStatusFailed, errorText, `{"error_category":"dispatch_denied"}`, now,
		syncRunUnitStatusDispatching, staleDispatchCutoff(now))
	if err != nil {
		return 0, fmt.Errorf("fail stale dispatching units: %w", err)
	}
	return int(tag.RowsAffected()), nil
}

// isAtomicProviderFamilyDirectAlias ports
// provider_unit_route.py's is_atomic_provider_family_direct_alias verbatim,
// reusing providerfamilycontract.PolicyFor rather than re-deriving the
// same family/mode/canonical-dataset lookup a third time (this codebase's
// single source of truth for atomic-family membership, already the basis
// for ValidateClaim).
func isAtomicProviderFamilyDirectAlias(provider, dataset string) bool {
	policy, ok := providerfamilycontract.PolicyFor(provider, dataset)
	if !ok || policy.Mode != providerfamilycontract.AtomicCanonical {
		return false
	}
	return strings.ToLower(strings.TrimSpace(dataset)) != policy.CanonicalDataset
}

// unroutableReason ports _unroutable_reason verbatim: the operator-facing
// sentence for a (provider, dataset) pair no runtime will execute. Two
// distinct causes, each needing a different operator action, so the reason
// must name the right one (review finding) -- neither is ever "a switch is
// off" (CHAOS-4054 deleted that plane) or "the fallback runtime was down"
// (step 4 deleted the Celery fallthrough; River is the only runtime left).
func unroutableReason(provider, dataset string) string {
	prefix := fmt.Sprintf("no worker can execute %s/%s", provider, dataset)
	if isAtomicProviderFamilyDirectAlias(provider, dataset) {
		return prefix + ": it is a non-canonical member of an atomic provider " +
			"family, which is never routed on its own (the family is served " +
			"by its canonical work-items claim)"
	}
	return prefix + ": the provider capability matrix does not mark it " +
		"route-ready and plannable, so no shipped writer owns it"
}

// terminalizeUnroutableUnits ports _terminalize_unroutable_units verbatim
// (CHAOS-3941): fail claimed units that no runtime can execute. Write-time
// CAS on status='dispatching' keeps a concurrent worker's DISPATCHING ->
// RUNNING claim from being overwritten. featureDisabledErrorCategory is the
// SAME terminal-denial idiom already used to recover units wedged by a
// disabled feature (native_finalize_sync_run.go), reused here rather than
// minted again, so finalize can aggregate the run and DispatchGuard's
// budget is released instead of held forever.
//
// CHAOS-3990: units are grouped by (provider, dataset_key) pair so the
// durable reason NAMES the pair and why it was refused -- a bulk update
// stamping only the bare category is what left an operator staring at a
// retry loop with thousands of attempts and no reason attached.
func terminalizeUnroutableUnits(ctx context.Context, tx pgx.Tx, units []budgetUnit, now time.Time) (int, error) {
	if len(units) == 0 {
		return 0, nil
	}
	type pairKey struct{ provider, datasetKey string }
	unitsByPair := map[pairKey][]string{}
	for _, unit := range units {
		key := pairKey{provider: unit.provider, datasetKey: unit.datasetKey}
		unitsByPair[key] = append(unitsByPair[key], unit.id)
	}

	pairs := make([]pairKey, 0, len(unitsByPair))
	for key := range unitsByPair {
		pairs = append(pairs, key)
	}
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].provider != pairs[j].provider {
			return pairs[i].provider < pairs[j].provider
		}
		return pairs[i].datasetKey < pairs[j].datasetKey
	})

	var terminalized int
	for _, pair := range pairs {
		unitIDs := unitsByPair[pair]
		reason := unroutableReason(pair.provider, pair.datasetKey)
		resultJSON, err := json.Marshal(map[string]any{
			"error_category": featureDisabledErrorCategory,
			"reason":         reason,
			"provider":       pair.provider,
			"dataset_key":    pair.datasetKey,
		})
		if err != nil {
			return terminalized, fmt.Errorf("marshal unroutable-unit result: %w", err)
		}
		tag, err := tx.Exec(ctx, `
UPDATE public.sync_run_units
SET status = $2, available_at = NULL, error = $3, last_retry_reason = $4,
    result = $5::json, lease_owner = NULL, lease_expires_at = NULL, updated_at = $6
WHERE id = ANY($1::uuid[]) AND status = $7`,
			unitIDs, syncRunUnitStatusFailed, featureDisabledErrorCategory, reason, resultJSON, now, syncRunUnitStatusDispatching)
		if err != nil {
			return terminalized, fmt.Errorf("terminalize unroutable units: %w", err)
		}
		terminalized += int(tag.RowsAffected())
	}
	return terminalized, nil
}
