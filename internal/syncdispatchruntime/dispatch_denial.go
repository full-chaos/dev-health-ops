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

// invalidProviderFamilyClaimErrorCategory marks a persisted claim
// providerfamilycontract.ValidateClaim refuses: a malformed or stale
// atomic-family SyncRunUnit (missing one or more sibling family_dataset_*
// flags, or a non-canonical alias claimed directly instead of through its
// family's canonical dataset). Distinct from featureDisabledErrorCategory
// and unroutableReason's category so an operator reading
// sync_run_units.error_category can tell "this org's feature is off" and
// "no runtime owns this pair" apart from "this specific claim is
// corrupt/stale and needs replanning, not redispatch" (CHAOS-4550).
const invalidProviderFamilyClaimErrorCategory = "invalid_provider_family_claim"

// invalidClaimUnit pairs one claimed unit that failed ValidateClaim with the
// reason captured at the moment the error was observed. The reason is never
// re-derived later from a group's first member: two units sharing a
// (provider, dataset_key) pair can still fail ValidateClaim for different
// underlying causes (one missing a sibling flag, another carrying a stray
// cross-family flag), so each unit's own error is the one that gets
// persisted for it.
type invalidClaimUnit struct {
	unit   budgetUnit
	reason string
}

// invalidClaimReason names the specific defect ValidateClaim found, so an
// operator reading sync_run_units.error/result does not have to re-derive it
// from the provider-family policy tables by hand.
func invalidClaimReason(provider, dataset string, claimErr error) string {
	return fmt.Sprintf(
		"provider-family claim for %s/%s is invalid (%s): a stale or malformed "+
			"persisted claim that must be replanned, not redispatched",
		provider, dataset, claimErr.Error(),
	)
}

// invalidClaimTerminalizableStatuses names every pre-terminal status a unit
// can be in when terminalizeInvalidClaimUnits is asked to fail it: DISPATCHING
// (a unit claimUnits already claimed this pass -- the ORIGINAL, CHAOS-4550
// call site) or PLANNED/RETRYING (a unit invalidClaimsAmongDispatchCandidates
// found and terminalized BEFORE it was ever claimed -- the CHAOS-4556 call
// site, added so a malformed claim never enters concurrency/budget capacity
// bookkeeping in the first place). Write-time CAS: whichever status a call
// site expects, Postgres re-checks it AT THE UPDATE, so a unit a concurrent
// worker has already moved to RUNNING (or another pass has already
// terminalized) is never clobbered.
var invalidClaimTerminalizableStatuses = []string{
	syncRunUnitStatusPlanned, syncRunUnitStatusRetrying, syncRunUnitStatusDispatching,
}

// invalidClaimsAmongDispatchCandidates runs providerfamilycontract.ValidateClaim
// over EVERY unit currently eligible for this dispatch pass -- the identical
// PLANNED / due-RETRYING / stale-DISPATCHING predicate claimUnits itself
// applies (dispatchCandidateUnits, the same query BudgetGuard's
// observeRun/enforceRun already share) -- rather than only over whatever
// claimUnits ends up actually claiming.
//
// CHAOS-4556: authorizeRun's concurrency cap and BudgetGuard's budget/cooldown
// deferrals are computed from this SAME candidate set, and claimUnits then
// excludes every id either one decided (claim_units.go:71,88,103, the
// cappedUnitIDs parameter, built before Dispatch's per-claimed-unit
// ValidateClaim loop ever runs). Validating only claimUnits' own return value
// means a malformed/stale claim ordered past a saturated bucket's
// allowedSlots is deferred every pass without ever reaching ValidateClaim --
// it stays PLANNED/RETRYING indefinitely under sustained concurrency/budget
// pressure, all the while still occupying a "counted candidate" slot in the
// guard's own bucket math. Calling this BEFORE authorizeRun and terminalizing
// what it finds removes a malformed claim from the candidate set before ANY
// capacity computation sees it, so it can never be capped in the first place
// and always terminalizes on the very pass it becomes dispatch-eligible,
// independent of concurrency/budget pressure.
func invalidClaimsAmongDispatchCandidates(ctx context.Context, tx pgx.Tx, syncRunID string, now time.Time) ([]invalidClaimUnit, error) {
	candidates, err := dispatchCandidateUnits(ctx, tx, syncRunID, nil, now)
	if err != nil {
		return nil, err
	}
	var invalid []invalidClaimUnit
	for _, unit := range candidates {
		if err := providerfamilycontract.ValidateClaim(unit.provider, unit.datasetKey, unit.processorFlags, true); err != nil {
			invalid = append(invalid, invalidClaimUnit{
				unit:   unit,
				reason: invalidClaimReason(unit.provider, unit.datasetKey, err),
			})
		}
	}
	return invalid, nil
}

// terminalizeInvalidClaimUnits fails units whose persisted
// dataset_key/processor_flags fail providerfamilycontract.ValidateClaim
// (CHAOS-4550/CHAOS-4556): a claim this malformed can never become
// dispatchable by itself being retried, so leaving it PLANNED/RETRYING/
// DISPATCHING re-poisons every future dispatch pass for the whole run (the
// discard-storm class CHAOS-3990's terminalizeUnroutableUnits already exists
// to prevent for the no-route case -- this is the same idiom for the
// malformed-claim case). Called from two sites now (see
// invalidClaimTerminalizableStatuses): pre-capacity, over every dispatch
// candidate regardless of status, and post-claim, as a backstop over
// whatever claimUnits actually claimed (still needed: a unit can become
// newly dispatch-eligible, e.g. a RETRYING unit's available_at arriving,
// between the pre-capacity read and claimUnits' own write inside this same
// transaction). Units are grouped by their exact reason string (not just
// provider/dataset, since two units of the same pair can fail for different
// reasons) so the durable per-unit reason always names the specific defect,
// matching CHAOS-3990's own "never a bare category with no reason attached"
// rule.
func terminalizeInvalidClaimUnits(ctx context.Context, tx pgx.Tx, units []invalidClaimUnit, now time.Time) (int, error) {
	if len(units) == 0 {
		return 0, nil
	}
	unitIDsByReason := map[string][]string{}
	for _, entry := range units {
		unitIDsByReason[entry.reason] = append(unitIDsByReason[entry.reason], entry.unit.id)
	}
	reasons := make([]string, 0, len(unitIDsByReason))
	for reason := range unitIDsByReason {
		reasons = append(reasons, reason)
	}
	sort.Strings(reasons)

	var terminalized int
	for _, reason := range reasons {
		unitIDs := unitIDsByReason[reason]
		sort.Strings(unitIDs)
		resultJSON, err := json.Marshal(map[string]any{
			"error_category": invalidProviderFamilyClaimErrorCategory,
			"reason":         reason,
		})
		if err != nil {
			return terminalized, fmt.Errorf("marshal invalid-claim unit result: %w", err)
		}
		tag, err := tx.Exec(ctx, `
UPDATE public.sync_run_units
SET status = $2, available_at = NULL, error = $3, last_retry_reason = $4,
    result = $5::json, lease_owner = NULL, lease_expires_at = NULL, updated_at = $6
WHERE id = ANY($1::uuid[]) AND status = ANY($7::text[])`,
			unitIDs, syncRunUnitStatusFailed, invalidProviderFamilyClaimErrorCategory, reason, resultJSON, now, invalidClaimTerminalizableStatuses)
		if err != nil {
			return terminalized, fmt.Errorf("terminalize invalid-claim units: %w", err)
		}
		terminalized += int(tag.RowsAffected())
	}
	return terminalized, nil
}

// armFinalizeSyncRunWakeup ports BOTH of dispatch_sync_run's own "get
// finalize to run soon" call sites onto the ONE mechanism finalize_sync_run
// actually runs through today, not their two different literal Python
// shapes:
//   - _enqueue_denied_active_finalize's Celery task-queue push of
//     finalize_sync_run (the total-cap-denial-with-active-units branch); and
//   - dispatch_sync_run's own bare, SYNCHRONOUS `finalize_sync_run(sync_run_id)`
//     call (the "no pending work" tail branch, once river_queued and every
//     pending-unit-counts check comes back empty).
//
// finalize_sync_run's transport route is river
// (sync_dispatch_transport_routes), so both of those Python shapes reduce
// to the SAME thing every OTHER "get finalize to run soon" call site in
// this codebase already does: arm a finalize_sync_run outbox wakeup, via
// upsertDiscoveryOutboxWakeup (already generic over kind -- the SAME call
// native_reference_discovery.go's own terminal-failure path makes to arm
// finalize on ITS OWN denial path). A direct in-process Finalize(...) call
// was considered and rejected: Finalize's own precondition
// (currentTransportReference) expects a real, committed outbox row with a
// live route generation, which an ad-hoc synchronous call has no natural
// way to construct -- the outbox-wakeup path is what lets the SAME relay
// infrastructure build that context properly.
//
// Runs in the CALLER's transaction (unlike scheduleRedispatch's own
// separate session) at BOTH call sites: the denial branch commits it
// together with failPlannedUnits/failStaleDispatchingUnits as one atomic
// "terminal state + wakeup" unit; the tail branch commits it as the sole
// content of dispatch_sync_run's own third transaction. Neither is an
// improvement over Python's ordering, since the underlying mechanisms
// differ in a way that removes the question entirely: the outbox is
// pull-based (a poller picks up an eligible row whenever it next runs),
// unlike Celery's push-based task-queue send or Python's own synchronous
// call, neither of which has an equivalent commit-ordering race to preserve
// or accidentally fix.
func armFinalizeSyncRunWakeup(ctx context.Context, tx pgx.Tx, syncRunID string, now time.Time) error {
	return upsertDiscoveryOutboxWakeup(ctx, tx, "", syncRunID, outboxKindFinalizeSyncRun, now)
}
