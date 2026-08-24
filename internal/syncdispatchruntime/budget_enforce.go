package syncdispatchruntime

import (
	"context"
	"log/slog"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
)

// enforceRunResult ports BudgetGuardResult verbatim (frozen dataclass ->
// plain struct; Go has no immutability enforcement to port, callers simply
// must not mutate it).
type enforceRunResult struct {
	observations    []map[string]any
	deferredUnitIDs map[string]bool
	nextDeferredAt  *time.Time
	// surplusAdmittedUnitIDs / surplusPriorAvailableAt: units NOT candidates
	// this pass but pulled forward by the surplus phase (CHAOS-3465), and
	// each one's pre-promotion available_at, so a later stage that must
	// withdraw the promotion (reconfirmCooldowns) can put the unit back
	// exactly where it was.
	surplusAdmittedUnitIDs map[string]bool
	surplusPriorAvailableAt map[string]time.Time
	// candidateUnits / estimatesByUnit / jitterSeconds: CHAOS-2760 TOCTOU
	// closure -- the already-loaded, credential-decryption-free-to-reuse
	// candidates and estimates from THIS pass, so reconfirmCooldowns can run
	// one more cheap cooldown re-check immediately before the atomic claim
	// without re-loading estimates, using the SAME jitter config this pass
	// used (not re-reading the env var and risking mid-pass drift).
	candidateUnits  []budgetUnit
	estimatesByUnit map[string][]budgetEstimate
	jitterSeconds   int
}

func budgetDefaultLimit() int          { return budgetEnvInt("SYNC_BUDGET_DEFAULT_LIMIT", 1_000_000) }
func budgetDeferralSeconds() int       { return budgetEnvInt("SYNC_BUDGET_DEFERRAL_SECONDS", 60) }
func budgetDeferralJitterSeconds() int { return budgetEnvInt("SYNC_BUDGET_DEFERRAL_JITTER_SECONDS", 5) }

// earlierOf returns the earlier of current (if set) and candidate --
// ports the "if next_deferred_at is None or available_at < next_deferred_at"
// pattern repeated at every deferral site in enforce_run.
func earlierOf(current *time.Time, candidate time.Time) *time.Time {
	if current == nil || candidate.Before(*current) {
		value := candidate
		return &value
	}
	return current
}

func cloneSlotHeadroom(headroom map[dispatchBucket]int) map[dispatchBucket]int {
	cloned := make(map[dispatchBucket]int, len(headroom))
	for key, value := range headroom {
		cloned[key] = value
	}
	return cloned
}

func unitIDSet(units map[string]time.Time) map[string]bool {
	set := make(map[string]bool, len(units))
	for id := range units {
		set[id] = true
	}
	return set
}

// enforceRun ports BudgetGuard.enforce_run verbatim: admit or defer this
// pass's dispatch candidates against the budget, then spend whatever budget
// is left over retrying units an EARLIER deferral is still holding back
// (CHAOS-3465).
//
// slotHeadroom is DispatchGuard's per-(org_id, provider, cost_class) count
// of concurrency slots still free after this pass's own candidates.
// Surplus retry admits units the concurrency guard never saw, so without it
// there is no way to know whether admitting one would breach
// SYNC_UNIT_CONCURRENCY_PER_BUCKET. A nil/empty slotHeadroom therefore
// DISABLES surplus retry rather than guessing (surplusRetryCandidates
// itself fails closed on empty headroom) -- surplus relaxes the budget
// admission and nothing else.
//
// orgID is passed explicitly (matching authorizeRun's own convention)
// rather than read off the first candidate unit: a run with zero
// candidates but non-empty surplus candidates still needs it for the
// estimate-bridge call, and every candidate/surplus unit in one run shares
// one org by construction, so there is no ambiguity to resolve from data.
func enforceRun(
	ctx context.Context, tx pgx.Tx, bridge budgetEstimator, logger *slog.Logger,
	orgID, syncRunID string, cappedUnitIDs map[string]bool, slotHeadroom map[dispatchBucket]int, now time.Time,
) (enforceRunResult, error) {
	if logger == nil {
		logger = slog.Default()
	}

	units, err := dispatchCandidateUnits(ctx, tx, syncRunID, cappedUnitIDs, now)
	if err != nil {
		return enforceRunResult{}, err
	}
	surplusCandidates, err := surplusRetryCandidates(ctx, tx, logger, syncRunID, cappedUnitIDs, slotHeadroom, now)
	if err != nil {
		return enforceRunResult{}, err
	}
	if len(units) == 0 && len(surplusCandidates) == 0 {
		return enforceRunResult{}, nil
	}

	limits := enforcedBudgetLimits()
	defaultLimit := budgetDefaultLimit()
	deferralSeconds := budgetDeferralSeconds()
	jitterSeconds := budgetDeferralJitterSeconds()

	// Surplus candidates are estimated HERE, alongside the real candidates,
	// rather than after admission: their budget keys have to join the same
	// sorted advisory-lock batch below. A second, later acquisition would
	// take locks out of order relative to a concurrent pass that already
	// holds them, which is how the sorting rule stops being a deadlock
	// defence.
	allCandidates := make([]budgetUnit, 0, len(units)+len(surplusCandidates))
	allCandidates = append(allCandidates, units...)
	allCandidates = append(allCandidates, surplusCandidates...)
	allUnitIDs := make([]string, len(allCandidates))
	for i, unit := range allCandidates {
		allUnitIDs[i] = unit.id
	}

	estimatesByUnit := map[string][]budgetEstimate{}
	if len(allUnitIDs) > 0 {
		bridgeEstimates, bridgeErr := bridge.DispatchBudgetEstimate(ctx, orgID, syncRunID, allUnitIDs)
		if bridgeErr != nil {
			// No per-unit precedent for a whole-batch failure (Python calls
			// SyncTaskBootstrap.load per unit, each independently
			// try/excepted) -- degrading every unit in this batch to "no
			// estimate" and logging each individually is the same choice
			// activeBudgetConsumption makes for a failed group, extending
			// Python's per-unit fail-open discipline to the batched case.
			for _, unit := range allCandidates {
				logger.WarnContext(ctx, "dispatch_sync_run.budget_guard_enforce_failed",
					attrsToAny(append(unitLogAttrs(syncRunID, unit), slog.String("error", bridgeErr.Error())))...)
			}
		} else {
			for _, unit := range allCandidates {
				// A unit id absent from the response, or mapped to an empty
				// slice, both mean "no budget constraint for this unit" --
				// see dispatchBudgetEstimateResponse's doc comment. A Go map
				// read on a missing key already returns nil, so no
				// special-casing is needed here.
				estimatesByUnit[unit.id] = bridgeEstimates[unit.id]
			}
		}
	}

	budgetKeySet := map[string]bool{}
	for _, unit := range allCandidates {
		for _, estimate := range estimatesByUnit[unit.id] {
			budgetKeySet[budgetKeyFor(estimate.Bucket, estimate.RouteFamily)] = true
		}
	}
	sortedBudgetKeys := make([]string, 0, len(budgetKeySet))
	for key := range budgetKeySet {
		sortedBudgetKeys = append(sortedBudgetKeys, key)
	}
	sort.Strings(sortedBudgetKeys)
	if err := acquireBudgetAdvisoryLocks(ctx, tx, sortedBudgetKeys); err != nil {
		return enforceRunResult{}, err
	}

	deferredUnitIDs := map[string]bool{}
	var nextDeferredAt *time.Time
	cooldownHandledUnitIDs := map[string]bool{}

	// --- Shared cooldown gating (CHAOS-2760) -- BEFORE budget admission, so
	// a unit gated by a known cooldown never also reserves budget capacity
	// it will not use this pass. Surplus candidates are included in the one
	// observation query so the surplus phase can gate on the same maps
	// rather than issuing a second read.
	familyCooldowns, dimensionCooldowns := activeCooldowns(ctx, tx, logger, syncRunID, allCandidates, now)

	for _, unit := range units {
		estimates := estimatesByUnit[unit.id]
		if len(estimates) == 0 {
			continue
		}
		cooldownExpiry, hasCooldown := matchingCooldownExpiry(estimates, unit.orgID, unit.provider, unit.integrationID, familyCooldowns, dimensionCooldowns)

		var at time.Time
		var terminalized, ok, handled bool
		if hasCooldown {
			var resolveErr error
			at, terminalized, ok, resolveErr = resolveCooldownBlockedUnit(ctx, tx, logger, unit, cooldownExpiry, jitterSeconds, now)
			if resolveErr != nil {
				return enforceRunResult{}, resolveErr
			}
			handled = true
		} else if rateLimitDeferralExhausted(unit, now) {
			// Review finding: termination must not depend on a currently-
			// visible cooldown observation -- the lookback window can age
			// the causing row out of visibility at roughly the SAME instant
			// the unit's own wall-clock deferral budget expires.
			// Terminalize from the unit's own persisted
			// rate_limit_deferrals/rate_limit_first_seen_at state instead of
			// letting it dispatch and burn a worker slot only to
			// rediscover the same exhaustion in-worker. No cooldown gates
			// this unit, so a refused verdict simply falls through to
			// normal budget admission, not to a claim into an active
			// cooldown.
			decision, decErr := terminalizeRateLimitExhausted(ctx, tx, logger, unit, now)
			if decErr != nil {
				return enforceRunResult{}, decErr
			}
			at, terminalized, ok = settleOrSkip(decision)
			handled = true
		}
		if !handled {
			continue
		}
		if !ok {
			// CAS lost the race (unit moved on concurrently) -- leave it
			// for the budget loop / claim_units to sort out, same as a lost
			// deferUnitForBudget race.
			continue
		}
		cooldownHandledUnitIDs[unit.id] = true
		if terminalized {
			continue
		}
		deferredUnitIDs[unit.id] = true
		nextDeferredAt = earlierOf(nextDeferredAt, at)
	}

	consumedByBucket, err := activeBudgetConsumption(ctx, tx, bridge, logger, now, budgetKeySet)
	if err != nil {
		return enforceRunResult{}, err
	}
	// The DURABLE baseline (review round 2, R2-F1): consumption from work
	// already dispatching/running, captured BEFORE the admission loop
	// starts adding this pass's own admissions to consumedByBucket. Any
	// terminal verdict below is measured against this snapshot, so it is a
	// fact about the unit and the world rather than about the order the
	// candidate loop happened to visit siblings in.
	baselineConsumption := make(map[string]int, len(consumedByBucket))
	for key, value := range consumedByBucket {
		baselineConsumption[key] = value
	}

	var observations []map[string]any

	for _, unit := range units {
		if cooldownHandledUnitIDs[unit.id] {
			continue
		}
		estimates := estimatesByUnit[unit.id]
		if len(estimates) == 0 {
			continue
		}
		logFields := unitLogFields(syncRunID, unit)
		unitObservations := make([]map[string]any, 0, len(estimates))
		wouldDefer := false
		for _, estimate := range estimates {
			observation := observeEstimate(estimate, logFields, consumedByBucket, limits, defaultLimit, now, deferralSeconds, false)
			unitObservations = append(unitObservations, observation)
			if observation["decision"] == "would_defer" {
				wouldDefer = true
			}
		}

		if wouldDefer {
			// Exhaustion is evaluated HERE, not before admission (review
			// round 2, F1): the question is never "has this unit been
			// deferred a lot", it is "has this unit been deferred a lot AND
			// does it still not fit RIGHT NOW". A sibling finishing or a
			// bucket rolling over between passes frees capacity, and a unit
			// that would be admitted on this pass must be admitted, not
			// killed on last pass's evidence. Every observation above was
			// computed under the advisory locks this pass holds, so
			// wouldDefer is the current, authoritative answer.
			handledTerminal := false
			unfitness := baselineUnfitness(estimates, baselineConsumption, limits, defaultLimit)
			if unfitness != nil && budgetDeferralExhausted(unit, now) {
				// The episode cap may only end a unit whose misfit is real
				// independent of this pass's optional admissions (R2-F1). A
				// unit deferred purely because a sibling was admitted first
				// keeps deferring -- its counter still advances, and if the
				// contention genuinely never clears the aggregate clock
				// below is the loud backstop.
				decision, decErr := terminalizeBudgetExhausted(ctx, tx, logger, unit, now, unitObservations, *unfitness)
				if decErr != nil {
					return enforceRunResult{}, decErr
				}
				_, _, handledTerminal = settleOrSkip(decision)
			} else if deferralTotalExhausted(unit, now) {
				// Checked second: a unit with one identifiable cause fails
				// with that cause's specific category and error text; the
				// aggregate clock is the backstop for the alternating case
				// no single-cause cap can reach.
				decision, decErr := terminalizeDeferralTotalExhausted(ctx, tx, logger, unit, now)
				if decErr != nil {
					return enforceRunResult{}, decErr
				}
				_, _, handledTerminal = settleOrSkip(decision)
			}
			if handledTerminal {
				for _, observation := range unitObservations {
					observation["decision"] = "exhausted"
				}
				observations = append(observations, unitObservations...)
				continue
			}

			// Either not exhausted, or the CAS lost the race (the unit
			// moved on concurrently) -- fall through and defer normally,
			// exactly as a lost deferUnitForBudget race does.
			availableAt := now.Add(time.Duration(deferralSeconds) * time.Second).Add(cooldownJitter(jitterSeconds))
			for _, observation := range unitObservations {
				observation["decision"] = "deferred"
				observation["available_at"] = availableAt.Format(time.RFC3339Nano)
			}
			deferred, deferErr := deferUnitForBudget(ctx, tx, unit, availableAt, now, unitObservations)
			if deferErr != nil {
				return enforceRunResult{}, deferErr
			}
			if !deferred {
				// CAS lost: this unit's observations are dropped entirely
				// (not logged, not appended) -- matches Python exactly:
				// _defer_unit_for_budget's False return hits a bare
				// `continue` before the shared observations.extend at the
				// end of the loop body.
				continue
			}
			deferredUnitIDs[unit.id] = true
			nextDeferredAt = earlierOf(nextDeferredAt, availableAt)
			for _, observation := range unitObservations {
				logger.InfoContext(ctx, "dispatch_sync_run.budget_guard_deferred", observationToAnyArgs(observation)...)
			}
		} else {
			for _, estimate := range estimates {
				budgetKey := budgetKeyFor(estimate.Bucket, estimate.RouteFamily)
				consumedByBucket[budgetKey] += estimate.EstimatedUnits
			}
			for _, observation := range unitObservations {
				observation["decision"] = "allowed"
				logger.InfoContext(ctx, "dispatch_sync_run.budget_guard_allowed", observationToAnyArgs(observation)...)
			}
		}
		observations = append(observations, unitObservations...)
	}

	// --- In-cycle surplus retry (CHAOS-3465) -- AFTER admission, because
	// "what is left over" is only knowable once every real candidate has
	// taken its share. consumedByBucket now holds durable consumption plus
	// this pass's own admissions, which is exactly the baseline a surplus
	// admission must fit on top of. A CLONE of slotHeadroom is passed in
	// (matching Python's dict(slot_headroom or {})): admitSurplusRetries
	// decrements it as it admits, and the caller's own headroom map is
	// deliberately left untouched by this call.
	surplusAdmitted, updatedObservations, err := admitSurplusRetries(
		ctx, tx, logger, syncRunID, surplusCandidates, estimatesByUnit, consumedByBucket,
		limits, defaultLimit, cloneSlotHeadroom(slotHeadroom), familyCooldowns, dimensionCooldowns, observations, now,
	)
	if err != nil {
		return enforceRunResult{}, err
	}
	observations = updatedObservations

	candidateUnits := make([]budgetUnit, 0, len(units)+len(surplusAdmitted))
	candidateUnits = append(candidateUnits, units...)
	for _, unit := range surplusCandidates {
		if _, ok := surplusAdmitted[unit.id]; ok {
			candidateUnits = append(candidateUnits, unit)
		}
	}

	return enforceRunResult{
		observations:            observations,
		deferredUnitIDs:         deferredUnitIDs,
		nextDeferredAt:          nextDeferredAt,
		surplusAdmittedUnitIDs:  unitIDSet(surplusAdmitted),
		surplusPriorAvailableAt: surplusAdmitted,
		candidateUnits:          candidateUnits,
		estimatesByUnit:         estimatesByUnit,
		jitterSeconds:           jitterSeconds,
	}, nil
}
