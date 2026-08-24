package syncdispatchruntime

import (
	"context"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
)

// cooldownReconfirmResult ports CooldownReconfirmResult verbatim.
type cooldownReconfirmResult struct {
	excludedUnitIDs map[string]bool
	nextDeferredAt  *time.Time
}

// reconfirmCooldowns ports BudgetGuard.reconfirm_cooldowns verbatim: close
// the TOCTOU window between enforceRun's cooldown snapshot and the atomic
// claim (CHAOS-2760 review finding). enforceRun reads
// provider_rate_limit_observations once, early in its pass, then does real
// DB work of its own (activeBudgetConsumption re-estimates every active
// unit across the bucket) before returning -- under READ COMMITTED, a
// sibling unit's 429 can commit a brand-new observation row in that
// window, one enforceRun's snapshot never saw, and without a second look
// claimUnits (not yet ported) would dispatch straight into it. This
// re-runs the SAME cheap query (activeCooldowns) and the SAME per-unit
// matching (matchingCooldownExpiry) against the estimates enforceRun
// already computed -- no re-estimation, no credential decryption -- as
// the LAST read before the claim.
//
// A unit caught here is NOT merely excluded (review finding, round 2): a
// bare exclusion would leave it PLANNED with no RETRYING stamp, which
// both breaks "cooldown deferrals count against the shared rate-limit
// budget" AND livelocks the run (a PLANNED unit redispatches on a bare
// countdown forever, re-triggering this same exclusion indefinitely).
// Every match goes through the EXACT SAME write path enforceRun's own
// cooldown loop uses (resolveCooldownBlockedUnit) -- one deferral
// semantics, reused, not a second weaker one.
//
// surplusPromotedAt is enforceRun's OWN `now` argument, NOT reconfirmCooldowns'
// own `now`/checkedAt. This is the one place a naive line-for-line port
// would silently diverge from Python: Python's _admit_unit_from_surplus
// mutates its ORM unit object in place (`unit.available_at = now`) after a
// successful promotion, so by the time enforce_run returns candidate_units,
// Python's own surplus-admitted unit objects already carry the PROMOTED
// value, and _withdraw_surplus_admission reads `unit.available_at`
// straight off that mutated object. budgetUnit is a plain Go value type --
// nothing here mutates the caller's copy -- so the promoted value cannot
// be recovered from the unit struct itself and MUST be threaded through
// explicitly as its own argument. Every surplus admission within one
// enforceRun pass shares exactly one `now`, so a single timestamp
// (not a per-unit map) is enough.
func reconfirmCooldowns(
	ctx context.Context, tx pgx.Tx, logger *slog.Logger,
	syncRunID string, units []budgetUnit, estimatesByUnit map[string][]budgetEstimate,
	alreadyExcludedIDs map[string]bool, jitterSeconds int,
	surplusPriorAvailableAt map[string]time.Time, surplusPromotedAt time.Time, now time.Time,
) (cooldownReconfirmResult, error) {
	if logger == nil {
		logger = slog.Default()
	}

	candidates := make([]budgetUnit, 0, len(units))
	for _, unit := range units {
		if !alreadyExcludedIDs[unit.id] {
			candidates = append(candidates, unit)
		}
	}
	if len(candidates) == 0 {
		return cooldownReconfirmResult{}, nil
	}

	familyCooldowns, dimensionCooldowns := activeCooldowns(ctx, tx, logger, syncRunID, candidates, now)

	excluded := map[string]bool{}
	var nextDeferredAt *time.Time

	for _, unit := range candidates {
		estimates := estimatesByUnit[unit.id]
		if len(estimates) == 0 {
			continue
		}
		cooldownExpiry, hasCooldown := matchingCooldownExpiry(estimates, unit.orgID, unit.provider, unit.integrationID, familyCooldowns, dimensionCooldowns)
		priorAvailableAt, hasSurplusPrior := surplusPriorAvailableAt[unit.id]

		if hasCooldown && hasSurplusPrior {
			// SURPLUS-ADMITTED UNITS ARE WITHDRAWN, NOT DEFERRED (CHAOS-3465
			// review, CRITICAL). This unit is only in this pass's candidate
			// set because the surplus phase OFFERED it a slot it was not
			// otherwise going to get this pass. Running the ordinary
			// cooldown deferral on it would end its budget episode (a
			// fabricated episode reset the surplus phase must never cause)
			// -- so the offer is simply withdrawn: available_at goes back to
			// what it was before promotion, nothing else touched.
			if _, err := withdrawSurplusAdmission(ctx, tx, logger, syncRunID, unit, surplusPromotedAt, priorAvailableAt, now); err != nil {
				return cooldownReconfirmResult{}, err
			}
			// UNCONDITIONAL, including when the withdrawal CAS lost (review
			// round 2). The ordinary path below treats a lost race as
			// "leave it to claimUnits" because there a lost race PROVES the
			// unit stopped being claimable. Here it proves nothing: a
			// failed withdrawal leaves the unit due at the promoted
			// available_at with a cooldown we just matched, so skipping the
			// exclusion would dispatch it straight into that cooldown -- the
			// exact thing this whole re-check exists to prevent.
			excluded[unit.id] = true
			continue
		}

		var at time.Time
		var terminalized, ok, handled bool
		if hasCooldown {
			var err error
			at, terminalized, ok, err = resolveCooldownBlockedUnit(ctx, tx, logger, unit, cooldownExpiry, jitterSeconds, now)
			if err != nil {
				return cooldownReconfirmResult{}, err
			}
			handled = true
		} else if rateLimitDeferralExhausted(unit, now) {
			decision, decErr := terminalizeRateLimitExhausted(ctx, tx, logger, unit, now)
			if decErr != nil {
				return cooldownReconfirmResult{}, decErr
			}
			at, terminalized, ok = settleOrSkip(decision)
			handled = true
		}
		if !handled {
			continue
		}
		if !ok {
			// CAS lost the race -- unit moved on concurrently since the
			// candidate snapshot was built; leave it for claimUnits to sort
			// out on its own terms.
			continue
		}
		excluded[unit.id] = true
		logger.InfoContext(ctx, "dispatch_sync_run.rate_limit_cooldown_reconfirmed",
			slog.String("sync_run_id", syncRunID), slog.String("unit_id", unit.id), slog.Bool("terminalized", terminalized))
		if !terminalized {
			nextDeferredAt = earlierOf(nextDeferredAt, at)
		}
	}

	return cooldownReconfirmResult{excludedUnitIDs: excluded, nextDeferredAt: nextDeferredAt}, nil
}
