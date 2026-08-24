package syncdispatchruntime

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/big"
	"time"

	"github.com/jackc/pgx/v5"
)

// cooldownJitter draws a uniform random duration in [0, maxSeconds) from
// crypto/rand, at millisecond granularity -- the same technique (and same
// rationale: Semgrep's math/rand audit rule flags any use regardless of
// purpose, and repo policy forbids nosemgrep suppressions) already
// established for reference-discovery backoff jitter
// (native_reference_discovery.go's referenceDiscoveryJitter) and
// internal/providerfoundation/http.go's retryJitter. The DISTRIBUTION
// matches Python's random.uniform(0, float(jitter_seconds)) -- a uniform
// draw over the same range, only the entropy source and the sub-second
// granularity (millisecond steps instead of a continuous float) differ,
// neither of which is observable at the seconds-scale this jitter exists
// to de-correlate.
func cooldownJitter(maxSeconds int) time.Duration {
	if maxSeconds <= 0 {
		return 0
	}
	boundMillis := int64(maxSeconds) * 1000
	value, err := rand.Int(rand.Reader, big.NewInt(boundMillis))
	if err != nil {
		return 0
	}
	return time.Duration(value.Int64()) * time.Millisecond
}

// applyCooldownDeferral ports _apply_cooldown_deferral verbatim: write the
// cooldown deferral stamp for a unit whose plan is already known to be
// live. Callers reach this only through resolveCooldownBlockedUnit, which
// owns the cap ordering. ok=false is a lost CAS race (Python: bare nil),
// matching the identical 3-clause claim predicate every write in this
// family shares.
//
// available_at is deferral.notBefore plus fresh jitter (not_before itself
// carries none), re-clamped to firstSeenAt+RATE_LIMIT_MAX_TOTAL_WAIT_SECONDS:
// jitter added on top of an already-clamped not_before could otherwise push
// past the shared deferral budget's wall-clock deadline a second time.
func applyCooldownDeferral(
	ctx context.Context, tx pgx.Tx, logger *slog.Logger, unit budgetUnit, deferral rateLimitDeferralPlan, jitterSeconds int, now time.Time,
) (time.Time, bool, error) {
	if logger == nil {
		logger = slog.Default()
	}
	wallClockDeadline := deferral.firstSeenAt.Add(rateLimitMaxTotalWaitSecondsBudget * time.Second)
	availableAt := deferral.notBefore.Add(cooldownJitter(jitterSeconds))
	if availableAt.After(wallClockDeadline) {
		availableAt = wallClockDeadline
	}
	resultJSON, err := json.Marshal(map[string]any{
		"error_category":       rateLimitCooldownDeferredCategory,
		"not_before":           availableAt.Format(time.RFC3339Nano),
		"rate_limit_deferrals": deferral.attempts,
	})
	if err != nil {
		return time.Time{}, false, fmt.Errorf("marshal cooldown deferral result: %w", err)
	}
	tag, err := tx.Exec(ctx, `
UPDATE public.sync_run_units
SET status = $2, available_at = $3,
    rate_limit_deferrals = $4, rate_limit_first_seen_at = $5,
    budget_deferrals = 0, budget_first_deferred_at = NULL,
    first_blocked_at = COALESCE(first_blocked_at, $6),
    error = $7, result = $8::json,
    lease_owner = NULL, lease_expires_at = NULL, last_heartbeat_at = $6, updated_at = $6
WHERE id = $1::uuid
  AND (
        status = $9
     OR (status = $10 AND available_at IS NOT NULL AND available_at <= $6)
     OR (status = $11 AND updated_at <= $12)
      )`,
		unit.id, syncRunUnitStatusRetrying, availableAt,
		deferral.attempts, deferral.firstSeenAt,
		now,
		"deferred by sync cooldown guard", resultJSON,
		syncRunUnitStatusPlanned, syncRunUnitStatusRetrying, syncRunUnitStatusDispatching, staleDispatchCutoff(now))
	if err != nil {
		return time.Time{}, false, err
	}
	if tag.RowsAffected() == 0 {
		return time.Time{}, false, nil
	}
	logger.InfoContext(ctx, "dispatch_sync_run.rate_limit_cooldown_deferred",
		slog.String("unit_id", unit.id), slog.Time("available_at", availableAt), slog.Int("rate_limit_deferrals", deferral.attempts))
	return availableAt, true, nil
}

// deferUnitForBudget ports _defer_unit_for_budget verbatim: the budget-
// episode CAS write, symmetric to applyCooldownDeferral -- each clears the
// OTHER episode's counters (CHAOS-3412 episode-pair symmetry), both
// COALESCE first_blocked_at (never overwrite it), both clear the lease.
func deferUnitForBudget(
	ctx context.Context, tx pgx.Tx, unit budgetUnit, availableAt, now time.Time, observations []map[string]any,
) (bool, error) {
	resultJSON, err := json.Marshal(map[string]any{
		"error_category": budgetDeferredCategory,
		"not_before":     availableAt.Format(time.RFC3339Nano),
		"budget_guard":   observations,
	})
	if err != nil {
		return false, fmt.Errorf("marshal budget deferral result: %w", err)
	}
	tag, err := tx.Exec(ctx, `
UPDATE public.sync_run_units
SET status = $2, available_at = $3, error = $4, result = $5::json,
    budget_deferrals = budget_deferrals + 1,
    budget_first_deferred_at = COALESCE(budget_first_deferred_at, $6),
    first_blocked_at = COALESCE(first_blocked_at, $6),
    rate_limit_deferrals = 0, rate_limit_first_seen_at = NULL,
    lease_owner = NULL, lease_expires_at = NULL, last_heartbeat_at = $6, updated_at = $6
WHERE id = $1::uuid
  AND (
        status = $7
     OR (status = $8 AND available_at IS NOT NULL AND available_at <= $6)
     OR (status = $9 AND updated_at <= $10)
      )`,
		unit.id, syncRunUnitStatusRetrying, availableAt, "deferred by sync budget guard", resultJSON, now,
		syncRunUnitStatusPlanned, syncRunUnitStatusRetrying, syncRunUnitStatusDispatching, staleDispatchCutoff(now))
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() > 0, nil
}

// planCooldownDeferral ports _plan_cooldown_deferral verbatim: the
// rate-limit deferral plan for a cooldown-gated unit, using the unit's OWN
// live episode counters (fresh, if its last cause isn't rate-limit-related).
func planCooldownDeferral(unit budgetUnit, cooldownExpiry, now time.Time) (rateLimitDeferralPlan, bool) {
	attempts, firstSeenAt := liveRateLimitEpisode(unit)
	retryAfter := cooldownExpiry.Sub(now).Seconds()
	if retryAfter < 0 {
		retryAfter = 0
	}
	return planRateLimitDeferral(&retryAfter, attempts, firstSeenAt, now)
}

// resolveCooldownBlockedUnit ports _resolve_cooldown_blocked_unit verbatim
// -- THE decision for a unit gated by an active shared cooldown, called by
// BOTH enforceRun and reconfirmCooldowns. The ordering, which is the
// actual contract:
//  1. EPISODE-SPECIFIC caps first, always -- a unit with one identifiable
//     cause must fail with THAT cause's category and error text.
//  2. The AGGREGATE clock second, as the backstop for the case no
//     single-cause cap can see.
//  3. Otherwise defer.
//
// A REFUSED verdict never leaves this function as "skip" -- the unit is
// blocked by a live cooldown right now, so it must leave here stamped:
// refusal drops through to a FRESH deferral. Returning "skip" would let
// the claim step dispatch it straight into the cooldown and reset the
// aggregate clock on the way.
//
// Return shape matches settleOrSkip's convention: (at, terminalized, ok).
// ok=false means a genuine lost CAS race (the unit moved on concurrently);
// the caller leaves it for the next pass.
func resolveCooldownBlockedUnit(
	ctx context.Context, tx pgx.Tx, logger *slog.Logger, unit budgetUnit, cooldownExpiry time.Time, jitterSeconds int, now time.Time,
) (time.Time, bool, bool, error) {
	refused := false

	// 1a. Episode cap from the unit's own persisted state.
	if rateLimitDeferralExhausted(unit, now) {
		decision, err := terminalizeRateLimitExhausted(ctx, tx, logger, unit, now)
		if err != nil {
			return time.Time{}, false, false, err
		}
		if at, terminalized, result := settleTerminalDecision(decision); result != settleCarryOn {
			return at, terminalized, result == settleWritten, nil
		}
		refused = true
	}

	// 1b. Episode cap as the shared planner sees it. Skipped after a
	// refusal: the planner reads the same counters the refusal just
	// rejected.
	var deferral rateLimitDeferralPlan
	var deferralOK bool
	if !refused {
		deferral, deferralOK = planCooldownDeferral(unit, cooldownExpiry, now)
	}
	if !refused && !deferralOK {
		decision, err := terminalizeRateLimitExhausted(ctx, tx, logger, unit, now)
		if err != nil {
			return time.Time{}, false, false, err
		}
		if at, terminalized, result := settleTerminalDecision(decision); result != settleCarryOn {
			return at, terminalized, result == settleWritten, nil
		}
		refused = true
	}

	// 2. Aggregate backstop.
	if deferralTotalExhausted(unit, now) {
		decision, err := terminalizeDeferralTotalExhausted(ctx, tx, logger, unit, now)
		if err != nil {
			return time.Time{}, false, false, err
		}
		if at, terminalized, result := settleTerminalDecision(decision); result != settleCarryOn {
			return at, terminalized, result == settleWritten, nil
		}
	}

	// 3. Defer. After a refusal this is a FRESH episode: the counters that
	// were refused as evidence are not reused as a starting point either.
	if !deferralOK {
		retryAfter := cooldownExpiry.Sub(now).Seconds()
		if retryAfter < 0 {
			retryAfter = 0
		}
		var ok bool
		deferral, ok = planRateLimitDeferral(&retryAfter, 0, nil, now)
		if !ok {
			// A fresh plan is never exhausted; if that ever changes, fail
			// loudly rather than silently leaving a cooldown-blocked unit
			// claimable (matches Python's TerminalVerdictError here --
			// same invariant-violation class as assertVerdictWellformed).
			panic("fresh rate-limit deferral plan came back exhausted; a cooldown-blocked unit would be left unstamped")
		}
	}
	availableAt, applied, err := applyCooldownDeferral(ctx, tx, logger, unit, deferral, jitterSeconds, now)
	if err != nil {
		return time.Time{}, false, false, err
	}
	if !applied {
		return time.Time{}, false, false, nil
	}
	return availableAt, false, true, nil
}
