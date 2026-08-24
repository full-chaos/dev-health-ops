package syncdispatchruntime

import "time"

// rateLimitDeferralPlan carries the fields _apply_cooldown_deferral and
// _rate_limit_deferral_exhausted actually read from Python's
// RateLimitDeferral dataclass (rate_limit_defer.py). countdown/jitter are
// deliberately omitted: no BudgetGuard caller ever reads
// deferral.countdown, only .not_before/.attempts/.first_seen_at, so porting
// Python's own internal _jittered() for an unused field would be dead code
// here.
type rateLimitDeferralPlan struct {
	notBefore   time.Time
	attempts    int
	firstSeenAt time.Time
}

// These mirror rate_limit_defer.py's own RATE_LIMIT_MAX_DEFERRALS /
// RATE_LIMIT_MAX_TOTAL_WAIT_SECONDS / RATE_LIMIT_DEFAULT_COUNTDOWN_SECONDS
// EXACTLY -- shared with the in-worker 429 path (unit execution), so they
// are hardcoded, not env-tunable, on the Python side too (CHAOS-4175 map).
// Confirmed identical to the values already ported for unit EXECUTION's own
// rate-limit handling in
// internal/jobs/providerunit/rate_limit_defer.go (rateLimitMaxDeferrals,
// rateLimitMaxTotalWait) -- not reused directly: that port is unexported,
// lives in a different package, and returns a shape (jittered countdown
// keyed off a deterministic per-unit-ID hash, no first_seen_at) built for a
// different caller's needs, not BudgetGuard's.
const (
	rateLimitMaxDeferralsBudget        = 10
	rateLimitMaxTotalWaitSecondsBudget = 2 * 60 * 60
	rateLimitDefaultCountdownSeconds   = 60.0
)

// planRateLimitDeferral ports rate_limit_defer.py's plan_rate_limit_deferral
// verbatim, restricted to the fields BudgetGuard's cooldown path reads. A
// false return means the episode's count or wall-clock budget is spent --
// the caller must fall through to terminal failure handling.
func planRateLimitDeferral(retryAfterSeconds *float64, attempts int, firstSeenAt *time.Time, now time.Time) (rateLimitDeferralPlan, bool) {
	first := now
	if firstSeenAt != nil {
		first = *firstSeenAt
	}
	elapsed := now.Sub(first).Seconds()
	if attempts >= rateLimitMaxDeferralsBudget || elapsed >= float64(rateLimitMaxTotalWaitSecondsBudget) {
		return rateLimitDeferralPlan{}, false
	}
	delay := rateLimitDefaultCountdownSeconds
	if retryAfterSeconds != nil && *retryAfterSeconds > 0 {
		delay = *retryAfterSeconds
	}
	if remaining := float64(rateLimitMaxTotalWaitSecondsBudget) - elapsed; delay > remaining {
		delay = remaining
	}
	notBefore := now.Add(time.Duration(delay * float64(time.Second)))
	return rateLimitDeferralPlan{notBefore: notBefore, attempts: attempts + 1, firstSeenAt: first}, true
}

// liveRateLimitEpisode ports _live_rate_limit_episode verbatim: this unit's
// rate-limit episode counters IF its own last recorded cause evidences the
// episode is live, otherwise a fresh (0, nil) episode.
func liveRateLimitEpisode(unit budgetUnit) (attempts int, firstSeenAt *time.Time) {
	if rateLimitEpisodeErrorCategories[unit.lastErrorCategory()] {
		return unit.rateLimitDeferrals, unit.rateLimitFirstSeenAt
	}
	return 0, nil
}

// rateLimitDeferralExhausted ports _rate_limit_deferral_exhausted verbatim:
// true when this unit's SHARED rate-limit-deferral budget is already spent,
// computed purely from the unit's OWN persisted state.
func rateLimitDeferralExhausted(unit budgetUnit, now time.Time) bool {
	attempts, firstSeenAt := liveRateLimitEpisode(unit)
	if attempts <= 0 && firstSeenAt == nil {
		return false
	}
	_, ok := planRateLimitDeferral(nil, attempts, firstSeenAt, now)
	return !ok
}

// budgetDeferralExhausted ports _budget_deferral_exhausted verbatim: the
// budget episode's own count/wall-clock caps, gated by the same
// evidence-in-result.error_category defense-in-depth the rate-limit
// predicate uses.
func budgetDeferralExhausted(unit budgetUnit, now time.Time) bool {
	deferrals := unit.budgetDeferrals
	firstDeferredAt := unit.budgetFirstDeferredAt
	if deferrals <= 0 && firstDeferredAt == nil {
		return false
	}
	if !budgetEpisodeErrorCategories[unit.lastErrorCategory()] {
		return false
	}
	if deferrals >= budgetMaxDeferrals() {
		return true
	}
	if firstDeferredAt == nil {
		return false
	}
	return now.Sub(*firstDeferredAt) >= budgetDeferralWallClockSeconds()
}

// deferralTotalExhausted ports _deferral_total_exhausted verbatim: the
// aggregate backstop measured from first_blocked_at ALONE -- it does not
// gate on error_category or any per-episode column, by design (the whole
// point is to catch a unit whose blocking reason keeps alternating between
// episodes, each reset clearing the other's counters).
func deferralTotalExhausted(unit budgetUnit, now time.Time) bool {
	if unit.firstBlockedAt == nil {
		return false
	}
	return now.Sub(*unit.firstBlockedAt) >= deferralTotalWallClockSeconds()
}
