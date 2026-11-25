package providerunit

import (
	"crypto/sha256"
	"encoding/binary"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providersync"
)

// Rate-limit deferral policy, ported from
// src/dev_health_ops/workers/rate_limit_defer.py. A provider 429 is deferred
// work, not a task failure: Python re-enqueues the unit as RETRYING without
// consuming the genuine-failure retry budget, bounded by a count budget and a
// wall-clock budget so a permanently rate-limited provider still eventually
// surfaces as a real failure.
//
// Go had no rate-limit branch at all: a 429 burned one of max_attempts: 5 on a
// 5s-5m backoff, so a 30-60 minute GitHub reset window exhausted every attempt
// in two or three minutes and terminalized the unit (CHAOS-3868).
const (
	rateLimitMaxDeferrals = 10
	rateLimitMaxTotalWait = 2 * time.Hour
	// A single countdown chunk. Longer provider windows are chunked: the
	// absolute not-before fence is carried on the row, so the unit re-defers
	// without calling the provider again until the window elapses.
	rateLimitMaxCountdown = 600 * time.Second
	// Used when the provider signalled a rate limit without a usable delay.
	rateLimitDefaultDelay = 60 * time.Second
	// Additive jitter de-correlates many orgs waking against one provider at
	// the same instant. Derived from the unit ID rather than a random source so
	// a retry of the same unit is reproducible, matching how
	// providerBudgetContentionDelay jitters.
	rateLimitMaxJitter = 5 * time.Second
)

// rateLimitDeferral is one planned deferral step.
type rateLimitDeferral struct {
	// countdown is the attempt-neutral River snooze for this step.
	countdown time.Duration
	// notBefore is the absolute fence persisted on the unit, so a process
	// restart resumes the same window instead of calling the provider early.
	notBefore time.Time
}

// planRateLimitDeferral returns the next deferral, or ok=false when the
// episode's count or wall-clock budget is spent and the caller must fall
// through to terminal failure handling.
func planRateLimitDeferral(
	retryAfter time.Duration,
	episode providersync.RateLimitEpisode,
	unitID string,
	now time.Time,
) (rateLimitDeferral, bool) {
	firstSeen := now
	if episode.FirstSeenAt != nil && !episode.FirstSeenAt.IsZero() {
		firstSeen = *episode.FirstSeenAt
	}
	elapsed := now.Sub(firstSeen)
	if elapsed < 0 {
		elapsed = 0
	}
	if episode.Deferrals >= rateLimitMaxDeferrals || elapsed >= rateLimitMaxTotalWait {
		return rateLimitDeferral{}, false
	}
	delay := retryAfter
	if delay <= 0 {
		delay = rateLimitDefaultDelay
	}
	// Never schedule a wait that runs past the wall-clock deadline.
	if remaining := rateLimitMaxTotalWait - elapsed; delay > remaining {
		delay = remaining
	}
	countdown := delay
	if countdown > rateLimitMaxCountdown {
		countdown = rateLimitMaxCountdown
	}
	countdown += rateLimitJitter(unitID)
	return rateLimitDeferral{countdown: countdown, notBefore: now.Add(delay)}, true
}

func rateLimitJitter(unitID string) time.Duration {
	digest := sha256.Sum256([]byte("rate-limit:" + unitID))
	return time.Duration(binary.BigEndian.Uint64(digest[:8])%uint64(rateLimitMaxJitter/time.Millisecond)) * time.Millisecond
}

const (
	gatewayBurstMaxDeferrals = 12
	gatewayBurstMaxTotalWait = time.Hour
	gatewayBurstFirstDelay   = 60 * time.Second
	gatewayBurstMaxDelay     = 10 * time.Minute
)

// planGatewayBurstDeferral is the 5xx / gateway burst twin of
// planRateLimitDeferral: 60 s doubling to 10 min per snooze, bounded by 12
// deferrals and one hour since the episode's first deferral (CHAOS-14696). It
// returns ok=false once the budget is spent, and the caller falls through to the
// ordinary retry arms.
func planGatewayBurstDeferral(
	episode providersync.RateLimitEpisode,
	unitID string,
	now time.Time,
) (rateLimitDeferral, bool) {
	firstSeen := now
	if episode.FirstSeenAt != nil && !episode.FirstSeenAt.IsZero() {
		firstSeen = *episode.FirstSeenAt
	}
	elapsed := now.Sub(firstSeen)
	if elapsed < 0 {
		elapsed = 0
	}
	if episode.Deferrals >= gatewayBurstMaxDeferrals || elapsed >= gatewayBurstMaxTotalWait {
		return rateLimitDeferral{}, false
	}
	delay := gatewayBurstFirstDelay
	for step := 0; step < episode.Deferrals && delay < gatewayBurstMaxDelay; step++ {
		delay *= 2
	}
	if delay > gatewayBurstMaxDelay {
		delay = gatewayBurstMaxDelay
	}
	if remaining := gatewayBurstMaxTotalWait - elapsed; delay > remaining {
		delay = remaining
	}
	return rateLimitDeferral{countdown: delay + rateLimitJitter(unitID), notBefore: now.Add(delay)}, true
}
