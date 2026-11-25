package providerunit

import (
	"math"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providersync"
)

// A RetryAfter of the largest Duration is cut to the episode's remaining budget
// before any sum: the countdown is one chunk plus jitter and the fence is
// remaining past now, never a wrapped time.
func TestPlanRateLimitDeferralCutsASaturatedRetryAfterToTheEpisode(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC)
	for _, test := range []struct {
		name      string
		firstSeen *time.Time
		remaining time.Duration
	}{
		{"a fresh episode", nil, rateLimitMaxTotalWait},
		{"half of the budget spent", timePointer(now.Add(-rateLimitMaxTotalWait / 2)), rateLimitMaxTotalWait / 2},
		{"one nanosecond left", timePointer(now.Add(-rateLimitMaxTotalWait + 1)), time.Nanosecond},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			plan, ok := planRateLimitDeferral(
				time.Duration(math.MaxInt64), providersync.RateLimitEpisode{Deferrals: 1, FirstSeenAt: test.firstSeen}, "unit-1", now,
			)
			if !ok {
				t.Fatal("an episode with budget left refused to defer")
			}
			if !plan.notBefore.Equal(now.Add(test.remaining)) {
				t.Fatalf("not-before = %s, want now + %s", plan.notBefore, test.remaining)
			}
			if plan.countdown <= 0 || plan.countdown > rateLimitMaxCountdown+rateLimitMaxJitter {
				t.Fatalf("countdown = %s, want 0 < countdown <= %s", plan.countdown, rateLimitMaxCountdown+rateLimitMaxJitter)
			}
		})
	}
}
