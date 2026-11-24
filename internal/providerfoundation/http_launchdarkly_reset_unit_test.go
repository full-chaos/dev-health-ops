package providerfoundation

import (
	"net/http"
	"strconv"
	"testing"
	"time"
)

// LaunchDarkly documents X-Ratelimit-Reset as "the time at which the current
// rate limit window resets in epoch milliseconds" (REST API overview,
// https://launchdarkly.com/docs/api/overview). The shared reset reader treats
// every reset header as epoch seconds. The delay of a 429 with only that
// header must be the reset minus now, a few seconds.
func TestLaunchDarklyResetHeaderIsReadInEpochMilliseconds(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	reset := now.Add(5 * time.Second)

	t.Run("reset reader with a fixed now", func(t *testing.T) {
		t.Parallel()
		raw := strconv.FormatInt(reset.UnixMilli(), 10)
		got := rateLimitRetryAfter(http.Header{"X-Ratelimit-Reset": {raw}}, 0, now)
		if got != 5*time.Second {
			t.Fatalf("X-Ratelimit-Reset %s (epoch ms, now+5s) gave RetryAfter %s, want 5s", raw, got)
		}
	})

	t.Run("classified 429", func(t *testing.T) {
		t.Parallel()
		raw := strconv.FormatInt(time.Now().Add(5*time.Second).UnixMilli(), 10)
		classification := ClassifyHTTPWithMessage("launchdarkly", http.StatusTooManyRequests,
			http.Header{"X-Ratelimit-Reset": {raw}}, `{"message":"rate limited"}`)
		if classification == nil || classification.Class != ErrorRateLimited {
			t.Fatalf("classification = %+v, want rate_limited", classification)
		}
		if classification.RetryAfter < 3*time.Second || classification.RetryAfter > 6*time.Second {
			t.Fatalf("X-Ratelimit-Reset %s (epoch ms, about now+5s) gave RetryAfter %s, want about 5s", raw, classification.RetryAfter)
		}
	})
}
