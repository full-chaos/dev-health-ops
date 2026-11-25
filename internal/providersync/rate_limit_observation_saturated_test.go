package providersync

import (
	"math"
	"testing"
	"time"
)

// The stored delay is finite and never negative: a RetryAfter of the largest
// Duration is cut to the longest deferral this repository schedules, and a
// negative or zero one is stored as no value.
func TestRateLimitObservationRetryAfterSecondsCutsASaturatedDelay(t *testing.T) {
	t.Parallel()
	got := RateLimitObservation{RetryAfter: time.Duration(math.MaxInt64)}.retryAfterSeconds()
	if got == nil || *got != rateLimitMaxTotalWait.Seconds() {
		t.Fatalf("retryAfterSeconds = %v, want %v", got, rateLimitMaxTotalWait.Seconds())
	}
	for _, retryAfter := range []time.Duration{0, -time.Second, time.Duration(math.MinInt64)} {
		if got := (RateLimitObservation{RetryAfter: retryAfter}).retryAfterSeconds(); got != nil {
			t.Fatalf("RetryAfter %d ns stored %v, want no value", int64(retryAfter), *got)
		}
	}
}
