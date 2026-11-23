package providerfoundation

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"
)

// A rate-limit answer whose Retry-After is longer than the time left before the
// context deadline must end the try at once as a rate-limit deferral carrying
// that wait (as awaitGate does), not sleep to the deadline and end as a
// time-limit failure. Scaled: Retry-After 2 s against a 300 ms deadline.
func TestRetryWaitLongerThanTheDeadlineEndsAsRateLimitedNotTimeLimit(t *testing.T) {
	t.Parallel()
	for _, status := range []int{http.StatusTooManyRequests, http.StatusForbidden} {
		status := status
		t.Run(http.StatusText(status), func(t *testing.T) {
			t.Parallel()
			policy := RetryPolicy{MaxAttempts: 3, InitialWait: time.Millisecond, MaxWait: 5 * time.Millisecond}
			client := newTestHTTPClient(t, HTTPDoerFunc(func(request *http.Request) (*http.Response, error) {
				return testHTTPResponse(request, status, http.Header{"Retry-After": {"2"}, "X-Ratelimit-Remaining": {"0"}}, `{"message":"rate limit"}`), nil
			}), policy)
			ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
			defer cancel()

			started := time.Now()
			_, err := client.Do(ctx, http.MethodGet, "/items", nil)
			elapsed := time.Since(started)

			var providerErr *ProviderError
			if !errors.As(err, &providerErr) || providerErr.Class != ErrorRateLimited || providerErr.RetryAfter != 2*time.Second {
				t.Fatalf("status %d, Retry-After 2s, deadline in 300ms: err = %v after %s, want rate_limited carrying RetryAfter 2s (not a deadline error)", status, err, elapsed)
			}
		})
	}
}
