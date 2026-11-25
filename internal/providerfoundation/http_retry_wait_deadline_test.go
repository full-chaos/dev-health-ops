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
			if !errors.As(err, &providerErr) || providerErr.Class != ErrorRateLimited || providerErr.RetryAfter != 2*time.Second || !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("status %d, Retry-After 2s, deadline in 300ms: err = %v after %s, want rate_limited carrying RetryAfter 2s that is also a deadline error", status, err, elapsed)
			}
		})
	}
}

func TestRetryWaitDeadlineBoundaryIsTheGateRule(t *testing.T) {
	t.Parallel()
	now := time.Unix(1000, 0)
	wait := 2 * time.Second
	for name, tc := range map[string]struct {
		deadline time.Time
		want     bool
	}{
		"deadline equals wait":       {now.Add(wait), true},
		"deadline is wait plus 1ns":  {now.Add(wait + time.Nanosecond), false},
		"deadline is wait minus 1ns": {now.Add(wait - time.Nanosecond), true},
	} {
		ctx, cancel := context.WithDeadline(context.Background(), tc.deadline)
		if got := deadlineWithinAt(ctx, now, wait); got != tc.want {
			t.Errorf("%s: deadlineWithinAt = %v, want %v", name, got, tc.want)
		}
		cancel()
	}
	if deadlineWithinAt(context.Background(), now, wait) {
		t.Error("a context with no deadline must never defer")
	}
}

type penalizeGate struct{ penalties []time.Duration }

func (g *penalizeGate) Wait(context.Context) (time.Duration, error) { return 0, nil }
func (g *penalizeGate) Penalize(_ context.Context, d time.Duration) error {
	g.penalties = append(g.penalties, d)
	return nil
}

func retryDeadlineClient(t *testing.T, status int, headers http.Header, requests *int, policy RetryPolicy) *HTTPClient {
	t.Helper()
	return newTestHTTPClient(t, HTTPDoerFunc(func(request *http.Request) (*http.Response, error) {
		*requests++
		return testHTTPResponse(request, status, headers, `{"message":"rate limit"}`), nil
	}), policy)
}

func TestRetryWaitDeadlineDefersOnceAndKeepsTheGatePenalty(t *testing.T) {
	t.Parallel()
	var requests int
	policy := RetryPolicy{MaxAttempts: 3, InitialWait: time.Millisecond, MaxWait: 5 * time.Millisecond}
	client := retryDeadlineClient(t, http.StatusTooManyRequests, http.Header{"Retry-After": {"2"}}, &requests, policy)
	gate := &penalizeGate{}
	client.Gate = gate
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	_, err := client.Do(ctx, http.MethodGet, "/items", nil)
	var providerErr *ProviderError
	if !errors.As(err, &providerErr) || providerErr.Class != ErrorRateLimited || providerErr.RetryAfter != 2*time.Second {
		t.Fatalf("err = %v, want rate_limited carrying 2s", err)
	}
	if requests != 1 || len(gate.penalties) != 1 || gate.penalties[0] != 2*time.Second || ctx.Err() != nil {
		t.Fatalf("requests=%d penalties=%v ctxErr=%v, want 1 request, one 2s penalty, live context", requests, gate.penalties, ctx.Err())
	}
}

func TestRetryWaitDeadlineBackoffOnlyRateLimitCarriesTheBackoff(t *testing.T) {
	t.Parallel()
	var requests int
	policy := RetryPolicy{MaxAttempts: 3, InitialWait: 2 * time.Second, MaxWait: 4 * time.Second}
	client := retryDeadlineClient(t, http.StatusTooManyRequests, http.Header{}, &requests, policy)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	_, err := client.Do(ctx, http.MethodGet, "/items", nil)
	var providerErr *ProviderError
	if !errors.As(err, &providerErr) || providerErr.Class != ErrorRateLimited || providerErr.RetryAfter < 2*time.Second {
		t.Fatalf("err = %v, want rate_limited with a non-zero RetryAfter from the backoff", err)
	}
	if requests != 1 {
		t.Fatalf("requests = %d, want 1", requests)
	}
}

func TestRetryWaitDeadlineTransientStillSleepsToTheDeadline(t *testing.T) {
	t.Parallel()
	var requests int
	policy := RetryPolicy{MaxAttempts: 3, InitialWait: 5 * time.Second, MaxWait: 5 * time.Second}
	client := retryDeadlineClient(t, http.StatusServiceUnavailable, nil, &requests, policy)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	_, err := client.Do(ctx, http.MethodGet, "/items", nil)
	var providerErr *ProviderError
	if !errors.Is(err, context.DeadlineExceeded) || errors.As(err, &providerErr) || requests != 1 {
		t.Fatalf("err = %v after %d request(s), want the base behaviour for a transient answer: the wait slept out to the deadline and a plain deadline error", err, requests)
	}
}

func TestRetryWaitThatFitsTheDeadlineStillRetries(t *testing.T) {
	t.Parallel()
	var requests int
	policy := RetryPolicy{MaxAttempts: 3, InitialWait: time.Millisecond, MaxWait: 5 * time.Millisecond}
	client := newTestHTTPClient(t, HTTPDoerFunc(func(request *http.Request) (*http.Response, error) {
		requests++
		if requests == 1 {
			return testHTTPResponse(request, http.StatusTooManyRequests, http.Header{"Retry-After": {"1"}}, `{}`), nil
		}
		return testHTTPResponse(request, http.StatusOK, http.Header{}, `{}`), nil
	}), policy)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	response, err := client.Do(ctx, http.MethodGet, "/items", nil)
	if err != nil {
		t.Fatalf("Do = %v, want the retry to succeed", err)
	}
	_ = response.Body.Close()
	if requests != 2 {
		t.Fatalf("requests = %d, want 2", requests)
	}
}

func TestTransportFailureNearTheDeadlineStaysADeadlineError(t *testing.T) {
	t.Parallel()
	policy := RetryPolicy{MaxAttempts: 3, InitialWait: 2 * time.Second, MaxWait: 4 * time.Second}
	client := newTestHTTPClient(t, HTTPDoerFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("connection reset")
	}), policy)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err := client.Do(ctx, http.MethodGet, "/items", nil)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want the transport arm's deadline error, unchanged", err)
	}
}

// CHAOS-14696: a context marked by the chunked provider unit keeps the class of
// a 500/502/503/504 answer beside the deadline error; an unmarked context (every
// other caller, see TestRetryWaitDeadlineTransientStillSleepsToTheDeadline) and
// a marked context with a status outside the four keep the slept-out wait.
func TestRetryWaitDeadlineMarkedContextKeepsTheGatewayClass(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		status    int
		wantClass bool
	}{
		{http.StatusInternalServerError, true}, {http.StatusBadGateway, true},
		{http.StatusServiceUnavailable, true}, {http.StatusGatewayTimeout, true},
		{http.StatusNotImplemented, false},
	} {
		var requests int
		policy := RetryPolicy{MaxAttempts: 3, InitialWait: 5 * time.Second, MaxWait: 5 * time.Second}
		client := retryDeadlineClient(t, test.status, nil, &requests, policy)
		ctx, cancel := context.WithTimeout(WithGatewayDeadlineClass(context.Background()), 300*time.Millisecond)
		_, err := client.Do(ctx, http.MethodGet, "/items", nil)
		liveContext := ctx.Err() == nil
		cancel()
		var providerErr *ProviderError
		gotClass := errors.As(err, &providerErr) && providerErr.Class == ErrorTransient && providerErr.StatusCode == test.status
		if !errors.Is(err, context.DeadlineExceeded) || gotClass != test.wantClass || requests != 1 {
			t.Fatalf("status %d: err = %v requests=%d classKept=%v, want classKept=%v, a deadline error and one request",
				test.status, err, requests, gotClass, test.wantClass)
		}
		if test.wantClass && !liveContext {
			t.Fatalf("status %d: the context died before Client.Do returned, want the try ended at once", test.status)
		}
	}
}
