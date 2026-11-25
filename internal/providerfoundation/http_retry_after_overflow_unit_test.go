package providerfoundation

import (
	"context"
	"math"
	"net/http"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

const maxRetryAfter = time.Duration(math.MaxInt64)

// A Retry-After the float-to-integer conversion cannot hold must give the same
// answer on every CPU: past 2^63 ns it saturates, NaN and negatives are 0, and
// every in-range value is unchanged.
func TestParseRetryAfterOutOfRangeIsTheSameOnEveryArch(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC)
	for _, test := range []struct {
		name, raw string
		want      time.Duration
	}{
		{"120 seconds", "120", 120 * time.Second},
		{"a fraction", "1.5", 1500 * time.Millisecond},
		{"zero", "0", 0},
		{"9223372036 seconds, just under 2^63 ns", "9223372036", 9223372036 * time.Second},
		{"9223372037 seconds, just over 2^63 ns", "9223372037", maxRetryAfter},
		{"1e30", "1e30", maxRetryAfter},
		{"+Inf", "+Inf", maxRetryAfter},
		{"Inf", "Inf", maxRetryAfter},
		{"NaN", "NaN", 0},
		{"-Inf", "-Inf", 0},
		{"negative", "-5", 0},
		{"1e400, a range error", "1e400", 0},
		{"text", "abc", 0},
		{"empty", "", 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := ParseRetryAfter(test.raw, now); got != test.want {
				t.Fatalf("ParseRetryAfter(%q) = %d ns, want %d ns", test.raw, int64(got), int64(test.want))
			}
		})
	}
}

// The classified answer carries the saturated delay, never a negative one, for a
// rate limit and for a 5xx.
func TestClassifyAHugeRetryAfterIsNeverNegative(t *testing.T) {
	t.Parallel()
	for _, status := range []int{http.StatusTooManyRequests, http.StatusServiceUnavailable} {
		got := ClassifyHTTPWithMessage("github", status, http.Header{"Retry-After": {"1e30"}}, "")
		if got == nil || got.RetryAfter != maxRetryAfter {
			t.Fatalf("status %d: %+v, want RetryAfter %d ns", status, got, int64(maxRetryAfter))
		}
	}
}

// Consumers: a saturated RetryAfter reaches each consumer's own cap, not a sum.
func TestRetryDelayCapsASaturatedRetryAfter(t *testing.T) {
	t.Parallel()
	policy := RetryPolicy{MaxAttempts: 2, InitialWait: time.Millisecond, MaxWait: 25 * time.Millisecond}
	client := newTestHTTPClient(t, fakehttp.Client(HTTPDoerFunc(func(*http.Request) (*http.Response, error) {
		t.Fatal("unexpected request")
		return nil, nil
	})), policy)
	if got := client.retryDelay(1, maxRetryAfter, ErrorRateLimited); got != RateLimitMaxHonoredDelay {
		t.Fatalf("rate-limit delay = %s, want %s", got, RateLimitMaxHonoredDelay)
	}
	if got := client.retryDelay(1, maxRetryAfter, ErrorTransient); got != policy.MaxWait {
		t.Fatalf("transient delay = %s, want %s", got, policy.MaxWait)
	}
}

func TestAuthStepGatePenaltyCapsASaturatedRetryAfter(t *testing.T) {
	t.Parallel()
	policy := RetryPolicy{MaxAttempts: 1, InitialWait: time.Millisecond, MaxWait: 25 * time.Millisecond}
	client := newTestHTTPClient(t, fakehttp.Client(HTTPDoerFunc(func(*http.Request) (*http.Response, error) {
		t.Fatal("unexpected request")
		return nil, nil
	})), policy)
	gate := &recordingGate{}
	client.Gate = gate
	limited := &ProviderError{Class: ErrorRateLimited, StatusCode: http.StatusForbidden, RetryAfter: maxRetryAfter}
	if err := client.authFailure(context.Background(), limited, 1); err != limited {
		t.Fatalf("authFailure = %v, want the error unchanged", err)
	}
	if len(gate.penalties) != 1 || gate.penalties[0] != RateLimitMaxHonoredDelay {
		t.Fatalf("gate penalties = %v, want one %s penalty", gate.penalties, RateLimitMaxHonoredDelay)
	}
}

// A 429 whose Retry-After overflows arms the shared gate with the ceiling and
// waits the ceiling, not the generic backoff a negative value fell to.
func TestAHugeRetryAfter429PenalizesTheGateWithTheCeiling(t *testing.T) {
	t.Parallel()
	policy := RetryPolicy{MaxAttempts: 1, InitialWait: time.Millisecond, MaxWait: 25 * time.Millisecond}
	client := newTestHTTPClient(t, fakehttp.Client(HTTPDoerFunc(func(request *http.Request) (*http.Response, error) {
		return testHTTPResponse(request, http.StatusTooManyRequests, http.Header{"Retry-After": {"1e30"}}, `{"message":"rate limit"}`), nil
	})), policy)
	gate := &recordingGate{}
	client.Gate = gate
	if _, err := client.Do(context.Background(), http.MethodGet, "/items", nil); err == nil {
		t.Fatal("rate-limited response did not surface an error")
	}
	if len(gate.penalties) != 1 || gate.penalties[0] != RateLimitMaxHonoredDelay {
		t.Fatalf("gate penalties = %v, want one %s penalty", gate.penalties, RateLimitMaxHonoredDelay)
	}
}
