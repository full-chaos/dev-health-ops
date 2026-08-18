package providerfoundation

import (
	"context"
	"net/http"
	"strconv"
	"testing"
	"time"
)

// TestClassifyDerivesRateLimitDelayFromResetHeaders is CHAOS-3868 evidence.
// GitHub primary limits usually carry only x-ratelimit-reset and no
// Retry-After, and GitLab uses RateLimit-Reset; Go read retry-after alone and
// fell back to a generic 5s-5m backoff, so it re-hit the provider well inside
// its own stated window -- which is what escalates a primary limit into a
// secondary one.
func TestClassifyDerivesRateLimitDelayFromResetHeaders(t *testing.T) {
	t.Parallel()
	reset := time.Now().Add(20 * time.Minute)
	tests := []struct {
		name     string
		provider string
		status   int
		headers  http.Header
	}{
		{
			name: "github primary limit", provider: "github", status: http.StatusForbidden,
			headers: http.Header{
				"X-Ratelimit-Remaining": {"0"},
				"X-Ratelimit-Reset":     {epochSeconds(reset)},
			},
		},
		{
			name: "gitlab 429 reset", provider: "gitlab", status: http.StatusTooManyRequests,
			headers: http.Header{"Ratelimit-Reset": {epochSeconds(reset)}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			classification := ClassifyHTTP(test.provider, test.status, test.headers)
			if classification == nil || classification.Class != ErrorRateLimited {
				t.Fatalf("classification=%+v, want rate limited", classification)
			}
			// Allow a second of slack for the clock read inside the classifier.
			if classification.RetryAfter < 19*time.Minute || classification.RetryAfter > 20*time.Minute+time.Second {
				t.Fatalf("RetryAfter=%s, want ~20m derived from the reset header", classification.RetryAfter)
			}
		})
	}
}

// An explicit Retry-After still wins over the reset header, matching Python's
// resolution order.
func TestClassifyPrefersRetryAfterOverResetHeader(t *testing.T) {
	t.Parallel()
	classification := ClassifyHTTP("github", http.StatusTooManyRequests, http.Header{
		"Retry-After":       {"120"},
		"X-Ratelimit-Reset": {epochSeconds(time.Now().Add(time.Hour))},
	})
	if classification == nil || classification.RetryAfter != 2*time.Minute {
		t.Fatalf("classification=%+v, want the explicit 120s Retry-After", classification)
	}
}

// A reset header rides along on responses that are not rate limits at all, so
// it must not become a retry delay for them.
func TestClassifyIgnoresResetHeaderOnNonRateLimitedResponses(t *testing.T) {
	t.Parallel()
	classification := ClassifyHTTP("github", http.StatusInternalServerError, http.Header{
		"X-Ratelimit-Reset": {epochSeconds(time.Now().Add(time.Hour))},
	})
	if classification == nil || classification.Class != ErrorTransient {
		t.Fatalf("classification=%+v, want transient", classification)
	}
	if classification.RetryAfter != 0 {
		t.Fatalf("RetryAfter=%s, want 0 for a non-rate-limited response", classification.RetryAfter)
	}
}

type recordingGate struct {
	penalties []time.Duration
}

func (gate *recordingGate) Wait(context.Context) (time.Duration, error) { return 0, nil }

func (gate *recordingGate) Penalize(_ context.Context, delay time.Duration) error {
	gate.penalties = append(gate.penalties, delay)
	return nil
}

// TestRateLimitPenalizesSharedGateWithUntruncatedDelay is CHAOS-3868 evidence.
// The Valkey gate is shared cross-runtime -- Celery workers read the same key
// -- and accepts up to 300s, but it was armed with this client's already
// truncated retry wait, so it under-penalized the Celery workers too.
func TestRateLimitPenalizesSharedGateWithUntruncatedDelay(t *testing.T) {
	t.Parallel()
	policy := RetryPolicy{MaxAttempts: 1, InitialWait: time.Millisecond, MaxWait: 25 * time.Millisecond}
	client := newTestHTTPClient(t, HTTPDoerFunc(func(request *http.Request) (*http.Response, error) {
		return testHTTPResponse(request, http.StatusTooManyRequests, http.Header{
			"Retry-After": {"240"},
		}, `{"message":"rate limit"}`), nil
	}), policy)
	gate := &recordingGate{}
	client.Gate = gate

	if _, err := client.Do(context.Background(), http.MethodGet, "/items", nil); err == nil {
		t.Fatal("rate-limited response did not surface an error")
	}
	if len(gate.penalties) != 1 || gate.penalties[0] != 4*time.Minute {
		t.Fatalf("gate penalties=%v, want one 4m penalty, not the %s retry wait", gate.penalties, policy.MaxWait)
	}
}

// A provider delay beyond the honoured ceiling is capped rather than parking
// the shared gate unbounded, exactly as Python clamps it.
func TestRateLimitGatePenaltyIsCappedAtTheHonouredCeiling(t *testing.T) {
	t.Parallel()
	policy := RetryPolicy{MaxAttempts: 1, InitialWait: time.Millisecond, MaxWait: 25 * time.Millisecond}
	client := newTestHTTPClient(t, HTTPDoerFunc(func(request *http.Request) (*http.Response, error) {
		return testHTTPResponse(request, http.StatusTooManyRequests, http.Header{
			"Retry-After": {"3600"},
		}, `{"message":"rate limit"}`), nil
	}), policy)
	gate := &recordingGate{}
	client.Gate = gate

	if _, err := client.Do(context.Background(), http.MethodGet, "/items", nil); err == nil {
		t.Fatal("rate-limited response did not surface an error")
	}
	if len(gate.penalties) != 1 || gate.penalties[0] != RateLimitMaxHonoredDelay {
		t.Fatalf("gate penalties=%v, want one %s penalty", gate.penalties, RateLimitMaxHonoredDelay)
	}
}

func epochSeconds(at time.Time) string {
	return strconv.FormatInt(at.Unix(), 10)
}
