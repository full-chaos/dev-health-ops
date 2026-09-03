package categorize

import (
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestSanitizeMessageRedactsCredentials(t *testing.T) {
	cases := []struct {
		name    string
		message string
		mustNot string
	}{
		{"openai style key", "auth failed for key sk-abcdefgh12345678", "sk-abcdefgh12345678"},
		{"bearer token", "request failed: Bearer aVeryLongToken1234567890", "aVeryLongToken1234567890"},
		{"api_key field", `api_key: "supersecretvalue123"`, "supersecretvalue123"},
		// codex round 1 (#2178) P1: a real 403 body used "api key" with a
		// literal space, not "api_key" -- the old pattern's [_-]? only
		// allowed a single underscore/hyphen, missing this shape entirely.
		{"api key with space", "http 403: api key: supersecretvalue123", "supersecretvalue123"},
		{"bearer with colon separator", "request failed: Bearer: aVeryLongToken1234567890", "aVeryLongToken1234567890"},
		// codex round 2 (#2178, bigboy) P1: no punctuation at all between the
		// label and the value -- the round's own repro body.
		{"api key with no punctuation", "http 403: api key review-secret-123", "review-secret-123"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := sanitizeMessage(tc.message)
			if strings.Contains(got, tc.mustNot) {
				t.Fatalf("sanitizeMessage(%q) = %q, still contains secret %q", tc.message, got, tc.mustNot)
			}
		})
	}
}

func TestSanitizeMessageCapsLength(t *testing.T) {
	long := strings.Repeat("a", 1000)
	got := sanitizeMessage(long)
	if len(got) > 500 {
		t.Fatalf("sanitizeMessage did not cap length: got %d chars", len(got))
	}
}

func TestClassifyProviderErrorRateLimitCarriesRetryAfter(t *testing.T) {
	header := http.Header{"Retry-After": []string{"12"}}
	err := classifyProviderError(errors.New("http 429: rate_limit_exceeded"), 429, header, "openai", "gpt-5-nano")
	if err.kind != llmErrorRateLimit {
		t.Fatalf("kind = %v, want llmErrorRateLimit", err.kind)
	}
	if !isRetryable(err) {
		t.Fatal("rate limit error must be retryable")
	}
	if err.retryAfter != 12*time.Second {
		t.Fatalf("retryAfter = %v, want 12s", err.retryAfter)
	}
}

func TestClassifyProviderErrorRateLimitClampsHostileRetryAfter(t *testing.T) {
	header := http.Header{"Retry-After": []string{"86400"}}
	err := classifyProviderError(errors.New("429 too many requests"), 429, header, "openai", "gpt-5-nano")
	if err.retryAfter != maxRetryAfterSeconds*time.Second {
		t.Fatalf("retryAfter = %v, want clamped to %vs", err.retryAfter, maxRetryAfterSeconds)
	}
}

func TestClassifyProviderErrorAuthIsNotRetryable(t *testing.T) {
	err := classifyProviderError(errors.New("http 401: invalid_api_key"), 401, nil, "openai", "gpt-5-nano")
	if err.kind != llmErrorAuth {
		t.Fatalf("kind = %v, want llmErrorAuth", err.kind)
	}
	if isRetryable(err) {
		t.Fatal("auth error must not be retryable")
	}
}

func TestClassifyProviderErrorServerIsRetryable(t *testing.T) {
	err := classifyProviderError(errors.New("http 503: server error, try again"), 503, nil, "local", "gemma3")
	if err.kind != llmErrorServer {
		t.Fatalf("kind = %v, want llmErrorServer", err.kind)
	}
	if !isRetryable(err) {
		t.Fatal("server error must be retryable")
	}
}

func TestClassifyProviderErrorTransportIsRetryable(t *testing.T) {
	err := classifyProviderError(errors.New("dial tcp: connection refused"), 0, nil, "local", "gemma3")
	if err.kind != llmErrorTransport {
		t.Fatalf("kind = %v, want llmErrorTransport", err.kind)
	}
	if !isRetryable(err) {
		t.Fatal("transport error must be retryable")
	}
}

func TestClassifyProviderErrorInvalidRequestIsNotRetryable(t *testing.T) {
	err := classifyProviderError(errors.New("http 400: unsupported_parameter"), 400, nil, "local", "gemma3")
	if err.kind != llmErrorInvalidRequest {
		t.Fatalf("kind = %v, want llmErrorInvalidRequest", err.kind)
	}
	if isRetryable(err) {
		t.Fatal("invalid-request error must not be retryable")
	}
}

func TestRetryDelayExponentialBackoffCapped(t *testing.T) {
	cases := []struct {
		attempt int
		want    time.Duration
	}{
		{0, 500 * time.Millisecond},
		{1, 1 * time.Second},
		{2, 2 * time.Second},
		{6, 30 * time.Second}, // 0.5 * 2^6 = 32, capped at 30
		{20, 30 * time.Second},
	}
	for _, tc := range cases {
		if got := retryDelay(tc.attempt); got != tc.want {
			t.Errorf("retryDelay(%d) = %v, want %v", tc.attempt, got, tc.want)
		}
	}
}
