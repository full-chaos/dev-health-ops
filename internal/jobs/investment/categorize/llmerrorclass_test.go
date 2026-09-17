package categorize

import (
	"errors"
	"fmt"
	"net/http"
	"testing"
)

// TestClassifyLLMErrorMapsTheThreeHTTPRelevantKinds drives real errors out
// of classifyProviderError -- the one function that mints this package's
// LLM errors -- rather than constructing llmError values by hand, so the
// mapping is proven against the kinds a live provider failure actually
// produces.
func TestClassifyLLMErrorMapsTheThreeHTTPRelevantKinds(t *testing.T) {
	cases := []struct {
		name       string
		cause      error
		statusCode int
		want       LLMErrorClass
	}{
		{
			name:  "invalid api key is auth",
			cause: errors.New("invalid_api_key: the supplied credential was rejected"),
			want:  LLMErrorClassAuth,
		},
		{
			name:  "exhausted quota is auth",
			cause: errors.New("insufficient_quota for this account"),
			want:  LLMErrorClassAuth,
		},
		{
			name:       "429 is a rate limit",
			cause:      errors.New("too many requests"),
			statusCode: http.StatusTooManyRequests,
			want:       LLMErrorClassRateLimit,
		},
		{
			name:       "5xx is a server error",
			cause:      errors.New("upstream failure"),
			statusCode: http.StatusBadGateway,
			want:       LLMErrorClassServer,
		},
		{
			name:  "model not found is neither of the three",
			cause: errors.New("model_not_found for the configured deployment"),
			want:  LLMErrorClassOther,
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			err := classifyProviderError(testCase.cause, testCase.statusCode, nil, "openai", "gpt-5-mini")
			if err == nil {
				t.Fatal("classifyProviderError returned nil for a non-nil cause")
			}
			class, ok := ClassifyLLMError(err)
			if !ok {
				t.Fatal("ClassifyLLMError reported this is not an LLM error")
			}
			if class != testCase.want {
				t.Errorf("class = %v, want %v", class, testCase.want)
			}
		})
	}
}

// TestClassifyLLMErrorRefusesANonLLMError is the red half of the seam a
// caller depends on: Python's `except LLMError` branch never sees an
// unrelated failure, so an HTTP caller must not answer a provider-shaped
// status for one. ok=false is how that is reported, and it must survive
// wrapping.
func TestClassifyLLMErrorRefusesANonLLMError(t *testing.T) {
	for _, err := range []error{
		errors.New("clickhouse read failed"),
		fmt.Errorf("query work unit investments: %w", errors.New("connection reset")),
		nil,
	} {
		if class, ok := ClassifyLLMError(err); ok {
			t.Errorf("ClassifyLLMError(%v) = (%v, true), want ok=false", err, class)
		}
	}
}

// TestClassifyLLMErrorSeesThroughWrapping pins that a caller which wraps a
// provider failure on its way up (every seam between the provider and the
// HTTP handler does) still gets the right class.
func TestClassifyLLMErrorSeesThroughWrapping(t *testing.T) {
	inner := classifyProviderError(errors.New("rate_limit exceeded"), 0, nil, "openai", "gpt-5-mini")
	wrapped := fmt.Errorf("complete work unit explanation: %w", inner)
	class, ok := ClassifyLLMError(wrapped)
	if !ok || class != LLMErrorClassRateLimit {
		t.Fatalf("ClassifyLLMError(wrapped) = (%v, %v), want (%v, true)", class, ok, LLMErrorClassRateLimit)
	}
}
