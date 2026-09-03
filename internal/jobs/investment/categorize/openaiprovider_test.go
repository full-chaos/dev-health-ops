package categorize

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func newTestOpenAIProvider(t *testing.T, handler http.HandlerFunc) (*OpenAIProvider, *int) {
	t.Helper()
	var calls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.URL.Path != "/responses" {
			t.Fatalf("unexpected path %q", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer test-key" {
			t.Fatalf("Authorization header = %q, want Bearer test-key", got)
		}
		handler(w, r)
	}))
	t.Cleanup(server.Close)

	provider := NewOpenAIProvider(OpenAIProviderConfig{
		APIKey:  "test-key",
		BaseURL: server.URL,
		Model:   "gpt-5-nano",
	})
	t.Cleanup(func() { provider.Close() })
	return provider, &calls
}

func decodeOpenAIRequest(t *testing.T, r *http.Request) openAIResponsesRequest {
	t.Helper()
	var body openAIResponsesRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		t.Fatalf("decode request body: %v", err)
	}
	return body
}

func TestOpenAIProviderCompleteSuccessViaOutputText(t *testing.T) {
	provider, calls := newTestOpenAIProvider(t, func(w http.ResponseWriter, r *http.Request) {
		req := decodeOpenAIRequest(t, r)
		if req.Model != "gpt-5-nano" {
			t.Errorf("model = %q, want gpt-5-nano", req.Model)
		}
		if req.Text.Format.Type != "json_schema" || req.Text.Format.Name != "categorization" {
			t.Errorf("unexpected text.format: %+v", req.Text.Format)
		}
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(openAIResponsesResponse{
			OutputText: `{"a": 1}`,
			Usage: &openAIUsage{
				InputTokens:  intPtr(10),
				OutputTokens: intPtr(5),
			},
		})
	})

	result, err := provider.Complete(context.Background(), "DEV_HEALTH_RESPONSE_FORMAT=investment_categorization\nprompt")
	if err != nil {
		t.Fatalf("Complete returned error: %v", err)
	}
	if result.Text != `{"a":1}` {
		t.Fatalf("Text = %q, want compact JSON", result.Text)
	}
	if result.InputTokens == nil || *result.InputTokens != 10 {
		t.Errorf("InputTokens = %v, want 10", result.InputTokens)
	}
	if *calls != 1 {
		t.Fatalf("calls = %d, want 1", *calls)
	}
}

func TestOpenAIProviderCompleteSuccessViaOutputContentFallback(t *testing.T) {
	provider, _ := newTestOpenAIProvider(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(openAIResponsesResponse{
			OutputText: "",
			Output: []openAIResponseOutput{
				{Content: []openAIResponseContent{{Type: "output_text", Text: `{"b"`}}},
				{Content: []openAIResponseContent{{Type: "output_text", Text: `: 2}`}}},
			},
		})
	})

	result, err := provider.Complete(context.Background(), "prompt")
	if err != nil {
		t.Fatalf("Complete returned error: %v", err)
	}
	if result.Text != `{"b":2}` {
		t.Fatalf("Text = %q, want assembled+compacted JSON", result.Text)
	}
}

func TestOpenAIProviderCompleteRetriesOnTruncatedOutputThenSucceeds(t *testing.T) {
	var seenMaxTokens []int
	provider, calls := newTestOpenAIProvider(t, func(w http.ResponseWriter, r *http.Request) {
		req := decodeOpenAIRequest(t, r)
		seenMaxTokens = append(seenMaxTokens, req.MaxOutputTokens)
		w.WriteHeader(http.StatusOK)
		if len(seenMaxTokens) == 1 {
			_ = json.NewEncoder(w).Encode(openAIResponsesResponse{
				OutputText:        "",
				IncompleteDetails: &openAIIncompleteDetails{Reason: "max_output_tokens"},
			})
			return
		}
		_ = json.NewEncoder(w).Encode(openAIResponsesResponse{OutputText: `{"ok": true}`})
	})

	result, err := provider.Complete(context.Background(), "prompt")
	if err != nil {
		t.Fatalf("Complete returned error: %v", err)
	}
	if result.Text != `{"ok":true}` {
		t.Fatalf("Text = %q after retry, want valid JSON", result.Text)
	}
	if *calls != 2 {
		t.Fatalf("calls = %d, want 2 (one retry)", *calls)
	}
	if len(seenMaxTokens) != 2 || seenMaxTokens[1] <= seenMaxTokens[0] {
		t.Fatalf("max_output_tokens did not increase on retry: %v", seenMaxTokens)
	}
}

func TestOpenAIProviderCompleteReturnsEmptyAfterExhaustingRetries(t *testing.T) {
	provider, calls := newTestOpenAIProvider(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(openAIResponsesResponse{
			OutputText:        "",
			IncompleteDetails: &openAIIncompleteDetails{Reason: "max_output_tokens"},
		})
	})

	result, err := provider.Complete(context.Background(), "prompt")
	if err != nil {
		t.Fatalf("Complete returned an error for an exhausted-retry truncation, want empty text: %v", err)
	}
	if result.Text != "" {
		t.Fatalf("Text = %q, want empty after exhausting retries", result.Text)
	}
	if *calls != openAIMaxRetries+1 {
		t.Fatalf("calls = %d, want %d", *calls, openAIMaxRetries+1)
	}
}

func TestOpenAIProviderCompleteRetriesOnRateLimitThenSucceeds(t *testing.T) {
	var requestsHandled int
	provider, calls := newTestOpenAIProvider(t, func(w http.ResponseWriter, r *http.Request) {
		requestsHandled++
		if requestsHandled == 1 {
			w.Header().Set("Retry-After", "0")
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"error": "rate_limit_exceeded"}`))
			return
		}
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(openAIResponsesResponse{OutputText: `{"ok": true}`})
	})

	result, err := provider.Complete(context.Background(), "prompt")
	if err != nil {
		t.Fatalf("Complete returned error after rate-limit retry: %v", err)
	}
	if result.Text != `{"ok":true}` {
		t.Fatalf("Text = %q, want valid JSON after retry", result.Text)
	}
	if *calls != 2 {
		t.Fatalf("calls = %d, want 2", *calls)
	}
}

func TestOpenAIProviderCompleteDoesNotRetryAuthError(t *testing.T) {
	provider, calls := newTestOpenAIProvider(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error": {"code": "invalid_api_key", "message": "Bearer sk-realsecretvalue1234"}}`))
	})

	_, err := provider.Complete(context.Background(), "prompt")
	if err == nil {
		t.Fatal("expected an error for a 401 response")
	}
	if *calls != 1 {
		t.Fatalf("calls = %d, want 1 (auth errors must not retry)", *calls)
	}
	if strings.Contains(err.Error(), "sk-realsecretvalue1234") {
		t.Fatalf("returned error leaked the raw API key: %v", err)
	}
}

func intPtr(v int) *int { return &v }
