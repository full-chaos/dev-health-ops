package categorize

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func newTestLMStudioProvider(t *testing.T, handler http.HandlerFunc) (*LMStudioProvider, *int) {
	t.Helper()
	var calls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.URL.Path != "/api/v0/chat/completions" {
			t.Fatalf("unexpected path %q, want /api/v0/chat/completions", r.URL.Path)
		}
		handler(w, r)
	}))
	t.Cleanup(server.Close)

	provider := NewLMStudioProvider(LMStudioProviderConfig{
		BaseURL: server.URL,
		Model:   "gemma-3-4b",
	})
	t.Cleanup(func() { provider.Close() })
	return provider, &calls
}

func decodeLMStudioRequest(t *testing.T, r *http.Request) lmStudioChatRequest {
	t.Helper()
	var body lmStudioChatRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		t.Fatalf("decode request body: %v", err)
	}
	return body
}

func TestLMStudioProviderDefaults(t *testing.T) {
	provider := NewLMStudioProvider(LMStudioProviderConfig{})
	if provider.cfg.BaseURL != defaultLMStudioBaseURL {
		t.Errorf("BaseURL = %q, want %q", provider.cfg.BaseURL, defaultLMStudioBaseURL)
	}
	if provider.cfg.Model != defaultLMStudioModel {
		t.Errorf("Model = %q, want %q (DEFAULT_MODEL_BY_PROVIDER[\"lmstudio\"])", provider.cfg.Model, defaultLMStudioModel)
	}
	if provider.cfg.APIKey != defaultLMStudioAPIKey {
		t.Errorf("APIKey = %q, want %q", provider.cfg.APIKey, defaultLMStudioAPIKey)
	}
}

func TestLMStudioProviderCompleteSuccess(t *testing.T) {
	provider, calls := newTestLMStudioProvider(t, func(w http.ResponseWriter, r *http.Request) {
		req := decodeLMStudioRequest(t, r)
		if req.Model != "gemma-3-4b" {
			t.Errorf("model = %q, want gemma-3-4b", req.Model)
		}
		if len(req.Messages) != 2 || req.Messages[0].Role != "system" || req.Messages[1].Role != "user" {
			t.Fatalf("unexpected messages: %+v", req.Messages)
		}
		if req.ResponseFormat == nil || req.ResponseFormat.Type != "json_schema" {
			t.Fatalf("expected a json_schema response_format on the first attempt, got %+v", req.ResponseFormat)
		}
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(lmStudioChatResponse{
			Choices: []lmStudioChatChoice{{Message: lmStudioChatMessage{Content: `{"a": 1}`}}},
			Usage:   &lmStudioChatUsage{PromptTokens: intPtr(20), CompletionTokens: intPtr(8)},
		})
	})

	result, err := provider.Complete(context.Background(), CategorizationRequest("prompt"))
	if err != nil {
		t.Fatalf("Complete returned error: %v", err)
	}
	if result.Text != `{"a":1}` {
		t.Fatalf("Text = %q, want compact JSON", result.Text)
	}
	if result.InputTokens == nil || *result.InputTokens != 20 {
		t.Errorf("InputTokens = %v, want 20", result.InputTokens)
	}
	if *calls != 1 {
		t.Fatalf("calls = %d, want 1", *calls)
	}
}

func TestLMStudioProviderNilSchemaPreservesPlainText(t *testing.T) {
	provider, _ := newTestLMStudioProvider(t, func(w http.ResponseWriter, r *http.Request) {
		req := decodeLMStudioRequest(t, r)
		if req.ResponseFormat != nil {
			t.Errorf("response_format = %+v, want nil (omitted) when JSONSchema is nil", req.ResponseFormat)
		}
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(lmStudioChatResponse{
			Choices: []lmStudioChatChoice{{Message: lmStudioChatMessage{Content: "plain explanation"}}},
		})
	})

	result, err := provider.Complete(context.Background(), CompletionRequest{
		Prompt:        "prompt",
		SystemMessage: "explain this",
	})
	if err != nil {
		t.Fatalf("Complete returned error: %v", err)
	}
	if result.Text != "plain explanation" {
		t.Fatalf("Text = %q, want unstructured response preserved", result.Text)
	}
}

func TestLMStudioProviderRetriesWithPlainTextOn400(t *testing.T) {
	var formats []string
	provider, calls := newTestLMStudioProvider(t, func(w http.ResponseWriter, r *http.Request) {
		req := decodeLMStudioRequest(t, r)
		if req.ResponseFormat != nil {
			formats = append(formats, req.ResponseFormat.Type)
		} else {
			formats = append(formats, "")
		}
		if len(formats) == 1 {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error": "response_format not supported"}`))
			return
		}
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(lmStudioChatResponse{
			Choices: []lmStudioChatChoice{{Message: lmStudioChatMessage{Content: `{"ok": true}`}}},
		})
	})

	result, err := provider.Complete(context.Background(), CategorizationRequest("prompt"))
	if err != nil {
		t.Fatalf("Complete returned error after 400 fallback: %v", err)
	}
	if result.Text != `{"ok":true}` {
		t.Fatalf("Text = %q, want valid JSON after fallback", result.Text)
	}
	if *calls != 2 {
		t.Fatalf("calls = %d, want 2 (one retry)", *calls)
	}
	if len(formats) != 2 || formats[0] != "json_schema" || formats[1] != "text" {
		t.Fatalf("response_format sequence = %v, want [json_schema text]", formats)
	}
}

func TestLMStudioProviderRedactsSecretInErrorBody(t *testing.T) {
	// Same structural-sanitizer parity LocalProvider/OpenAIProvider get:
	// classifyProviderError's Error() runs every raw provider error message
	// through sanitizeMessage before it ever reaches a caller/log.
	provider, _ := newTestLMStudioProvider(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error": {"code": "invalid_api_key", "message": "Authorization: Bearer sk-realsecretvalue1234"}}`))
	})

	_, err := provider.Complete(context.Background(), CategorizationRequest("prompt"))
	if err == nil {
		t.Fatal("expected an error for a 401 response")
	}
	if strings.Contains(err.Error(), "sk-realsecretvalue1234") {
		t.Fatalf("returned error leaked the raw API key: %v", err)
	}
}

func TestLMStudioProviderDoesNotRetryNonRetryableAfter400Exhausted(t *testing.T) {
	provider, calls := newTestLMStudioProvider(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error": "still invalid_request_error"}`))
	})

	_, err := provider.Complete(context.Background(), CategorizationRequest("prompt"))
	if err == nil {
		t.Fatal("expected an error once the 400 fallback retry is also rejected")
	}
	if *calls != lmStudioMaxRetries+1 {
		t.Fatalf("calls = %d, want %d", *calls, lmStudioMaxRetries+1)
	}
}

func TestLMStudioProviderExplicitZeroTemperatureIsSentVerbatim(t *testing.T) {
	var calls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		req := decodeLMStudioRequest(t, r)
		if req.Temperature != 0 {
			t.Errorf("Temperature = %v, want 0 (explicit zero must not be overwritten)", req.Temperature)
		}
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(lmStudioChatResponse{
			Choices: []lmStudioChatChoice{{Message: lmStudioChatMessage{Content: `{"ok": true}`}}},
		})
	}))
	t.Cleanup(server.Close)

	zero := 0.0
	provider := NewLMStudioProvider(LMStudioProviderConfig{
		BaseURL:     server.URL,
		Model:       "gemma-3-4b",
		Temperature: &zero,
	})
	t.Cleanup(func() { provider.Close() })

	if _, err := provider.Complete(context.Background(), CategorizationRequest("prompt")); err != nil {
		t.Fatalf("Complete returned error: %v", err)
	}
	if calls != 1 {
		t.Fatalf("calls = %d, want 1", calls)
	}
}
