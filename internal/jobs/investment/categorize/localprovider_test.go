package categorize

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func newTestLocalProvider(t *testing.T, handler http.HandlerFunc) (*LocalProvider, *int) {
	t.Helper()
	var calls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.URL.Path != "/chat/completions" {
			t.Fatalf("unexpected path %q", r.URL.Path)
		}
		handler(w, r)
	}))
	t.Cleanup(server.Close)

	provider := NewLocalProvider(LocalProviderConfig{
		BaseURL: server.URL,
		Model:   "gemma3",
	})
	t.Cleanup(func() { provider.Close() })
	return provider, &calls
}

func decodeLocalRequest(t *testing.T, r *http.Request) localChatRequest {
	t.Helper()
	var body localChatRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		t.Fatalf("decode request body: %v", err)
	}
	return body
}

func TestLocalProviderDefaults(t *testing.T) {
	provider := NewLocalProvider(LocalProviderConfig{})
	if provider.cfg.BaseURL != defaultLocalBaseURL {
		t.Errorf("BaseURL = %q, want %q", provider.cfg.BaseURL, defaultLocalBaseURL)
	}
	if provider.cfg.Model != defaultLocalModel {
		t.Errorf("Model = %q, want %q", provider.cfg.Model, defaultLocalModel)
	}
	if provider.cfg.APIKey != defaultLocalAPIKey {
		t.Errorf("APIKey = %q, want %q", provider.cfg.APIKey, defaultLocalAPIKey)
	}
}

func TestLocalProviderCompleteSuccess(t *testing.T) {
	provider, calls := newTestLocalProvider(t, func(w http.ResponseWriter, r *http.Request) {
		req := decodeLocalRequest(t, r)
		if req.Model != "gemma3" {
			t.Errorf("model = %q, want gemma3", req.Model)
		}
		if len(req.Messages) != 2 || req.Messages[0].Role != "system" || req.Messages[1].Role != "user" {
			t.Fatalf("unexpected messages: %+v", req.Messages)
		}
		if req.ResponseFormat == nil || req.ResponseFormat.Type != "json_schema" {
			t.Fatalf("expected a json_schema response_format on the first attempt, got %+v", req.ResponseFormat)
		}
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(localChatResponse{
			Choices: []localChatChoice{{Message: localChatMessage{Content: `{"a": 1}`}}},
			Usage:   &localChatUsage{PromptTokens: intPtr(20), CompletionTokens: intPtr(8)},
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

func TestLocalProviderExplicitZeroTemperatureIsSentVerbatim(t *testing.T) {
	// codex round 1 (#2178) P2: Temperature was a plain float64, so
	// LocalProviderConfig{Temperature: 0} was indistinguishable from an
	// unset field and got silently overwritten with the 0.3 default. 0.0
	// is a valid, meaningfully different (fully deterministic) Chat
	// Completions temperature.
	var calls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		req := decodeLocalRequest(t, r)
		if req.Temperature != 0 {
			t.Errorf("Temperature = %v, want 0 (explicit zero must not be overwritten)", req.Temperature)
		}
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(localChatResponse{
			Choices: []localChatChoice{{Message: localChatMessage{Content: `{"ok": true}`}}},
		})
	}))
	t.Cleanup(server.Close)

	zero := 0.0
	provider := NewLocalProvider(LocalProviderConfig{
		BaseURL:     server.URL,
		Model:       "gemma3",
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

func TestLocalProviderRetriesWithPlainTextOn400(t *testing.T) {
	var formats []string
	provider, calls := newTestLocalProvider(t, func(w http.ResponseWriter, r *http.Request) {
		req := decodeLocalRequest(t, r)
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
		_ = json.NewEncoder(w).Encode(localChatResponse{
			Choices: []localChatChoice{{Message: localChatMessage{Content: `{"ok": true}`}}},
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

func TestLocalProviderDoesNotRetryNonRetryableAfter400Exhausted(t *testing.T) {
	provider, calls := newTestLocalProvider(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error": "still invalid_request_error"}`))
	})

	_, err := provider.Complete(context.Background(), CategorizationRequest("prompt"))
	if err == nil {
		t.Fatal("expected an error once the 400 fallback retry is also rejected")
	}
	if *calls != localMaxRetries+1 {
		t.Fatalf("calls = %d, want %d", *calls, localMaxRetries+1)
	}
}

type localPathCapturingRoundTripper struct{ path string }

func (rt *localPathCapturingRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	rt.path = request.URL.Path
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     make(http.Header),
		Body: io.NopCloser(strings.NewReader(
			`{"choices":[{"message":{"role":"assistant","content":"{\"ok\":true}"}}]}`,
		)),
	}, nil
}

func TestLocalProviderTrimsTrailingSlashFromBaseURL(t *testing.T) {
	// codex round 3 (#2178, bigboy) P2: the same trailing-slash defect as
	// OpenAIProvider, for the Chat Completions route.
	transport := &localPathCapturingRoundTripper{}
	provider := NewLocalProvider(LocalProviderConfig{
		BaseURL:    "http://127.0.0.1:1234/v1/",
		Model:      "gemma3",
		HTTPClient: &http.Client{Transport: transport},
	})
	t.Cleanup(func() { provider.Close() })

	if _, err := provider.Complete(context.Background(), CategorizationRequest("prompt")); err != nil {
		t.Fatal(err)
	}
	if transport.path != "/v1/chat/completions" {
		t.Errorf("request path = %q, want \"/v1/chat/completions\" (no double slash)", transport.path)
	}
}
