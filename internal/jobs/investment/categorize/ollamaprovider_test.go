package categorize

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func newTestOllamaProvider(t *testing.T, handler http.HandlerFunc) (*OllamaProvider, *int) {
	t.Helper()
	var calls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.URL.Path != "/api/chat" {
			t.Fatalf("unexpected path %q, want /api/chat", r.URL.Path)
		}
		handler(w, r)
	}))
	t.Cleanup(server.Close)

	provider := NewOllamaProvider(OllamaProviderConfig{
		BaseURL: server.URL,
		Model:   "gemma3",
	})
	t.Cleanup(func() { provider.Close() })
	return provider, &calls
}

func decodeOllamaRequest(t *testing.T, r *http.Request) ollamaChatRequest {
	t.Helper()
	var body ollamaChatRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		t.Fatalf("decode request body: %v", err)
	}
	return body
}

func TestOllamaProviderDefaults(t *testing.T) {
	provider := NewOllamaProvider(OllamaProviderConfig{})
	if provider.cfg.BaseURL != defaultOllamaBaseURL {
		t.Errorf("BaseURL = %q, want %q", provider.cfg.BaseURL, defaultOllamaBaseURL)
	}
	if provider.cfg.Model != defaultOllamaModel {
		t.Errorf("Model = %q, want %q", provider.cfg.Model, defaultOllamaModel)
	}
}

func TestOllamaProviderSendsNativeChatShape(t *testing.T) {
	provider, calls := newTestOllamaProvider(t, func(w http.ResponseWriter, r *http.Request) {
		req := decodeOllamaRequest(t, r)
		if req.Model != "gemma3" {
			t.Errorf("model = %q, want gemma3", req.Model)
		}
		if req.Stream {
			t.Error("Stream = true, want false (non-streaming completion)")
		}
		if len(req.Messages) != 2 || req.Messages[0].Role != "system" || req.Messages[1].Role != "user" {
			t.Fatalf("unexpected messages: %+v", req.Messages)
		}
		if len(req.Format) == 0 {
			t.Fatal("expected a non-empty Format (JSON schema) on the first attempt")
		}
		var schema map[string]any
		if err := json.Unmarshal(req.Format, &schema); err != nil {
			t.Fatalf("Format is not valid JSON: %v", err)
		}
		promptEval, evalCount := 20, 8
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(ollamaChatResponse{
			Message:         ollamaChatMessage{Role: "assistant", Content: `{"a": 1}`},
			Done:            true,
			PromptEvalCount: &promptEval,
			EvalCount:       &evalCount,
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
	if result.OutputTokens == nil || *result.OutputTokens != 8 {
		t.Errorf("OutputTokens = %v, want 8", result.OutputTokens)
	}
	if *calls != 1 {
		t.Fatalf("calls = %d, want 1", *calls)
	}
}

func TestOllamaProviderNilSchemaOmitsFormatAndPreservesPlainText(t *testing.T) {
	provider, _ := newTestOllamaProvider(t, func(w http.ResponseWriter, r *http.Request) {
		req := decodeOllamaRequest(t, r)
		if len(req.Format) != 0 {
			t.Errorf("Format = %s, want omitted when JSONSchema is nil", req.Format)
		}
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(ollamaChatResponse{
			Message: ollamaChatMessage{Role: "assistant", Content: "plain explanation"},
			Done:    true,
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

func TestOllamaProviderRetriesWithoutFormatOn400(t *testing.T) {
	var formats []bool
	provider, calls := newTestOllamaProvider(t, func(w http.ResponseWriter, r *http.Request) {
		req := decodeOllamaRequest(t, r)
		formats = append(formats, len(req.Format) > 0)
		if len(formats) == 1 {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error": "model does not support format"}`))
			return
		}
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(ollamaChatResponse{
			Message: ollamaChatMessage{Role: "assistant", Content: `{"ok": true}`},
			Done:    true,
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
	if len(formats) != 2 || formats[0] != true || formats[1] != false {
		t.Fatalf("format-present sequence = %v, want [true false]", formats)
	}
}

func TestOllamaProviderTreats200WithEmbeddedErrorAsFailure(t *testing.T) {
	// Some Ollama versions report a mid-generation failure as an "error"
	// field on an otherwise 200 response rather than a non-2xx status.
	provider, _ := newTestOllamaProvider(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(ollamaChatResponse{Error: "model runner exited unexpectedly"})
	})

	if _, err := provider.Complete(context.Background(), CategorizationRequest("prompt")); err == nil {
		t.Fatal("expected an error when the response body carries a non-empty error field")
	}
}

func TestOllamaProviderRejectsIncompleteResponse(t *testing.T) {
	// codex round 1 (#2189) P2: a 200 response with no `error` field but
	// `done:false` (Ollama's own signal that generation did not actually
	// finish -- see https://docs.ollama.com/api/chat) was silently
	// returned as a successful, complete CompletionResult, letting a
	// truncated answer flow into downstream processing undetected.
	provider, _ := newTestOllamaProvider(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(ollamaChatResponse{
			Message: ollamaChatMessage{Role: "assistant", Content: "partial result"},
			Done:    false,
		})
	})

	result, err := provider.Complete(context.Background(), CategorizationRequest("prompt"))
	if err == nil {
		t.Fatalf("Complete accepted done=false as success, text = %q", result.Text)
	}
	typed, ok := err.(*llmError)
	if !ok {
		t.Fatalf("error type = %T, want *llmError", err)
	}
	if typed.kind != llmErrorOutput {
		t.Errorf("kind = %v, want llmErrorOutput", typed.kind)
	}
}

func TestOllamaProviderExplicitZeroTemperatureIsSentVerbatim(t *testing.T) {
	var calls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		req := decodeOllamaRequest(t, r)
		if req.Options == nil || req.Options.Temperature != 0 {
			t.Errorf("Options.Temperature = %v, want 0 (explicit zero must not be overwritten)", req.Options)
		}
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(ollamaChatResponse{
			Message: ollamaChatMessage{Role: "assistant", Content: `{"ok": true}`},
			Done:    true,
		})
	}))
	t.Cleanup(server.Close)

	zero := 0.0
	provider := NewOllamaProvider(OllamaProviderConfig{
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

func TestOllamaProviderRedactsSecretInErrorBody(t *testing.T) {
	// Same structural-sanitizer parity LocalProvider/OpenAIProvider get:
	// classifyProviderError's Error() runs every raw provider error message
	// through sanitizeMessage before it ever reaches a caller/log, and this
	// provider's own error paths (both the non-2xx httpStatusError and the
	// 200-with-embedded-Error field one) must not be exempt from that.
	//
	// codex round 1 (#2189) P3: this fixture used HTTP 401, which
	// classifyProviderError maps to the FIXED message "Invalid or missing
	// LLM API key." regardless of body content -- sanitizeMessage's own
	// regex-based redaction of the RAW body never actually ran, so this
	// test passed even with sanitizeMessage reduced to a no-op (proven:
	// mutating sanitizeMessage to `return message` left this test green
	// while TestOllamaProviderRedactsSecretInEmbedded200Error correctly
	// went red). 403 has no special classifyProviderError case, so the
	// raw (sanitized) body text is what actually reaches the caller --
	// this is the status code that genuinely exercises the sanitizer.
	provider, _ := newTestOllamaProvider(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"error": "Authorization: Bearer sk-realsecretvalue1234"}`))
	})

	_, err := provider.Complete(context.Background(), CategorizationRequest("prompt"))
	if err == nil {
		t.Fatal("expected an error for a 403 response")
	}
	if strings.Contains(err.Error(), "sk-realsecretvalue1234") {
		t.Fatalf("returned error leaked the raw API key: %v", err)
	}
}

func TestOllamaProviderRedactsURICredentialInErrorBody(t *testing.T) {
	// codex round 1 (#2189) P1: a DSN's userinfo credential (Postgres/
	// Redis/ClickHouse/an internal gateway URL) quoted verbatim in an
	// upstream diagnostic leaked through unredacted -- neither the
	// label-based nor the length-24-opaque-run sanitizer pattern caught
	// it. Percent-encoded/special characters in the password are the
	// round's own repro shape.
	const credential = "billing_user:pa%24s%2Fwd!"
	provider, _ := newTestOllamaProvider(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"error": "upstream diagnostic: postgres://` + credential + `@db.internal"}`))
	})

	_, err := provider.Complete(context.Background(), CategorizationRequest("prompt"))
	if err == nil {
		t.Fatal("expected an error for a 403 response")
	}
	if strings.Contains(err.Error(), credential) {
		t.Fatalf("returned error leaked the raw database-URL credential: %v", err)
	}
}

func TestOllamaProviderRedactsSecretInEmbedded200Error(t *testing.T) {
	provider, _ := newTestOllamaProvider(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(ollamaChatResponse{
			Error: "upstream rejected api_key=sk-realsecretvalue1234",
		})
	})

	_, err := provider.Complete(context.Background(), CategorizationRequest("prompt"))
	if err == nil {
		t.Fatal("expected an error when the response body carries a non-empty error field")
	}
	if strings.Contains(err.Error(), "sk-realsecretvalue1234") {
		t.Fatalf("returned error leaked the raw API key: %v", err)
	}
}

func TestOllamaProviderDoesNotRetryNonRetryableAfter400Exhausted(t *testing.T) {
	provider, calls := newTestOllamaProvider(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error": "still invalid_request_error"}`))
	})

	_, err := provider.Complete(context.Background(), CategorizationRequest("prompt"))
	if err == nil {
		t.Fatal("expected an error once the 400 fallback retry is also rejected")
	}
	if *calls != ollamaMaxRetries+1 {
		t.Fatalf("calls = %d, want %d", *calls, ollamaMaxRetries+1)
	}
}
