package categorize

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"testing"

	"github.com/full-chaos/dev-health-go/llm"

	"github.com/full-chaos/dev-health-ops/internal/llmbudget"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

// Ruling D6044: the structured-output format is chosen before the call, and a
// provider 400 on it is a final invalid_request error. A silent retry as plain
// text would bill the input twice and hand categorization an answer the
// contract never asked for. One call, one reservation settled as failed.
func TestSharedAnthropicFormat400IsFinal(t *testing.T) {
	server, sent := scriptedSyncServer(t, []sharedSyncStep{{
		Status: http.StatusBadRequest,
		Body:   `{"type":"error","error":{"type":"invalid_request_error","message":"output_config.format: schema is not supported"}}`,
	}})
	shared, err := NewSharedProvider(SharedProviderConfig{Kind: ProviderKindAnthropic, APIKey: secrets.NewHidden("test-key-value"), BaseURL: server.URL, Model: "claude-haiku-5-5"})
	if err != nil {
		t.Fatal(err)
	}
	store := &recordingStore{}
	guarded := WithBudget(shared, llmbudget.Guard{Store: store, OrgID: "org", Provider: "anthropic", Model: "claude-haiku-5-5"})

	_, err = guarded.Complete(context.Background(), CategorizationRequest("p"))

	class, isProvider := llm.ClassOf(err)
	if !isProvider || class != llm.ClassInvalidRequest || llm.IsBudgetRefusal(err) {
		t.Fatalf("err %v (class %q), want a final invalid_request provider error", err, class)
	}
	var typed *llm.Error
	if !errors.As(err, &typed) || typed.Retryable() {
		t.Fatalf("err %v is retryable, want final", err)
	}
	bodies := sent()
	if len(bodies) != 1 {
		t.Fatalf("calls = %d, want 1: a format refusal is never retried in another format", len(bodies))
	}
	var body map[string]any
	if err := json.Unmarshal([]byte(bodies[0]), &body); err != nil {
		t.Fatal(err)
	}
	if body["output_config"] == nil && body["tools"] == nil {
		t.Fatalf("the categorization request carried no structured format: %s", bodies[0])
	}
	if len(store.admits) != 1 || len(store.settles) != 1 || store.settles[0].Outcome != llmbudget.OutcomeFailed {
		t.Fatalf("admits %d settles %+v, want one reservation settled as failed", len(store.admits), store.settles)
	}
}

// D6044 for the Chat Completions kinds (local, gemini, qwen): a 400 on the
// structured-output format is final, with one call and one budget attempt.
// Named change for local: the ops LocalProvider retried such a request as
// plain text (two calls, an answer the categorization contract never asked
// for).
func TestSharedChatCompletionsFormat400IsFinal(t *testing.T) {
	refused := []sharedSyncStep{
		{Status: http.StatusBadRequest, Body: `{"error":{"message":"response_format json_schema is not supported","type":"invalid_request_error"}}`},
		{Status: http.StatusOK, Body: `{"choices":[{"message":{"role":"assistant","content":"{\"a\":1}"},"finish_reason":"stop"}]}`},
	}
	for _, c := range []struct {
		kind  ProviderKind
		model string
	}{
		{ProviderKindLocal, "gemma3:4b"},
		{ProviderKindGemini, "gemini-3.8-flash"},
		{ProviderKindQwen, "qwen-plus"},
	} {
		t.Run(string(c.kind), func(t *testing.T) {
			server, sent := scriptedSyncServer(t, refused)
			shared, err := NewSharedProvider(SharedProviderConfig{Kind: c.kind, APIKey: secrets.NewHidden("test-key-value"), BaseURL: server.URL, Model: c.model})
			if err != nil {
				t.Fatal(err)
			}
			store := &recordingStore{}
			guarded := WithBudget(shared, llmbudget.Guard{Store: store, OrgID: "org", Provider: string(c.kind), Model: c.model})

			_, err = guarded.Complete(context.Background(), CategorizationRequest("p"))

			if class, ok := llm.ClassOf(err); !ok || class != llm.ClassInvalidRequest {
				t.Fatalf("err %v (class %q), want a final invalid_request provider error", err, class)
			}
			bodies := sent()
			if len(bodies) != 1 {
				t.Fatalf("calls = %d, want 1: a format refusal is never retried in another format", len(bodies))
			}
			var body map[string]any
			if err := json.Unmarshal([]byte(bodies[0]), &body); err != nil {
				t.Fatal(err)
			}
			if format, _ := body["response_format"].(map[string]any); format["type"] != "json_schema" {
				t.Fatalf("response_format = %v, want the json_schema the categorization asks for", body["response_format"])
			}
			if len(store.admits) != 1 || len(store.settles) != 1 || store.settles[0].Outcome != llmbudget.OutcomeFailed {
				t.Fatalf("admits %d settles %+v, want one budget attempt settled as failed", len(store.admits), store.settles)
			}
		})
	}
}
