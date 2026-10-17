package categorize

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

// The local (OpenAI-compatible) kind runs on the shared client. Each case is
// scripted as it was for the ops LocalProvider, whose answers were recorded at
// b430ad3a (the last tree that held it, where the shared client matched it
// live): what callers rely on (text, model, usage, failure class,
// deterministic, call count) and the request body (model, the two messages,
// max_completion_tokens 4096, temperature 0.3, and response_format json_schema
// strict with the request's own schema, or no response_format without one)
// must stay equal.
func TestSharedLocalKeepsTheOpsLocalProviderAnswers(t *testing.T) {
	chat := func(content string, usage string) string {
		encoded, _ := json.Marshal(content)
		body := `{"choices":[{"message":{"role":"assistant","content":` + string(encoded) + `},"finish_reason":"stop"}]`
		if usage != "" {
			body += `,"usage":` + usage
		}
		return body + `}`
	}
	tokens := func(n int) *int { return &n }
	for _, c := range []struct {
		name          string
		request       CompletionRequest
		steps         []sharedSyncStep
		ok            bool
		failureClass  string
		deterministic bool
		calls         int
		text          string
		input, output *int
	}{
		{"schema_json", CategorizationRequest("p"), []sharedSyncStep{{Status: 200, Body: chat(`{"a": 1}`, `{"prompt_tokens":7,"completion_tokens":3}`)}}, true, "", false, 1, `{"a":1}`, tokens(7), tokens(3)},
		{"schema_not_json", CategorizationRequest("p"), []sharedSyncStep{{Status: 200, Body: chat("not json", `{"prompt_tokens":7,"completion_tokens":3}`)}}, true, "", false, 1, "", tokens(7), tokens(3)},
		{"schema_no_usage", CategorizationRequest("p"), []sharedSyncStep{{Status: 200, Body: chat(`{"a":1}`, "")}}, true, "", false, 1, `{"a":1}`, nil, nil},
		{"no_schema_raw_text", WorkUnitExplanationRequest("p"), []sharedSyncStep{{Status: 200, Body: chat(`{"summary": "x"}`, `{"prompt_tokens":1,"completion_tokens":1}`)}}, true, "", false, 1, `{"summary": "x"}`, tokens(1), tokens(1)},
		{"server_error_then_ok", InvestmentMixExplanationRequest("p"), []sharedSyncStep{{Status: 503, Body: `{}`}, {Status: 200, Body: chat(`{"summary":"x"}`, "")}}, true, "", false, 2, `{"summary":"x"}`, nil, nil},
		{"server_error_twice", CategorizationRequest("p"), []sharedSyncStep{{Status: 500, Body: `{}`}}, false, "server_error", false, 2, "", nil, nil},
		{"unauthorized", CategorizationRequest("p"), []sharedSyncStep{{Status: 401, Body: `{"error":"unauthorized"}`}}, false, "missing_key", true, 1, "", nil, nil},
		{"rate_limited", CategorizationRequest("p"), []sharedSyncStep{{Status: 429, Header: map[string]string{"Retry-After": "0"}, Body: `{}`}}, false, "rate_limit", false, 2, "", nil, nil},
		{"model_not_found_text", CategorizationRequest("p"), []sharedSyncStep{{Status: 404, Body: `{"error":"model not found"}`}}, false, "model_not_found", true, 1, "", nil, nil},
		{"no_choices", CategorizationRequest("p"), []sharedSyncStep{{Status: 200, Body: `{"choices":[]}`}}, false, "llm_error", false, 1, "", nil, nil},
	} {
		t.Run(c.name, func(t *testing.T) {
			server, sent := scriptedSyncServer(t, c.steps)
			shared, err := NewSharedProvider(SharedProviderConfig{Kind: ProviderKindLocal, APIKey: secrets.NewHidden("test-key-value"), BaseURL: server.URL, Model: "gemma3:4b"})
			if err != nil {
				t.Fatal(err)
			}
			result, err := shared.Complete(context.Background(), c.request)

			if (err == nil) != c.ok || FailureClass(err) != c.failureClass || IsDeterministicFailure(err) != c.deterministic {
				t.Fatalf("err %v (%s, deterministic %v), ops LocalProvider ok=%v (%s, deterministic %v)",
					err, FailureClass(err), IsDeterministicFailure(err), c.ok, c.failureClass, c.deterministic)
			}
			if len(sent()) != c.calls {
				t.Fatalf("calls %d, ops LocalProvider %d", len(sent()), c.calls)
			}
			if c.ok && (result.Text != c.text || result.Model != "gemma3:4b" ||
				!samePointer(result.InputTokens, c.input) || !samePointer(result.OutputTokens, c.output)) {
				t.Fatalf("result %+v, ops LocalProvider text %q model gemma3:4b tokens %v/%v", result, c.text, c.input, c.output)
			}
			if result.CachedInputTokens != nil {
				t.Fatalf("cached tokens %v: a Chat Completions server reports none", *result.CachedInputTokens)
			}
			want := map[string]any{
				"model":                 "gemma3:4b",
				"max_completion_tokens": float64(4096),
				"temperature":           0.3,
				"messages": []any{
					map[string]any{"role": "system", "content": c.request.SystemMessage},
					map[string]any{"role": "user", "content": c.request.Prompt},
				},
			}
			if c.request.JSONSchema != nil {
				want["response_format"] = map[string]any{"type": "json_schema", "json_schema": map[string]any{
					"name": c.request.ResponseFormatName, "schema": jsonCopy(t, c.request.JSONSchema), "strict": true,
				}}
			}
			for index, body := range sent() {
				var got map[string]any
				if err := json.Unmarshal([]byte(body), &got); err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("request %d differs from the ops LocalProvider's:\n shared %s", index, body)
				}
			}
		})
	}
}

// A schema-free request (the work-unit explanation) to a local
// OpenAI-compatible server that accepts no response_format (an LM Studio or
// vLLM build without json_object) is served, as the ops LocalProvider served
// it: the request names no format the server cannot honour.
func TestSharedLocalSchemaFreeRequestNamesNoFormat(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		if _, named := body["response_format"]; named {
			http.Error(w, `{"error":"'response_format.type' must be 'json_schema'"}`, http.StatusBadRequest)
			return
		}
		_, _ = w.Write([]byte(`{"choices":[{"message":{"role":"assistant","content":"{\"summary\":\"x\"}"},"finish_reason":"stop"}]}`))
	}))
	t.Cleanup(server.Close)
	for _, kind := range []ProviderKind{ProviderKindLocal, ProviderKindOllama} {
		provider, err := newProviderFromCredentialsWithTransport(kind, "", server.URL, "gemma3:4b", http.DefaultTransport)
		if err != nil {
			t.Fatal(err)
		}
		result, err := provider.Complete(context.Background(), WorkUnitExplanationRequest("p"))
		if err != nil || result.Text != `{"summary":"x"}` {
			t.Fatalf("%s: result %q err %v, want the schema-free answer served", kind, result.Text, err)
		}
	}
}
