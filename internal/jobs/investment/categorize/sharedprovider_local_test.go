package categorize

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

// The local (OpenAI-compatible) kind now runs on the shared client. Each case
// runs through the ops LocalProvider and the shared client against the same
// scripted server: what callers rely on (text, model, usage, failure class,
// deterministic, call count) must be equal, and so must the request bodies
// (since dev-health-go v0.24.4 a schema-free request to a local server names
// no response_format, as the ops port did).
func TestSharedLocalMatchesTheOpsLocalProviderForCallers(t *testing.T) {
	chat := func(content string, usage string) string {
		encoded, _ := json.Marshal(content)
		body := `{"choices":[{"message":{"role":"assistant","content":` + string(encoded) + `},"finish_reason":"stop"}]`
		if usage != "" {
			body += `,"usage":` + usage
		}
		return body + `}`
	}
	for _, c := range []struct {
		name    string
		request CompletionRequest
		steps   []sharedSyncStep
	}{
		{"schema_json", CategorizationRequest("p"), []sharedSyncStep{{Status: 200, Body: chat(`{"a": 1}`, `{"prompt_tokens":7,"completion_tokens":3}`)}}},
		{"schema_not_json", CategorizationRequest("p"), []sharedSyncStep{{Status: 200, Body: chat("not json", `{"prompt_tokens":7,"completion_tokens":3}`)}}},
		{"schema_no_usage", CategorizationRequest("p"), []sharedSyncStep{{Status: 200, Body: chat(`{"a":1}`, "")}}},
		{"no_schema_raw_text", WorkUnitExplanationRequest("p"), []sharedSyncStep{{Status: 200, Body: chat(`{"summary": "x"}`, `{"prompt_tokens":1,"completion_tokens":1}`)}}},
		{"server_error_then_ok", InvestmentMixExplanationRequest("p"), []sharedSyncStep{{Status: 503, Body: `{}`}, {Status: 200, Body: chat(`{"summary":"x"}`, "")}}},
		{"server_error_twice", CategorizationRequest("p"), []sharedSyncStep{{Status: 500, Body: `{}`}}},
		{"unauthorized", CategorizationRequest("p"), []sharedSyncStep{{Status: 401, Body: `{"error":"unauthorized"}`}}},
		{"rate_limited", CategorizationRequest("p"), []sharedSyncStep{{Status: 429, Header: map[string]string{"Retry-After": "0"}, Body: `{}`}}},
		{"model_not_found_text", CategorizationRequest("p"), []sharedSyncStep{{Status: 404, Body: `{"error":"model not found"}`}}},
		{"no_choices", CategorizationRequest("p"), []sharedSyncStep{{Status: 200, Body: `{"choices":[]}`}}},
	} {
		t.Run(c.name, func(t *testing.T) {
			oldServer, oldSent := scriptedSyncServer(t, c.steps)
			old := NewLocalProvider(LocalProviderConfig{APIKey: secrets.NewHidden("test-key-value"), BaseURL: oldServer.URL, Model: "gemma3:4b"})
			oldResult, oldErr := old.Complete(context.Background(), c.request)

			server, sent := scriptedSyncServer(t, c.steps)
			shared, err := NewSharedProvider(SharedProviderConfig{Kind: ProviderKindLocal, APIKey: secrets.NewHidden("test-key-value"), BaseURL: server.URL, Model: "gemma3:4b"})
			if err != nil {
				t.Fatal(err)
			}
			result, err := shared.Complete(context.Background(), c.request)

			if (err == nil) != (oldErr == nil) || FailureClass(err) != FailureClass(oldErr) || IsDeterministicFailure(err) != IsDeterministicFailure(oldErr) {
				t.Fatalf("err %v (%s, deterministic %v), ops LocalProvider %v (%s, deterministic %v)",
					err, FailureClass(err), IsDeterministicFailure(err), oldErr, FailureClass(oldErr), IsDeterministicFailure(oldErr))
			}
			if len(sent()) != len(oldSent()) {
				t.Fatalf("calls %d, ops LocalProvider %d", len(sent()), len(oldSent()))
			}
			if err == nil && (result.Text != oldResult.Text || result.Model != oldResult.Model ||
				!samePointer(result.InputTokens, oldResult.InputTokens) || !samePointer(result.OutputTokens, oldResult.OutputTokens)) {
				t.Fatalf("result %+v, ops LocalProvider %+v", result, oldResult)
			}
			if result.CachedInputTokens != nil {
				t.Fatalf("cached tokens %v: a Chat Completions server reports none", *result.CachedInputTokens)
			}
			for index, body := range sent() {
				var mine, theirs map[string]any
				_ = json.Unmarshal([]byte(body), &mine)
				_ = json.Unmarshal([]byte(oldSent()[index]), &theirs)
				mineRaw, _ := json.Marshal(mine)
				theirsRaw, _ := json.Marshal(theirs)
				if string(mineRaw) != string(theirsRaw) {
					t.Fatalf("request %d differs:\n shared %s\n ops    %s", index, mineRaw, theirsRaw)
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
