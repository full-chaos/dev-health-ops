package categorize

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/units"
	"github.com/full-chaos/dev-health-ops/internal/llmbudget"
)

// testdata/workersai-qwen-categorization-response.json is a real answer of
// Cloudflare Workers AI (@cf/qwen/qwen3-30b-a3b-fp8, 2026-10-16) to the
// categorization request this package sends for workersAIBundle, made through
// the Cloudflare MCP plugin (account route /ai/run, which answers the same
// chat.completion object the OpenAI-compatible /ai/v1/chat/completions route
// answers, inside the API envelope; the plugin returned it parsed, so the
// file holds that object re-serialized, not the raw bytes). No credential is
// in the file.
const workersAIModel = "@cf/qwen/qwen3-30b-a3b-fp8"

func workersAIBundle(t *testing.T) units.TextBundle {
	t.Helper()
	bundle, err := units.BuildTextBundle(units.BuildTextBundleInput{
		WorkUnitID: "live-smoke",
		IssueIDs:   []string{"ENG-1"},
		WorkItemMap: map[string]map[string]any{
			"ENG-1": {"title": "Export job times out for large workspaces", "type": "Bug",
				"description": "The nightly export worker times out for workspaces over 10k items and needs a streamed writer with retries."},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return bundle
}

// A Qwen model on Workers AI is reached as the qwen kind with the account's
// OpenAI-compatible base URL and an @cf model id, from the platform env and
// from an org's BYO settings alike; the recorded answer categorizes cleanly
// and a budgeted org is billed at the Workers AI list price.
func TestQwenOnWorkersAIThroughTheSettingsPaths(t *testing.T) {
	recorded, err := os.ReadFile("testdata/workersai-qwen-categorization-response.json")
	if err != nil {
		t.Fatal(err)
	}
	var (
		mu    sync.Mutex
		paths []string
		auths []string
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.ReadAll(r.Body)
		mu.Lock()
		paths = append(paths, r.URL.Path)
		auths = append(auths, r.Header.Get("Authorization"))
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(recorded)
	}))
	t.Cleanup(server.Close)
	accountBase := server.URL + "/client/v4/accounts/acct/ai/v1"

	build := map[string]func(t *testing.T) Provider{
		"platform env": func(t *testing.T) Provider {
			clearProviderEnv(t)
			t.Setenv("QWEN_BASE_URL", accountBase)
			t.Setenv("QWEN_MODEL", workersAIModel)
			t.Setenv("QWEN_API_KEY", "workers-ai-token-value")
			provider, err := NewProviderFromEnvWithModel(ProviderKindQwen, ResolveModelName(ProviderKindQwen, ""))
			if err != nil {
				t.Fatal(err)
			}
			return provider
		},
		"org BYO": func(t *testing.T) Provider {
			provider, err := newProviderFromCredentialsWithTransport(ProviderKindQwen, "workers-ai-token-value", accountBase, workersAIModel, http.DefaultTransport)
			if err != nil {
				t.Fatal(err)
			}
			store := &recordingStore{}
			t.Cleanup(func() {
				if len(store.settles) != 1 || store.settles[0].Cost == nil || *store.settles[0].Cost != 371 {
					t.Errorf("settles %+v, want one at 371 micro-USD (684 x 0.051 + 1001 x 0.335 per MTok)", store.settles)
				}
			})
			return WithBudget(provider, llmbudget.Guard{Store: store, OrgID: "org", Provider: "qwen", Model: provider.Model(),
				BaseURL: "https://api.cloudflare.com/client/v4/accounts/acct/ai/v1"})
		},
	}
	for name, newProvider := range build {
		t.Run(name, func(t *testing.T) {
			mu.Lock()
			paths, auths = nil, nil
			mu.Unlock()
			provider := newProvider(t)
			if provider.Model() != workersAIModel {
				t.Fatalf("Model() = %q, want %s", provider.Model(), workersAIModel)
			}
			outcome, err := CategorizeTextBundle(context.Background(), workersAIBundle(t), CategorizeOptions{Provider: provider, ProviderName: "qwen", Model: provider.Model()})
			if err != nil {
				t.Fatal(err)
			}
			if outcome.Status != StatusOK || outcome.InputTokens != 684 || outcome.OutputTokens != 1001 || outcome.LLMModel != workersAIModel {
				t.Fatalf("outcome status %s tokens %d/%d model %s, want ok 684/1001 %s", outcome.Status, outcome.InputTokens, outcome.OutputTokens, outcome.LLMModel, workersAIModel)
			}
			mu.Lock()
			defer mu.Unlock()
			if len(paths) != 1 || paths[0] != "/client/v4/accounts/acct/ai/v1/chat/completions" || auths[0] != "Bearer workers-ai-token-value" {
				t.Fatalf("requests %v (auth set: %v), want one POST to the account's /ai/v1/chat/completions with the bearer token", paths, len(auths) == 1 && auths[0] != "")
			}
		})
	}
}
