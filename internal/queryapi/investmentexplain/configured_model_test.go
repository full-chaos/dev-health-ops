package investmentexplain

import (
	"context"
	"net/http"
	"testing"

	"github.com/full-chaos/dev-health-go/llm"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize"
	"github.com/full-chaos/dev-health-ops/internal/llmbudget"
	"github.com/full-chaos/dev-health-ops/internal/llmorgsettings"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

// Ruling D6043: an org whose BYO settings store no model runs the one shared
// default (llmbudget.DefaultModel), and the admin budget status prices that
// same model. On the platform's setting (LLM_PROVIDER=openai,
// LLM_MODEL=gpt-5-nano) an OpenAI org runs nano; another provider's org is
// not given OpenAI's model.
func TestOrgWithNoStoredModelRunsTheSharedDefault(t *testing.T) {
	for _, name := range []string{"LLM_MODEL_OPENAI", "LLM_MODEL_ANTHROPIC", "LLM_MODEL_GEMINI", "GEMINI_MODEL", "LLM_MODEL_QWEN", "QWEN_MODEL", "LLM_MODEL_LOCAL", "LOCAL_LLM_MODEL"} {
		t.Setenv(name, "")
	}
	t.Setenv("LLM_PROVIDER", "openai")
	t.Setenv("LLM_MODEL", "gpt-5-nano")
	want := map[categorize.ProviderKind]string{
		categorize.ProviderKindOpenAI:    "gpt-5-nano",
		categorize.ProviderKindAnthropic: "claude-haiku-5-5",
		categorize.ProviderKindGemini:    "gemini-3.8-flash",
		categorize.ProviderKindQwen:      "qwen-plus",
		categorize.ProviderKindLocal:     "llama3.2",
	}
	for kind, model := range want {
		resolver := &fakeOrgResolver{matches: true, model: ""}
		got, found := ResolveModelNameForOrg(context.Background(), kind, "", "org-1", resolver)
		status := llmbudget.DefaultModel(string(kind), secrets.ProcessLookup)
		if !found || got != model || status != model {
			t.Errorf("%s: runtime %q (found %v), status default %q, want both %q", kind, got, found, status, model)
		}
	}

	// A generic LLM_MODEL meant for the platform's own provider never becomes
	// a BYO org's default for another provider, at runtime or in its status.
	// (The platform path, an org with no BYO settings, keeps the
	// generic-first ruling; the budget status describes only BYO orgs.)
	t.Setenv("LLM_PROVIDER", "")
	t.Setenv("LLM_MODEL", "generic-model-for-the-platform")
	resolver := &fakeOrgResolver{matches: true, model: ""}
	got, _ := ResolveModelNameForOrg(context.Background(), categorize.ProviderKindGemini, "", "org-1", resolver)
	status := llmbudget.DefaultModel("gemini", secrets.ProcessLookup)
	if got != "gemini-3.8-flash" || status != got {
		t.Errorf("BYO gemini with a platform-only LLM_MODEL: runtime %q, status %q, want both gemini-3.8-flash", got, status)
	}
}

// With no model configured, a hosted kind uses the shared package's default,
// on the wire and in the resolved model.
func TestExplainHostedKindsDefaultToThePackageDefault(t *testing.T) {
	srv := newModelCapturingServer(t)
	for kind, llmKind := range map[categorize.ProviderKind]llm.Kind{
		categorize.ProviderKindAnthropic: llm.KindAnthropic,
		categorize.ProviderKindGemini:    llm.KindGemini,
		categorize.ProviderKindQwen:      llm.KindQwen,
	} {
		t.Run(string(kind), func(t *testing.T) {
			srv.reset()
			for _, name := range []string{"LLM_MODEL", "LLM_MODEL_ANTHROPIC", "LLM_MODEL_GEMINI", "GEMINI_MODEL", "LLM_MODEL_QWEN", "QWEN_MODEL"} {
				t.Setenv(name, "")
			}
			t.Setenv("LLM_BASE_URL", srv.URL)
			t.Setenv("LLM_API_KEY", "dummy-test-key")
			_, _, model, _ := CompleteWorkUnitExplanationForOrg(context.Background(), string(kind), "", "org-1", nil, nil, "prompt text")
			if want := llm.DefaultModel(llmKind); model != want || srv.lastModel() != want {
				t.Fatalf("resolved %q, wire %q, want the package default %q", model, srv.lastModel(), want)
			}
		})
	}
}

// The platform is set to gpt-5-nano and the shared llm package defaults OpenAI
// to gpt-5-mini: both explain routes must send the configured nano, from the
// platform env and from an org's stored BYO model.
func TestExplainConfiguredNanoReachesTheWire(t *testing.T) {
	if llm.DefaultModel(llm.KindOpenAI) == "gpt-5-nano" {
		t.Fatal("the package default is nano: this test no longer tells a configured model from the default")
	}
	srv := newModelCapturingServer(t)
	routes := map[string]func(ctx context.Context, orgSettings llmorgsettings.Resolver) string{
		"investment_mix": func(ctx context.Context, orgSettings llmorgsettings.Resolver) string {
			_, _, model, _ := CompleteInvestmentMixExplanationForOrg(ctx, "openai", "", "org-1", orgSettings, nil, "prompt text")
			return model
		},
		"work_unit": func(ctx context.Context, orgSettings llmorgsettings.Resolver) string {
			_, _, model, _ := CompleteWorkUnitExplanationForOrg(ctx, "openai", "", "org-1", orgSettings, nil, "prompt text")
			return model
		},
	}
	for name, complete := range routes {
		t.Run(name+"/platform_LLM_MODEL", func(t *testing.T) {
			srv.reset()
			t.Setenv("LLM_MODEL_OPENAI", "")
			t.Setenv("LLM_MODEL", "gpt-5-nano")
			t.Setenv("LLM_BASE_URL", srv.URL)
			t.Setenv("LLM_API_KEY", "dummy-test-key")
			if model := complete(context.Background(), nil); model != "gpt-5-nano" || srv.lastModel() != "gpt-5-nano" {
				t.Fatalf("resolved %q, wire %q, want gpt-5-nano", model, srv.lastModel())
			}
		})
		t.Run(name+"/org_stored_model", func(t *testing.T) {
			srv.reset()
			t.Setenv("LLM_MODEL", "platform-model-must-not-be-used")
			previous := newProviderFromCredentials
			newProviderFromCredentials = func(kind categorize.ProviderKind, apiKey, baseURL, model string) (categorize.Provider, error) {
				// The real constructor dials an org base URL through the SSRF
				// guard, which refuses this loopback server.
				return categorize.NewSharedProvider(categorize.SharedProviderConfig{
					Kind: kind, APIKey: secrets.NewHidden(apiKey), BaseURL: baseURL, Model: model, HTTPClient: &http.Client{},
				})
			}
			t.Cleanup(func() { newProviderFromCredentials = previous })
			resolver := &fakeOrgResolver{
				usableProvider: "openai",
				credentials:    llmorgsettings.Credentials{APIKey: secrets.NewHidden("org-secret-key"), BaseURL: srv.URL},
				credentialsOK:  true,
				matches:        true,
				model:          "gpt-5-nano",
			}
			if model := complete(context.Background(), resolver); model != "gpt-5-nano" || srv.lastModel() != "gpt-5-nano" {
				t.Fatalf("resolved %q, wire %q, want gpt-5-nano", model, srv.lastModel())
			}
		})
	}
}
