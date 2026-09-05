package categorize

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestNewProviderFromCredentialsMock(t *testing.T) {
	provider, err := NewProviderFromCredentials(ProviderKindMock, "", "", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := provider.(MockProvider); !ok {
		t.Fatalf("provider = %T, want MockProvider", provider)
	}
}

func TestNewProviderFromCredentialsOpenAIRequiresAPIKey(t *testing.T) {
	if _, err := NewProviderFromCredentials(ProviderKindOpenAI, "", "https://example.test/v1", "gpt-5-mini"); err == nil {
		t.Fatal("expected an error constructing openai with no api_key")
	}
}

func TestNewProviderFromCredentialsOpenAIUsesExplicitValues(t *testing.T) {
	provider, err := NewProviderFromCredentials(
		ProviderKindOpenAI, "sk-org-secret", "https://org-gateway.example.com/v1", "gpt-5-mini")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	openAIProvider, ok := provider.(*OpenAIProvider)
	if !ok {
		t.Fatalf("provider = %T, want *OpenAIProvider", provider)
	}
	if openAIProvider.cfg.APIKey != "sk-org-secret" {
		t.Error("APIKey not set from the explicit apiKey argument")
	}
	if openAIProvider.cfg.BaseURL != "https://org-gateway.example.com/v1" {
		t.Errorf("BaseURL = %q, want the explicit baseURL argument", openAIProvider.cfg.BaseURL)
	}
	if openAIProvider.cfg.Model != "gpt-5-mini" {
		t.Errorf("Model = %q, want the explicit model argument", openAIProvider.cfg.Model)
	}
}

// TestNewProviderFromCredentialsIgnoresAmbientEnv proves the "no generic-
// LLM_*-env layering" half of this function's own doc comment: an
// ambient LLM_API_KEY/LLM_BASE_URL/LLM_MODEL must NEVER leak into an
// org-BYO construction -- doing so would silently blend a platform
// credential into what must be an all-org construction (CHAOS-2550).
func TestNewProviderFromCredentialsIgnoresAmbientEnv(t *testing.T) {
	t.Setenv("LLM_API_KEY", "sk-platform-must-not-be-used")
	t.Setenv("LLM_BASE_URL", "https://platform.invalid/v1")
	t.Setenv("LLM_MODEL", "platform-model-must-not-be-used")
	t.Setenv("OPENAI_API_KEY", "sk-platform-openai-must-not-be-used")

	provider, err := NewProviderFromCredentials(
		ProviderKindOpenAI, "sk-org-secret", "https://org-gateway.example.com/v1", "org-model")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	openAIProvider := provider.(*OpenAIProvider)
	if openAIProvider.cfg.APIKey != "sk-org-secret" {
		t.Fatalf("APIKey = %q, an ambient env var leaked into an org-BYO construction", openAIProvider.cfg.APIKey)
	}
	if openAIProvider.cfg.BaseURL != "https://org-gateway.example.com/v1" {
		t.Fatalf("BaseURL = %q, an ambient env var leaked into an org-BYO construction", openAIProvider.cfg.BaseURL)
	}
	if openAIProvider.cfg.Model != "org-model" {
		t.Fatalf("Model = %q, an ambient env var leaked into an org-BYO construction", openAIProvider.cfg.Model)
	}
}

func TestNewProviderFromCredentialsLocalFallsBackWithEmptyBaseURL(t *testing.T) {
	provider, err := NewProviderFromCredentials(ProviderKindLocal, "", "", "")
	if err != nil {
		t.Fatalf("unexpected error: local must fall back to a default endpoint, not require a base_url: %v", err)
	}
	localProvider, ok := provider.(*LocalProvider)
	if !ok {
		t.Fatalf("provider = %T, want *LocalProvider", provider)
	}
	if localProvider.cfg.BaseURL != defaultLocalBaseURL {
		t.Errorf("BaseURL = %q, want the package default %q", localProvider.cfg.BaseURL, defaultLocalBaseURL)
	}
}

// TestNewProviderFromCredentialsOllamaUsesExplicitValues codex round 3
// (#2234), P1 fix: org-BYO "ollama" now constructs *LocalProvider (the
// OpenAI-compatible client Python's own OllamaProvider actually is), not
// *OllamaProvider (the native /api/chat client) -- see the case's own
// doc comment in providerkind.go for why NewProviderFromEnv's sibling
// case is deliberately left unchanged.
func TestNewProviderFromCredentialsOllamaUsesExplicitValues(t *testing.T) {
	provider, err := NewProviderFromCredentials(
		ProviderKindOllama, "org-key", "https://org-ollama.example.com/v1", "gemma3:4b")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	localProvider, ok := provider.(*LocalProvider)
	if !ok {
		t.Fatalf("provider = %T, want *LocalProvider (OpenAI-compatible, matching Python's OllamaProvider)", provider)
	}
	if localProvider.cfg.BaseURL != "https://org-ollama.example.com/v1" {
		t.Errorf("BaseURL = %q, want the explicit argument", localProvider.cfg.BaseURL)
	}
	if localProvider.cfg.APIKey != "org-key" {
		t.Errorf("APIKey = %q, want the explicit argument", localProvider.cfg.APIKey)
	}
	if localProvider.cfg.Model != "gemma3:4b" {
		t.Errorf("Model = %q, want the explicit argument", localProvider.cfg.Model)
	}
}

// TestReviewReproOrgBYOOllamaV1URLHitsChatCompletionsNotNativeAPIChat is
// the EXECUTED red/green proof for codex round 3's P1. Before the fix,
// this failed: NewProviderFromCredentials(ProviderKindOllama, ...)
// returned *OllamaProvider, whose Complete posts to
// BaseURL+"/api/chat" (ollamaprovider.go:202) -- for the org's stored
// "/v1" base_url (Python's own default shape, local.py:42) that is
// ".../v1/api/chat", a path the OpenAI-compatible server behind it
// doesn't serve. After the fix it constructs *LocalProvider, which
// posts to BaseURL+"/chat/completions" (localprovider.go:195) --
// ".../v1/chat/completions", the correct OpenAI-compatible route.
func TestReviewReproOrgBYOOllamaV1URLHitsChatCompletionsNotNativeAPIChat(t *testing.T) {
	var gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"role":"assistant","content":"{}"}}],"usage":{}}`))
	}))
	defer srv.Close()

	provider, err := NewProviderFromCredentials(ProviderKindOllama, "org-key", srv.URL+"/v1", "gemma3:4b")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := provider.(*LocalProvider); !ok {
		t.Fatalf("provider = %T, want *LocalProvider", provider)
	}
	if _, err := provider.Complete(context.Background(), CompletionRequest{
		Prompt:        "user",
		SystemMessage: "system",
	}); err != nil {
		t.Fatalf("Complete against the controlled OpenAI-compatible endpoint failed: %v", err)
	}
	if gotPath != "/v1/chat/completions" {
		t.Fatalf("server saw path %q, want \"/v1/chat/completions\" (native \"/v1/api/chat\" would mean the P1 regressed)", gotPath)
	}
}

func TestNewProviderFromCredentialsUnimplementedKindsRefuseExplicitly(t *testing.T) {
	for _, kind := range []ProviderKind{ProviderKindAnthropic, ProviderKindGemini, ProviderKindQwen} {
		if _, err := NewProviderFromCredentials(kind, "org-key", "", "org-model"); err != nil {
			t.Fatalf("NewProviderFromCredentials(%q) returned an error, want a stub Provider: %v", kind, err)
		}
	}
}

func TestNewProviderFromCredentialsUnknownKind(t *testing.T) {
	if _, err := NewProviderFromCredentials(ProviderKind("not-a-real-provider"), "k", "", "m"); err == nil {
		t.Fatal("expected an error for an unknown provider kind")
	}
}

// TestNewProviderFromCredentialsNeverLeaksSecretsInErrors is the proof
// team-lead's PR3 ruling asked for: no returned error string, across
// every failure path this function has today, may contain the secret
// values passed in. A future edit that starts interpolating apiKey or
// baseURL into an error message fails this test immediately.
func TestNewProviderFromCredentialsNeverLeaksSecretsInErrors(t *testing.T) {
	const secretAPIKey = "sk-canary-secret-must-never-appear-in-any-error"
	const secretBaseURL = "https://canary-secret-host.invalid/v1"

	cases := []struct {
		name string
		kind ProviderKind
	}{
		{"openai missing key still carries the canary as the (empty) key arg", ProviderKindOpenAI},
		{"unknown kind", ProviderKind("not-a-real-provider")},
	}
	// The openai case above always errors ONLY when apiKey is empty (its
	// one failure path) -- so drive it with an EMPTY key but the canary
	// base_url, proving base_url never leaks into that error either.
	if _, err := NewProviderFromCredentials(ProviderKindOpenAI, "", secretBaseURL, "m"); err == nil {
		t.Fatal("expected an error constructing openai with no api_key")
	} else if strings.Contains(err.Error(), secretBaseURL) {
		t.Fatalf("openai missing-api-key error leaked the base_url: %v", err)
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewProviderFromCredentials(tc.kind, secretAPIKey, secretBaseURL, "m")
			if err == nil {
				return // this kind's own success path is covered elsewhere
			}
			if strings.Contains(err.Error(), secretAPIKey) {
				t.Fatalf("error leaked the api_key: %v", err)
			}
			if strings.Contains(err.Error(), secretBaseURL) {
				t.Fatalf("error leaked the base_url: %v", err)
			}
		})
	}
}
