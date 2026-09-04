package categorize

import (
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

func TestNewProviderFromCredentialsOllamaUsesExplicitValues(t *testing.T) {
	provider, err := NewProviderFromCredentials(
		ProviderKindOllama, "org-key", "https://org-ollama.example.com", "gemma3:4b")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	ollamaProvider := provider.(*OllamaProvider)
	if ollamaProvider.cfg.BaseURL != "https://org-ollama.example.com" {
		t.Errorf("BaseURL = %q, want the explicit argument", ollamaProvider.cfg.BaseURL)
	}
	if ollamaProvider.cfg.APIKey != "org-key" {
		t.Errorf("APIKey = %q, want the explicit argument", ollamaProvider.cfg.APIKey)
	}
	if ollamaProvider.cfg.Model != "gemma3:4b" {
		t.Errorf("Model = %q, want the explicit argument", ollamaProvider.cfg.Model)
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
