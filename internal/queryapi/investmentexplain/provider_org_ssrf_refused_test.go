package investmentexplain

import (
	"context"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize"
	"github.com/full-chaos/dev-health-ops/internal/llmorgsettings"
)

// Every path of this package that reaches llmorgsettings.Store.Credentials must act on its ok result. The stub below
// answers as the Store would if its SSRF refusal were switched off: ok=false is the contract for a refused base_url, and
// a double that ALSO fills the bundle shows what a caller does with key material it must not use. The key is a made-up
// value; the base_url is one the SSRF guard refuses.

const refusedBundleKey = "synthetic-refused-bundle-value-not-a-key"

func refusedBundleResolver() *fakeOrgResolver {
	return &fakeOrgResolver{
		usableProvider: "openai",
		credentials:    llmorgsettings.Credentials{APIKey: refusedBundleKey, BaseURL: "http://169.254.169.254/latest"},
		credentialsOK:  false,
	}
}

func clearPlatformProviderEnv(t *testing.T) {
	t.Helper()
	for _, name := range []string{"OPENAI_API_KEY", "ANTHROPIC_API_KEY", "GEMINI_API_KEY", "GOOGLE_API_KEY", "LLM_API_KEY", "OLLAMA_BASE_URL", "LOCAL_LLM_BASE_URL", "LLM_PROVIDER"} {
		t.Setenv(name, "")
	}
}

// Caller path 1: newProviderForOrg builds the provider from the org bundle only when ok.
func TestNewProviderForOrgIgnoresABundleThatIsNotOK(t *testing.T) {
	clearPlatformProviderEnv(t)
	captured := withCapturedProviderConstruction(t, func(*capturedConstruction) {
		if _, err := newProviderForOrg(context.Background(), categorize.ProviderKindOpenAI, "org-1", "", refusedBundleResolver()); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})
	if captured.credentialsCalled {
		t.Fatalf("a provider was built from a bundle the resolver marked not ok (api key used: %v, base_url used: %v)", captured.credentialsAPIKey != "", captured.credentialsBase != "")
	}
	if !captured.envCalled {
		t.Fatal("the platform fallback was not used")
	}
}

// Caller path 2: the availability check (IsLLMAvailableForOrg -> Credentials) counts a bundle only when ok.
func TestIsLLMAvailableForOrgIgnoresABundleThatIsNotOK(t *testing.T) {
	clearPlatformProviderEnv(t)
	if IsLLMAvailableForOrg(context.Background(), "auto", "org-1", refusedBundleResolver()) {
		t.Fatal("an org whose bundle the resolver marked not ok was reported as having an LLM available")
	}
	// Control: the same bundle with ok=true is available.
	ok := refusedBundleResolver()
	ok.credentialsOK = true
	if !IsLLMAvailableForOrg(context.Background(), "auto", "org-1", ok) {
		t.Fatal("control: a bundle marked ok was not reported as available")
	}
}

// Caller path 3: ProviderValueError reads only the error of Credentials, so a not-ok bundle without an error is no value
// error and reaches nothing.
func TestProviderValueErrorReadsNoBundleThatIsNotOK(t *testing.T) {
	resolver := refusedBundleResolver()
	if text, found := ProviderValueError(context.Background(), nil, categorize.ProviderKindOpenAI, "org-1", resolver); found || text != "" {
		t.Fatalf("ProviderValueError = %q, %v for a not-ok bundle with no error", text, found)
	}
	if len(resolver.credentialsCalledWith) != 1 {
		t.Fatalf("Credentials was asked %d times, want once", len(resolver.credentialsCalledWith))
	}
}
