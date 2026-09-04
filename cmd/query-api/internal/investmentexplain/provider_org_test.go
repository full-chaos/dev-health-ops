package investmentexplain

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize"
	"github.com/full-chaos/dev-health-ops/internal/llmorgsettings"
)

// fakeOrgResolver is a llmorgsettings.Resolver test double -- no
// Postgres, fixed answers per test case. It records every call it
// receives so a test can additionally assert WHICH provider string this
// package's own wiring queried it with.
type fakeOrgResolver struct {
	usableProvider string
	usableErr      error

	credentials    llmorgsettings.Credentials
	credentialsOK  bool
	credentialsErr error

	matches    bool
	matchesErr error

	model    string
	modelErr error

	credentialsCalledWith []string // providers passed to Credentials, in call order
}

var _ llmorgsettings.Resolver = (*fakeOrgResolver)(nil)

func (f *fakeOrgResolver) ResolveUsableProvider(context.Context, string) (string, error) {
	return f.usableProvider, f.usableErr
}

func (f *fakeOrgResolver) Credentials(_ context.Context, _ string, provider string) (llmorgsettings.Credentials, bool, error) {
	f.credentialsCalledWith = append(f.credentialsCalledWith, provider)
	return f.credentials, f.credentialsOK, f.credentialsErr
}

func (f *fakeOrgResolver) Matches(context.Context, string, string) (bool, error) {
	return f.matches, f.matchesErr
}

func (f *fakeOrgResolver) Model(context.Context, string, string) (string, error) {
	return f.model, f.modelErr
}

// withCapturedProviderConstruction swaps newProviderFromCredentials/
// newProviderFromEnv for capturing fakes for the duration of fn, then
// restores the real (categorize-backed) ones -- never leaves the swap in
// place for another test.
type capturedConstruction struct {
	credentialsCalled bool
	credentialsKind   categorize.ProviderKind
	credentialsAPIKey string
	credentialsBase   string
	credentialsModel  string

	envCalled bool
	envKind   categorize.ProviderKind
}

func withCapturedProviderConstruction(t *testing.T, fn func(*capturedConstruction)) *capturedConstruction {
	t.Helper()
	captured := &capturedConstruction{}

	origCreds := newProviderFromCredentials
	origEnv := newProviderFromEnv
	t.Cleanup(func() {
		newProviderFromCredentials = origCreds
		newProviderFromEnv = origEnv
	})

	newProviderFromCredentials = func(kind categorize.ProviderKind, apiKey, baseURL, model string) (categorize.Provider, error) {
		captured.credentialsCalled = true
		captured.credentialsKind = kind
		captured.credentialsAPIKey = apiKey
		captured.credentialsBase = baseURL
		captured.credentialsModel = model
		return categorize.MockProvider{}, nil
	}
	newProviderFromEnv = func(kind categorize.ProviderKind) (categorize.Provider, error) {
		captured.envCalled = true
		captured.envKind = kind
		return categorize.MockProvider{}, nil
	}

	fn(captured)
	return captured
}

// TestNewProviderForOrg_UsesOrgCredentialsWhenMatched is the proof
// team-lead's PR3 ruling asked for: a BYO org's request is constructed
// with ITS OWN key/base_url/model, not the platform environment.
func TestNewProviderForOrg_UsesOrgCredentialsWhenMatched(t *testing.T) {
	resolver := &fakeOrgResolver{
		credentials: llmorgsettings.Credentials{
			APIKey:  "org-secret-key",
			BaseURL: "https://org-gateway.example.com/v1",
		},
		credentialsOK: true,
		model:         "org-chosen-model",
	}

	captured := withCapturedProviderConstruction(t, func(c *capturedConstruction) {
		provider, err := newProviderForOrg(context.Background(), categorize.ProviderKindOpenAI, "org-1", resolver)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if provider == nil {
			t.Fatal("expected a non-nil provider")
		}
	})

	if !captured.credentialsCalled {
		t.Fatal("expected newProviderFromCredentials to be called for a matched BYO org")
	}
	if captured.envCalled {
		t.Fatal("newProviderFromEnv must NOT be called when the org's BYO provider matched -- CHAOS-2550 source-bound invariant")
	}
	if captured.credentialsKind != categorize.ProviderKindOpenAI {
		t.Errorf("kind = %q, want openai", captured.credentialsKind)
	}
	if captured.credentialsAPIKey != "org-secret-key" {
		t.Errorf("APIKey = %q, want the org's own key", captured.credentialsAPIKey)
	}
	if captured.credentialsBase != "https://org-gateway.example.com/v1" {
		t.Errorf("BaseURL = %q, want the org's own base_url", captured.credentialsBase)
	}
	if captured.credentialsModel != "org-chosen-model" {
		t.Errorf("Model = %q, want the org's own model", captured.credentialsModel)
	}
	if len(resolver.credentialsCalledWith) != 1 || resolver.credentialsCalledWith[0] != "openai" {
		t.Errorf("Credentials queried with %v, want exactly one call for %q", resolver.credentialsCalledWith, "openai")
	}
}

// TestNewProviderForOrg_FallsBackToEnvWhenNotMatched is the second proof
// team-lead's ruling asked for: a non-BYO org (or an org whose settings
// don't match kind) still uses the platform environment, unchanged.
func TestNewProviderForOrg_FallsBackToEnvWhenNotMatched(t *testing.T) {
	cases := []struct {
		name        string
		orgSettings llmorgsettings.Resolver
	}{
		{"nil orgSettings (no Postgres wiring)", nil},
		{"org has no usable BYO credentials for this kind", &fakeOrgResolver{credentialsOK: false}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			captured := withCapturedProviderConstruction(t, func(c *capturedConstruction) {
				if _, err := newProviderForOrg(context.Background(), categorize.ProviderKindOpenAI, "org-1", tc.orgSettings); err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
			})
			if !captured.envCalled {
				t.Fatal("expected newProviderFromEnv to be called")
			}
			if captured.credentialsCalled {
				t.Fatal("newProviderFromCredentials must NOT be called when org BYO did not match")
			}
			if captured.envKind != categorize.ProviderKindOpenAI {
				t.Errorf("kind = %q, want openai", captured.envKind)
			}
		})
	}
}

func TestNewProviderForOrg_PropagatesCredentialsLookupError(t *testing.T) {
	sentinel := errors.New("byo_llm feature flag state could not be determined")
	resolver := &fakeOrgResolver{credentialsErr: sentinel}
	captured := withCapturedProviderConstruction(t, func(c *capturedConstruction) {
		_, err := newProviderForOrg(context.Background(), categorize.ProviderKindOpenAI, "org-1", resolver)
		if !errors.Is(err, sentinel) {
			t.Fatalf("err = %v, want it to wrap the resolver's sentinel error", err)
		}
	})
	if captured.credentialsCalled || captured.envCalled {
		t.Fatal("a fail-closed lookup error must not fall through to either construction path")
	}
}

func TestResolveModelNameForOrg_OrgModelWinsOverEnvWhenMatched(t *testing.T) {
	t.Setenv("LLM_MODEL", "platform-model-must-lose")
	resolver := &fakeOrgResolver{matches: true, model: "org-model"}
	model, found := ResolveModelNameForOrg(context.Background(), categorize.ProviderKindOpenAI, "", "org-1", resolver)
	if !found || model != "org-model" {
		t.Fatalf("model = %q found=%v, want (org-model, true)", model, found)
	}
}

func TestResolveModelNameForOrg_ExplicitModelAlwaysWins(t *testing.T) {
	resolver := &fakeOrgResolver{matches: true, model: "org-model-must-lose"}
	model, found := ResolveModelNameForOrg(context.Background(), categorize.ProviderKindOpenAI, "explicit-model", "org-1", resolver)
	if !found || model != "explicit-model" {
		t.Fatalf("model = %q found=%v, want (explicit-model, true)", model, found)
	}
}

func TestResolveModelNameForOrg_FallsBackToEnvWhenNotMatched(t *testing.T) {
	t.Setenv("LLM_MODEL_OPENAI", "")
	t.Setenv("LLM_MODEL", "platform-model")
	resolver := &fakeOrgResolver{matches: false}
	model, found := ResolveModelNameForOrg(context.Background(), categorize.ProviderKindOpenAI, "", "org-1", resolver)
	if !found || model != "platform-model" {
		t.Fatalf("model = %q found=%v, want (platform-model, true)", model, found)
	}
}

func TestResolveUnsupportedProviderKindForOrg_OrgResolvedUnsupportedKind(t *testing.T) {
	resolver := &fakeOrgResolver{usableProvider: "lmstudio"}
	kind, unsupported := ResolveUnsupportedProviderKindForOrg(context.Background(), "auto", "org-1", resolver)
	if !unsupported {
		t.Fatalf("kind = %q, expected unsupported=true for an org resolved to lmstudio", kind)
	}
}

func TestIsLLMAvailableForOrg_TrueWhenOrgCredentialsComplete(t *testing.T) {
	resolver := &fakeOrgResolver{
		usableProvider: "openai",
		credentials:    llmorgsettings.Credentials{APIKey: "org-key"},
		credentialsOK:  true,
	}
	if !IsLLMAvailableForOrg(context.Background(), "auto", "org-1", resolver) {
		t.Fatal("expected available=true for an org with a complete BYO openai credential")
	}
}

func TestIsLLMAvailableForOrg_NilResolverMatchesUnwidenedBehavior(t *testing.T) {
	t.Setenv("LLM_PROVIDER", "mock")
	got := IsLLMAvailableForOrg(context.Background(), "auto", "org-1", nil)
	want := IsLLMAvailable("auto", "org-1")
	if got != want {
		t.Fatalf("IsLLMAvailableForOrg(nil resolver) = %v, want IsLLMAvailable's own answer %v", got, want)
	}
}

// TestCompleteInvestmentMixExplanationForOrg_NeverLeaksCredentialsInErrors
// is the proof team-lead's PR3 ruling asked for at this package's own
// level (categorize's own equivalent,
// TestNewProviderFromCredentialsNeverLeaksSecretsInErrors, proves the
// constructor itself never interpolates a credential into an error --
// this test proves THIS package's wrapping doesn't introduce one either,
// covering the one place a caller (the route handler's
// `log.Printf("... streaming error: %v", err)`) ever prints this error).
// A capturing newProviderFromCredentials stands in for the real
// constructor and returns an error carrying the canary ON PURPOSE
// (worst case: even if categorize's own guarantee were ever broken, this
// package's error-wrapping path must not make it worse) -- this test
// therefore also fails loudly if construct-provider errors ever start
// getting silently swallowed instead of wrapped through.
func TestCompleteInvestmentMixExplanationForOrg_NeverLeaksCredentialsInErrors(t *testing.T) {
	const secretAPIKey = "sk-canary-secret-must-never-appear-in-any-error"
	const secretBaseURL = "https://canary-secret-host.invalid/v1"

	resolver := &fakeOrgResolver{
		credentials:   llmorgsettings.Credentials{APIKey: secretAPIKey, BaseURL: secretBaseURL},
		credentialsOK: true,
		model:         "org-model",
	}

	withCapturedProviderConstruction(t, func(c *capturedConstruction) {
		newProviderFromCredentials = func(kind categorize.ProviderKind, apiKey, baseURL, model string) (categorize.Provider, error) {
			// A deliberately worst-case construction error naming the
			// caller-visible surface an accidental leak would most
			// plausibly go through -- this stand-in never actually
			// embeds apiKey/baseURL, proving the WRAPPING path
			// (CompleteInvestmentMixExplanationForOrg's own
			// fmt.Errorf("construct llm provider: %w", err)) doesn't add
			// them either.
			return nil, errNoSuchModel
		}

		_, _, _, err := CompleteInvestmentMixExplanationForOrg(
			context.Background(), "openai", "", "org-1", resolver, "prompt text")
		if err == nil {
			t.Fatal("expected an error from the failing construction")
		}
		if strings.Contains(err.Error(), secretAPIKey) {
			t.Fatalf("returned error leaked the api_key: %v", err)
		}
		if strings.Contains(err.Error(), secretBaseURL) {
			t.Fatalf("returned error leaked the base_url: %v", err)
		}
	})
}

var errNoSuchModel = errors.New("no such model")
