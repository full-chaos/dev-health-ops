package investmentexplain

import (
	"bytes"
	"context"
	"errors"
	"log"
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

	rawBaseURL       string
	rawConfigured    bool
	rawBaseURLErr    error
	rawBaseURLCalled bool

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

func (f *fakeOrgResolver) RawBaseURL(context.Context, string, string) (string, bool, error) {
	f.rawBaseURLCalled = true
	return f.rawBaseURL, f.rawConfigured, f.rawBaseURLErr
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
		provider, err := newProviderForOrg(context.Background(), categorize.ProviderKindOpenAI, "org-1", "", resolver)
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

// TestNewProviderForOrg_ExplicitModelOverridesOrgStoredModel is the codex
// round 1 (#2234), P2 regression: an earlier version ignored the
// caller's own explicit requestedModel parameter here and always
// re-derived the model from orgSettings.Model, so a request-level model
// override was reported as "resolved" by ResolveModelNameForOrg but
// never reached the actual provider construction when org BYO was the
// active source -- Python passes the request model straight into
// get_provider, applying the same override.
func TestNewProviderForOrg_ExplicitModelOverridesOrgStoredModel(t *testing.T) {
	resolver := &fakeOrgResolver{
		credentials: llmorgsettings.Credentials{
			APIKey:  "org-secret-key",
			BaseURL: "https://org-gateway.example.com/v1",
		},
		credentialsOK: true,
		model:         "org-chosen-model",
	}

	captured := withCapturedProviderConstruction(t, func(c *capturedConstruction) {
		_, err := newProviderForOrg(context.Background(), categorize.ProviderKindOpenAI, "org-1", "request-model", resolver)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	if !captured.credentialsCalled {
		t.Fatal("expected newProviderFromCredentials to be called for a matched BYO org")
	}
	if captured.credentialsModel != "request-model" {
		t.Errorf("Model = %q, want the explicit request-level override, not the org's stored model", captured.credentialsModel)
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
				if _, err := newProviderForOrg(context.Background(), categorize.ProviderKindOpenAI, "org-1", "", tc.orgSettings); err != nil {
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
		_, err := newProviderForOrg(context.Background(), categorize.ProviderKindOpenAI, "org-1", "", resolver)
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

// TestLogOrgBaseURLSSRFFallback_FiresOnSSRFRejectionOnly is the codex
// round 1 P2 fix (SSRF-fallback telemetry, team-lead ruling): the REAL
// log line fires ONLY when the org's base_url specifically failed the
// SSRF guard, not for the overwhelmingly common "org configured no BYO
// for this provider at all" case (that would be noise on every ordinary
// request, not a security signal) or a safe base_url that was not usable
// for some other reason (e.g. an incomplete api_key).
func TestLogOrgBaseURLSSRFFallback_FiresOnSSRFRejectionOnly(t *testing.T) {
	cases := []struct {
		name     string
		resolver *fakeOrgResolver
		wantFire bool
	}{
		{
			name:     "org configured no BYO for this provider at all",
			resolver: &fakeOrgResolver{rawConfigured: false},
			wantFire: false,
		},
		{
			name:     "org configured BYO with a safe, public base_url (not usable for some other reason)",
			resolver: &fakeOrgResolver{rawBaseURL: "https://my-gateway.example.com/v1", rawConfigured: true},
			wantFire: false,
		},
		{
			name:     "org configured BYO with an SSRF-unsafe base_url",
			resolver: &fakeOrgResolver{rawBaseURL: "http://169.254.169.254/latest/meta-data", rawConfigured: true},
			wantFire: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			origOutput := log.Writer()
			log.SetOutput(&buf)
			t.Cleanup(func() { log.SetOutput(origOutput) })

			// Call the REAL function directly -- this test is about ITS
			// OWN internal fire/no-fire decision, not about whether
			// newProviderForOrg's wiring reaches the call site (which it
			// always does for any not-ok Credentials() result, by
			// design; the decision of whether to actually emit a log
			// line belongs to this function, not its caller).
			logOrgBaseURLSSRFFallback(context.Background(), tc.resolver, "org-1", "openai")

			fired := buf.Len() > 0
			if fired != tc.wantFire {
				t.Fatalf("fired = %v (output %q), want %v", fired, buf.String(), tc.wantFire)
			}
		})
	}
}

// TestNewProviderForOrg_CallsFallbackTelemetryHook proves the WIRING:
// newProviderForOrg calls the fallback-telemetry hook whenever org BYO
// was not usable (the hook itself, tested separately above, decides
// whether that becomes a real log line).
func TestNewProviderForOrg_CallsFallbackTelemetryHook(t *testing.T) {
	resolver := &fakeOrgResolver{credentialsOK: false}

	var calledWithOrgID, calledWithProvider string
	origHook := logOrgBaseURLSSRFFallback
	logOrgBaseURLSSRFFallback = func(_ context.Context, _ llmorgsettings.Resolver, orgID, provider string) {
		calledWithOrgID, calledWithProvider = orgID, provider
	}
	t.Cleanup(func() { logOrgBaseURLSSRFFallback = origHook })

	withCapturedProviderConstruction(t, func(c *capturedConstruction) {
		if _, err := newProviderForOrg(context.Background(), categorize.ProviderKindOpenAI, "org-42", "", resolver); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})
	if calledWithOrgID != "org-42" || calledWithProvider != "openai" {
		t.Fatalf("fallback hook called with (%q, %q), want (org-42, openai)", calledWithOrgID, calledWithProvider)
	}
}

// TestLogOrgBaseURLSSRFFallback_LogContent proves the REAL log line (not
// a capturing swap) carries the org id and the URL's scheme/host ONLY --
// never the full URL, its path/query, or any credential value, even one
// embedded directly in the base_url (userinfo or a query-string token).
func TestLogOrgBaseURLSSRFFallback_LogContent(t *testing.T) {
	const secretPathSegment = "canary-secret-path-segment-must-never-appear-in-any-log"
	const secretQueryToken = "canary-secret-query-token-must-never-appear-in-any-log"
	badURL := "http://169.254.169.254/" + secretPathSegment + "?token=" + secretQueryToken

	resolver := &fakeOrgResolver{rawBaseURL: badURL, rawConfigured: true}

	var buf bytes.Buffer
	origOutput := log.Writer()
	origFlags := log.Flags()
	log.SetOutput(&buf)
	log.SetFlags(0)
	t.Cleanup(func() {
		log.SetOutput(origOutput)
		log.SetFlags(origFlags)
	})

	logOrgBaseURLSSRFFallback(context.Background(), resolver, "org-canary-42", "openai")

	output := buf.String()
	if output == "" {
		t.Fatal("expected a log line for an SSRF-rejected base_url, got none")
	}
	if !strings.Contains(output, "org-canary-42") {
		t.Errorf("log output missing the org id: %q", output)
	}
	if !strings.Contains(output, "169.254.169.254") {
		t.Errorf("log output missing the rejected host: %q", output)
	}
	if strings.Contains(output, secretPathSegment) {
		t.Fatalf("log output leaked the URL PATH: %q", output)
	}
	if strings.Contains(output, secretQueryToken) {
		t.Fatalf("log output leaked the URL QUERY (a credential-shaped token): %q", output)
	}
	if strings.Contains(output, badURL) {
		t.Fatalf("log output leaked the FULL base_url verbatim: %q", output)
	}
}

// TestLogOrgBaseURLSSRFFallback_NoFireWhenLookupErrors proves a
// RawBaseURL error (fail-closed elsewhere in the resolver chain) never
// itself produces a log line -- this is telemetry, not a second place to
// surface a fail-closed error.
func TestLogOrgBaseURLSSRFFallback_NoFireWhenLookupErrors(t *testing.T) {
	resolver := &fakeOrgResolver{rawBaseURLErr: errNoSuchModel}

	var buf bytes.Buffer
	origOutput := log.Writer()
	log.SetOutput(&buf)
	t.Cleanup(func() { log.SetOutput(origOutput) })

	logOrgBaseURLSSRFFallback(context.Background(), resolver, "org-1", "openai")

	if buf.Len() != 0 {
		t.Fatalf("expected no log output on a RawBaseURL lookup error, got: %q", buf.String())
	}
}
