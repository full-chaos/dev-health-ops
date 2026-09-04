package categorize

import (
	"bytes"
	"context"
	"errors"
	"log"
	"strings"
	"testing"
)

// staticOrgResolver returns provider (or err) for every org, ignoring
// orgID -- these tests only need to control ResolveProviderKindForOrg's
// PRECEDENCE, not exercise a real settings lookup (that is
// internal/llmorgsettings' job, proven against real Postgres).
func staticOrgResolver(provider string, err error) OrgProviderResolver {
	return func(context.Context, string) (string, error) {
		return provider, err
	}
}

func TestResolveProviderKindForOrg_ExplicitRequestNeverConsultsOrg(t *testing.T) {
	clearProviderEnv(t)
	called := false
	resolver := OrgProviderResolver(func(context.Context, string) (string, error) {
		called = true
		return "anthropic", nil
	})
	kind, err := ResolveProviderKindForOrg(context.Background(), "mock", "org-1", resolver)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if kind != ProviderKindMock {
		t.Fatalf("kind = %q, want mock", kind)
	}
	if called {
		t.Fatal("an explicit non-auto request must never consult the org resolver")
	}
}

func TestResolveProviderKindForOrg_KillSwitchBeatsOrgBYO(t *testing.T) {
	clearProviderEnv(t)
	t.Setenv("LLM_PROVIDER", "none")
	kind, err := ResolveProviderKindForOrg(
		context.Background(), "auto", "org-1", staticOrgResolver("openai", nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if kind != ProviderKindNone {
		t.Fatalf("kind = %q, want none (LLM_PROVIDER=none must beat org BYO)", kind)
	}
}

func TestResolveProviderKindForOrg_OrgBYOBeatsPlatformEnv(t *testing.T) {
	// The CHAOS-5006 defect this whole function exists to fix: an org's
	// BYO provider must win even when the platform LLM_PROVIDER env names
	// a DIFFERENT, otherwise-selectable provider.
	clearProviderEnv(t)
	t.Setenv("LLM_PROVIDER", "openai")
	t.Setenv("OPENAI_API_KEY", "sk-platform")
	kind, err := ResolveProviderKindForOrg(
		context.Background(), "auto", "org-1", staticOrgResolver("ollama", nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if kind != ProviderKindOllama {
		t.Fatalf("kind = %q, want ollama (org BYO must win over platform LLM_PROVIDER)", kind)
	}
}

func TestResolveProviderKindForOrg_OrgBYOBeatsAutoDetection(t *testing.T) {
	clearProviderEnv(t)
	t.Setenv("OPENAI_API_KEY", "sk-would-otherwise-auto-detect")
	kind, err := ResolveProviderKindForOrg(
		context.Background(), "auto", "org-1", staticOrgResolver("local", nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if kind != ProviderKindLocal {
		t.Fatalf("kind = %q, want local (org BYO must win over env auto-detection)", kind)
	}
}

func TestResolveProviderKindForOrg_NoUsableOrgProviderFallsBackToPlatformEnv(t *testing.T) {
	clearProviderEnv(t)
	t.Setenv("LLM_PROVIDER", "local")
	kind, err := ResolveProviderKindForOrg(
		context.Background(), "auto", "org-1", staticOrgResolver("", nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if kind != ProviderKindLocal {
		t.Fatalf("kind = %q, want local (empty org resolution falls back to platform env)", kind)
	}
}

func TestResolveProviderKindForOrg_NoUsableOrgProviderFallsBackToAutoDetection(t *testing.T) {
	clearProviderEnv(t)
	t.Setenv("OPENAI_API_KEY", "sk-x")
	kind, err := ResolveProviderKindForOrg(
		context.Background(), "auto", "org-1", staticOrgResolver("", nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if kind != ProviderKindOpenAI {
		t.Fatalf("kind = %q, want openai (empty org resolution falls back to auto-detection)", kind)
	}
}

func TestResolveProviderKindForOrg_ResolverErrorPropagatesFailClosed(t *testing.T) {
	// Mirrors _apply_byo_llm_flag_gate's `raise LLMAuthError`: a
	// flag-lookup failure for a BYO-configured org must fail the WHOLE
	// resolution, never silently fall through to the platform default.
	clearProviderEnv(t)
	t.Setenv("OPENAI_API_KEY", "sk-must-not-be-reached")
	sentinel := errors.New("byo_llm feature flag state could not be determined")
	_, err := ResolveProviderKindForOrg(
		context.Background(), "auto", "org-1", staticOrgResolver("", sentinel))
	if !errors.Is(err, sentinel) {
		t.Fatalf("err = %v, want it to wrap the resolver's sentinel error", err)
	}
}

func TestResolveProviderKindForOrg_NilResolverMatchesOldBehavior(t *testing.T) {
	clearProviderEnv(t)
	t.Setenv("OPENAI_API_KEY", "sk-x")
	withNilResolver, err := ResolveProviderKindForOrg(context.Background(), "auto", "org-1", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	viaOldEntryPoint, err := ResolveProviderKind("auto")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if withNilResolver != viaOldEntryPoint {
		t.Fatalf("nil-resolver result %q != ResolveProviderKind's result %q", withNilResolver, viaOldEntryPoint)
	}
}

func TestResolveProviderKindForOrg_OrgProviderIsNormalized(t *testing.T) {
	clearProviderEnv(t)
	kind, err := ResolveProviderKindForOrg(
		context.Background(), "auto", "org-1", staticOrgResolver("  OLLAMA  ", nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if kind != ProviderKindOllama {
		t.Fatalf("kind = %q, want ollama (an org-resolved provider name must be normalized same as any other)", kind)
	}
}

// TestResolveProviderKindForOrg_LogsOnOrgBYOWin is the codex round 1
// (#2223), P3 regression: the org-BYO-wins branch -- the whole point of
// CHAOS-5006 -- previously had no telemetry at all, making it operationally
// indistinguishable from every other resolution path. Asserts the log line
// fires with the org id and resolved provider, and NOT on any other
// resolution path (kill-switch, platform env, auto-detect).
func TestResolveProviderKindForOrg_LogsOnOrgBYOWin(t *testing.T) {
	clearProviderEnv(t)
	var buf bytes.Buffer
	orig := log.Writer()
	log.SetOutput(&buf)
	t.Cleanup(func() { log.SetOutput(orig) })

	kind, err := ResolveProviderKindForOrg(
		context.Background(), "auto", "org-42", staticOrgResolver("ollama", nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if kind != ProviderKindOllama {
		t.Fatalf("kind = %q, want ollama", kind)
	}
	got := buf.String()
	if !strings.Contains(got, "org-42") || !strings.Contains(got, "ollama") {
		t.Fatalf("log output = %q, want it to mention the org id and resolved provider", got)
	}
}

// TestResolveProviderKindForOrg_LogsNormalizedValue is the codex round 2,
// P3 regression: the log line previously logged the RAW resolver output
// (e.g. "  OLLAMA  ") while the function RETURNED its normalized form
// ("ollama") -- the log never matched the value actually selected, which
// breaks any search/aggregation keyed on the logged provider name.
func TestResolveProviderKindForOrg_LogsNormalizedValue(t *testing.T) {
	clearProviderEnv(t)
	var buf bytes.Buffer
	orig := log.Writer()
	log.SetOutput(&buf)
	t.Cleanup(func() { log.SetOutput(orig) })

	kind, err := ResolveProviderKindForOrg(
		context.Background(), "auto", "org-1", staticOrgResolver("  OLLAMA  ", nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if kind != ProviderKindOllama {
		t.Fatalf("kind = %q, want ollama", kind)
	}
	got := buf.String()
	if strings.Contains(got, "OLLAMA") || strings.Contains(got, "  ollama  ") {
		t.Fatalf("log output = %q, must log the NORMALIZED value (%q), not the raw resolver output", got, kind)
	}
	if !strings.Contains(got, string(kind)) {
		t.Fatalf("log output = %q, must contain the actual returned kind %q", got, kind)
	}
}

// TestResolveProviderKindForOrg_NoLogOnNonOrgPaths proves the new log line
// is scoped to the org-BYO-wins branch only -- it must not fire on the
// kill-switch, no-org-provider-available, or auto-detect paths.
func TestResolveProviderKindForOrg_NoLogOnNonOrgPaths(t *testing.T) {
	cases := []struct {
		name     string
		setup    func(t *testing.T)
		resolver OrgProviderResolver
	}{
		{
			name: "kill switch beats org BYO",
			setup: func(t *testing.T) {
				clearProviderEnv(t)
				t.Setenv("LLM_PROVIDER", "none")
			},
			resolver: staticOrgResolver("ollama", nil),
		},
		{
			name: "no org provider available, falls to platform env",
			setup: func(t *testing.T) {
				clearProviderEnv(t)
				t.Setenv("LLM_PROVIDER", "openai")
			},
			resolver: staticOrgResolver("", nil),
		},
		{
			name: "nil resolver, auto-detect",
			setup: func(t *testing.T) {
				clearProviderEnv(t)
				t.Setenv("OPENAI_API_KEY", "sk-test")
			},
			resolver: nil,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tc.setup(t)
			var buf bytes.Buffer
			orig := log.Writer()
			log.SetOutput(&buf)
			t.Cleanup(func() { log.SetOutput(orig) })

			if _, err := ResolveProviderKindForOrg(context.Background(), "auto", "org-1", tc.resolver); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got := buf.String(); got != "" {
				t.Fatalf("log output = %q, want empty -- the org-BYO log line must not fire on this path", got)
			}
		})
	}
}
