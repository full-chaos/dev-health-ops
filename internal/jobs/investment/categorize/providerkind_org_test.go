package categorize

import (
	"context"
	"errors"
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
