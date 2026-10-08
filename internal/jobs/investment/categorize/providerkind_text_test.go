package categorize

import (
	"context"
	"errors"
	"testing"
)

var textResolutionEnv = []string{
	"LLM_PROVIDER", "LLM_MODEL", "OPENAI_API_KEY", "ANTHROPIC_API_KEY", "GEMINI_API_KEY", "LOCAL_LLM_BASE_URL",
	"DASHSCOPE_API_KEY", "QWEN_API_KEY", "OLLAMA_MODEL", "OLLAMA_BASE_URL", "TYPESAFE_API_KEY",
}

func setTextResolutionEnv(t *testing.T, values map[string]string) {
	t.Helper()
	for _, name := range textResolutionEnv {
		t.Setenv(name, values[name])
	}
}

// The decision kinds are a closed set of one: every other name of the
// closed set of kinds, and the auto request, is not a decision kind.
func TestTheDecisionProviderKindsAreAClosedSetOfOne(t *testing.T) {
	for _, kind := range []ProviderKind{
		ProviderKindOpenAI, ProviderKindAnthropic, ProviderKindGemini, ProviderKindQwen, ProviderKindLocal,
		ProviderKindOllama, ProviderKindMock, ProviderKindNone, providerKindAuto, "", "lmstudio", "TypeSafe",
	} {
		if IsDecisionProviderKind(kind) {
			t.Errorf("%q is a decision kind", kind)
		}
	}
	if !IsDecisionProviderKind(ProviderKindTypeSafe) || len(decisionProviderKinds) != 1 {
		t.Fatalf("decision kinds = %v", decisionProviderKinds)
	}
	for kind := range decisionProviderKinds {
		if IsProviderKindImplemented(kind) {
			t.Errorf("decision kind %q is also a generative kind", kind)
		}
	}
}

// LLM_PROVIDER=typesafe is a platform default that is not a text provider: an
// auto TEXT resolution gives what it gives with LLM_PROVIDER unset. The order
// of the key auto-detection is the one of today (OpenAI before Anthropic), so
// with both keys set the answer is openai, and the model is LLM_MODEL.
func TestATextResolutionTreatsTheDecisionDefaultAsUnset(t *testing.T) {
	keys := map[string]string{"OPENAI_API_KEY": "plain words used as a test value", "ANTHROPIC_API_KEY": "plain-test-value", "LLM_MODEL": "gpt-5-nano"}
	setTextResolutionEnv(t, keys)
	today, todayErr := ResolveTextProviderKind("auto")
	withDecision := map[string]string{"LLM_PROVIDER": "typesafe", "TYPESAFE_API_KEY": "plain-test-value"}
	for k, v := range keys {
		withDecision[k] = v
	}
	setTextResolutionEnv(t, withDecision)
	got, err := ResolveTextProviderKind("auto")
	if todayErr != nil || err != nil || got != today || got != ProviderKindOpenAI {
		t.Fatalf("text resolution with LLM_PROVIDER=typesafe = %q (%v); with it unset %q (%v); want openai both", got, err, today, todayErr)
	}
	if model := ResolveModelName(got, ""); model != "gpt-5-nano" {
		t.Fatalf("model = %q, want LLM_MODEL", model)
	}
	// The served (categorization) resolution still gives the decision kind.
	if kind, err := ResolveProviderKind("auto"); err != nil || kind != ProviderKindTypeSafe {
		t.Fatalf("categorization resolution = %q (%v), want typesafe", kind, err)
	}
	// Only Anthropic: the next one of today's order.
	delete(withDecision, "OPENAI_API_KEY")
	setTextResolutionEnv(t, withDecision)
	if got, err := ResolveTextProviderKind("auto"); err != nil || got != ProviderKindAnthropic {
		t.Fatalf("with the Anthropic key only: %q (%v)", got, err)
	}
}

// With no generative key, the text resolution fails with the error of a
// process with no provider at all: the same text as LLM_PROVIDER unset.
func TestATextResolutionWithTheDecisionDefaultAndNoKeyFailsAsWithNone(t *testing.T) {
	setTextResolutionEnv(t, nil)
	_, todayErr := ResolveTextProviderKind("auto")
	setTextResolutionEnv(t, map[string]string{"LLM_PROVIDER": "typesafe", "TYPESAFE_API_KEY": "plain-test-value"})
	kind, err := ResolveTextProviderKind("")
	if err == nil || todayErr == nil || err.Error() != todayErr.Error() || kind != "" {
		t.Fatalf("got %q / %v, want the error %v", kind, err, todayErr)
	}
}

// An explicit request and an org BYO provider are returned as they are: the
// fallback is for the platform default only. A resolver error is passed on.
func TestATextResolutionKeepsExplicitAndOrgChoices(t *testing.T) {
	setTextResolutionEnv(t, map[string]string{"LLM_PROVIDER": "typesafe", "OPENAI_API_KEY": "plain words used as a test value"})
	if kind, err := ResolveTextProviderKind("typesafe"); err != nil || kind != ProviderKindTypeSafe {
		t.Fatalf("explicit request = %q (%v)", kind, err)
	}
	if _, err := NewProviderFromEnvWithModel(ProviderKindTypeSafe, ""); err == nil {
		t.Fatal("an explicit decision request built a text provider")
	}
	byo := func(context.Context, string) (string, error) { return "ollama", nil }
	if kind, err := ResolveTextProviderKindForOrg(context.Background(), "auto", "org-a", byo); err != nil || kind != ProviderKindOllama {
		t.Fatalf("org BYO = %q (%v)", kind, err)
	}
	none := func(context.Context, string) (string, error) { return "", nil }
	if kind, err := ResolveTextProviderKindForOrg(context.Background(), "auto", "org-a", none); err != nil || kind != ProviderKindOpenAI {
		t.Fatalf("org with no BYO = %q (%v), want openai", kind, err)
	}
	failing := errors.New("lookup failed")
	broken := func(context.Context, string) (string, error) { return "", failing }
	if _, err := ResolveTextProviderKindForOrg(context.Background(), "auto", "org-a", broken); !errors.Is(err, failing) {
		t.Fatalf("resolver error = %v", err)
	}
	t.Setenv("LLM_PROVIDER", "mock")
	if kind, err := ResolveTextProviderKind("auto"); err != nil || kind != ProviderKindMock {
		t.Fatalf("the kill switch = %q (%v)", kind, err)
	}
}
