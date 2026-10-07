package investmentexplain

import (
	"context"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize"
)

var decisionTestEnv = []string{
	"LLM_PROVIDER", "LLM_MODEL", "LLM_MODEL_OPENAI", "LLM_API_KEY", "LLM_BASE_URL", "OPENAI_API_KEY", "ANTHROPIC_API_KEY",
	"GEMINI_API_KEY", "LOCAL_LLM_BASE_URL", "DASHSCOPE_API_KEY", "QWEN_API_KEY", "OLLAMA_MODEL", "OLLAMA_BASE_URL",
	"TYPESAFE_API_KEY",
}

type explainChoice struct {
	kind        categorize.ProviderKind
	err         string
	model       string
	available   bool
	unsupported bool
}

func explainChoiceNow(t *testing.T) explainChoice {
	t.Helper()
	var choice explainChoice
	kind, err := ResolveProviderKindForOrg(context.Background(), "auto", "org-a", nil)
	choice.kind = kind
	if err != nil {
		choice.err = err.Error()
	}
	choice.model, _ = ResolveModelName(kind, "")
	choice.available = IsLLMAvailableForOrg(context.Background(), "auto", "org-a", nil) && IsLLMAvailable("auto", "org-a")
	_, choice.unsupported = ResolveUnsupportedProviderKindForOrg(context.Background(), "auto", "org-a", nil)
	return choice
}

// CHAOS-8874: LLM_PROVIDER=typesafe selects the decision backend for investment
// categorization only. Every explanation entry point of this package answers
// what it answers with LLM_PROVIDER unset: with an OpenAI and an Anthropic key,
// openai and LLM_MODEL; with no generative key, the same failure.
func TestTheExplanationProviderIgnoresTheDecisionDefault(t *testing.T) {
	for _, tc := range []struct {
		name string
		keys map[string]string
		want categorize.ProviderKind
	}{
		{"an OpenAI and an Anthropic key", map[string]string{
			"OPENAI_API_KEY": "plain words used as a test value", "ANTHROPIC_API_KEY": "plain words used as a test value",
			"LLM_MODEL": "gpt-5-nano",
		}, categorize.ProviderKindOpenAI},
		{"no generative key", nil, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, name := range decisionTestEnv {
				t.Setenv(name, tc.keys[name])
			}
			today := explainChoiceNow(t)
			t.Setenv("LLM_PROVIDER", "typesafe")
			t.Setenv("TYPESAFE_API_KEY", "plain words used as a test value")
			got := explainChoiceNow(t)
			if got != today || got.kind != tc.want {
				t.Fatalf("with LLM_PROVIDER=typesafe %+v; unset %+v; want kind %q", got, today, tc.want)
			}
			if tc.want == categorize.ProviderKindOpenAI && (got.model != "gpt-5-nano" || !got.available) {
				t.Fatalf("model %q available %v", got.model, got.available)
			}
			if tc.want == "" && (got.err == "" || got.available) {
				t.Fatalf("no generative key: %+v, want a failure", got)
			}
		})
	}
}
