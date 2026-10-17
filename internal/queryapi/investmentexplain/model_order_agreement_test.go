package investmentexplain

import (
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize"
)

// Categorize and explain resolve the model through one resolver
// (llmbudget.DefaultModel, D6043 + CHAOS-4978; CHAOS-10011): a configured
// model, then LLM_MODEL_<P>, then LLM_MODEL only when P is LLM_PROVIDER, then
// the package default. Both routes must agree for every env.
func TestCategorizeAndExplainResolveTheSameModel(t *testing.T) {
	cases := []struct {
		name string
		env  map[string]string
		kind categorize.ProviderKind
		want string
	}{
		{"generic of another provider is not used", map[string]string{"LLM_PROVIDER": "openai", "LLM_MODEL": "gpt-5-nano"}, categorize.ProviderKindAnthropic, "claude-haiku-5-5"},
		{"generic with no platform provider is not used for a named kind", map[string]string{"LLM_MODEL": "gpt-5-nano"}, categorize.ProviderKindAnthropic, "claude-haiku-5-5"},
		{"specific beats generic on the platform provider", map[string]string{"LLM_PROVIDER": "openai", "LLM_MODEL": "generic", "LLM_MODEL_OPENAI": "specific"}, categorize.ProviderKindOpenAI, "specific"},
		{"generic applies to the platform provider", map[string]string{"LLM_PROVIDER": "openai", "LLM_MODEL": "generic"}, categorize.ProviderKindOpenAI, "generic"},
		{"specific applies off the platform provider", map[string]string{"LLM_PROVIDER": "openai", "LLM_MODEL": "generic", "LLM_MODEL_ANTHROPIC": "claude-x"}, categorize.ProviderKindAnthropic, "claude-x"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for _, name := range []string{"LLM_PROVIDER", "LLM_MODEL", "LLM_MODEL_OPENAI", "LLM_MODEL_ANTHROPIC"} {
				t.Setenv(name, "")
			}
			for name, value := range tc.env {
				t.Setenv(name, value)
			}
			fromCategorize := categorize.ResolveModelName(tc.kind, "")
			fromExplain, found := ResolveModelName(tc.kind, "")
			if !found || fromCategorize != fromExplain || fromExplain != tc.want {
				t.Fatalf("categorize = %q, explain = %q (found=%v), want both %q", fromCategorize, fromExplain, found, tc.want)
			}
		})
	}
}
