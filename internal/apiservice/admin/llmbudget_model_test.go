package admin

import "testing"

// The budget status prices the model the runtime would use: with LLM_PROVIDER
// unset, OPENAI_API_KEY set and a generic LLM_MODEL, an OpenAI org with no
// stored model runs LLM_MODEL (CHAOS-10011), so the status must price it too.
func TestBudgetStatusDefaultModelFollowsTheAutoDetectedPlatformProvider(t *testing.T) {
	for _, name := range []string{"LLM_PROVIDER", "LLM_MODEL", "LLM_MODEL_OPENAI", "LLM_MODEL_ANTHROPIC", "ANTHROPIC_API_KEY", "GEMINI_API_KEY", "QWEN_API_KEY", "DASHSCOPE_API_KEY", "LLM_API_KEY"} {
		t.Setenv(name, "")
	}
	t.Setenv("OPENAI_API_KEY", "plain words used as a test value")
	t.Setenv("LLM_MODEL", "gpt-5-nano")
	if got := budgetStatusDefaultModel("openai"); got != "gpt-5-nano" {
		t.Fatalf("openai status model = %q, want the generic LLM_MODEL of the auto-detected platform provider", got)
	}
	if got := budgetStatusDefaultModel("anthropic"); got != "claude-haiku-5-5" {
		t.Fatalf("anthropic status model = %q, want its default, not another provider's LLM_MODEL", got)
	}
}
