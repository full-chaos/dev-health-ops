package llmbudget

import "testing"

func TestOpenAIPlatformDefaultIsGPT6Luna(t *testing.T) {
	none := func(string) (string, bool) { return "", false }
	if got := DefaultModel("openai", none); got != "gpt-6-luna" {
		t.Fatalf("DefaultModel(openai) = %q, want gpt-6-luna", got)
	}
}

func TestOpenAIEnvOverrideBeatsTheDefault(t *testing.T) {
	env := func(values map[string]string) LookupEnv {
		return func(name string) (string, bool) {
			value, ok := values[name]
			return value, ok
		}
	}
	if got := DefaultModel("openai", env(map[string]string{"LLM_MODEL_OPENAI": "gpt-5-mini"})); got != "gpt-5-mini" {
		t.Errorf("LLM_MODEL_OPENAI: got %q, want gpt-5-mini", got)
	}
	generic := map[string]string{"LLM_PROVIDER": "openai", "LLM_MODEL": "gpt-5-nano"}
	if got := DefaultModel("openai", env(generic)); got != "gpt-5-nano" {
		t.Errorf("LLM_MODEL: got %q, want gpt-5-nano", got)
	}
}

func TestGPT6LunaCostUsesItsOwnRow(t *testing.T) {
	cost, err := CostMicroUSD("openai", "gpt-6-luna", "", 1_000_000, 1_000_000, 0, 0)
	if err != nil || cost == nil || *cost != 600_000 {
		t.Fatalf("gpt-6-luna cost = %v (err %v), want 600000", cost, err)
	}
}
