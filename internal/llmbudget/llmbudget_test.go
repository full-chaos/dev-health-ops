package llmbudget

import (
	"testing"
	"time"
)

func TestMonthWindow(t *testing.T) {
	cases := []struct {
		now         time.Time
		start, next time.Time
	}{
		{time.Date(2026, 9, 25, 3, 4, 5, 0, time.UTC), time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC), time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)},
		{time.Date(2026, 12, 31, 23, 59, 59, 0, time.UTC), time.Date(2026, 12, 1, 0, 0, 0, 0, time.UTC), time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC)},
		{time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)},
		// A local time is converted to UTC first: 22:00 on the 30th in UTC-5 is 03:00 on the 1st UTC.
		{time.Date(2026, 9, 30, 22, 0, 0, 0, time.FixedZone("x", -5*3600)), time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC), time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)},
	}
	for _, c := range cases {
		start, next := MonthWindow(c.now)
		if !start.Equal(c.start) || !next.Equal(c.next) {
			t.Errorf("MonthWindow(%s) = %s, %s, want %s, %s", c.now, start, next, c.start, c.next)
		}
	}
}

func TestOperatorMaximum(t *testing.T) {
	lookup := func(value string, set bool) LookupEnv {
		return func(string) (string, bool) { return value, set }
	}
	for _, c := range []struct {
		name  string
		value string
		set   bool
		want  string
	}{
		{"unset", "", false, "100000000"},
		{"plain", "5000", true, "5000"},
		{"padded", " 7 ", true, "7"},
		{"underscored", "1_000", true, "1000"},
		{"empty", "", true, "0"},
		{"text", "abc", true, "0"},
		{"negative", "-5", true, "0"},
	} {
		if got := OperatorMaximum(lookup(c.value, c.set)).String(); got != c.want {
			t.Errorf("%s: %s, want %s", c.name, got, c.want)
		}
	}
}

// reliable_price's decision: openai only, on the official endpoint (or the
// acceptance stack's own transport for its scripted model), for a model the
// price book names. The cases are the rows of the venue oracle.
func TestReliablePrice(t *testing.T) {
	for _, c := range []struct {
		provider, model, baseURL string
		want                     bool
		wantErr                  bool
	}{
		{"openai", "gpt-5-mini", "", true, false},
		{"OpenAI", "gpt-5-mini", "", true, false},
		{" openai ", "gpt-5-mini", "", true, false},
		{"openai", "GPT-5-MINI", "", true, false},
		{"openai", "  gpt-5-mini  ", "", true, false},
		{"openai", "gpt-5-mini-2025-08-07", "", true, false},
		{"openai", "gpt-5-nano", "", true, false},
		{"openai", "gpt-5-nano-2026-01-01", "", true, false},
		{"openai", " gpt-5-nano ", "", true, false},
		{"openai", "GPT-5-NANO", "", false, false}, // the borrowed table is case-sensitive
		{"openai", "gpt-5", "", false, false},
		{"anthropic", "gpt-5-mini", "", false, false},
		{"", "", "", false, false},
		{"openai", "gpt-5-mini", "https://api.openai.com", true, false},
		{"openai", "gpt-5-mini", "https://api.openai.com/v1", true, false},
		{"openai", "gpt-5-mini", "https://API.OPENAI.COM/v1", true, false},
		{"openai", "gpt-5-mini", "http://api.openai.com", false, false},
		{"openai", "gpt-5-mini", "https://api.openai.com.evil.example", false, false},
		{"openai", "gpt-5-mini", "https://user@api.openai.com", true, false},
		{"openai", "gpt-5-mini", "   ", true, false},
		{"openai", "gpt-5-mini", "not a url", false, false},
		{"openai", "gpt-5-mini", "https://[::1", false, false},
		{"openai", "gpt-5-mini", "https://example.openai.azure.com", false, false},
		{"openai", "ask-dev-scripted-v1", "", false, false},
		{"openai", "ask-dev-scripted-v1-v2", "http://ask-dev-scripted-openai:8001", false, false},
		{"openai", "ask-dev-scripted-v1", "http://ask-dev-scripted-openai:8001", true, false},
		{"openai", "ask-dev-scripted-v1", "http://ask-dev-scripted-openai:8002", false, false},
		{"openai", "ask-dev-scripted-v1", "https://ask-dev-scripted-openai:8001", false, false},
		{"openai", "ask-dev-scripted-v1", "http://ask-dev-scripted-openai", false, false},
		{"openai", "ask-dev-scripted-v1", "http://ask-dev-scripted-openai:abc", false, true},
	} {
		got, err := ReliablePrice(c.provider, c.model, c.baseURL)
		if (err != nil) != c.wantErr || got != c.want {
			t.Errorf("ReliablePrice(%q, %q, %q) = %v, %v, want %v (err %v)", c.provider, c.model, c.baseURL, got, err, c.want, c.wantErr)
		}
	}
}

// DEFAULT_MODEL_BY_PROVIDER, every entry (llm/providers/base.py).
func TestDefaultModelByProvider(t *testing.T) {
	want := map[string]string{
		"openai":        "gpt-5-mini",
		"anthropic":     "claude-3-haiku-20240307",
		"gemini":        "gemini-3",
		"local":         "llama3.2",
		"ollama":        "llama3.2",
		"lmstudio":      "local-model",
		"qwen":          "qwen-plus",
		"qwen-local":    "qwen2.5:7b",
		"qwen-lmstudio": "local-model",
	}
	if len(DefaultModelByProvider) != len(want) {
		t.Fatalf("DefaultModelByProvider has %d entries, want %d: %v", len(DefaultModelByProvider), len(want), DefaultModelByProvider)
	}
	for provider, model := range want {
		if got := DefaultModelByProvider[provider]; got != model {
			t.Errorf("DefaultModelByProvider[%q] = %q, want %q", provider, got, model)
		}
	}
	if got, present := DefaultModelByProvider[""]; present || got != "" {
		t.Errorf("an empty provider has a default model %q", got)
	}
}
