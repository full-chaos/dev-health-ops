package admin

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/llmbudget"
)

func TestOperatorMaximumMicroUSD(t *testing.T) {
	defer func(saved func(string) (string, bool)) { lookupEnv = saved }(lookupEnv)
	for _, tc := range []struct {
		name  string
		set   bool
		value string
		want  string
	}{
		{"unset", false, "", "100000000"},
		{"plain", true, "5000", "5000"},
		{"padded", true, " 7 ", "7"},
		{"underscored", true, "1_000", "1000"},
		{"empty", true, "", "0"},
		{"text", true, "abc", "0"},
		{"negative", true, "-5", "0"},
		{"beyond int64", true, "99999999999999999999", "99999999999999999999"},
	} {
		lookupEnv = func(string) (string, bool) { return tc.value, tc.set }
		if got := llmbudget.OperatorMaximum(lookupEnv).String(); got != tc.want {
			t.Errorf("%s: got %s, want %s", tc.name, got, tc.want)
		}
	}
}

func TestMaskAPIKey(t *testing.T) {
	for in, want := range map[string]string{"": "", "1234": "********", "12345678": "********", "123456789": "1234…6789", "ключ-секрет-ключ": "ключ…ключ"} {
		got := maskAPIKey(in)
		if in == "" {
			if got != nil {
				t.Errorf("empty: %v", *got)
			}
			continue
		}
		if got == nil || *got != want {
			t.Errorf("%q: got %v, want %q", in, got, want)
		}
	}
}

// TestBudgetLockKeyMatchesPython pins the advisory-lock key against values
// computed by llm/budget.py's _acquire_advisory_lock formula
// (int.from_bytes(sha256("byo-llm-budget:<org>")[:8], "big") & (1<<63)-1), so
// a Go writer and a Python reservation contend on the same lock.
func TestBudgetLockKeyMatchesPython(t *testing.T) {
	for org, want := range map[string]int64{
		"00000000-0000-0000-0000-000000000000": 734291639604921375,
		"70d529e0-0000-4000-8000-000000000001": 4899195206064546492,
		"3f2b8c1e-5a7d-4e9a-9b1c-2d4e6f8a0b1c": 9008887641742855183,
	} {
		if got := llmbudget.LockKey(org); got != want {
			t.Errorf("budgetLockKey(%s) = %d, python %d", org, got, want)
		}
	}
}

func TestApplyLLMSettingsRefusesABYOProviderWithoutAModel(t *testing.T) {
	key := "sk-rotated-" + "0123456789"
	const want = `{"detail":{"error":"model_required","feature":"byo_llm","message":"model is required when a BYO LLM provider is saved"}}`
	for name, in := range map[string]llmUpsert{
		"model absent":           {provider: "openai"},
		"model empty":            {provider: "openai", model: strPtr("")},
		"model blank":            {provider: "openai", model: strPtr(" \t\n ")},
		"key rotation, no model": {provider: "openai", apiKey: &key},
		"key and base url":       {provider: "anthropic", apiKey: &key, baseURL: strPtr("https://example.invalid/v1")},
		"padded capital":         {provider: "  OpenAI  "},
		"unknown provider":       {provider: "ollama"},
	} {
		in.requireModel = true
		h := &handlers{
			validateURL: func(context.Context, string) (bool, string, error) { return true, "", nil },
			storedKey:   func(context.Context, string) (string, error) { return "", nil },
		}
		out, refusal, err := h.applyLLMSettings(context.Background(), "org", in)
		if err != nil || out != nil || refusal == nil || refusal.status != 400 {
			t.Fatalf("%s: out=%v refusal=%v err=%v", name, out, refusal, err)
		}
		rec := httptest.NewRecorder()
		refusal.write(rec)
		if got := strings.TrimSpace(rec.Body.String()); got != want {
			t.Errorf("%s: body %s, want %s", name, got, want)
		}
		if strings.Contains(rec.Body.String(), key) {
			t.Errorf("%s: body carries the key", name)
		}
	}
}

func TestByoProviderNeedsModel(t *testing.T) {
	for name, tc := range map[string]struct {
		in   llmUpsert
		need bool
	}{
		"openai with model":        {llmUpsert{provider: "openai", model: strPtr("gpt-4o")}, false},
		"model with outer blanks":  {llmUpsert{provider: "openai", model: strPtr(" m ")}, false},
		"auto, no model":           {llmUpsert{provider: "auto"}, false},
		"mock, no model":           {llmUpsert{provider: "MOCK"}, false},
		"none, no model":           {llmUpsert{provider: " none "}, false},
		"blank provider, no model": {llmUpsert{provider: "   "}, false},
		"openai, no model":         {llmUpsert{provider: "openai"}, true},
		"openai, blank model":      {llmUpsert{provider: "openai", model: strPtr("  ")}, true},
	} {
		if got := byoProviderNeedsModel(tc.in); got != tc.need {
			t.Errorf("%s: got %v, want %v", name, got, tc.need)
		}
	}
}

func TestApplyLLMSettingsChecksTheBaseURLBeforeTheModel(t *testing.T) {
	h := &handlers{
		validateURL: func(context.Context, string) (bool, string, error) { return false, "bad host", nil },
		storedKey:   func(context.Context, string) (string, error) { return "", nil },
	}
	_, refusal, err := h.applyLLMSettings(context.Background(), "org", llmUpsert{provider: "openai", baseURL: strPtr("http://x.invalid"), requireModel: true})
	if err != nil || refusal == nil || refusal.status != 400 || refusal.message() != "bad host" {
		t.Fatalf("refusal=%v err=%v", refusal, err)
	}
}
