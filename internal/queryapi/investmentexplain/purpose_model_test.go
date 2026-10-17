package investmentexplain

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize"
)

// D6071: LLM_MODEL_EXPLAIN is the platform's explain model. It beats LLM_MODEL
// and the provider-specific names on the platform path, never an explicit
// model or the organization's own model, and it is not applied to an
// organization's own provider.
func TestPlatformExplainModelOrder(t *testing.T) {
	clear := func(t *testing.T) {
		for _, name := range []string{"LLM_MODEL_EXPLAIN", "LLM_MODEL", "LLM_MODEL_OPENAI", "LLM_MODEL_ANTHROPIC", "LLM_PROVIDER"} {
			t.Setenv(name, "")
		}
	}
	kind := categorize.ProviderKindOpenAI

	t.Run("beats LLM_MODEL and the provider-specific name", func(t *testing.T) {
		clear(t)
		t.Setenv("LLM_MODEL", "generic")
		t.Setenv("LLM_MODEL_OPENAI", "specific")
		t.Setenv("LLM_MODEL_EXPLAIN", "explain-model")
		if got, _ := ResolveModelName(kind, ""); got != "explain-model" {
			t.Fatalf("platform path: %q", got)
		}
		if got, _ := ResolveModelNameForOrg(context.Background(), kind, "", "org-1", nil); got != "explain-model" {
			t.Fatalf("org path without BYO: %q", got)
		}
	})
	t.Run("every platform provider kind reads it, Ollama included", func(t *testing.T) {
		clear(t)
		t.Setenv("LLM_MODEL", "generic")
		t.Setenv("LLM_MODEL_EXPLAIN", "explain-model")
		t.Setenv("LLM_MODEL_OLLAMA", "provider-model")
		for _, k := range []categorize.ProviderKind{
			categorize.ProviderKindOpenAI, categorize.ProviderKindAnthropic, categorize.ProviderKindGemini,
			categorize.ProviderKindQwen, categorize.ProviderKindLocal, categorize.ProviderKindOllama,
		} {
			if got, _ := ResolveModelName(k, ""); got != "explain-model" {
				t.Errorf("%s: platform path %q", k, got)
			}
			if got, _ := ResolveModelNameForOrg(context.Background(), k, "", "org-1", nil); got != "explain-model" {
				t.Errorf("%s: org path %q", k, got)
			}
		}
	})
	t.Run("unset leaves the existing order", func(t *testing.T) {
		clear(t)
		t.Setenv("LLM_MODEL", "generic")
		t.Setenv("LLM_MODEL_OPENAI", "specific")
		if got, _ := ResolveModelName(kind, ""); got != "specific" {
			t.Fatalf("got %q", got)
		}
	})
	t.Run("an explicit model wins", func(t *testing.T) {
		clear(t)
		t.Setenv("LLM_MODEL_EXPLAIN", "explain-model")
		if got, _ := ResolveModelName(kind, "asked"); got != "asked" {
			t.Fatalf("got %q", got)
		}
	})
	t.Run("the organization's own model wins", func(t *testing.T) {
		clear(t)
		t.Setenv("LLM_MODEL_EXPLAIN", "explain-model")
		resolver := &fakeOrgResolver{matches: true, model: "org-model"}
		if got, _ := ResolveModelNameForOrg(context.Background(), kind, "", "org-1", resolver); got != "org-model" {
			t.Fatalf("got %q", got)
		}
	})
	t.Run("an organization provider with no model does not get the platform explain model", func(t *testing.T) {
		clear(t)
		t.Setenv("LLM_MODEL_EXPLAIN", "explain-model")
		resolver := &fakeOrgResolver{matches: true, model: ""}
		got, _ := ResolveModelNameForOrg(context.Background(), categorize.ProviderKindAnthropic, "", "org-1", resolver)
		if got == "explain-model" {
			t.Fatalf("BYO anthropic org was given the platform explain model")
		}
	})
	t.Run("the other purposes do not read it", func(t *testing.T) {
		clear(t)
		t.Setenv("LLM_MODEL_EXPLAIN", "explain-model")
		t.Setenv("LLM_MODEL", "generic")
		if got := categorize.ResolveModelName(kind, ""); got != "generic" {
			t.Fatalf("categorize resolved %q", got)
		}
	})
}

// The setting taking effect is visible at Info, with the setting name and the model.
func TestPlatformExplainModelIsLoggedWhenItDecides(t *testing.T) {
	var buf bytes.Buffer
	saved := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	defer slog.SetDefault(saved)
	t.Setenv("LLM_MODEL_EXPLAIN", "explain-model")
	if got, _ := ResolveModelName(categorize.ProviderKindOpenAI, ""); got != "explain-model" {
		t.Fatalf("got %q", got)
	}
	out := buf.String()
	if !strings.Contains(out, "LLM_MODEL_EXPLAIN") || !strings.Contains(out, "explain-model") || !strings.Contains(out, "level=INFO") {
		t.Fatalf("no Info line naming the setting and model: %q", out)
	}
	buf.Reset()
	t.Setenv("LLM_MODEL_EXPLAIN", "")
	t.Setenv("LLM_MODEL", "generic")
	ResolveModelName(categorize.ProviderKindOpenAI, "")
	if strings.Contains(buf.String(), "LLM_MODEL_EXPLAIN") {
		t.Fatalf("logged the setting while unset: %q", buf.String())
	}
}
