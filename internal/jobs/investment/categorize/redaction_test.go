package categorize

import (
	"bytes"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

const llmKeySentinel = "sentinel-value-8716-not-a-key"

// assertNoKey formats subject every way the ticket names and fails if the
// sentinel appears in any output.
func assertNoKey(t *testing.T, name string, subject any) {
	t.Helper()
	outputs := map[string]string{
		"%v": fmt.Sprintf("%v", subject), "%+v": fmt.Sprintf("%+v", subject),
		"%#v": fmt.Sprintf("%#v", subject), "%s": fmt.Sprintf("%s", subject),
		"%d": fmt.Sprintf("%d", subject), "%x": fmt.Sprintf("%x", subject),
		"%q": fmt.Sprintf("%q", subject), "Sprint": fmt.Sprint(subject),
	}
	for label, handler := range map[string]func(*bytes.Buffer) slog.Handler{
		"text": func(b *bytes.Buffer) slog.Handler { return slog.NewTextHandler(b, nil) },
		"json": func(b *bytes.Buffer) slog.Handler { return slog.NewJSONHandler(b, nil) },
	} {
		var anyBuf, groupBuf bytes.Buffer
		slog.New(handler(&anyBuf)).Info("m", slog.Any("subject", subject))
		slog.New(handler(&groupBuf)).Info("m", slog.Group("g", slog.Any("subject", subject)))
		outputs["slog.Any/"+label] = anyBuf.String()
		outputs["slog.Group/"+label] = groupBuf.String()
	}
	wrapped := fmt.Errorf("op failed: %v", subject)
	outputs["error %v"] = wrapped.Error()
	outputs["error %w"] = fmt.Errorf("op failed: %w", wrapped).Error()
	holder := struct{ hidden any }{subject}
	holderPtr := &holder
	for _, verb := range []string{"%v", "%+v", "%#v", "%s", "%q", "%d", "%x"} {
		outputs["unexported holder "+verb] = fmt.Sprintf(verb, holder)
		outputs["unexported holder ptr "+verb] = fmt.Sprintf(verb, holderPtr)
		outputs["slice "+verb] = fmt.Sprintf(verb, []any{subject})
	}
	for label, out := range outputs {
		if strings.Contains(out, llmKeySentinel) {
			t.Errorf("%s: %s output leaks the API key: %s", name, label, out)
		}
		if out == "" {
			t.Errorf("%s: %s output is empty", name, label)
		}
	}
	valuer, ok := subject.(slog.LogValuer)
	if !ok {
		t.Errorf("%s: does not implement slog.LogValuer", name)
	} else if got := valuer.LogValue().String(); strings.Contains(got, llmKeySentinel) || !strings.Contains(got, "[REDACTED]") {
		t.Errorf("%s: LogValue is %q", name, got)
	}
	if !strings.Contains(fmt.Sprintf("%+v", subject), "[REDACTED]") {
		t.Errorf("%s: %%+v does not show the redaction marker", name)
	}
}

func TestProviderStructsRedactAPIKey(t *testing.T) {
	ollama := OllamaProviderConfig{APIKey: secrets.NewHidden(llmKeySentinel)}
	subjects := map[string]any{
		"OllamaProviderConfig": ollama, "*OllamaProviderConfig": &ollama,
		"OllamaProvider":        NewOllamaProvider(ollama),
		"OllamaProvider(value)": *NewOllamaProvider(ollama),
	}
	for _, kind := range []ProviderKind{ProviderKindOpenAI, ProviderKindAnthropic, ProviderKindGemini, ProviderKindQwen, ProviderKindLocal} {
		cfg := SharedProviderConfig{Kind: kind, APIKey: secrets.NewHidden(llmKeySentinel), BaseURL: "https://provider.example/v1", Model: "m"}
		shared, err := NewSharedProvider(cfg)
		if err != nil {
			t.Fatal(err)
		}
		subjects["SharedProviderConfig/"+string(kind)] = cfg
		subjects["*SharedProviderConfig/"+string(kind)] = &cfg
		subjects["SharedProvider/"+string(kind)] = shared
		subjects["SharedProvider(value)/"+string(kind)] = *shared
	}
	for name, subject := range subjects {
		assertNoKey(t, name, subject)
	}
}

func TestProviderConfigStillCarriesRealKey(t *testing.T) {
	shared, err := NewSharedProvider(SharedProviderConfig{Kind: ProviderKindOpenAI, APIKey: secrets.NewHidden(llmKeySentinel)})
	if err != nil {
		t.Fatal(err)
	}
	if got := shared.cfg.APIKey.Reveal(); got != llmKeySentinel {
		t.Fatalf("Shared cfg.APIKey changed: %q", got)
	}
	if got := NewOllamaProvider(OllamaProviderConfig{APIKey: secrets.NewHidden(llmKeySentinel)}).cfg.APIKey.Reveal(); got != llmKeySentinel {
		t.Fatalf("Ollama cfg.APIKey changed: %q", got)
	}
}
