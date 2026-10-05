package llmorgsettings

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

func TestCredentialsRedactAPIKey(t *testing.T) {
	creds := Credentials{APIKey: secrets.NewHidden(llmKeySentinel), BaseURL: "https://example.invalid"}
	assertNoKey(t, "Credentials", creds)
	assertNoKey(t, "*Credentials", &creds)
	if creds.APIKey.Reveal() != llmKeySentinel {
		t.Fatal("APIKey field value changed")
	}
}

// A map prints its entries through an unexported field, so only a top-level
// print of orgSettings is guarded; the test pins that.
func TestOrgSettingsRedactAPIKey(t *testing.T) {
	settings := orgSettings{keyAPIKey: llmKeySentinel, keyProvider: "openai"}
	for _, verb := range []string{"%v", "%+v", "%#v", "%s", "%q"} {
		for name, subject := range map[string]any{"value": settings, "slice": []any{settings}} {
			out := fmt.Sprintf(verb, subject)
			if strings.Contains(out, llmKeySentinel) || !strings.Contains(out, "[REDACTED]") || !strings.Contains(out, "openai") {
				t.Errorf("%s %s: %s", name, verb, out)
			}
		}
	}
	for _, newHandler := range []func(*bytes.Buffer) slog.Handler{
		func(b *bytes.Buffer) slog.Handler { return slog.NewTextHandler(b, nil) },
		func(b *bytes.Buffer) slog.Handler { return slog.NewJSONHandler(b, nil) },
	} {
		var anyBuf, groupBuf bytes.Buffer
		slog.New(newHandler(&anyBuf)).Info("m", slog.Any("s", settings))
		slog.New(newHandler(&groupBuf)).Info("m", slog.Group("g", slog.Any("s", settings)))
		for _, out := range []string{anyBuf.String(), groupBuf.String()} {
			if strings.Contains(out, llmKeySentinel) || !strings.Contains(out, "REDACTED") {
				t.Errorf("slog output: %s", out)
			}
		}
	}
	if settings[keyAPIKey] != llmKeySentinel {
		t.Fatal("the stored value changed")
	}
	if got := fmt.Sprint(orgSettings{keyProvider: "openai"}); strings.Contains(got, "REDACTED") {
		t.Errorf("no api_key row, yet a marker: %s", got)
	}
}
