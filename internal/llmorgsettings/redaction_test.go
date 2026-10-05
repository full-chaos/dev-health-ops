package llmorgsettings

import (
	"bytes"
	"fmt"
	"log/slog"
	"strings"
	"testing"
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
	creds := Credentials{APIKey: llmKeySentinel, BaseURL: "https://example.invalid"}
	assertNoKey(t, "Credentials", creds)
	assertNoKey(t, "*Credentials", &creds)
	if creds.APIKey != llmKeySentinel {
		t.Fatal("APIKey field value changed")
	}
}
