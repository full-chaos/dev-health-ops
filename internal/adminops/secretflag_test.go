package adminops

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"testing"
)

const secretFlagSentinel = "sentinel-value-8716-not-a-key"

func TestSecretOptStringPrintsNoValue(t *testing.T) {
	flags := flag.NewFlagSet("t", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	var key secretOptString
	flags.Var(&key, "api-key", "")
	if err := flags.Parse([]string{"--api-key", secretFlagSentinel}); err != nil {
		t.Fatal(err)
	}
	if key.ptr() == nil || *key.ptr() != secretFlagSentinel || !key.wasSet() || !key.value.Configured() {
		t.Fatal("the flag no longer carries the real value to the settings call")
	}
	holder := struct{ flagValue any }{&key}
	subjects := map[string]any{"value": key, "ptr": &key, "unexported holder": holder, "flag.Value": flag.Value(&key)}
	for name, subject := range subjects {
		outputs := map[string]string{"Sprint": fmt.Sprint(subject)}
		for _, verb := range []string{"%v", "%+v", "%#v", "%s", "%q", "%d", "%x"} {
			outputs[verb] = fmt.Sprintf(verb, subject)
		}
		for label, newHandler := range map[string]func(*bytes.Buffer) slog.Handler{
			"text": func(b *bytes.Buffer) slog.Handler { return slog.NewTextHandler(b, nil) },
			"json": func(b *bytes.Buffer) slog.Handler { return slog.NewJSONHandler(b, nil) },
		} {
			var anyBuf, groupBuf bytes.Buffer
			slog.New(newHandler(&anyBuf)).Info("m", slog.Any("k", subject))
			slog.New(newHandler(&groupBuf)).Info("m", slog.Group("g", slog.Any("k", subject)))
			outputs["slog.Any/"+label], outputs["slog.Group/"+label] = anyBuf.String(), groupBuf.String()
		}
		for label, out := range outputs {
			if strings.Contains(out, secretFlagSentinel) {
				t.Errorf("%s %s leaks the value: %s", name, label, out)
			}
		}
	}
	if got := fmt.Sprintf("%+v", key); !strings.Contains(got, "[REDACTED]") {
		t.Errorf("%%+v lacks the marker: %s", got)
	}
	var unset secretOptString
	if unset.ptr() != nil || unset.wasSet() {
		t.Error("an unset flag reports a value")
	}
}
