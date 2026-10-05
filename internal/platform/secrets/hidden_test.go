package secrets

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"testing"
)

const hiddenSentinel = "sentinel-value-8716-not-a-key"

type hiddenHolder struct {
	Exported Hidden
	hidden   Hidden
	ptr      *Hidden
}

func TestHiddenNeverPrintsCredential(t *testing.T) {
	h := NewHidden(hiddenSentinel)
	holder := hiddenHolder{h, h, &h}
	for _, verb := range []string{"%v", "%+v", "%#v", "%s", "%q", "%d", "%x", "%t", "%8v"} {
		for name, subject := range map[string]any{"value": h, "holder": holder, "holder ptr": &holder, "ptr": &h} {
			if out := fmt.Sprintf(verb, subject); strings.Contains(out, hiddenSentinel) {
				t.Errorf("%s %s leaks: %s", name, verb, out)
			}
		}
	}
	raw, err := json.Marshal(holder)
	if err != nil || strings.Contains(string(raw), hiddenSentinel) {
		t.Errorf("json: %v %s", err, raw)
	}
}

func TestHiddenEmptyStaysEmptyAndRevealWorks(t *testing.T) {
	if got := NewHidden("").String(); got != "" {
		t.Errorf("empty Hidden prints %q", got)
	}
	var zero Hidden
	if zero.Reveal() != "" || zero.Configured() {
		t.Error("zero Hidden is configured")
	}
	if got := NewHidden(hiddenSentinel); got.Reveal() != hiddenSentinel || !got.Configured() || got.String() != "[REDACTED]" {
		t.Error("Hidden does not round-trip or does not redact")
	}
	if RedactString("") != "" || RedactString("x") != "[REDACTED]" || RedactPtr(nil) != nil {
		t.Error("RedactString/RedactPtr")
	}
}

type formatProbe struct{ A, B int }

func (p formatProbe) Format(state fmt.State, verb rune) {
	FormatRedacted(state, verb, struct{ A, B int }(p))
}

func TestFormatRedactedPassesVerbAndFlags(t *testing.T) {
	p := formatProbe{1, 2}
	if got := fmt.Sprintf("%v", p); got != "{1 2}" {
		t.Errorf("%%v = %q", got)
	}
	if got := fmt.Sprintf("%+v", p); got != "{A:1 B:2}" {
		t.Errorf("%%+v = %q", got)
	}
	if got := fmt.Sprintf("%x", p); got != "{1 2}" {
		t.Errorf("%%x = %q", got)
	}
}

func TestBareHiddenInSlogShowsMarkerOrNothing(t *testing.T) {
	for _, tc := range []struct {
		name, key, want string
	}{{"set", hiddenSentinel, "[REDACTED]"}, {"empty", "", `""`}} {
		for handlerName, newHandler := range map[string]func(*bytes.Buffer) slog.Handler{
			"text": func(b *bytes.Buffer) slog.Handler { return slog.NewTextHandler(b, nil) },
			"json": func(b *bytes.Buffer) slog.Handler { return slog.NewJSONHandler(b, nil) },
		} {
			var anyBuf, groupBuf bytes.Buffer
			slog.New(newHandler(&anyBuf)).Info("m", slog.Any("k", NewHidden(tc.key)))
			slog.New(newHandler(&groupBuf)).Info("m", slog.Group("g", slog.Any("k", NewHidden(tc.key))))
			for form, out := range map[string]string{"Any": anyBuf.String(), "Group": groupBuf.String()} {
				if strings.Contains(out, hiddenSentinel) {
					t.Errorf("%s/%s/%s leaks: %s", tc.name, handlerName, form, out)
				}
				if tc.name == "set" && !strings.Contains(out, tc.want) {
					t.Errorf("%s/%s/%s lacks the marker: %s", tc.name, handlerName, form, out)
				}
				if tc.name == "empty" && strings.Contains(out, "REDACTED") {
					t.Errorf("%s/%s/%s shows the marker for an empty key: %s", tc.name, handlerName, form, out)
				}
			}
		}
	}
}
