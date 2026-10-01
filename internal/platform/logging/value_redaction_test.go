package logging

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
)

type stringer struct{ text string }

func (s stringer) String() string { return s.text }

// WithValueRedaction removes a caller-supplied value from the message, every
// string, error and Stringer attribute, nested groups, and attributes bound with
// With and WithGroup; text that holds none of it is unchanged.
func TestWithValueRedactionRemovesTheValueEverywhere(t *testing.T) {
	const secret = "planted_login_c6644"
	var buffer bytes.Buffer
	redact := func(text string) string { return strings.ReplaceAll(text, secret, "[REDACTED]") }
	logger := slog.New(WithValueRedaction(slog.NewTextHandler(&buffer, &slog.HandlerOptions{}), redact))

	logger.Error("refused for "+secret,
		"text", "login "+secret,
		"error", errors.New("access denied for "+secret),
		"stringer", stringer{"as " + secret},
		slog.Group("nested", slog.String("inner", secret), slog.Group("deeper", slog.String("x", secret))),
		"count", 3)
	logger.With("bound", secret).WithGroup("g").With("later", "again "+secret).Warn("done")
	logger.Info("unrelated text", "key", "value")

	out := buffer.String()
	if strings.Contains(out, secret) {
		t.Fatalf("the value is still in the log:\n%s", out)
	}
	for _, want := range []string{"refused for [REDACTED]", "access denied for [REDACTED]", "count=3", "unrelated text", "key=value", "bound=[REDACTED]"} {
		if !strings.Contains(out, want) {
			t.Errorf("%q is missing from:\n%s", want, out)
		}
	}
}

func TestWithValueRedactionKeepsTheInnerHandlersLevel(t *testing.T) {
	var buffer bytes.Buffer
	handler := WithValueRedaction(slog.NewTextHandler(&buffer, &slog.HandlerOptions{Level: slog.LevelWarn}), func(s string) string { return s })
	if handler.Enabled(context.Background(), slog.LevelInfo) || !handler.Enabled(context.Background(), slog.LevelError) {
		t.Fatal("Enabled must follow the wrapped handler")
	}
}
