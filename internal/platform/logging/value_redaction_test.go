package logging

import (
	"bytes"
	"context"
	"errors"
	"log"
	"log/slog"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
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

const builtinDefaultProbeEnv = "DEV_HEALTH_LOGGING_BUILTIN_DEFAULT_PROBE"

// A redactor wrapped around slog's own default handler and installed as the
// process default logs the record, redacted, and returns. The record runs in a
// child process, so a record that blocks fails this test at a deadline instead
// of hanging the package.
func TestWithValueRedactionOfTheBuiltinDefaultInstalledAsDefaultLogs(t *testing.T) {
	const secret = "planted_login_d8944"
	if os.Getenv(builtinDefaultProbeEnv) == "1" {
		if !isSlogBuiltinDefault(slog.Default().Handler()) {
			t.Fatalf("the probe process does not start on slog's built-in default handler: %T", slog.Default().Handler())
		}
		redact := func(text string) string { return strings.ReplaceAll(text, secret, "[REDACTED]") }
		restore := InstallDefault(slog.New(WithValueRedaction(slog.Default().Handler(), redact)))
		slog.Default().InfoContext(context.Background(), "probe for "+secret, "login", secret)
		log.Print("std log for " + secret)
		restore()
		_, _ = os.Stdout.WriteString("probe returned\n")
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, os.Args[0], "-test.run=^"+t.Name()+"$", "-test.count=1")
	command.Env = append(os.Environ(), builtinDefaultProbeEnv+"=1")
	output, err := command.CombinedOutput()
	if ctx.Err() != nil {
		t.Fatalf("the probe did not return within 30s: a record through a redactor of slog's built-in default handler, installed as the default, blocked\n%s", output)
	}
	if err != nil {
		t.Fatalf("the probe failed: %v\n%s", err, output)
	}
	out := string(output)
	if strings.Contains(out, secret) {
		t.Fatalf("the value is still in the log:\n%s", out)
	}
	for _, want := range []string{"probe for [REDACTED]", "login=[REDACTED]", "std log for [REDACTED]", "probe returned"} {
		if !strings.Contains(out, want) {
			t.Errorf("%q is missing from:\n%s", want, out)
		}
	}
}

// The check names slog's built-in default and nothing else: a handler built
// by slog's constructors is not it.
func TestIsSlogBuiltinDefaultNamesOnlyTheBuiltinHandler(t *testing.T) {
	var buffer bytes.Buffer
	for _, handler := range []slog.Handler{
		slog.NewTextHandler(&buffer, nil),
		slog.NewJSONHandler(&buffer, nil),
		NewJSON(&buffer, slog.LevelInfo).Handler(),
		WithValueRedaction(slog.NewTextHandler(&buffer, nil), func(text string) string { return text }),
	} {
		if isSlogBuiltinDefault(handler) {
			t.Errorf("%T is taken for slog's built-in default handler", handler)
		}
	}
}
