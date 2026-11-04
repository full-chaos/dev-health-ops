package logging_test

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
)

func logOne(t *testing.T, args ...any) string {
	t.Helper()
	var buf bytes.Buffer
	logging.NewJSON(&buf, slog.LevelInfo).Info("m", args...)
	return buf.String()
}

func TestBooleanFlagSessionsStubStaysVisible(t *testing.T) {
	t.Parallel()
	for _, want := range []bool{true, false} {
		got := logOne(t, "sessions_stub", want)
		wantText := `"sessions_stub":true`
		if !want {
			wantText = `"sessions_stub":false`
		}
		if !strings.Contains(got, wantText) {
			t.Fatalf("bool %v logged %s; want %s", want, got, wantText)
		}
	}
}

func TestBooleanFlagSessionsStubWithAttrsKeepsBool(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	logging.NewJSON(&buf, slog.LevelInfo).With("sessions_stub", true).Info("m")
	if !strings.Contains(buf.String(), `"sessions_stub":true`) {
		t.Fatalf("With attr logged %s", buf.String())
	}
}

func TestBooleanFlagExemptionIsExactKeyAndBoolOnly(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		args []any
		bad  string
	}{
		{"string value", []any{"sessions_stub", "abc-secret-value"}, "abc-secret-value"},
		{"string true", []any{"sessions_stub", "true"}, `"sessions_stub":"true"`},
		{"int value", []any{"sessions_stub", 123456789}, "123456789"},
		{"bytes value", []any{"sessions_stub", []byte("abc-secret-value")}, "abc-secret-value"},
		{"any struct", []any{"sessions_stub", struct{ A string }{"abc-secret-value"}}, "abc-secret-value"},
		{"Sessions_stub", []any{"Sessions_stub", true}, `"Sessions_stub":true`},
		{"SessionsStub", []any{"SessionsStub", true}, `"SessionsStub":true`},
		{"sessions-stub", []any{"sessions-stub", true}, `"sessions-stub":true`},
		{"session_stub", []any{"session_stub", true}, `"session_stub":true`},
		{"sessions_stub_extra", []any{"sessions_stub_token", true}, `"sessions_stub_token":true`},
		{"under protected group", []any{slog.Group("session", slog.Bool("sessions_stub", true))}, `"sessions_stub":true`},
	}
	for _, tc := range cases {
		if got := logOne(t, tc.args...); strings.Contains(got, tc.bad) {
			t.Errorf("%s: logged %s; must not contain %s", tc.name, got, tc.bad)
		}
	}
}

func TestBooleanFlagExemptionDoesNotOpenAnyCredentialKey(t *testing.T) {
	t.Parallel()
	keys := []string{"token", "secret", "password", "passwd", "credential", "authorization", "apikey", "api_key",
		"private_key", "client_secret", "access_key", "cookie", "session", "sessions", "session_token", "sessions_cookie",
		"database_url", "dsn", "bearer", "auth", "access_token", "signature", "cert", "pem", "header", "uri"}
	for _, key := range keys {
		for _, value := range []any{true, false} {
			got := logOne(t, key, value)
			if strings.Contains(got, `"`+key+`":true`) || strings.Contains(got, `"`+key+`":false`) {
				t.Errorf("key %s value %v reached the log: %s", key, value, got)
			}
		}
	}
}

func TestBooleanFlagExemptionLeavesSharedKeyRuleUnchanged(t *testing.T) {
	t.Parallel()
	if !logging.ProtectedKey("sessions_stub") {
		t.Fatal("ProtectedKey(sessions_stub) must stay true: the rule is shared with spans, JSON keys and text")
	}
	var buf bytes.Buffer
	logging.NewJSON(&buf, slog.LevelInfo).Info("m", "body", map[string]any{"sessions_stub": true})
	if strings.Contains(buf.String(), `"sessions_stub":true`) {
		t.Fatalf("JSON object key under a value was opened: %s", buf.String())
	}
	if got := logging.RedactText("sessions_stub=abc-secret-value"); strings.Contains(got, "abc-secret-value") {
		t.Fatalf("free text kept the value: %s", got)
	}
}
