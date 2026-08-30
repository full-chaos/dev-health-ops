package logging

import (
	"bytes"
	"errors"
	"log/slog"
	"strings"
	"testing"
)

func TestJSONLoggerRedactsSensitiveKeysDSNsAndErrorText(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	logger := NewJSON(&output, slog.LevelDebug)
	logger.Error(
		"dependency failed",
		"token", "literal-token",
		"safe_error", errors.New("dial postgres://worker:database-secret@db/app: refused"),
		"upstream", "https://api-user:api-secret@example.test/path",
		"clickhouse_error", errors.New("dial https://ch.internal/db?password=query-secret: refused"),
	)

	logLine := output.String()
	for _, forbidden := range []string{
		"literal-token",
		"database-secret",
		"postgres://",
		"api-user",
		"api-secret",
		"query-secret",
	} {
		if strings.Contains(logLine, forbidden) {
			t.Fatalf("log leaked %q: %s", forbidden, logLine)
		}
	}
	if strings.Count(logLine, redacted) < 3 {
		t.Fatalf("expected redaction markers: %s", logLine)
	}
}

func TestRedactTextLeavesSafeOperationalMessageIntact(t *testing.T) {
	t.Parallel()

	message := "queue-control dependency timed out after 2s"
	if got := RedactText(message); got != message {
		t.Fatalf("safe message changed: %q", got)
	}
}

// TestRedactTextCatchesBareCredentialShapedText pins the CHAOS-4582 codex
// review gap: dsnPattern/credentialURLPattern only match URL-shaped text, so
// a credential embedded OUTSIDE a URL -- a raw request/response header, or a
// bare "token=" pair -- passed through unredacted. Untrusted third-party
// response bodies are exactly the kind of free-form text that can contain
// this shape without ever looking like a URL.
func TestRedactTextCatchesBareCredentialShapedText(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		input  string
		secret string
	}{
		{"authorization header", "rejected: Authorization: Bearer sk-abcdef0123456789", "sk-abcdef0123456789"},
		{"bare bearer token", "upstream sent Bearer eyJhbGciOiJIUzI1NiJ9.secret.sig", "eyJhbGciOiJIUzI1NiJ9.secret.sig"},
		{"bare token= pair", "config error: token=super-secret-value rejected", "super-secret-value"},
		{"bare access_token= pair", "oauth failed access_token=abc123secret", "abc123secret"},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			got := RedactText(testCase.input)
			if strings.Contains(got, testCase.secret) {
				t.Fatalf("RedactText(%q) = %q, leaked the secret", testCase.input, got)
			}
			if !strings.Contains(got, redacted) {
				t.Fatalf("RedactText(%q) = %q, want a %s marker", testCase.input, got, redacted)
			}
		})
	}
}
