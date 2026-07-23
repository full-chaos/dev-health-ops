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
