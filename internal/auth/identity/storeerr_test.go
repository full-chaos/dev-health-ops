package identity

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
)

// TestStoreErrLogsNoDatabaseText: a PostgreSQL message can quote an input
// value; the store failure log names the SQLSTATE, never the text. The
// returned error still wraps ErrStore and the PostgreSQL error.
func TestStoreErrLogsNoDatabaseText(t *testing.T) {
	const email = "someone@example.com"
	pgErr := &pgconn.PgError{Severity: "ERROR", Code: "22P02", Message: `invalid input syntax for type uuid: "` + email + `"`}
	var buf bytes.Buffer
	s := &Store{logger: logging.NewJSON(&buf, slog.LevelDebug)}
	err := s.storeErr(context.Background(), "read user by email", pgErr)
	out := buf.String()
	if strings.Contains(out, email) || !strings.Contains(out, `"code":"22P02"`) || !strings.Contains(out, `"op":"read user by email"`) {
		t.Fatalf("store failure logged as %s", out)
	}
	if !errors.Is(err, ErrStore) || !errors.As(err, new(*pgconn.PgError)) {
		t.Fatalf("returned error %v lost its sentinel or cause", err)
	}
}

// panickingUnwrapError is a code-produced error whose Unwrap panics.
type panickingUnwrapError struct{}

func (panickingUnwrapError) Error() string { return "wrapper" }
func (panickingUnwrapError) Unwrap() error { panic("unwrap panic") }

// TestStoreErrNeverPanicsOnAnError: classifying an error for the log must not
// turn a failed operation into a panic.
func TestStoreErrNeverPanicsOnAnError(t *testing.T) {
	var buf bytes.Buffer
	s := &Store{logger: logging.NewJSON(&buf, slog.LevelDebug)}
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("storeErr panicked: %v", r)
		}
	}()
	err := s.storeErr(context.Background(), "probe", panickingUnwrapError{})
	if !errors.Is(err, ErrStore) {
		t.Fatalf("returned %v", err)
	}
}

// TestStoreErrNeverLogsAMalformedSQLState: a PostgreSQL error code is logged
// only when it is a SQLSTATE (five digits or capitals); any other value is
// text and stays out of the log.
func TestStoreErrNeverLogsAMalformedSQLState(t *testing.T) {
	const email = "someone@example.com"
	var buf bytes.Buffer
	s := &Store{logger: logging.NewJSON(&buf, slog.LevelDebug)}
	_ = s.storeErr(context.Background(), "probe", &pgconn.PgError{Severity: "ERROR", Code: email, Message: "m"})
	out := buf.String()
	if strings.Contains(out, email) || strings.Contains(out, `"so"`) {
		t.Fatalf("a malformed code reached the log: %s", out)
	}
}
