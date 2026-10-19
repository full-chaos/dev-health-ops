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
	if strings.Contains(out, email) || !strings.Contains(out, `"sqlstate":"22P02"`) || !strings.Contains(out, `"op":"read user by email"`) {
		t.Fatalf("store failure logged as %s", out)
	}
	if !errors.Is(err, ErrStore) || !errors.As(err, new(*pgconn.PgError)) {
		t.Fatalf("returned error %v lost its sentinel or cause", err)
	}
}
