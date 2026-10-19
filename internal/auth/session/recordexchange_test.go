package session

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"go.opentelemetry.io/otel/metric/noop"

	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
)

// TestFailedExchangeLogsNoDatabaseText: a PostgreSQL message can quote an
// input value; the exchange failure log names the SQLSTATE, never the text.
func TestFailedExchangeLogsNoDatabaseText(t *testing.T) {
	const email = "someone@example.com"
	counter, err := noop.NewMeterProvider().Meter("test").Int64Counter("x")
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	s := &Store{logger: logging.NewJSON(&buf, slog.LevelDebug), exchanges: counter}
	pgErr := &pgconn.PgError{Severity: "ERROR", Code: "22P02", Message: `invalid input syntax for type uuid: "` + email + `"`}
	s.recordExchange(context.Background(), Result{Reason: ReasonStoreUnavailable}, fmt.Errorf("%w: refresh exchange: %w", ErrStore, pgErr))
	out := buf.String()
	if strings.Contains(out, email) || !strings.Contains(out, `"sqlstate":"22P02"`) || !strings.Contains(out, `"reason_code":"store_unavailable"`) {
		t.Fatalf("exchange failure logged as %s", out)
	}
}
