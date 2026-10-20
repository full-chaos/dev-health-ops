package audit

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
)

// piiEmail is an address a database message could quote back.
const piiEmail = "someone@example.com"

// pgErrorQuotingInput is a PostgreSQL error whose MESSAGE carries the input
// that failed to parse, as 22P02 does; Detail carries it too.
func pgErrorQuotingInput() *pgconn.PgError {
	return &pgconn.PgError{
		Severity: "ERROR", Code: "22P02",
		Message: `invalid input syntax for type uuid: "` + piiEmail + `"`,
		Detail:  "Key (email_lower)=(" + piiEmail + ") already exists.",
	}
}

func TestOutboxStepFailureLogsNoDatabaseText(t *testing.T) {
	var buf bytes.Buffer
	s := &scriptedStepper{err: func(int) error { return fmt.Errorf("audit: publishing: %w", pgErrorQuotingInput()) }}
	loop, _, stepped := testLoop(t, s)
	loop.logger = logging.NewJSON(&buf, slog.LevelDebug)
	if err := loop.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = loop.Shutdown(context.Background()) })
	awaitStep(t, stepped) // the first step runs at Start and fails
	out := buf.String()
	if !strings.Contains(out, "outbox step failed") {
		t.Fatalf("no step failure logged: %s", out)
	}
	if strings.Contains(out, piiEmail) || !strings.Contains(out, `"code":"22P02"`) {
		t.Fatalf("step failure logged as %s", out)
	}
}
