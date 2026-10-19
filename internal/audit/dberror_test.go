package audit

import (
	"bytes"
	"context"
	"errors"
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

func logWith(t *testing.T, attr slog.Attr) string {
	t.Helper()
	var buf bytes.Buffer
	logging.NewJSON(&buf, slog.LevelDebug).Error("probe", attr)
	return buf.String()
}

func TestDBErrorNeverLogsDatabaseText(t *testing.T) {
	wrapped := fmt.Errorf("identity: store unavailable: read: %w", pgErrorQuotingInput())
	out := logWith(t, DBError(wrapped))
	if strings.Contains(out, piiEmail) || strings.Contains(out, "invalid input syntax") {
		t.Fatalf("the log carries the database message: %s", out)
	}
	for _, want := range []string{`"sqlstate":"22P02"`, `"class":"22"`} {
		if !strings.Contains(out, want) {
			t.Fatalf("the log lacks %s: %s", want, out)
		}
	}

	constraint := &pgconn.PgError{Severity: "ERROR", Code: "23505", ConstraintName: "users_email_lower_key",
		Message: `duplicate key value violates unique constraint "users_email_lower_key"`, Detail: "Key (email_lower)=(" + piiEmail + ")"}
	out = logWith(t, DBError(constraint))
	if strings.Contains(out, piiEmail) || !strings.Contains(out, `"constraint":"users_email_lower_key"`) {
		t.Fatalf("constraint error logged as %s", out)
	}
}

func TestDBErrorOfANonDatabaseErrorIsAKindNotText(t *testing.T) {
	for name, c := range map[string]struct {
		err  error
		want string
	}{
		"driver text":   {fmt.Errorf("failed to encode args[0]: unable to encode %q into binary format for uuid: %w", piiEmail, errors.New("bad")), `"kind":"*errors.errorString"`},
		"canceled":      {fmt.Errorf("query: %w", context.Canceled), `"kind":"context_canceled"`},
		"deadline":      {fmt.Errorf("query: %w", context.DeadlineExceeded), `"kind":"context_deadline_exceeded"`},
		"joined errors": {errors.Join(errors.New(piiEmail), errors.New("x")), `"kind":"*errors.errorString"`},
	} {
		out := logWith(t, DBError(c.err))
		if strings.Contains(out, piiEmail) || !strings.Contains(out, c.want) {
			t.Errorf("%s: logged %s, want %s and no error text", name, out, c.want)
		}
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
	if strings.Contains(out, piiEmail) || !strings.Contains(out, `"sqlstate":"22P02"`) {
		t.Fatalf("step failure logged as %s", out)
	}
}
