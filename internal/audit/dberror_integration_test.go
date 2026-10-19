//go:build integration

package audit

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// captureDefault routes slog's default logger (audit.Commit logs through it)
// into a buffer through the auth-service handler for the test's duration.
func captureDefault(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(logging.NewJSON(&buf, slog.LevelDebug))
	t.Cleanup(func() { slog.SetDefault(previous) })
	return &buf
}

// TestCommitFailureLogsNoDatabaseText: a mutation that fails with a
// PostgreSQL error whose message quotes an input value is logged by its
// SQLSTATE, not its text. The error is raised by the server, not built by
// the test.
func TestCommitFailureLogsNoDatabaseText(t *testing.T) {
	ctx := context.Background()
	env := newAuditFixture(t, ctx)
	buf := captureDefault(t)
	err := Commit(ctx, env.pool, env.schema, Mutation{
		Apply: func(ctx context.Context, tx TxOps) error {
			_, err := tx.Exec(ctx, `SELECT $1::text::uuid`, piiEmail)
			return err
		},
		Audit: AuditEvent{EventType: "auth.probe.failed", Outcome: OutcomeError},
		Event: OutboxEvent{AggregateType: "principal", AggregateID: "p", EventType: "principal.probe", IdempotencyKey: "probe:1"},
	})
	if err == nil {
		t.Fatal("the probe mutation succeeded")
	}
	out := buf.String()
	if !strings.Contains(out, "audit: Commit failed") {
		t.Fatalf("no Commit failure logged: %s", out)
	}
	if strings.Contains(out, piiEmail) || !strings.Contains(out, `"sqlstate":"22P02"`) {
		t.Fatalf("Commit failure logged as %s", out)
	}
}

// TestOutboxLagReadFailureLogsNoDatabaseText: the lag gauge's read failure is
// logged by its SQLSTATE; the server's message (which names the relation) is
// not.
func TestOutboxLagReadFailureLogsNoDatabaseText(t *testing.T) {
	ctx := context.Background()
	env := newAuditFixture(t, ctx)
	missing, err := authschema.NewValidatedIdentifier("lag_probe_missing_schema")
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	lag := &lagTracker{pool: env.pool, schema: missing, logger: logging.NewJSON(&buf, slog.LevelDebug)}
	lag.observe(ctx, time.Now())
	out := buf.String()
	if !strings.Contains(out, "outbox lag read failed") {
		t.Fatalf("no lag failure logged: %s", out)
	}
	if strings.Contains(out, "lag_probe_missing_schema") || !strings.Contains(out, `"sqlstate":"42P01"`) {
		t.Fatalf("lag failure logged as %s", out)
	}
}
