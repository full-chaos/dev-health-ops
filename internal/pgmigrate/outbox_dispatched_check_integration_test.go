//go:build integration

package pgmigrate_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/pgmigrate"
)

const outboxDispatchedCheck = "ck_sync_dispatch_outbox_dispatched_route_coherence"

// outboxCheckRevision is the chain index of the last revision that redefines the dispatched route CHECK.
func outboxCheckRevision(t *testing.T, chain []pgmigrate.ChainFile) int {
	t.Helper()
	at := -1
	for i, file := range chain {
		if strings.Contains(file.SQL, outboxDispatchedCheck) {
			at = i
		}
	}
	if at < 0 {
		t.Fatalf("no chain revision redefines %s", outboxDispatchedCheck)
	}
	return at
}

// seedOutboxRun writes the integration and sync run an outbox row references and returns the run id.
func seedOutboxRun(t *testing.T, conn *pgx.Conn) string {
	t.Helper()
	ctx := context.Background()
	var integration, run string
	if err := conn.QueryRow(ctx, `INSERT INTO integrations (id, org_id, provider, name, config, is_active, created_at, updated_at)
		VALUES (gen_random_uuid(), 'o', 'github', 'outbox cell', '{}'::json, TRUE, now(), now()) RETURNING id::text`).Scan(&integration); err != nil {
		t.Fatal(err)
	}
	if err := conn.QueryRow(ctx, `INSERT INTO sync_runs (id, org_id, integration_id, triggered_by, mode, status, total_units, completed_units, failed_units, created_at)
		VALUES (gen_random_uuid(), 'o', $1::uuid, 'schedule', 'incremental', 'running', 0, 0, 0, now()) RETURNING id::text`, integration).Scan(&run); err != nil {
		t.Fatal(err)
	}
	return run
}

// insertOutboxRow writes one outbox row of run with the given kind, status, last_error and dispatched route (nil = NULL).
func insertOutboxRow(conn *pgx.Conn, run, kind, status string, lastError, transport *string, generation *int64) error {
	_, err := conn.Exec(context.Background(), `INSERT INTO sync_dispatch_outbox (id, org_id, sync_run_id, kind, status,
		available_at, attempts, last_error, dispatched_transport, dispatched_route_generation, created_at, updated_at)
		VALUES (gen_random_uuid(), 'o', $1::uuid, $2, $3, now(), 0, $4, $5, $6, now(), now())`,
		run, kind, status, lastError, transport, generation)
	return err
}

// insertReproRow writes the ticket's row: dispatched, no last_error, no claim, no transport.
func insertReproRow(conn *pgx.Conn, run string) error {
	_, err := conn.Exec(context.Background(), `INSERT INTO sync_dispatch_outbox (id, org_id, sync_run_id, kind, status, available_at, attempts, created_at, updated_at)
		VALUES (gen_random_uuid(), 'o', $1::uuid, 'post_sync', 'dispatched', now(), 0, now(), now())`, run)
	return err
}

func refusedByOutboxCheck(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23514" && pgErr.ConstraintName == outboxDispatchedCheck
}

func outboxCheckDef(t *testing.T, conn *pgx.Conn) string {
	t.Helper()
	var def string
	if err := conn.QueryRow(context.Background(), `SELECT pg_get_constraintdef(con.oid) FROM pg_constraint con
		JOIN pg_class c ON c.oid = con.conrelid WHERE c.relname = 'sync_dispatch_outbox' AND con.conname = $1`,
		outboxDispatchedCheck).Scan(&def); err != nil {
		t.Fatal(err)
	}
	return def
}

func outboxRows(t *testing.T, conn *pgx.Conn) string {
	t.Helper()
	rows, err := conn.Query(context.Background(), `SELECT concat_ws('|', id, kind, status, coalesce(last_error, '<null>'),
		coalesce(dispatched_transport, '<null>'), coalesce(dispatched_route_generation::text, '<null>'), updated_at)
		FROM sync_dispatch_outbox ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	got, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		t.Fatal(err)
	}
	return strings.Join(got, "\n")
}

// At the head the CHECK refuses a dispatched row with no last_error and no transport (SQLSTATE 23514) and still
// admits each row shape the earlier CHECK admitted through a non-NULL branch.
func TestTheOutboxDispatchedCheckRefusesADispatchedRowWithNoErrorAndNoTransport(t *testing.T) {
	chain, err := pgmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	d := newDownInstance(t)
	conn := connect(t, d.at(t, len(chain)))
	run := seedOutboxRun(t, conn)
	if err := insertReproRow(conn, run); !refusedByOutboxCheck(err) {
		t.Fatalf("dispatched row with last_error NULL and no transport at head %s: err=%v, want SQLSTATE 23514 on %s",
			chain[len(chain)-1].Revision, err, outboxDispatchedCheck)
	}
	featureDisabled, transportLost, river := "feature_disabled", "transport_lost", "river"
	generation := int64(1)
	for _, row := range []struct {
		name, kind, status   string
		lastError, transport *string
		generation           *int64
	}{
		{"dispatched feature_disabled with no transport", "post_sync", "dispatched", &featureDisabled, nil, nil},
		{"dispatched with no error and a transport", "finalize_sync_run", "dispatched", nil, &river, &generation},
		{"dispatched with an error and a transport", "reference_discovery", "dispatched", &transportLost, &river, &generation},
		{"pending with no transport", "dispatch_sync_run", "pending", nil, nil, nil},
	} {
		if err := insertOutboxRow(conn, run, row.kind, row.status, row.lastError, row.transport, row.generation); err != nil {
			t.Errorf("%s: refused at the head: %v", row.name, err)
		}
	}
}

// A refused-shape row stored before the revision makes the up step fail with the count; no row and no CHECK changes.
func TestTheOutboxDispatchedCheckRevisionRefusesWhileARefusedRowExists(t *testing.T) {
	chain, err := pgmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	at := outboxCheckRevision(t, chain)
	d := newDownInstance(t)
	uri := d.at(t, at)
	conn := connect(t, uri)
	run := seedOutboxRun(t, conn)
	if err := insertReproRow(conn, run); err != nil {
		t.Fatalf("the schema before %s refused the repro row: %v", chain[at].Revision, err)
	}
	beforeDef, beforeRows, beforeShape := outboxCheckDef(t, conn), outboxRows(t, conn), schemaShape(t, uri)
	baseline, err := pgmigrate.LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	_, err = pgmigrate.Upgrade(context.Background(), conn, baseline, chain)
	if err == nil || !strings.Contains(err.Error(), "holds 1 dispatched row(s) with no last_error and no transport") ||
		!strings.Contains(err.Error(), chain[at].Name) {
		t.Fatalf("upgrade over a refused-shape row: err=%v, want the guard's count naming %s", err, chain[at].Name)
	}
	if got := outboxCheckDef(t, conn); got != beforeDef {
		t.Fatalf("the refused upgrade changed the CHECK:\n got %s\nwant %s", got, beforeDef)
	}
	if got := outboxRows(t, conn); got != beforeRows {
		t.Fatalf("the refused upgrade changed rows:\n got %s\nwant %s", got, beforeRows)
	}
	if got := schemaShape(t, uri); got != beforeShape {
		t.Fatalf("the refused upgrade changed the schema:\n%s", shapeDiff(beforeShape, got))
	}
}

// Up then down restores the CHECK exactly as a fresh database at the previous revision defines it.
func TestTheOutboxDispatchedCheckDownRestoresThePreviousCheck(t *testing.T) {
	chain, err := pgmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	at := outboxCheckRevision(t, chain)
	d := newDownInstance(t)
	want := outboxCheckDef(t, connect(t, d.at(t, at)))
	uri := d.at(t, len(chain))
	conn := connect(t, uri)
	if got := outboxCheckDef(t, conn); got == want {
		t.Fatalf("revision %s left the CHECK as it was: %s", chain[at].Revision, got)
	}
	code, _, stderr := goDowngrade(t, uri, chain[at-1].Revision)
	if code != cli.ExitOK {
		t.Fatalf("downgrade to %s: exit %d stderr %s", chain[at-1].Revision, code, stderr)
	}
	if got := outboxCheckDef(t, conn); got != want {
		t.Fatalf("after the downgrade the CHECK is\n %s\nwant the fresh %s one\n %s", got, chain[at-1].Revision, want)
	}
	if err := insertReproRow(conn, seedOutboxRun(t, conn)); err != nil {
		t.Fatalf("after the downgrade the previous CHECK must admit the repro row again: %v", err)
	}
}
