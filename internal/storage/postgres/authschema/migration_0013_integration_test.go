//go:build integration

package authschema

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// m13Armed runs fn in one transaction armed as audit.Commit arms it, as the
// runtime role (search_path pg_catalog), and commits.
func m13Armed(t *testing.T, ctx context.Context, pool *pgxpool.Pool, fn func(pgx.Tx) error) error {
	t.Helper()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SELECT set_config('dev_health.audit_txid', pg_current_xact_id()::text, true),
		set_config('dev_health.audit_changed', pg_current_xact_id()::text || ':0', true)`); err != nil {
		t.Fatal(err)
	}
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func TestMigration0013RevisionCounterRules(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newM10Fixture(t, ctx)
	bump := func(aggregateType, id string, revision int64) error {
		return m13Armed(t, ctx, env.runtime, func(tx pgx.Tx) error {
			_, err := tx.Exec(ctx, `INSERT INTO auth.aggregate_revisions (aggregate_type, aggregate_id, revision) VALUES ($1, $2, $3)
				ON CONFLICT (aggregate_type, aggregate_id) DO UPDATE SET revision = EXCLUDED.revision`, aggregateType, id, revision)
			return err
		})
	}
	if err := bump("principal", "prn_00000000-0000-4000-8000-000000000001", 1); err != nil {
		t.Fatalf("a counter row inside an armed transaction was refused: %v", err)
	}
	if err := bump("principal", "prn_00000000-0000-4000-8000-000000000001", 9007199254740991); err != nil {
		t.Fatalf("revision 2^53-1 was refused: %v", err)
	}
	m10Fail(t, bump("principal", "prn_00000000-0000-4000-8000-000000000002", 0), "revision 0")
	m10Fail(t, bump("principal", "prn_00000000-0000-4000-8000-000000000002", 9007199254740992), "revision 2^53")
	m10Fail(t, bump("session", "prn_00000000-0000-4000-8000-000000000002", 1), "an aggregate type outside provisioning-event.v1")
	m10Fail(t, bump("principal", "00000000-0000-4000-8000-000000000002", 1), "a raw uuid aggregate id")

	// Guarded: no write outside an armed transaction.
	if _, err := env.runtime.Exec(ctx, `INSERT INTO auth.aggregate_revisions (aggregate_type, aggregate_id, revision)
		VALUES ('principal', 'prn_00000000-0000-4000-8000-000000000003', 1)`); err == nil {
		t.Error("a counter row was written outside audit.Commit")
	}
	if _, err := env.runtime.Exec(ctx, `UPDATE auth.aggregate_revisions SET revision = 1`); err == nil {
		t.Error("a counter row was rewound outside audit.Commit")
	}
	// Never deleted by the runtime, even inside an armed transaction: a
	// deleted row restarts an aggregate at revision 1.
	m10Fail(t, m13Armed(t, ctx, env.runtime, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `DELETE FROM auth.aggregate_revisions`)
		return err
	}), "a runtime DELETE of counter rows")
	m10Fail(t, m13Armed(t, ctx, env.runtime, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `TRUNCATE auth.aggregate_revisions`)
		return err
	}), "a runtime TRUNCATE of counter rows")
}

func TestMigration0013CreateGuardCountsInsertedRowsOnly(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newM10Fixture(t, ctx)
	counted := func(sql string, args ...any) string {
		t.Helper()
		var got string
		if err := m13Armed(t, ctx, env.runtime, func(tx pgx.Tx) error {
			if _, err := tx.Exec(ctx, sql, args...); err != nil {
				return err
			}
			return tx.QueryRow(ctx, `SELECT split_part(current_setting('dev_health.audit_changed', true), ':', 2)`).Scan(&got)
		}); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
		return got
	}
	id := "00000000-0000-4000-8000-0000000000e1"
	if got := counted(`INSERT INTO auth.principals (id, kind) VALUES ($1, 'user')`, id); got != "1" {
		t.Errorf("a new principal row counted %q, want 1", got)
	}
	if got := counted(`INSERT INTO auth.users (principal_id, email, email_lower) VALUES ($1, 'e1@example.com', 'e1@example.com')`, id); got != "1" {
		t.Errorf("a new user row counted %q, want 1", got)
	}
	if got := counted(`INSERT INTO auth.principals (id, kind) VALUES ($1, 'user') ON CONFLICT (id) DO NOTHING`, id); got != "0" {
		t.Errorf("an insert that inserted nothing counted %q, want 0", got)
	}

	// Outside an armed transaction the tables stay unguarded and nothing is
	// counted.
	tx, err := env.runtime.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `INSERT INTO auth.principals (id, kind) VALUES ('00000000-0000-4000-8000-0000000000e2', 'user')`); err != nil {
		t.Fatalf("an unarmed principal insert was refused: %v", err)
	}
	var counter *string
	if err := tx.QueryRow(ctx, `SELECT current_setting('dev_health.audit_changed', true)`).Scan(&counter); err != nil {
		t.Fatal(err)
	}
	if counter != nil && *counter != "" {
		t.Errorf("an unarmed insert set the changed counter to %q", *counter)
	}
}
