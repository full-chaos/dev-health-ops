//go:build integration

package authschema

import (
	"context"
	"testing"
)

// m11ApplyBefore applies every migration below 0011 and returns 0011 itself.
func m11ApplyBefore(t *testing.T, ctx context.Context, env *migrationOnlyFixture) *Migration {
	t.Helper()
	migrations, err := Migrations()
	if err != nil {
		t.Fatal(err)
	}
	var eleventh *Migration
	for index := range migrations {
		switch {
		case migrations[index].Version < 11:
			if err := applyOne(ctx, env.conn, env.options, migrations[index]); err != nil {
				t.Fatalf("apply %04d: %v", migrations[index].Version, err)
			}
		case migrations[index].Version == 11:
			eleventh = &migrations[index]
		}
	}
	if eleventh == nil {
		t.Fatal("the lineage has no migration 0011")
	}
	return eleventh
}

// m11MigrationExec runs a statement on the migration-only connection inside a
// guarded transaction, the way audit.Commit writes a guarded table.
func m11MigrationExec(env *migrationOnlyFixture) func(context.Context, string, ...any) error {
	return func(ctx context.Context, sql string, args ...any) error {
		tx, err := env.conn.Begin(ctx)
		if err != nil {
			return err
		}
		defer func() { _ = tx.Rollback(ctx) }()
		if _, err := tx.Exec(ctx, `SELECT set_config('dev_health.audit_txid', pg_current_xact_id()::text, true)`); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, sql, args...); err != nil {
			return err
		}
		return tx.Commit(ctx)
	}
}
