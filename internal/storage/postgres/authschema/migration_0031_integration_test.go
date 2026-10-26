//go:build integration

package authschema

import (
	"context"
	"strings"
	"testing"
	"time"
)

// TestMigration0031ValidatesThePinCheckOnlyWithoutUnrevokedPinlessRows: 0031
// refuses while an unrevoked session has no pin (a count, no ids), and passes
// with a pinned live row and a revoked pinless row, after which the check is
// validated.
func TestMigration0031ValidatesThePinCheckOnlyWithoutUnrevokedPinlessRows(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	env := newM10Fixture(t, ctx)
	migrations, err := Migrations()
	if err != nil || len(migrations) < 31 || migrations[30].Version != 31 {
		t.Fatalf("the lineage has no migration 0031: %v", err)
	}
	sql := migrations[30].SQL
	exec := func(stmt string) {
		t.Helper()
		if _, err := env.admin.Exec(ctx, stmt); err != nil {
			t.Fatalf("%q: %v", stmt, err)
		}
	}
	run0031 := func() error {
		tx, err := env.admin.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		if _, err := tx.Exec(ctx, `SELECT set_config('search_path', 'auth', true)`); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(ctx, sql); err != nil {
			return err
		}
		return tx.Commit(ctx)
	}
	validated := func() bool {
		t.Helper()
		var v bool
		if err := env.admin.QueryRow(ctx, `SELECT convalidated FROM pg_constraint WHERE conname = 'sessions_pin_present' AND conrelid = 'auth.sessions'::regclass`).Scan(&v); err != nil {
			t.Fatal(err)
		}
		return v
	}
	exec(`INSERT INTO auth.principals (id, kind, display_name) VALUES ('` + m10Principal + `', 'user', 'm31')`)
	exec(`ALTER TABLE auth.sessions DROP CONSTRAINT sessions_pin_present`)
	exec(`INSERT INTO auth.sessions (principal_id, token_hash, expires_at, legacy_token_version) VALUES ('` + m10Principal + `', '\x30'::bytea, 'infinity', 0)`)
	exec(`INSERT INTO auth.sessions (principal_id, token_hash, expires_at) VALUES ('` + m10Principal + `', '\x31'::bytea, 'infinity')`)
	exec(`ALTER TABLE auth.sessions ADD CONSTRAINT sessions_pin_present
		CHECK (legacy_token_version IS NOT NULL OR revoked_at IS NOT NULL) NOT VALID`)
	err = run0031()
	if err == nil || !strings.Contains(err.Error(), "migration 0031 refused") || !strings.Contains(err.Error(), "1 unrevoked") {
		t.Fatalf("0031 over an unrevoked pinless row: err = %v, want a refusal with the count", err)
	}
	if validated() {
		t.Fatal("the check is validated after a refused 0031")
	}
	if err := guardedExec(ctx, env.admin, `UPDATE auth.sessions SET revoked_at = now(), revoked_reason = 'pin_missing' WHERE legacy_token_version IS NULL`); err != nil {
		t.Fatalf("revoke the pinless row: %v", err)
	}
	if err := run0031(); err != nil {
		t.Fatalf("0031 over a pinned live row and a revoked pinless row: %v", err)
	}
	if !validated() {
		t.Fatal("0031 passed but the check is not validated")
	}
}
