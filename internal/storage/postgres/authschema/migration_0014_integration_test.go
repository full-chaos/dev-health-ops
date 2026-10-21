//go:build integration

package authschema

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func TestMigration0014CreateGuardCountsAnInsertedSession(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newM10Fixture(t, ctx)
	principal := "00000000-0000-4000-8000-0000000000f1"
	if _, err := env.runtime.Exec(ctx, `INSERT INTO auth.principals (id, kind) VALUES ($1, 'user')`, principal); err != nil {
		t.Fatal(err)
	}
	insert := `INSERT INTO auth.sessions (id, principal_id, token_hash, expires_at, auth_time, amr)
		VALUES ($1, $2, $3, now() + interval '1 hour', now(), ARRAY['pwd']) ON CONFLICT (id) DO NOTHING`
	counted := func(id string, hash []byte) string {
		t.Helper()
		var got string
		if err := m13Armed(t, ctx, env.runtime, func(tx pgx.Tx) error {
			if _, err := tx.Exec(ctx, insert, id, principal, hash); err != nil {
				return err
			}
			return tx.QueryRow(ctx, `SELECT split_part(current_setting('dev_health.audit_changed', true), ':', 2)`).Scan(&got)
		}); err != nil {
			t.Fatalf("an armed session insert: %v", err)
		}
		return got
	}
	session := "00000000-0000-4000-8000-0000000000f2"
	if got := counted(session, []byte("m14-hash-1")); got != "1" {
		t.Errorf("a new session row counted %q, want 1", got)
	}
	if got := counted(session, []byte("m14-hash-2")); got != "0" {
		t.Errorf("a session insert that inserted nothing counted %q, want 0", got)
	}

	tx, err := env.runtime.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, insert, "00000000-0000-4000-8000-0000000000f3", principal, []byte("m14-hash-3")); err != nil {
		t.Fatalf("an unarmed session insert was refused: %v", err)
	}
	var counter *string
	if err := tx.QueryRow(ctx, `SELECT current_setting('dev_health.audit_changed', true)`).Scan(&counter); err != nil {
		t.Fatal(err)
	}
	if counter != nil && *counter != "" {
		t.Errorf("an unarmed session insert set the changed counter to %q", *counter)
	}
}
