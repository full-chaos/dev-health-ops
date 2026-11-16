//go:build integration

package authschema

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

// Migration 0058: the runtime role may DELETE a principal and its auth.users row (the admin force delete), but only
// inside audit.Commit, and there each deleted row counts as a state change.
func TestPrincipalAndUserDeleteOutsideCommitIsRefused(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newAuthFixture(t, ctx)

	seed := func(t *testing.T) uuid.UUID {
		t.Helper()
		id := uuid.New()
		if _, err := env.admin.Exec(ctx, `INSERT INTO auth.principals (id, kind, display_name) VALUES ($1, 'user', 'erase')`, id); err != nil {
			t.Fatal(err)
		}
		email := "erase-" + strings.ReplaceAll(uuid.NewString(), "-", "") + "@example.test"
		if _, err := env.admin.Exec(ctx, `INSERT INTO auth.users (principal_id, email, email_lower) VALUES ($1, $2, $2)`, id, email); err != nil {
			t.Fatal(err)
		}
		return id
	}
	refused := func(t *testing.T, statement string, id uuid.UUID) {
		t.Helper()
		_, err := env.runtime.Exec(ctx, statement, id)
		if err == nil || !strings.Contains(err.Error(), "only through audit.Commit") {
			t.Errorf("%s outside audit.Commit: %v, want the 0058 delete guard refusal", statement, err)
		}
	}

	id := seed(t)
	refused(t, `DELETE FROM auth.users WHERE principal_id = $1`, id)
	refused(t, `DELETE FROM auth.principals WHERE id = $1`, id)

	tx, err := env.runtime.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SELECT set_config('dev_health.audit_txid', pg_current_xact_id()::text, true),
		set_config('dev_health.audit_changed', pg_current_xact_id()::text || ':0', true)`); err != nil {
		t.Fatal(err)
	}
	for _, statement := range []string{`DELETE FROM auth.users WHERE principal_id = $1`, `DELETE FROM auth.principals WHERE id = $1`} {
		tag, err := tx.Exec(ctx, statement, id)
		if err != nil || tag.RowsAffected() != 1 {
			t.Fatalf("%s inside the armed transaction: %v rows %d, want 1 row", statement, err, tag.RowsAffected())
		}
	}
	var changed int64
	if err := tx.QueryRow(ctx, `SELECT split_part(current_setting('dev_health.audit_changed', true), ':', 2)::bigint`).Scan(&changed); err != nil {
		t.Fatal(err)
	}
	if changed != 2 {
		t.Errorf("changed-row count after deleting the user and the principal = %d, want 2", changed)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	var left int
	if err := env.admin.QueryRow(ctx, `SELECT count(*) FROM auth.principals WHERE id = $1`, id).Scan(&left); err != nil || left != 0 {
		t.Errorf("principal rows after the armed delete = %d, %v; want 0", left, err)
	}
}
