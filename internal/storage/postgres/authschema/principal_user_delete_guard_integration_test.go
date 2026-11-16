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

// With users NOT owned (UsersOwned false), the runtime role still holds DELETE on auth.principals, auth.users and
// auth.memberships: the auth manifest (RuntimePosture) is switch-independent by design, as its INSERT and UPDATE on
// principals and users are. The guard is the database's: a delete outside audit.Commit is refused by the 0058 delete
// guard (principals, users) and the 0006 commit guard (memberships), one sub-cell per table. RED CONTROL: with that
// table's trigger dropped, the same delete by the same role succeeds, so the refusal is the trigger's, not a grant's.
func TestAuthEraseDeletesAreRefusedOutsideCommitWhileUsersAreNotOwned(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newAuthFixture(t, ctx)

	org := uuid.New()
	if err := guardedExec(ctx, env.admin, `INSERT INTO auth.organizations (id, slug, name) VALUES ($1, $2, 'erase-off')`,
		org, "erase-off-"+strings.ReplaceAll(uuid.NewString(), "-", "")); err != nil {
		t.Fatal(err)
	}
	seed := func(t *testing.T) uuid.UUID {
		t.Helper()
		id := uuid.New()
		if _, err := env.admin.Exec(ctx, `INSERT INTO auth.principals (id, kind, display_name) VALUES ($1, 'user', 'erase-off')`, id); err != nil {
			t.Fatal(err)
		}
		email := "erase-off-" + strings.ReplaceAll(uuid.NewString(), "-", "") + "@example.test"
		if _, err := env.admin.Exec(ctx, `INSERT INTO auth.users (principal_id, email, email_lower) VALUES ($1, $2, $2)`, id, email); err != nil {
			t.Fatal(err)
		}
		if err := guardedExec(ctx, env.admin, `INSERT INTO auth.memberships (organization_id, principal_id, status) VALUES ($1, $2, 'invited')`,
			org, id); err != nil {
			t.Fatal(err)
		}
		return id
	}
	// Ordered so a refused delete never meets a foreign key first: memberships, then users, then the principal.
	cells := []struct {
		table, trigger, statement string
	}{
		{"memberships", "memberships_commit_guard", `DELETE FROM auth.memberships WHERE principal_id = $1`},
		{"users", "users_delete_guard", `DELETE FROM auth.users WHERE principal_id = $1`},
		{"principals", "principals_delete_guard", `DELETE FROM auth.principals WHERE id = $1`},
	}
	held := func(table string) bool {
		var ok bool
		if err := env.admin.QueryRow(ctx, `SELECT has_table_privilege($1, 'auth.'||$2, 'DELETE')`, env.runtimeRole, table).Scan(&ok); err != nil {
			t.Fatal(err)
		}
		return ok
	}

	id := seed(t)
	for _, c := range cells {
		t.Run("refused/"+c.table, func(t *testing.T) {
			if !held(c.table) {
				t.Fatalf("the runtime role holds no DELETE on auth.%s with users not owned: the cell proves nothing", c.table)
			}
			tag, err := env.runtime.Exec(ctx, c.statement, id)
			if err == nil || !strings.Contains(err.Error(), "only through audit.Commit") {
				t.Fatalf("%s outside audit.Commit with users not owned: %v (rows %d), want the guard's refusal", c.statement, err, tag.RowsAffected())
			}
		})
	}

	control := seed(t)
	for _, c := range cells {
		t.Run("red-control/"+c.table, func(t *testing.T) {
			if _, err := env.admin.Exec(ctx, `DROP TRIGGER `+c.trigger+` ON auth.`+c.table); err != nil {
				t.Fatal(err)
			}
			tag, err := env.runtime.Exec(ctx, c.statement, control)
			if err != nil || tag.RowsAffected() != 1 {
				t.Fatalf("%s with %s dropped: %v rows %d, want 1 row deleted (the refusal above is the trigger's)", c.statement, c.trigger, err, tag.RowsAffected())
			}
		})
	}
}
