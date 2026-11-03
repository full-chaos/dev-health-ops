//go:build integration

package authschema

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

const (
	dsActor  = "00000000-0000-4000-8000-000000109871"
	dsTarget = "00000000-0000-4000-8000-000000109872"
	dsOrg    = "00000000-0000-4000-8000-000000109873"
	dsBase   = "00000000-0000-4000-8000-000000109874"
)

func dsApply(t *testing.T, ctx context.Context, env *migrationOnlyFixture) {
	t.Helper()
	migrations, err := Migrations()
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range migrations {
		if err := applyOne(ctx, env.conn, env.options, m); err != nil {
			t.Fatalf("apply %04d: %v", m.Version, err)
		}
	}
}

func dsSeed(t *testing.T, ctx context.Context, env *migrationOnlyFixture) {
	t.Helper()
	for _, sql := range []string{
		`INSERT INTO auth.principals (id, kind, display_name) VALUES ('` + dsActor + `', 'user', 'a'), ('` + dsTarget + `', 'user', 't')`,
		`INSERT INTO auth.organizations (id, name, slug) VALUES ('` + dsOrg + `', 'o', 'm38-org')`,
		`INSERT INTO auth.sessions (id, principal_id, token_hash, issued_at, expires_at, amr, legacy_token_version)
		 VALUES ('` + dsBase + `', '` + dsActor + `', '\x01', now(), 'infinity', '{pwd}', 0)`,
	} {
		if err := domainOwnedExec(t, ctx, env, sql); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
}

// dsRow is a valid delegated row with the named column expressions replaced.
func dsRow(n string, replace map[string]string) string {
	cols := []string{"principal_id", "organization_id", "token_hash", "issued_at", "created_at", "expires_at", "amr", "legacy_token_version",
		"actor_principal_id", "delegated_from_session_id", "delegation_reason", "delegation_permitted_actions"}
	values := map[string]string{
		"principal_id": `'` + dsTarget + `'`, "organization_id": `'` + dsOrg + `'`, "token_hash": `'\xa` + n + `'`,
		"issued_at": `now()`, "created_at": `now()`, "expires_at": `now() + interval '15 minutes'`, "amr": `'{pwd}'`, "legacy_token_version": `0`,
		"actor_principal_id": `'` + dsActor + `'`, "delegated_from_session_id": `'` + dsBase + `'`,
		"delegation_reason": `'support'`, "delegation_permitted_actions": `'{analytics.read}'`,
	}
	for k, v := range replace {
		values[k] = v
	}
	out := make([]string, len(cols))
	for i, c := range cols {
		out[i] = values[c]
	}
	return `INSERT INTO auth.sessions (` + strings.Join(cols, ", ") + `) VALUES (` + strings.Join(out, ", ") + `)`
}

// The row shape: a valid delegated row inserts; every partial row, a row over
// 15 minutes, a row with no organization, an actor equal to the target and a
// blank reason are refused; the four columns cannot change.
func TestDelegatedSessionsMigrationRowShape(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	env := newAuthMigrationOnly(t, ctx)
	dsApply(t, ctx, env)
	dsSeed(t, ctx, env)
	if err := domainOwnedExec(t, ctx, env, dsRow("1", nil)); err != nil {
		t.Fatalf("a valid delegated row: %v", err)
	}
	for name, replace := range map[string]map[string]string{
		"sixteen_minutes":     {"expires_at": `now() + interval '16 minutes'`},
		"no_actor":            {"actor_principal_id": `NULL`},
		"no_base":             {"delegated_from_session_id": `NULL`},
		"no_reason":           {"delegation_reason": `NULL`},
		"no_actions":          {"delegation_permitted_actions": `NULL`},
		"no_organization":     {"organization_id": `NULL`},
		"actor_is_target":     {"actor_principal_id": `'` + dsTarget + `'`},
		"blank_reason":        {"delegation_reason": `'   '`},
		"reason_501":          {"delegation_reason": `repeat('x', 501)`},
		"plain_with_a_reason": {"actor_principal_id": `NULL`, "delegated_from_session_id": `NULL`, "delegation_permitted_actions": `NULL`},
	} {
		t.Run(name, func(t *testing.T) {
			err := domainOwnedExec(t, ctx, env, dsRow("2", replace))
			if err == nil || !strings.Contains(err.Error(), "sessions_delegation_shape") {
				t.Fatalf("insert = %v, want the refusal of sessions_delegation_shape", err)
			}
		})
	}
	for name, set := range map[string]string{
		"actor":   `actor_principal_id = '` + dsTarget + `'`,
		"base":    `delegated_from_session_id = NULL`,
		"reason":  `delegation_reason = 'other'`,
		"actions": `delegation_permitted_actions = '{}'`,
	} {
		t.Run("update_"+name, func(t *testing.T) {
			err := domainOwnedExec(t, ctx, env, `UPDATE auth.sessions SET `+set+` WHERE actor_principal_id IS NOT NULL`)
			if err == nil || !strings.Contains(err.Error(), "cannot change") {
				t.Fatalf("update = %v, want the refusal of sessions_delegation_immutable", err)
			}
		})
	}
}

// The partial unique index: a second unrevoked row of one actor is refused,
// also when the first has expired.
func TestDelegatedSessionsMigrationOneUnrevokedRowPerActor(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	env := newAuthMigrationOnly(t, ctx)
	dsApply(t, ctx, env)
	dsSeed(t, ctx, env)
	if err := domainOwnedExec(t, ctx, env, dsRow("1", map[string]string{"expires_at": `now() - interval '1 minute'`})); err != nil {
		t.Fatalf("first row: %v", err)
	}
	err := domainOwnedExec(t, ctx, env, dsRow("2", nil))
	if err == nil || !strings.Contains(err.Error(), "sessions_one_live_delegation") {
		t.Fatalf("a second unrevoked row of one actor = %v, want the refusal of sessions_one_live_delegation", err)
	}
}

// A writer that holds a table lock on auth.principals makes the first statement
// of the migration wait; lock_timeout ends the wait and nothing of it is left.
func TestDelegatedSessionsMigrationGivesUpBehindALockOnPrincipals(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	env := newAuthMigrationOnly(t, ctx)
	migrations, err := Migrations()
	if err != nil {
		t.Fatal(err)
	}
	options := env.options
	options.LockTimeout, options.LockAttempts = 100*time.Millisecond, 1
	for _, m := range migrations {
		if m.Name != delegatedSessionsMigrationName {
			if err := applyOne(ctx, env.conn, env.options, m); err != nil {
				t.Fatalf("apply %04d: %v", m.Version, err)
			}
			continue
		}
		holder, err := pgx.ConnectConfig(ctx, env.conn.Config())
		if err != nil {
			t.Fatal(err)
		}
		defer holder.Close(ctx)
		tx, err := holder.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(ctx, `LOCK TABLE auth.principals IN ROW EXCLUSIVE MODE`); err != nil {
			t.Fatal(err)
		}
		err = applyOne(ctx, env.conn, options, m)
		_ = tx.Rollback(ctx)
		if !errors.Is(err, ErrMigrationLockTimeout) {
			t.Fatalf("applyOne = %v, want ErrMigrationLockTimeout", err)
		}
		var columns int
		if err := env.conn.QueryRow(ctx, `SELECT count(*) FROM information_schema.columns
			WHERE table_schema = 'auth' AND table_name = 'sessions' AND column_name = 'actor_principal_id'`).Scan(&columns); err != nil {
			t.Fatal(err)
		}
		if columns != 0 {
			t.Fatal("a timed-out migration left its column behind")
		}
		return
	}
	t.Fatalf("the lineage lacks the migration %s", delegatedSessionsMigrationName)
}
