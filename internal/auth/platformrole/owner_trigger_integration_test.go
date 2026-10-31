//go:build integration

package platformrole

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/audit"
)

// runtimeExec runs one statement as the runtime role inside audit.Commit, which
// arms the commit guard, so a refusal is the owner guard's and not the commit
// guard's. The Apply error is what the test reads.
func (e *env) runtimeExec(t *testing.T, ctx context.Context, pool *pgxpool.Pool, sql string, args ...any) error {
	t.Helper()
	return audit.Commit(ctx, pool, e.schema, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			_, err := tx.Exec(ctx, sql, args...)
			return err
		},
		Audit:  operatorAudit("auth.platform_role.granted", "role_granted", uuid.NewString()),
		Events: []audit.OutboxEvent{roleEvent(uuid.New(), "platform_role_assignment.granted", "active")},
	})
}

// ownerGuardText is the owner trigger's own message. A privilege denial is also
// SQLSTATE 42501, so only this text proves the trigger refused the write.
const ownerGuardText = "platform_owner rows are written only by auth-migrate"

func mustRefuseOwner(t *testing.T, what string, err error) {
	t.Helper()
	if err == nil || !strings.Contains(err.Error(), ownerGuardText) {
		t.Errorf("%s: %v, want the owner trigger's refusal (%q)", what, err, ownerGuardText)
	}
}

// updaterPool is a pool for a scratch role that holds SELECT and UPDATE on the
// assignments (not the owner), so an UPDATE it makes is refused by the trigger
// or not at all, never by a missing grant.
func (e *env) updaterPool(t *testing.T, ctx context.Context) *pgxpool.Pool {
	t.Helper()
	role := "pr_updater_" + strings.ReplaceAll(uuid.NewString()[:12], "-", "")
	const password = "owner-trigger-test-only-not-a-credential"
	for _, stmt := range []string{
		fmt.Sprintf(`CREATE ROLE %q LOGIN PASSWORD '%s'`, role, password),
		fmt.Sprintf(`GRANT USAGE ON SCHEMA auth TO %q`, role),
		fmt.Sprintf(`GRANT SELECT, UPDATE ON auth.platform_role_assignments TO %q`, role),
	} {
		if _, err := e.admin.Exec(ctx, stmt); err != nil {
			t.Fatalf("scratch role: %v", err)
		}
	}
	cfg := e.admin.Config().Copy()
	cfg.ConnConfig.User = role
	cfg.ConnConfig.Password = password
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		pool.Close()
		cleanup := context.Background()
		_, _ = e.admin.Exec(cleanup, fmt.Sprintf(`DROP OWNED BY %q`, role))
		_, _ = e.admin.Exec(cleanup, fmt.Sprintf(`DROP ROLE %q`, role))
	})
	return pool
}

// The runtime role cannot insert, update or delete a platform_owner row, even
// by direct SQL; the table owner (the migration role, as the CLI verbs run) can.
func TestRuntimeRoleCannotWriteAnOwnerRow(t *testing.T) {
	e := getEnv(t)
	ctx := ctxT(t)
	target := e.user(t, ctx, true, false)
	other := e.user(t, ctx, true, false)
	granter := e.user(t, ctx, true, true)
	mustRefuseOwner(t, "INSERT", e.runtimeExec(t, ctx, e.runtime,
		`INSERT INTO auth.platform_role_assignments (principal_id, role_key) VALUES ($1, 'platform_owner')`, target))
	if n := e.ownerRows(t, target); n != 0 {
		t.Fatalf("the refused insert left %d owner rows", n)
	}
	if _, err := e.bootstrapStore(t).GrantOwner(ctx, other); err != nil {
		t.Fatalf("the table owner's GrantOwner: %v", err)
	}
	mustRefuseOwner(t, "DELETE", e.runtimeExec(t, ctx, e.runtime,
		`DELETE FROM auth.platform_role_assignments WHERE principal_id = $1 AND role_key = 'platform_owner'`, other))
	updater := e.updaterPool(t, ctx)
	mustRefuseOwner(t, "UPDATE of an owner row as a role with UPDATE", e.runtimeExec(t, ctx, updater,
		`UPDATE auth.platform_role_assignments SET expires_at = now() + interval '1 day' WHERE principal_id = $1 AND role_key = 'platform_owner'`, other))
	mustRefuseOwner(t, "UPDATE that renames an owner row away", e.runtimeExec(t, ctx, updater,
		`UPDATE auth.platform_role_assignments SET role_key = 'platform_admin' WHERE principal_id = $1 AND role_key = 'platform_owner'`, other))
	mustRefuseOwner(t, "UPDATE that renames a row to owner", e.runtimeExec(t, ctx, updater,
		`UPDATE auth.platform_role_assignments SET role_key = 'platform_owner' WHERE principal_id = $1 AND role_key = 'platform_admin'`, granter))
	if n := e.ownerRows(t, other); n != 1 {
		t.Fatalf("owner rows of the held owner after the refused writes = %d, want 1", n)
	}
	if err := e.bootstrapStore(t).RevokeOwner(ctx, other); err != nil {
		t.Fatalf("the table owner's RevokeOwner: %v", err)
	}
}

// The guard names platform_owner only: a platform_admin row is written and
// deleted by the runtime role's store as before, and a principal with no
// assignment still deletes.
func TestOwnerGuardLeavesAdminRowsAndPrincipalDeletesAlone(t *testing.T) {
	e := getEnv(t)
	ctx := ctxT(t)
	granter := e.user(t, ctx, true, true)
	target := e.user(t, ctx, true, false)
	if _, err := e.store.Grant(ctx, GrantInput{Actor: granter, Principal: target, Role: PlatformAdmin}); err != nil {
		t.Fatalf("API grant of platform_admin: %v", err)
	}
	if err := e.store.Revoke(ctx, RevokeInput{Actor: granter, Principal: target, Role: PlatformAdmin}); err != nil {
		t.Fatalf("API revoke of platform_admin: %v", err)
	}
	bare := uuid.New()
	if _, err := e.admin.Exec(ctx, `INSERT INTO auth.principals (id, kind, display_name) VALUES ($1, 'user', 'bare')`, bare); err != nil {
		t.Fatal(err)
	}
	if _, err := e.bootstrapStore(t).GrantOwner(ctx, bare); err != nil {
		t.Fatalf("GrantOwner on a bare principal: %v", err)
	}
	if _, err := e.admin.Exec(ctx, `DELETE FROM auth.principals WHERE id = $1`, bare); err == nil || !strings.Contains(err.Error(), "foreign key") {
		t.Errorf("deleting a principal that holds an owner row: %v, want the foreign key refusal (RESTRICT), not the guard", err)
	}
	if err := e.bootstrapStore(t).RevokeOwner(ctx, bare); err != nil {
		t.Fatal(err)
	}
	if _, err := e.admin.Exec(ctx, `DELETE FROM auth.principals WHERE id = $1`, bare); err != nil {
		t.Errorf("deleting a principal with no assignment: %v", err)
	}
}
