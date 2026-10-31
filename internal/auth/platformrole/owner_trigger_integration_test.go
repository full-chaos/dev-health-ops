//go:build integration

package platformrole

import (
	"context"
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

func mustRefuseOwner(t *testing.T, what string, err error) {
	t.Helper()
	if err == nil || !strings.Contains(err.Error(), "platform_owner") {
		t.Errorf("%s as the runtime role: %v, want the platform_owner guard's refusal", what, err)
	}
}

// The runtime role cannot insert, update or delete a platform_owner row, even
// by direct SQL; the table owner (the migration role, as the CLI verbs run) can.
func TestRuntimeRoleCannotWriteAnOwnerRow(t *testing.T) {
	e := getEnv(t)
	ctx := ctxT(t)
	target := e.user(t, ctx, true, false)
	other := e.user(t, ctx, true, false)
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
	if err := e.runtimeExec(t, ctx, e.runtime,
		`UPDATE auth.platform_role_assignments SET role_key = 'platform_admin' WHERE principal_id = $1 AND role_key = 'platform_owner'`, other); err == nil {
		t.Error("UPDATE of an owner row as the runtime role was accepted")
	}
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
