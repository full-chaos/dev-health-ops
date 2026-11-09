//go:build integration

package identity

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole"
)

type storedHolderFixture struct {
	roles  *platformrole.Store
	actor  uuid.UUID
	holder uuid.UUID
}

// disabledHolder is a user created by the admin create, granted platform_admin, then soft deleted: an inactive user
// whose assignment is still stored.
func (e *env) disabledHolder(t *testing.T, ctx context.Context) storedHolderFixture {
	t.Helper()
	none := func(context.Context, audit.TxOps) error { return nil }
	roles, err := platformrole.New(platformrole.Config{Pool: e.runtime, Schema: e.store.schema})
	if err != nil {
		t.Fatal(err)
	}
	actor := uuid.New()
	if _, err := e.store.BackfillUser(ctx, BackfillTarget{ID: actor, Email: uniqueEmail("holder-actor"), DisplayName: "holder actor", Active: true,
		Verified: true, Superuser: true}, false); err != nil {
		t.Fatal(err)
	}
	created, err := e.owned.AdminCreateUser(ctx, AdminCreateRequest{Email: uniqueEmail("disabled-holder"), Actor: actor, Authorize: none})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := roles.Grant(ctx, platformrole.GrantInput{Actor: actor, Principal: created.ID, Role: platformrole.PlatformAdmin}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.owned.AdminSoftDeleteUser(ctx, AdminSoftDeleteRequest{Principal: created.ID, Actor: actor, Authorize: platformrole.LockWrites}); err != nil {
		t.Fatal(err)
	}
	return storedHolderFixture{roles: roles, actor: actor, holder: created.ID}
}

func (e *env) assignments(t *testing.T, ctx context.Context, principal uuid.UUID) int {
	t.Helper()
	var n int
	if err := e.admin.QueryRow(ctx, `SELECT count(*) FROM auth.platform_role_assignments WHERE principal_id = $1 AND role_key = $2`,
		principal, platformrole.PlatformAdmin).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// patchSuperuser is the user-admin update of is_superuser as the route makes it: the role change is decided from
// what storedPlatformAdmin reads, and runs as the update's Superuser step.
func (e *env) patchSuperuser(ctx context.Context, f storedHolderFixture, superuser bool) error {
	held, err := storedPlatformAdmin(ctx, f.roles, e.runtime, f.holder)
	if err != nil {
		return err
	}
	cur, err := e.owned.AdminUpdateState(ctx, f.holder)
	if err != nil {
		return err
	}
	req := AdminUpdateRequest{Principal: f.holder, Actor: f.actor, Authorize: platformrole.LockWrites}
	if superuser != held {
		req.Superuser = func(ctx context.Context, tx audit.TxOps) error {
			if superuser {
				_, err := f.roles.GrantOn(ctx, tx, platformrole.GrantInput{Actor: f.actor, Principal: f.holder, Role: platformrole.PlatformAdmin})
				return err
			}
			return f.roles.RevokeOn(ctx, tx, platformrole.RevokeInput{Actor: f.actor, Principal: f.holder, Role: platformrole.PlatformAdmin})
		}
	}
	return e.owned.AdminUpdateUser(ctx, req, cur)
}

// is_superuser true for an inactive user who still holds platform_admin is no role change, as in go-api (it writes
// the same column value): 200 with the touch only, the assignment kept, exactly one auth.user.touched row.
func TestAdminUpdateSuperuserTrueOnADisabledHolderTouchesOnly(t *testing.T) {
	e := getEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	f := e.disabledHolder(t, ctx)
	since := e.dbNow(t, ctx)
	if err := e.patchSuperuser(ctx, f, true); err != nil {
		t.Fatalf("is_superuser true on a disabled holder: %v, want the touch only", err)
	}
	if n := e.assignments(t, ctx, f.holder); n != 1 {
		t.Errorf("assignments = %d, want the stored one kept", n)
	}
	if got := e.auditTypesSince(t, ctx, f.holder, since); len(got) != 1 || got["auth.user.touched"] != 1 {
		t.Errorf("audit rows %v, want exactly one auth.user.touched", got)
	}
}

// is_superuser false for an inactive user who still holds platform_admin revokes it in the update's transaction, as
// go-api clears its column: the assignment is gone, so a later reactivation gives no platform authority. A disabled
// holder is never the last active platform_admin, so the revoke is not refused as one.
func TestAdminUpdateSuperuserFalseOnADisabledHolderRevokes(t *testing.T) {
	e := getEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	f := e.disabledHolder(t, ctx)
	since := e.dbNow(t, ctx)
	err := e.patchSuperuser(ctx, f, false)
	if errors.Is(err, platformrole.ErrLastPlatformAdmin) {
		t.Fatalf("the revoke of a disabled holder was refused as the last platform admin: %v", err)
	}
	if err != nil {
		t.Fatalf("is_superuser false on a disabled holder: %v, want the revoke", err)
	}
	if n := e.assignments(t, ctx, f.holder); n != 0 {
		t.Fatalf("assignments = %d, want the stored one revoked", n)
	}
	got := e.auditTypesSince(t, ctx, f.holder, since)
	if len(got) != 2 || got["auth.platform_role.revoked"] != 1 || got["auth.user.touched"] != 1 {
		t.Errorf("audit rows %v, want exactly one auth.platform_role.revoked and one auth.user.touched", got)
	}
}
