//go:build integration

package identity

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/org"
	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole"
)

// go-api's admin create adds the user to a suspended organization (pm D6914): the auth-service admin create does too,
// with the membership rows of both schemas.
func TestAdminCreateIntoASuspendedOrganizationJoins(t *testing.T) {
	e := getEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	none := func(context.Context, audit.TxOps) error { return nil }
	actor := e.seedUser(t, ctx, uniqueEmail("suspended-actor"), true, true)
	e.project(t, ctx, actor)
	orgID := e.seedAuthOwnedOrg(t, ctx)
	if err := e.orgStore(t).Suspend(ctx, orgID, nil, org.Actor{}); err != nil {
		t.Fatal(err)
	}
	created, err := e.owned.AdminCreateUser(ctx, AdminCreateRequest{Email: uniqueEmail("suspended-join"), OrgID: &orgID, Role: "member",
		Actor: actor, Authorize: none})
	if err != nil {
		t.Fatalf("admin create into a suspended organization: %v, want the create and its membership", err)
	}
	if p, a := e.memberRows(t, ctx, created.ID, orgID); p != 1 || a != 1 {
		t.Fatalf("membership rows public=%d auth=%d, want 1 and 1", p, a)
	}
}

// A federated sign-in's join still refuses a suspended organization: only the admin create joins one.
func TestSignInJoinIntoASuspendedOrganizationStillRefused(t *testing.T) {
	e := getEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	user, orgID := e.joinFixture(t, ctx)
	if err := e.orgStore(t).Suspend(ctx, orgID, nil, org.Actor{}); err != nil {
		t.Fatal(err)
	}
	err := joinCommit(ctx, e.owned, membershipJoin{ID: uuid.New(), Principal: user, OrgID: orgID, Role: "member", SignIn: true}, nil)
	if !errors.Is(err, ErrOrganizationSuspended) {
		t.Fatalf("sign-in join into a suspended organization = %v, want ErrOrganizationSuspended", err)
	}
	e.requireNoMemberRows(t, ctx, user, orgID)
}

func (e *env) publicUpdatedAt(t *testing.T, ctx context.Context, id uuid.UUID) time.Time {
	t.Helper()
	var at time.Time
	if err := e.admin.QueryRow(ctx, `SELECT updated_at FROM public.users WHERE id = $1`, id).Scan(&at); err != nil {
		t.Fatal(err)
	}
	return at
}

func (e *env) auditTypesSince(t *testing.T, ctx context.Context, subject uuid.UUID, since time.Time) map[string]int {
	t.Helper()
	rows, err := e.admin.Query(ctx, `SELECT event_type FROM auth.security_audit_events WHERE subject_principal_id = $1 AND occurred_at >= $2`,
		subject.String(), since)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	types := map[string]int{}
	for rows.Next() {
		var eventType string
		if err := rows.Scan(&eventType); err != nil {
			t.Fatal(err)
		}
		types[eventType]++
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return types
}

func (e *env) dbNow(t *testing.T, ctx context.Context) time.Time {
	t.Helper()
	var at time.Time
	if err := e.admin.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&at); err != nil {
		t.Fatal(err)
	}
	return at
}

// go-api's PATCH bumps public.users.updated_at for every user it finds, also with no field change (pm D6915): the
// no-op admin update does too, with one auth.user.touched audit row.
func TestAdminNoOpUpdateBumpsUpdatedAt(t *testing.T) {
	e := getEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	none := func(context.Context, audit.TxOps) error { return nil }
	actor := e.seedUser(t, ctx, uniqueEmail("touch-actor"), true, true)
	e.project(t, ctx, actor)
	created, err := e.owned.AdminCreateUser(ctx, AdminCreateRequest{Email: uniqueEmail("touch"), Actor: actor, Authorize: none})
	if err != nil {
		t.Fatal(err)
	}
	cur, err := e.owned.AdminUpdateState(ctx, created.ID)
	if err != nil {
		t.Fatal(err)
	}
	before, since := e.publicUpdatedAt(t, ctx, created.ID), e.dbNow(t, ctx)
	if err := e.owned.AdminUpdateUser(ctx, AdminUpdateRequest{Principal: created.ID, Actor: actor, Authorize: none}, cur); err != nil {
		t.Fatalf("no-op admin update: %v, want the updated_at bump", err)
	}
	if after := e.publicUpdatedAt(t, ctx, created.ID); !after.After(before) {
		t.Errorf("public.users.updated_at %v -> %v, want it moved", before, after)
	}
	if got := e.auditTypesSince(t, ctx, created.ID, since); len(got) != 1 || got["auth.user.touched"] != 1 {
		t.Errorf("audit rows %v, want exactly one auth.user.touched", got)
	}
}

// A platform role change with no field change bumps public.users.updated_at in the role change's own commit: the
// assignment and the bump commit together, and the audit rows are exactly the grant and the touch (no
// auth.user.updated with an empty changed_fields).
func TestAdminRoleOnlyUpdateBumpsUpdatedAtWithTheGrant(t *testing.T) {
	e := getEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	none := func(context.Context, audit.TxOps) error { return nil }
	roles, err := platformrole.New(platformrole.Config{Pool: e.runtime, Schema: e.store.schema})
	if err != nil {
		t.Fatal(err)
	}
	actor := uuid.New()
	if _, err := e.store.BackfillUser(ctx, BackfillTarget{ID: actor, Email: uniqueEmail("role-actor"), DisplayName: "role actor", Active: true,
		Verified: true, Superuser: true}, false); err != nil {
		t.Fatal(err)
	}
	created, err := e.owned.AdminCreateUser(ctx, AdminCreateRequest{Email: uniqueEmail("role-only"), Actor: actor, Authorize: none})
	if err != nil {
		t.Fatal(err)
	}
	cur, err := e.owned.AdminUpdateState(ctx, created.ID)
	if err != nil {
		t.Fatal(err)
	}
	before, since := e.publicUpdatedAt(t, ctx, created.ID), e.dbNow(t, ctx)
	grant := func(ctx context.Context, tx audit.TxOps) error {
		_, err := roles.GrantOn(ctx, tx, platformrole.GrantInput{Actor: actor, Principal: created.ID, Role: platformrole.PlatformAdmin})
		return err
	}
	if err := e.owned.AdminUpdateUser(ctx, AdminUpdateRequest{Principal: created.ID, Actor: actor, Authorize: platformrole.LockWrites,
		Superuser: grant}, cur); err != nil {
		t.Fatalf("role-only admin update: %v, want the grant and the updated_at bump in one commit", err)
	}
	if after := e.publicUpdatedAt(t, ctx, created.ID); !after.After(before) {
		t.Errorf("public.users.updated_at %v -> %v, want it moved", before, after)
	}
	var held int
	if err := e.admin.QueryRow(ctx, `SELECT count(*) FROM auth.platform_role_assignments WHERE principal_id = $1 AND role_key = $2`,
		created.ID, platformrole.PlatformAdmin).Scan(&held); err != nil {
		t.Fatal(err)
	}
	if held != 1 {
		t.Errorf("platform_admin assignments = %d, want 1", held)
	}
	got := e.auditTypesSince(t, ctx, created.ID, since)
	if len(got) != 2 || got["auth.platform_role.granted"] != 1 || got["auth.user.touched"] != 1 {
		t.Errorf("audit rows %v, want exactly one auth.platform_role.granted and one auth.user.touched", got)
	}
}
