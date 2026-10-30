//go:build integration

package identity

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/membership"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

func (e *env) authRole(t *testing.T, ctx context.Context, id uuid.UUID) string {
	t.Helper()
	var role string
	if err := e.admin.QueryRow(ctx, `SELECT role FROM auth.memberships WHERE id = $1`, id).Scan(&role); err != nil {
		t.Fatalf("read the role: %v", err)
	}
	return role
}

func (e *env) roleAuditRows(t *testing.T, ctx context.Context) int {
	t.Helper()
	return e.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE event_type = 'auth.membership.role_changed'`)
}

// A role the update expects (the one the backfill read) that is no longer the stored role changes nothing: no row, no audit,
// no outbox event, and the report says no role change.
func TestRoleUpdateWithAStaleExpectedRoleChangesNothing(t *testing.T) {
	e := getEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	org := e.seedAuthOwnedOrg(t, ctx)
	member := e.seedUser(t, ctx, uniqueEmail("sm"), true, true)
	e.project(t, ctx, member)
	t0 := time.Now().UTC().Truncate(time.Second).Add(-72 * time.Hour)
	if _, err := e.store.BackfillMembership(ctx, MembershipBackfillTarget{OrgID: org, UserID: member, Role: "admin", JoinedAt: &t0}, false); err != nil {
		t.Fatal(err)
	}
	id := e.membershipID(t, ctx, org, member)
	stale := "viewer"
	before := e.roleAuditRows(t, ctx)
	changed, err := e.store.updateMembershipRole(ctx, id, MembershipBackfillTarget{OrgID: org, UserID: member, Role: "member", JoinedAt: &t0}, &stale)
	if err != nil {
		t.Fatal(err)
	}
	if changed {
		t.Fatal("update with a stale expected role: got changed=true, want false")
	}
	if got := e.authRole(t, ctx, id); got != "admin" {
		t.Fatalf("role after the stale update: got %q, want %q", got, "admin")
	}
	if got := e.roleAuditRows(t, ctx); got != before {
		t.Fatalf("role-changed audit rows: got %d, want %d", got, before)
	}
	current := "admin"
	changed, err = e.store.updateMembershipRole(ctx, id, MembershipBackfillTarget{OrgID: org, UserID: member, Role: "member", JoinedAt: &t0}, &current)
	if err != nil || !changed {
		t.Fatalf("update with the current expected role: got changed=%v err=%v, want true and nil", changed, err)
	}
	if got := e.authRole(t, ctx, id); got != "member" {
		t.Fatalf("role after the matching update: got %q, want %q", got, "member")
	}
}

// An ownership transfer lands between the backfill's read and its role UPDATE: the recipient stays owner, the backfill
// reports no role change and writes no role-changed audit row.
func TestBackfillRoleDoesNotOverwriteAConcurrentTransfer(t *testing.T) {
	e := getEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		t.Fatal(err)
	}
	ms, err := membership.New(membership.Config{Pool: e.runtime, Schema: schema, OwnsUsers: true})
	if err != nil {
		t.Fatal(err)
	}
	allow := func(context.Context, audit.TxOps, membership.Locked) error { return nil }
	org := e.seedAuthOwnedOrg(t, ctx)
	owner := e.seedUser(t, ctx, uniqueEmail("to"), true, true)
	member := e.seedUser(t, ctx, uniqueEmail("tm"), true, true)
	e.project(t, ctx, owner)
	e.project(t, ctx, member)
	if _, err := ms.Add(ctx, membership.Change{OrgID: org, PrincipalID: owner, Role: "owner", Authorize: allow}); err != nil {
		t.Fatalf("add the owner: %v", err)
	}
	if _, err := ms.Add(ctx, membership.Change{OrgID: org, PrincipalID: member, Role: "member", Authorize: allow}); err != nil {
		t.Fatalf("add the member: %v", err)
	}
	id := e.membershipID(t, ctx, org, member)
	t0 := time.Now().UTC().Truncate(time.Second).Add(-72 * time.Hour)
	var afterTransfer int
	afterRoleRead = func(ctx context.Context) {
		afterRoleRead = nil
		if err := ms.TransferOwnership(ctx, membership.Transfer{OrgID: org, From: owner, To: member, Authorize: allow}); err != nil {
			t.Errorf("transfer in the gap: %v", err)
		}
		afterTransfer = e.roleAuditRows(t, ctx)
	}
	t.Cleanup(func() { afterRoleRead = nil })
	res, err := e.store.BackfillMembership(ctx, MembershipBackfillTarget{OrgID: org, UserID: member, Role: "admin", JoinedAt: &t0}, false)
	if err != nil {
		t.Fatal(err)
	}
	if res.RoleUpdated {
		t.Fatal("report: got role_updated=true, want false (the role was changed by the transfer first)")
	}
	if got := e.authRole(t, ctx, id); got != "owner" {
		t.Fatalf("role after the backfill: got %q, want %q (the transfer's value)", got, "owner")
	}
	if got := e.roleAuditRows(t, ctx); got != afterTransfer {
		t.Fatalf("role-changed audit rows: got %d, want %d (the backfill writes none)", got, afterTransfer)
	}
}
