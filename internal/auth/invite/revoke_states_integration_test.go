//go:build integration

package invite

import (
	"context"
	"errors"
	"testing"
	"time"
)

// A revoke of an invite that can no longer be accepted, because it expired,
// is ErrNotPending, as for an accepted invite; both rows stay pending.
func TestRevokeOfAnExpiredInviteIsNotPending(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	f := e.invite(t, ctx, "member")
	saved := e.now
	e.now = f.created.ExpiresAt.Add(time.Second)
	defer func() { e.now = saved }()
	if err := e.store.Revoke(ctx, RevokeInput{OrgID: f.org, InviteID: f.created.ID, Authorize: allow}); !errors.Is(err, ErrNotPending) {
		t.Fatalf("revoke of an expired invite = %v, want ErrNotPending", err)
	}
	if r := e.inviteRows(t, ctx, f.created.ID); r.publicStatus != "pending" || r.authStatus != "pending" {
		t.Fatalf("invite rows = %+v, want both still pending", r)
	}
}

// A revoke of an invite already revoked is a no-op: nil, and no second audit
// row or event.
func TestARepeatRevokeIsANoOpWithNoAuditRow(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	f := e.invite(t, ctx, "member")
	if err := e.store.Revoke(ctx, RevokeInput{OrgID: f.org, InviteID: f.created.ID, Authorize: allow}); err != nil {
		t.Fatalf("first Revoke: %v", err)
	}
	audits := func() int {
		return e.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE event_type = 'auth.invite.revoked' AND organization_id = $1`, f.org.String())
	}
	if n := audits(); n != 1 {
		t.Fatalf("audit rows after the revoke = %d, want 1", n)
	}
	if err := e.store.Revoke(ctx, RevokeInput{OrgID: f.org, InviteID: f.created.ID, Authorize: allow}); err != nil {
		t.Fatalf("repeat Revoke = %v, want nil (a no-op)", err)
	}
	if n := audits(); n != 1 {
		t.Fatalf("audit rows after the repeat = %d, want still 1", n)
	}
}
