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

// A revoke that waits for the invite row past the invite's expiry reads the
// clock after the row lock: the invite has expired by then, so the revoke is
// ErrNotPending and both rows stay pending.
func TestRevokeThatWaitsPastTheExpiryIsNotPending(t *testing.T) {
	e := getEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	f := e.invite(t, ctx, "member")
	saved := e.now
	defer e.setNow(saved)
	e.setNow(f.created.ExpiresAt.Add(-time.Second))
	holder, err := e.admin.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = holder.Rollback(context.Background()) }()
	var holderPID int32
	if err := holder.QueryRow(ctx, `SELECT pg_backend_pid() FROM auth.invites WHERE id = $1 FOR UPDATE`, f.created.ID).Scan(&holderPID); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		done <- e.store.Revoke(ctx, RevokeInput{OrgID: f.org, InviteID: f.created.ID, Authorize: allow})
	}()
	bound := time.After(10 * time.Second)
	for waiting := 0; waiting == 0; {
		if err := e.admin.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity WHERE $1::int = ANY(pg_blocking_pids(pid))`, holderPID).
			Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting > 0 {
			break
		}
		select {
		case err := <-done:
			t.Fatalf("the revoke ended while the invite row was held: %v", err)
		case <-bound:
			t.Fatal("the revoke never waited for the invite row")
		case <-time.After(20 * time.Millisecond):
		}
	}
	e.setNow(f.created.ExpiresAt.Add(time.Second))
	if err := holder.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-done; !errors.Is(err, ErrNotPending) {
		t.Fatalf("a revoke that waited past the expiry = %v, want ErrNotPending", err)
	}
	if r := e.inviteRows(t, ctx, f.created.ID); r.publicStatus != "pending" || r.authStatus != "pending" {
		t.Fatalf("invite rows = %+v, want both still pending", r)
	}
}
