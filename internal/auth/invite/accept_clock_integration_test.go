//go:build integration

package invite

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/auth/signedtoken"
)

// holdRow locks one row with a separate transaction and returns the holder and its backend pid.
func (e *env) holdRow(t *testing.T, ctx context.Context, table string, id uuid.UUID) (pgx.Tx, int32) {
	t.Helper()
	holder, err := e.admin.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = holder.Rollback(context.Background()) })
	var pid int32
	if err := holder.QueryRow(ctx, `SELECT pg_backend_pid() FROM `+table+` WHERE id = $1 FOR UPDATE`, id).Scan(&pid); err != nil {
		t.Fatal(err)
	}
	return holder, pid
}

// acceptBehind runs an accept in a goroutine and returns once it waits on the holder.
func (e *env) acceptBehind(t *testing.T, ctx context.Context, holder int32, in AcceptInput) <-chan error {
	t.Helper()
	done := make(chan error, 1)
	go func() {
		_, err := e.store.Accept(ctx, in)
		done <- err
	}()
	bound := time.After(10 * time.Second)
	for {
		var waiting int
		if err := e.admin.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity WHERE $1::int = ANY(pg_blocking_pids(pid))`, holder).
			Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting > 0 {
			return done
		}
		select {
		case err := <-done:
			t.Fatalf("the accept ended while the row was held: %v", err)
		case <-bound:
			t.Fatal("the accept never waited for the row")
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// I-1: an accept that waits for the auth invite row past the expiry judges it with the clock read after the lock.
func TestAcceptThatWaitsPastTheExpiryIsRefused(t *testing.T) {
	e := getEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	f := e.invite(t, ctx, "member")
	user := e.seedUser(t, ctx, f.email, true)
	saved := e.now
	defer e.setNow(saved)
	e.setNow(f.created.ExpiresAt.Add(-time.Second))
	holder, pid := e.holdRow(t, ctx, "auth.invites", f.created.ID)
	done := e.acceptBehind(t, ctx, pid, AcceptInput{Token: f.created.Token, PrincipalID: user, PublicAudit: &PublicAudit{}})
	e.setNow(f.created.ExpiresAt.Add(time.Second))
	if err := holder.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-done; !errors.Is(err, ErrInviteInvalid) {
		t.Fatalf("an accept that waited past the expiry = %v, want ErrInviteInvalid", err)
	}
	if r := e.inviteRows(t, ctx, f.created.ID); r.publicStatus != "pending" || r.authStatus != "pending" {
		t.Fatalf("invite rows = %+v, want both still pending", r)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM public.memberships WHERE org_id = $1 AND user_id = $2`, f.org, user); n != 0 {
		t.Fatalf("membership rows = %d, want 0", n)
	}
}

// I-2: the same for an invite go-api made: the row the accept waits for is public.org_invites.
func TestAcceptOfAPublicInviteThatWaitsPastTheExpiryIsRefused(t *testing.T) {
	e := getEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	org, inviter, email := e.seedOrg(t, ctx), e.seedUser(t, ctx, address(), true), address()
	id := uuid.New()
	token, digest := signedtoken.Build(id, e.secret)
	expires := time.Now().UTC().Add(time.Hour).Truncate(time.Microsecond)
	if _, err := e.admin.Exec(ctx, `INSERT INTO public.org_invites (id, org_id, email, role, token_hash, invited_by_id, status, expires_at, created_at, updated_at)
VALUES ($1, $2, $3, 'viewer', $4, $5, 'pending', $6, now(), now())`, id, org, email, digest, inviter, expires); err != nil {
		t.Fatal(err)
	}
	user := e.seedUser(t, ctx, email, true)
	saved := e.now
	defer e.setNow(saved)
	e.setNow(expires.Add(-time.Second))
	holder, pid := e.holdRow(t, ctx, "public.org_invites", id)
	done := e.acceptBehind(t, ctx, pid, AcceptInput{Token: token, PrincipalID: user, PublicAudit: &PublicAudit{}})
	e.setNow(expires.Add(time.Second))
	if err := holder.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-done; !errors.Is(err, ErrInviteInvalid) {
		t.Fatalf("an accept that waited past the expiry = %v, want ErrInviteInvalid", err)
	}
	if r := e.inviteRows(t, ctx, id); r.publicStatus != "pending" || r.authStatus != "" {
		t.Fatalf("invite rows = %+v, want public pending and no auth row", r)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM public.memberships WHERE org_id = $1 AND user_id = $2`, org, user); n != 0 {
		t.Fatalf("membership rows = %d, want 0", n)
	}
}

// I-3: an accept that waits and is still valid at the lock stamps every write with the clock it read there.
func TestAcceptThatWaitsStampsItsWritesWithTheClockAtTheLock(t *testing.T) {
	e := getEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	f := e.invite(t, ctx, "member")
	user := e.seedUser(t, ctx, f.email, true)
	saved := e.now
	defer e.setNow(saved)
	e.setNow(f.created.ExpiresAt.Add(-time.Minute))
	holder, pid := e.holdRow(t, ctx, "auth.invites", f.created.ID)
	done := e.acceptBehind(t, ctx, pid, AcceptInput{Token: f.created.Token, PrincipalID: user, PublicAudit: &PublicAudit{}})
	moved := f.created.ExpiresAt.Add(-time.Second).Truncate(time.Microsecond)
	e.setNow(moved)
	if err := holder.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatalf("an accept still valid at the lock = %v, want nil", err)
	}
	var authAt, publicAt, joinedAt time.Time
	if err := e.admin.QueryRow(ctx, `SELECT a.accepted_at, p.accepted_at FROM auth.invites a JOIN public.org_invites p ON p.id = a.id WHERE a.id = $1`,
		f.created.ID).Scan(&authAt, &publicAt); err != nil {
		t.Fatal(err)
	}
	if err := e.admin.QueryRow(ctx, `SELECT created_at FROM public.audit_logs WHERE org_id = $1 AND action = 'member_joined'`, f.org).Scan(&joinedAt); err != nil {
		t.Fatal(err)
	}
	if !authAt.Equal(moved) || !publicAt.Equal(moved) || !joinedAt.Equal(moved) {
		t.Fatalf("auth %v public %v member_joined %v, want the clock at the lock %v", authAt, publicAt, joinedAt, moved)
	}
}
