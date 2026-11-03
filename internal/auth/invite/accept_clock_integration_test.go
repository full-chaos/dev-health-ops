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
	var authUpdated, publicUpdated time.Time
	if err := e.admin.QueryRow(ctx, `SELECT a.updated_at, p.updated_at FROM auth.invites a JOIN public.org_invites p ON p.id = a.id WHERE a.id = $1`,
		f.created.ID).Scan(&authUpdated, &publicUpdated); err != nil {
		t.Fatal(err)
	}
	if !authUpdated.Equal(moved) || !publicUpdated.Equal(moved) {
		t.Fatalf("updated_at auth %v public %v, want the clock at the lock %v", authUpdated, publicUpdated, moved)
	}
}

// A public-only invite taken over by an accept that waits: the auth.invites insert keeps the public row's
// created_at and expires_at and stamps the rest with the clock at the lock.
func TestTakeOverAfterAWaitStampsTheAuthInviteRow(t *testing.T) {
	e := getEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	org, inviter, email := e.seedOrg(t, ctx), e.seedUser(t, ctx, address(), true), address()
	id := uuid.New()
	token, digest := signedtoken.Build(id, e.secret)
	created := time.Now().UTC().Add(-2 * time.Hour).Truncate(time.Microsecond)
	expires := time.Now().UTC().Add(time.Hour).Truncate(time.Microsecond)
	if _, err := e.admin.Exec(ctx, `INSERT INTO public.org_invites (id, org_id, email, role, token_hash, invited_by_id, status, expires_at, created_at, updated_at)
VALUES ($1, $2, $3, 'viewer', $4, $5, 'pending', $6, $7, $7)`, id, org, email, digest, inviter, expires, created); err != nil {
		t.Fatal(err)
	}
	user := e.seedUser(t, ctx, email, true)
	saved := e.now
	defer e.setNow(saved)
	e.setNow(expires.Add(-time.Minute))
	holder, pid := e.holdRow(t, ctx, "public.org_invites", id)
	done := e.acceptBehind(t, ctx, pid, AcceptInput{Token: token, PrincipalID: user, PublicAudit: &PublicAudit{}})
	moved := expires.Add(-time.Second)
	e.setNow(moved)
	if err := holder.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatalf("an accept still valid at the lock = %v, want nil", err)
	}
	var aCreated, aExpires, aUpdated, aAccepted, pCreated, pExpires time.Time
	if err := e.admin.QueryRow(ctx, `SELECT a.created_at, a.expires_at, a.updated_at, a.accepted_at, p.created_at, p.expires_at
FROM auth.invites a JOIN public.org_invites p ON p.id = a.id WHERE a.id = $1`, id).
		Scan(&aCreated, &aExpires, &aUpdated, &aAccepted, &pCreated, &pExpires); err != nil {
		t.Fatal(err)
	}
	if !aCreated.Equal(pCreated) || !aExpires.Equal(pExpires) || !aCreated.Equal(created) || !aExpires.Equal(expires) {
		t.Fatalf("auth created %v expires %v, want the public row's %v and %v", aCreated, aExpires, created, expires)
	}
	if !aUpdated.Equal(moved) || !aAccepted.Equal(moved) {
		t.Fatalf("auth updated %v accepted %v, want the clock at the lock %v", aUpdated, aAccepted, moved)
	}
}

// CHAOS-12175: the accept projects the organization before the invite lock, on its own commit. An accept that
// then waits past the expiry is refused and keeps the projection, and nothing else.
func TestRefusedAcceptAtTheLockKeepsTheProjectedOrganization(t *testing.T) {
	e := getEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	inviter, email := e.seedUser(t, ctx, address(), true), address()
	org, id := uuid.New(), uuid.New()
	if _, err := e.admin.Exec(ctx, `INSERT INTO public.organizations (id, slug, name) VALUES ($1, $2, 'invite-test')`,
		org, "org-"+org.String()[:8]); err != nil {
		t.Fatal(err)
	}
	token, digest := signedtoken.Build(id, e.secret)
	expires := time.Now().UTC().Add(time.Hour).Truncate(time.Microsecond)
	if _, err := e.admin.Exec(ctx, `INSERT INTO public.org_invites (id, org_id, email, role, token_hash, invited_by_id, status, expires_at, created_at, updated_at)
VALUES ($1, $2, $3, 'viewer', $4, $5, 'pending', $6, now(), now())`, id, org, email, digest, inviter, expires); err != nil {
		t.Fatal(err)
	}
	user := e.seedUser(t, ctx, email, true)
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.organizations WHERE id = $1`, org); n != 0 {
		t.Fatalf("auth.organizations rows before the accept = %d, want 0", n)
	}
	in := AcceptInput{Token: token, PrincipalID: user, PublicAudit: &PublicAudit{}, EnsureOrganization: e.identity.EnsureOrganization}
	saved := e.now
	defer e.setNow(saved)
	e.setNow(expires.Add(-time.Second))
	holder, pid := e.holdRow(t, ctx, "public.org_invites", id)
	done := e.acceptBehind(t, ctx, pid, in)
	e.setNow(expires.Add(time.Second))
	if err := holder.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-done; !errors.Is(err, ErrInviteInvalid) {
		t.Fatalf("an accept that waited past the expiry = %v, want ErrInviteInvalid", err)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.organizations a JOIN public.organizations p ON p.id = a.id
WHERE a.id = $1 AND a.slug = p.slug AND a.name = p.name AND (a.disabled_at IS NULL) = p.is_active`, org); n != 1 {
		t.Fatalf("projected organization rows mirroring public = %d, want 1", n)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE event_type = 'auth.organization.projected' AND organization_id = $1`, org.String()); n != 1 {
		t.Fatalf("projection audit rows = %d, want 1", n)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.auth_outbox_events WHERE event_type = 'organization.created' AND aggregate_id = $1`, "org_"+org.String()); n != 1 {
		t.Fatalf("projection outbox rows = %d, want 1", n)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM public.memberships WHERE org_id = $1`, org); n != 0 {
		t.Fatalf("public membership rows = %d, want 0", n)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.memberships WHERE organization_id = $1`, org); n != 0 {
		t.Fatalf("auth membership rows = %d, want 0", n)
	}
	if r := e.inviteRows(t, ctx, id); r.publicStatus != "pending" || r.authStatus != "" {
		t.Fatalf("invite rows = %+v, want public pending and no auth row", r)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM public.audit_logs WHERE org_id = $1 AND action = 'member_joined'`, org); n != 0 {
		t.Fatalf("member_joined rows = %d, want 0", n)
	}
	if _, err := e.store.Accept(ctx, in); !errors.Is(err, ErrInviteInvalid) {
		t.Fatalf("a repeat accept = %v, want ErrInviteInvalid", err)
	}
}
