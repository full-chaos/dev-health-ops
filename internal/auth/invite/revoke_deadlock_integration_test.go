//go:build integration

package invite

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/membership"
	"github.com/full-chaos/dev-health-ops/internal/auth/signedtoken"
)

// A revoke of an invite go-api made (public.org_invites only) writes the
// auth.invites row naming its inviter, whose principal the revoke must hold
// from step 1 like the actor. Inviter B sorts before revoker A, so a
// membership change of B by A locks B, then waits for A: the revoke must not
// then wait for B (a deadlock). Both commit, one after the other.
func TestRevokeOfGoAPIInviteDoesNotDeadlockWithDemotionOfItsInviter(t *testing.T) {
	e := getEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	org := e.seedOrg(t, ctx)
	a, b := e.seedUser(t, ctx, address(), true), e.seedUser(t, ctx, address(), true)
	if bytes.Compare(b[:], a[:]) > 0 {
		a, b = b, a
	}
	allowAll := func(context.Context, audit.TxOps, membership.Locked) error { return nil }
	for _, m := range []struct {
		who  uuid.UUID
		role string
	}{{a, "owner"}, {b, "admin"}} {
		if _, err := e.members.Add(ctx, membership.Change{OrgID: org, PrincipalID: m.who, Role: m.role, Authorize: allowAll}); err != nil {
			t.Fatalf("seed membership: %v", err)
		}
	}
	id := uuid.New()
	_, digest := signedtoken.Build(id, e.secret)
	if _, err := e.admin.Exec(ctx, `INSERT INTO public.org_invites (id, org_id, email, role, token_hash, invited_by_id, status, expires_at, created_at, updated_at)
VALUES ($1, $2, $3, 'member', $4, $5, 'pending', $6, now(), now())`, id, org, address(), digest, b, e.now.Add(24*time.Hour)); err != nil {
		t.Fatal(err)
	}
	inAuthorize, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	free := func() { once.Do(func() { close(release) }) }
	defer free()
	revoked := make(chan error, 1)
	var revokePID int32
	go func() {
		revoked <- e.store.Revoke(ctx, RevokeInput{OrgID: org, InviteID: id, Actor: Actor{PrincipalID: &a},
			Authorize: func(ctx context.Context, tx audit.TxOps, _ uuid.UUID) error {
				err := tx.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&revokePID)
				close(inAuthorize)
				<-release
				return err
			}})
	}()
	select {
	case <-inAuthorize:
	case err := <-revoked:
		t.Fatalf("the revoke ended before its Authorize: %v", err)
	}
	// An unrelated lock wait in the same cluster, held for the whole test: a
	// wait that counted every ungranted lock would end at once, before the
	// demotion even starts.
	plantUnrelatedLockWait(t, ctx, e)
	demoted := make(chan error, 1)
	var startDemotion sync.Once
	started := false
	demote := func() {
		started = true
		go func() {
			_, err := e.members.ChangeRole(ctx, membership.Change{OrgID: org, PrincipalID: b, Role: "viewer",
				Actor: membership.Actor{PrincipalID: &a}, Authorize: allowAll})
			demoted <- err
		}()
	}
	bound := time.After(10 * time.Second)
	// Only a backend that the revoke blocks counts: the demotion. The first
	// check runs before the demotion starts, so it must find none.
	for waiting := 0; waiting == 0; {
		if err := e.admin.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity WHERE $1::int = ANY(pg_blocking_pids(pid))`, revokePID).
			Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting > 0 {
			break
		}
		startDemotion.Do(demote)
		select {
		case err := <-demoted:
			t.Fatalf("the demotion of the inviter ended while the revoke held its rows: %v", err)
		case <-bound:
			t.Fatal("the demotion of the inviter never waited for the revoke")
		case <-time.After(20 * time.Millisecond):
		}
	}
	if !started {
		t.Fatal("the wait for the demotion ended before the demotion started")
	}
	free()
	if err := <-revoked; err != nil {
		t.Fatalf("Revoke: %v", err)
	}
	if err := <-demoted; err != nil {
		t.Fatalf("ChangeRole of the inviter: %v", err)
	}
	if r := e.inviteRows(t, ctx, id); r.publicStatus != "revoked" || r.authStatus != "revoked" {
		t.Fatalf("invite rows = %+v, want revoked in both", r)
	}
	var invitedBy *uuid.UUID
	if err := e.admin.QueryRow(ctx, `SELECT invited_by FROM auth.invites WHERE id = $1`, id).Scan(&invitedBy); err != nil {
		t.Fatal(err)
	}
	if invitedBy == nil || *invitedBy != b {
		t.Fatalf("auth.invites.invited_by = %v, want the inviter %s", invitedBy, b)
	}
}

// plantUnrelatedLockWait makes one backend wait for a row lock another holds,
// on a scratch table, until the test ends.
func plantUnrelatedLockWait(t *testing.T, ctx context.Context, e *env) {
	t.Helper()
	if _, err := e.admin.Exec(ctx, `CREATE TABLE IF NOT EXISTS public.lock_wait_probe (id int PRIMARY KEY);
INSERT INTO public.lock_wait_probe (id) VALUES (1) ON CONFLICT DO NOTHING`); err != nil {
		t.Fatal(err)
	}
	holder, err := e.admin.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := holder.Exec(ctx, `SELECT id FROM public.lock_wait_probe WHERE id = 1 FOR UPDATE`); err != nil {
		t.Fatal(err)
	}
	waited := make(chan struct{})
	go func() {
		defer close(waited)
		_, _ = e.admin.Exec(context.Background(), `UPDATE public.lock_wait_probe SET id = 1 WHERE id = 1`)
	}()
	t.Cleanup(func() {
		_ = holder.Rollback(context.Background())
		<-waited
	})
	bound := time.After(10 * time.Second)
	for {
		var waiting int
		if err := e.admin.QueryRow(ctx, `SELECT count(*) FROM pg_locks WHERE NOT granted`).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting > 0 {
			return
		}
		select {
		case <-bound:
			t.Fatal("the unrelated lock wait never started")
		case <-time.After(20 * time.Millisecond):
		}
	}
}
