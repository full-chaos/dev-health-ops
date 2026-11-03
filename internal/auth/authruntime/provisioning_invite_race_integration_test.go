//go:build integration

package authruntime

import (
	"context"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/membership"
	"github.com/full-chaos/dev-health-ops/internal/auth/signedtoken"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// publicOnlyOrg inserts an organization as go-api writes it, with no
// auth.organizations row.
func (e *routesEnv) publicOnlyOrg(t *testing.T, ctx context.Context) uuid.UUID {
	t.Helper()
	org := uuid.New()
	if _, err := e.admin.Exec(ctx, `INSERT INTO organizations (id, slug, name, tier, is_active, settings, managed_by, created_at, updated_at)
VALUES ($1, $2, 'Invite Org', 'community', true, '{}', 'self', now(), now())`, org, "invite-"+org.String()[:8]); err != nil {
		t.Fatalf("seed organization: %v", err)
	}
	return org
}

// goAPIInvite inserts an invite as go-api writes it (public.org_invites only),
// its token signed with secret, and returns the token.
func (e *routesEnv) goAPIInvite(t *testing.T, ctx context.Context, org, inviter uuid.UUID, email, secret, status string, expires time.Time) string {
	t.Helper()
	id := uuid.New()
	token, digest := signedtoken.Build(id, secret)
	if _, err := e.admin.Exec(ctx, `INSERT INTO org_invites (id, org_id, email, role, token_hash, invited_by_id, status, expires_at, created_at, updated_at)
VALUES ($1, $2, $3, 'member', $4, $5, $6, $7, now(), now())`, id, org, email, digest, inviter, status, expires); err != nil {
		t.Fatalf("seed invite: %v", err)
	}
	return token
}

func (e *routesEnv) authOrgRows(t *testing.T, ctx context.Context, org uuid.UUID) int {
	t.Helper()
	var n int
	if err := e.admin.QueryRow(ctx, `SELECT count(*) FROM auth.organizations WHERE id = $1`, org).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func (e *routesEnv) inviteRowsFor(t *testing.T, ctx context.Context, org uuid.UUID) int {
	t.Helper()
	var n int
	if err := e.admin.QueryRow(ctx, `SELECT (SELECT count(*) FROM org_invites WHERE org_id = $1) + (SELECT count(*) FROM auth.invites WHERE organization_id = $1)`,
		org).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// waitForALockWait returns once a backend waits for a lock that the backend
// blocker holds, or fails the test if answered closes first or no wait shows
// within the bound.
func (e *routesEnv) waitForALockWait(t *testing.T, ctx context.Context, blocker int32, answered <-chan answer) {
	t.Helper()
	bound := time.After(10 * time.Second)
	for {
		var waiting int
		if err := e.admin.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity WHERE $1::int = ANY(pg_blocking_pids(pid))`, blocker).
			Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting > 0 {
			return
		}
		select {
		case got := <-answered:
			t.Fatalf("the invite create answered %d %s while the actor's rows were held", got.status, got.body)
		case <-bound:
			t.Fatal("the invite create never waited for the actor's rows")
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func (e *routesEnv) createInviteAsync(t *testing.T, header map[string]string, org, actor uuid.UUID, email string) <-chan answer {
	out := make(chan answer, 1)
	body := jsonBody(t, map[string]any{"organization_id": org.String(), "email": email, "role": "member", "actor_principal_id": actor.String()})
	go func() { out <- e.doWith(t, "/internal/v1/invites/create", body, header) }()
	return out
}

// An accept of a go-api invite whose organization auth-service has not
// projected yet projects it, then joins: one member row in each schema.
func TestInviteAcceptProjectsAMissingOrganization(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	e, header := newProvisioningEnv(t, ctx)
	org := e.publicOnlyOrg(t, ctx)
	inviter := e.account(t, ctx, acctMail(), false, true)
	address := acctMail()
	token := e.goAPIInvite(t, ctx, org, inviter, address, provisioningInviteSecret, "pending", time.Now().Add(24*time.Hour))
	invitee := e.account(t, ctx, address, false, true)
	got := e.doWith(t, "/internal/v1/invites/accept", jsonBody(t, map[string]any{"token": token, "principal_id": invitee.String()}), header)
	if got.status != http.StatusOK || object(t, got)["organization_id"] != org.String() {
		t.Fatalf("accept: %d %s", got.status, got.body)
	}
	if n := e.authOrgRows(t, ctx, org); n != 1 {
		t.Fatalf("auth.organizations rows = %d, want 1", n)
	}
	if pub, auth := e.memberRows(t, ctx, org, invitee); pub != "member" || auth != "active" {
		t.Fatalf("after accept: public %q auth %q", pub, auth)
	}
}

// A token that cannot be accepted projects no organization: the refusal is
// invite_invalid and auth.organizations stays without the row.
func TestInviteAcceptOfAnUnusableTokenProjectsNothing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	e, header := newProvisioningEnv(t, ctx)
	inviter := e.account(t, ctx, acctMail(), false, true)
	for _, c := range []struct {
		name, secret, status string
		expires              time.Time
	}{
		{"expired", provisioningInviteSecret, "pending", time.Now().Add(-time.Hour)},
		{"revoked", provisioningInviteSecret, "revoked", time.Now().Add(24 * time.Hour)},
		{"badly signed", "not-an-invite-key-of-this-env-0123", "pending", time.Now().Add(24 * time.Hour)},
	} {
		t.Run(c.name, func(t *testing.T) {
			org := e.publicOnlyOrg(t, ctx)
			address := acctMail()
			token := e.goAPIInvite(t, ctx, org, inviter, address, c.secret, c.status, c.expires)
			invitee := e.account(t, ctx, address, false, true)
			got := e.doWith(t, "/internal/v1/invites/accept", jsonBody(t, map[string]any{"token": token, "principal_id": invitee.String()}), header)
			if got.status != http.StatusBadRequest || object(t, got)["reason_code"] != "invite_invalid" {
				t.Fatalf("accept: %d %s, want 400 invite_invalid", got.status, got.body)
			}
			if n := e.authOrgRows(t, ctx, org); n != 0 {
				t.Fatalf("auth.organizations rows = %d, want 0", n)
			}
		})
	}
}

// The admin's demotion holds the admin's rows (the membership verb's locks)
// while an invite create by that admin starts: the create waits for it, then
// is refused as admin_required and writes no invite.
func TestInviteCreateWaitsForTheActorsDemotion(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	e, header := newProvisioningEnv(t, ctx)
	owner, org := e.seed(t, ctx)
	admin := e.account(t, ctx, acctMail(), false, true)
	e.addVia(t, header, org, admin, owner, "admin")
	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		t.Fatal(err)
	}
	members, err := membership.New(membership.Config{Pool: e.runtime, Schema: schema, OwnsUsers: true})
	if err != nil {
		t.Fatal(err)
	}
	held, release := make(chan struct{}), make(chan struct{})
	var demotionPID int32
	var once sync.Once
	free := func() { once.Do(func() { close(release) }) }
	defer free()
	demoted := make(chan error, 1)
	go func() {
		_, err := members.ChangeRole(ctx, membership.Change{OrgID: org, PrincipalID: admin, Role: "viewer",
			Actor: membership.Actor{PrincipalID: &owner},
			Authorize: func(ctx context.Context, tx audit.TxOps, _ membership.Locked) error {
				err := tx.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&demotionPID)
				close(held)
				<-release
				return err
			}})
		demoted <- err
	}()
	select {
	case <-held:
	case err := <-demoted:
		t.Fatalf("the demotion ended before its Authorize: %v", err)
	}
	created := e.createInviteAsync(t, header, org, admin, acctMail())
	e.waitForALockWait(t, ctx, demotionPID, created)
	free()
	if err := <-demoted; err != nil {
		t.Fatalf("demotion: %v", err)
	}
	got := <-created
	if got.status != http.StatusForbidden || object(t, got)["reason_code"] != "admin_required" {
		t.Fatalf("create after the demotion: %d %s, want 403 admin_required", got.status, got.body)
	}
	if n := e.inviteRowsFor(t, ctx, org); n != 0 {
		t.Fatalf("a refused create wrote %d invite rows", n)
	}
}

// go-api's writers lock the actor's users row (its guard, its user delete):
// a role change or a disable that holds that row makes the create wait, and
// after it commits the create is refused.
func TestInviteCreateWaitsForGoAPIsWriteOfTheActor(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	e, header := newProvisioningEnv(t, ctx)
	for _, c := range []struct{ name, sql string }{
		{"role change", `UPDATE memberships SET role = 'viewer' WHERE org_id = $1 AND user_id = $2`},
		{"disable", `UPDATE users SET is_active = false WHERE id = $2 AND $1::uuid IS NOT NULL`},
	} {
		t.Run(c.name, func(t *testing.T) {
			owner, org := e.seed(t, ctx)
			admin := e.account(t, ctx, acctMail(), false, true)
			e.addVia(t, header, org, admin, owner, "admin")
			tx, err := e.admin.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = tx.Rollback(context.Background()) }()
			if _, err := tx.Exec(ctx, `SELECT 1 FROM users WHERE id = $1 FOR UPDATE`, admin); err != nil {
				t.Fatal(err)
			}
			if _, err := tx.Exec(ctx, c.sql, org, admin); err != nil {
				t.Fatal(err)
			}
			var writerPID int32
			if err := tx.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&writerPID); err != nil {
				t.Fatal(err)
			}
			created := e.createInviteAsync(t, header, org, admin, acctMail())
			e.waitForALockWait(t, ctx, writerPID, created)
			if err := tx.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			got := <-created
			if got.status != http.StatusForbidden || object(t, got)["reason_code"] != "admin_required" {
				t.Fatalf("create after the %s: %d %s, want 403 admin_required", c.name, got.status, got.body)
			}
			if n := e.inviteRowsFor(t, ctx, org); n != 0 {
				t.Fatalf("a refused create wrote %d invite rows", n)
			}
		})
	}
}

// An invite create races the actor's disable: the create is refused, or it
// commits before the disable writes the users row. Never a create by an
// actor whose disable had already taken that row.
func TestInviteCreateRacesTheActorsDisable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	e, header := newProvisioningEnv(t, ctx)
	owner, org := e.seed(t, ctx)
	outcomes := map[int]int{}
	for round := range 20 {
		admin := e.account(t, ctx, acctMail(), false, true)
		e.addVia(t, header, org, admin, owner, "admin")
		created := e.createInviteAsync(t, header, org, admin, acctMail())
		var seen int
		tx, err := e.admin.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(ctx, `UPDATE users SET is_active = false WHERE id = $1`, admin); err != nil {
			t.Fatal(err)
		}
		// seen is what had committed when the disable took the users row.
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM auth.invites WHERE invited_by = $1`, admin).Scan(&seen); err != nil {
			t.Fatal(err)
		}
		// The disable holds the row; a create that does not wait for it has the time to commit now.
		if _, err := tx.Exec(ctx, `SELECT pg_sleep(0.05)`); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		got := <-created
		switch got.status {
		case http.StatusOK:
			if seen != 1 {
				t.Fatalf("round %d: a create by %s committed after its disable took the users row", round, admin)
			}
		case http.StatusForbidden:
			if object(t, got)["reason_code"] != "admin_required" && object(t, got)["reason_code"] != "principal_deactivated" {
				t.Fatalf("round %d: create refused %s", round, got.body)
			}
		default:
			t.Fatalf("round %d: create %d %s", round, got.status, got.body)
		}
		outcomes[got.status]++
	}
	t.Logf("create outcomes: %v", outcomes)
}

// Invite creates, the actor's demotion and the actor's member adds run at
// once: every answer is a 2xx or a refusal, never a 5xx (a deadlock is 5xx).
func TestInviteCreateDemotionAndAddTakeTheOneLockOrder(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	e, header := newProvisioningEnv(t, ctx)
	owner, org := e.seed(t, ctx)
	outcomes := map[string]int{}
	for round := range 20 {
		admin := e.account(t, ctx, acctMail(), false, true)
		e.addVia(t, header, org, admin, owner, "admin")
		target := e.account(t, ctx, acctMail(), false, true)
		var wg sync.WaitGroup
		answers := make([]answer, 3)
		calls := []struct{ path, body string }{
			{"/internal/v1/invites/create", jsonBody(t, map[string]any{"organization_id": org.String(), "email": acctMail(), "role": "member",
				"actor_principal_id": admin.String()})},
			{"/internal/v1/memberships/role", jsonBody(t, map[string]any{"organization_id": org.String(), "principal_id": admin.String(),
				"actor_principal_id": owner.String(), "role": "viewer"})},
			{"/internal/v1/memberships/add", jsonBody(t, map[string]any{"organization_id": org.String(), "principal_id": target.String(),
				"actor_principal_id": admin.String(), "role": "member"})},
		}
		for i, c := range calls {
			wg.Add(1)
			go func() {
				defer wg.Done()
				answers[i] = e.doWith(t, c.path, c.body, header)
			}()
		}
		wg.Wait()
		for i, got := range answers {
			if got.status >= 500 {
				t.Fatalf("round %d: %s answered %d %s", round, calls[i].path, got.status, got.body)
			}
			outcomes[calls[i].path+" "+http.StatusText(got.status)]++
		}
	}
	t.Logf("outcomes: %v", outcomes)
}
