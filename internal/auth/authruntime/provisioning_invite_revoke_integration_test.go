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
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

func (e *routesEnv) createInvite(t *testing.T, header map[string]string, org, actor uuid.UUID) string {
	t.Helper()
	got := e.doWith(t, "/internal/v1/invites/create", jsonBody(t, map[string]any{"organization_id": org.String(), "email": acctMail(),
		"role": "member", "actor_principal_id": actor.String()}), header)
	if got.status != http.StatusOK {
		t.Fatalf("create: %d %s", got.status, got.body)
	}
	id, _ := object(t, got)["invite_id"].(string)
	return id
}

func (e *routesEnv) revokeVia(t *testing.T, header map[string]string, org, actor uuid.UUID, inviteID string) answer {
	t.Helper()
	return e.doWith(t, "/internal/v1/invites/revoke", jsonBody(t, map[string]any{"organization_id": org.String(), "invite_id": inviteID,
		"actor_principal_id": actor.String()}), header)
}

func (e *routesEnv) inviteStatuses(t *testing.T, ctx context.Context, id string) (public, auth string) {
	t.Helper()
	if err := e.admin.QueryRow(ctx, `SELECT COALESCE((SELECT status FROM org_invites WHERE id = $1::uuid), ''),
  COALESCE((SELECT status FROM auth.invites WHERE id = $1::uuid), '')`, id).Scan(&public, &auth); err != nil {
		t.Fatal(err)
	}
	return public, auth
}

func (e *routesEnv) revokeAudits(t *testing.T, ctx context.Context, org uuid.UUID) int {
	t.Helper()
	var n int
	if err := e.admin.QueryRow(ctx, `SELECT count(*) FROM auth.security_audit_events WHERE event_type = 'auth.invite.revoked' AND organization_id = $1`,
		org.String()).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// The owner revokes a pending invite through the route: both rows revoked,
// one audit row. A repeat is a 200 no-op with no second audit row; an accepted
// invite is 409 invite_not_pending; an unknown id is 404 invite_not_found.
func TestInviteRevokeRoutesThroughConfigure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	e, header := newProvisioningEnv(t, ctx)
	owner, org := e.seed(t, ctx)
	id := e.createInvite(t, header, org, owner)
	got := e.revokeVia(t, header, org, owner, id)
	if got.status != http.StatusOK || object(t, got)["invite_id"] != id || object(t, got)["status"] != "revoked" {
		t.Fatalf("revoke: %d %s", got.status, got.body)
	}
	if pub, auth := e.inviteStatuses(t, ctx, id); pub != "revoked" || auth != "revoked" {
		t.Fatalf("after the revoke: public %q auth %q", pub, auth)
	}
	if n := e.revokeAudits(t, ctx, org); n != 1 {
		t.Fatalf("revoke audit rows = %d, want 1", n)
	}
	if again := e.revokeVia(t, header, org, owner, id); again.status != http.StatusOK {
		t.Fatalf("repeat: %d %s, want 200", again.status, again.body)
	}
	if n := e.revokeAudits(t, ctx, org); n != 1 {
		t.Fatalf("revoke audit rows after the repeat = %d, want 1", n)
	}
	address := acctMail()
	token := e.goAPIInvite(t, ctx, org, owner, address, provisioningInviteSecret, "pending", time.Now().Add(24*time.Hour))
	invitee := e.account(t, ctx, address, false, true)
	if accepted := e.doWith(t, "/internal/v1/invites/accept", jsonBody(t, map[string]any{"token": token, "principal_id": invitee.String()}), header); accepted.status != http.StatusOK {
		t.Fatalf("accept: %d %s", accepted.status, accepted.body)
	}
	var acceptedID string
	if err := e.admin.QueryRow(ctx, `SELECT id::text FROM org_invites WHERE org_id = $1 AND status = 'accepted'`, org).Scan(&acceptedID); err != nil {
		t.Fatal(err)
	}
	if refused := e.revokeVia(t, header, org, owner, acceptedID); refused.status != http.StatusConflict || object(t, refused)["reason_code"] != "invite_not_pending" {
		t.Fatalf("revoke of an accepted invite: %d %s, want 409 invite_not_pending", refused.status, refused.body)
	}
	if unknown := e.revokeVia(t, header, org, owner, uuid.NewString()); unknown.status != http.StatusNotFound || object(t, unknown)["reason_code"] != "invite_not_found" {
		t.Fatalf("revoke of an unknown invite: %d %s, want 404 invite_not_found", unknown.status, unknown.body)
	}
}

// A revoke of an invite go-api made (public.org_invites only) takes it over:
// auth.invites gets the row, revoked, naming its inviter.
func TestInviteRevokeOfAGoAPIOnlyInviteTakesItOver(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	e, header := newProvisioningEnv(t, ctx)
	owner, org := e.seed(t, ctx)
	e.goAPIInvite(t, ctx, org, owner, acctMail(), provisioningInviteSecret, "pending", time.Now().Add(24*time.Hour))
	var id string
	if err := e.admin.QueryRow(ctx, `SELECT id::text FROM org_invites WHERE org_id = $1`, org).Scan(&id); err != nil {
		t.Fatal(err)
	}
	if got := e.revokeVia(t, header, org, owner, id); got.status != http.StatusOK {
		t.Fatalf("revoke: %d %s", got.status, got.body)
	}
	var status string
	var invitedBy *uuid.UUID
	if err := e.admin.QueryRow(ctx, `SELECT status, invited_by FROM auth.invites WHERE id = $1::uuid`, id).Scan(&status, &invitedBy); err != nil {
		t.Fatalf("the take-over row: %v", err)
	}
	if status != "revoked" || invitedBy == nil || *invitedBy != owner {
		t.Fatalf("auth.invites row = %q invited_by %v, want revoked by %s", status, invitedBy, owner)
	}
}

// The admin's demotion holds the admin's rows while the admin's revoke
// starts: the revoke waits for it, then is refused, and the invite stays
// pending.
func TestInviteRevokeWaitsForTheActorsDemotion(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	e, header := newProvisioningEnv(t, ctx)
	owner, org := e.seed(t, ctx)
	admin := e.account(t, ctx, acctMail(), false, true)
	e.addVia(t, header, org, admin, owner, "admin")
	id := e.createInvite(t, header, org, owner)
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
	revoked := make(chan answer, 1)
	go func() { revoked <- e.revokeVia(t, header, org, admin, id) }()
	e.waitForALockWait(t, ctx, demotionPID, revoked)
	free()
	if err := <-demoted; err != nil {
		t.Fatalf("demotion: %v", err)
	}
	got := <-revoked
	if got.status != http.StatusForbidden || object(t, got)["reason_code"] != "admin_required" {
		t.Fatalf("revoke after the demotion: %d %s, want 403 admin_required", got.status, got.body)
	}
	if pub, auth := e.inviteStatuses(t, ctx, id); pub != "pending" || auth != "pending" {
		t.Fatalf("after a refused revoke: public %q auth %q, want pending", pub, auth)
	}
}

// A refused actor learns nothing: a viewer gets the same 403 for an existing
// and for an unknown invite, nothing is audited, the invite stays pending.
func TestInviteRevokeByARefusedActorLearnsNothing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	e, header := newProvisioningEnv(t, ctx)
	owner, org := e.seed(t, ctx)
	viewer := e.account(t, ctx, acctMail(), false, true)
	e.addVia(t, header, org, viewer, owner, "viewer")
	id := e.createInvite(t, header, org, owner)
	existing := e.revokeVia(t, header, org, viewer, id)
	unknown := e.revokeVia(t, header, org, viewer, uuid.NewString())
	for name, got := range map[string]answer{"existing": existing, "unknown": unknown} {
		if got.status != http.StatusForbidden || object(t, got)["reason_code"] != "admin_required" {
			t.Fatalf("%s invite: %d %s, want 403 admin_required", name, got.status, got.body)
		}
	}
	if n := e.revokeAudits(t, ctx, org); n != 0 {
		t.Fatalf("revoke audit rows = %d, want 0", n)
	}
	if pub, auth := e.inviteStatuses(t, ctx, id); pub != "pending" || auth != "pending" {
		t.Fatalf("after a refused revoke: public %q auth %q, want pending", pub, auth)
	}
}
