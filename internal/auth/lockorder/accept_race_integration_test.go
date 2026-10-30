//go:build integration

package lockorder

import (
	"net/http"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/auth/invite"
)

// The cells below race go-api's session invite accept with a writer that
// removes or revokes its invite or organization. The accept must answer the
// refusal the route already uses for that state, never a 500, and never accept
// an invite that is no longer pending.

// wantRefusal fails the cell unless p answered status with go-api's session
// refusal message (`{"detail": {"message": ...}}`).
func wantRefusal(t *testing.T, cell string, p *party, status int, message string) {
	t.Helper()
	detail, _ := p.resp.body["detail"].(map[string]any)
	if p.resp.status != status || len(detail) != 1 || detail["message"] != message {
		t.Fatalf("%s: race answer: %s, want %d %q", cell, p, status, message)
	}
}

// The organization delete commits after the accept committed its membership
// and before it writes the refresh token (which names the organization).
func TestAcceptAnswersOrgGoneWhenTheOrgIsDeletedBeforeTheTokenWrite(t *testing.T) {
	const cell = "accept/org-gone-at-token"
	e, ctx := getEnv(t), cellContext(t)
	e.cleanup(t)
	org := e.seedOrg(t, ctx)
	owner, acceptor := e.seedPair(t, ctx)
	e.add(t, ctx, org, owner, "owner", nil)
	_, token := e.goInvite(t, ctx, org, owner, email(acceptor))

	reached := e.goBarrier.Arm("INSERT INTO refresh_tokens")
	g := goAPI("go-api accept-invite", func() response { return e.acceptInvite(ctx, acceptor, token) })
	awaitReached(t, cell, "accept-invite's refresh token write", reached, g)
	d := goAPI("go-api delete organization", func() response { return e.deleteOrg(ctx, org) })
	select {
	case <-d.done:
	case <-time.After(2 * choreographyBound):
		t.Fatalf("%s: choreography: %s did not end while the accept was held", cell, d.name)
	}
	wantStatus(t, cell, d, http.StatusOK)
	e.goBarrier.Release()
	e.finish(t, cell, g, d)

	wantRefusal(t, cell, g, http.StatusNotFound, "Organization not found")
	if got := e.scalar(t, ctx, `SELECT (SELECT count(*) FROM public.refresh_tokens WHERE user_id = $1)
  + (SELECT count(*) FROM public.memberships WHERE org_id = $2)`, acceptor, org); got != "0" {
		t.Fatalf("%s: %s token or membership rows left", cell, got)
	}
}

// The organization delete holds the invite rows (its pre-lock) when the accept
// reaches its invite lock, and commits while the accept waits there.
func TestAcceptRefusesAnInviteTheOrgDeleteRemovedWhileItWaited(t *testing.T) {
	const cell = "accept/invite-gone-at-lock"
	e, ctx := getEnv(t), cellContext(t)
	e.cleanup(t)
	org := e.seedOrg(t, ctx)
	owner, acceptor := e.seedPair(t, ctx)
	e.add(t, ctx, org, owner, "owner", nil)
	_, token := e.goInvite(t, ctx, org, owner, email(acceptor))

	reached := e.goBarrier.Arm("DELETE FROM organizations WHERE")
	d := goAPI("go-api delete organization", func() response { return e.deleteOrg(ctx, org) })
	awaitReached(t, cell, "the delete's organization row delete", reached, d)
	g := goAPI("go-api accept-invite", func() response { return e.acceptInvite(ctx, acceptor, token) })
	e.awaitWaiters(t, ctx, cell, 1, g, d)
	e.goBarrier.Release()
	e.finish(t, cell, g, d)

	wantStatus(t, cell, d, http.StatusOK)
	wantRefusal(t, cell, g, http.StatusBadRequest, "Invalid or expired invite")
	if got := e.scalar(t, ctx, `SELECT count(*)::text FROM public.memberships WHERE user_id = $1`, acceptor); got != "0" {
		t.Fatalf("%s: %s membership rows of the account", cell, got)
	}
}

// auth-service revokes the go-api invite while the accept waits on the invite
// row: the accept must see the revoke, not mark a revoked invite accepted.
func TestAcceptRefusesAnInviteRevokedWhileItWaited(t *testing.T) {
	const cell = "accept/invite-revoked-at-lock"
	e, ctx := getEnv(t), cellContext(t)
	e.cleanup(t)
	org := e.seedOrg(t, ctx)
	owner, acceptor := e.seedPair(t, ctx)
	e.add(t, ctx, org, owner, "owner", nil)
	inviteID, token := e.goInvite(t, ctx, org, owner, email(acceptor))

	reached := e.authBarrier.Arm("UPDATE public.org_invites SET status = 'revoked'")
	s := goAuth("invite Revoke", func() error {
		return e.invites.Revoke(ctx, invite.RevokeInput{OrgID: org, InviteID: inviteID, Actor: invite.Actor{PrincipalID: &owner}, Authorize: allowInvite})
	})
	awaitReached(t, cell, "Revoke's update of go-api's invite row", reached, s)
	g := goAPI("go-api accept-invite", func() response { return e.acceptInvite(ctx, acceptor, token) })
	e.awaitWaiters(t, ctx, cell, 1, g, s)
	e.authBarrier.Release()
	e.finish(t, cell, g, s)

	wantNoErr(t, cell, s)
	wantRefusal(t, cell, g, http.StatusBadRequest, "Invalid or expired invite")
	if got := e.scalar(t, ctx, `SELECT status FROM public.org_invites WHERE id = $1`, inviteID); got != "revoked" {
		t.Fatalf("%s: race answer: go-api invite status %q, want revoked", cell, got)
	}
	if got := e.scalar(t, ctx, `SELECT count(*)::text FROM public.memberships WHERE org_id = $1 AND user_id = $2`, org, acceptor); got != "0" {
		t.Fatalf("%s: race answer: %s membership rows for a revoked invite", cell, got)
	}
}
