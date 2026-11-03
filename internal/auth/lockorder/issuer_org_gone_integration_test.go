//go:build integration

package lockorder

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/sessionissue"
)

// With auth-service owning sessions (AUTH_PROXY_SESSIONS), go-api's accept
// commits the membership and then asks auth-service's REAL issue route for the
// pair. The cells hold that route at its membership read and change the
// organization in between.

// issuerAccept is go-api's accept-invite on the issuer-mode session routes.
func (e *env) issuerAccept(ctx context.Context, user uuid.UUID, token string) response {
	bearer, err := e.token(user, false)
	if err != nil {
		return response{status: -1, raw: err.Error()}
	}
	return e.post(ctx, e.issuerServer.URL, "/api/v1/auth/accept-invite", bearer, map[string]any{"token": token})
}

// holdIssueThenRun holds the issue route at its membership read, runs the
// change while it waits, then releases it and returns the accept.
func (e *env) holdIssueThenRun(t *testing.T, ctx context.Context, cell string, acceptor uuid.UUID, token, name string, f func() response) *party {
	t.Helper()
	reached := e.authBarrier.Arm("SELECT role FROM public.memberships WHERE user_id = $1 AND org_id = $2")
	g := goAPI("go-api accept-invite (issuer)", func() response { return e.issuerAccept(ctx, acceptor, token) })
	awaitReached(t, cell, "the issue route's membership read", reached, g)
	change := goAPI(name, f)
	select {
	case <-change.done:
	case <-time.After(2 * choreographyBound):
		t.Fatalf("%s: choreography: %s did not end while the issue was held", cell, change.name)
	}
	wantStatus(t, cell, change, http.StatusOK)
	e.authBarrier.Release()
	e.finish(t, cell, g, change)
	return g
}

// The organization delete commits after the accept committed its membership
// and before auth-service read it: auth-service refuses not_entitled, and the
// accept answers the local path's 404, not 503.
func TestIssuerAcceptAnswersOrgGoneWhenTheOrgIsDeletedBeforeTheIssue(t *testing.T) {
	const cell = "issuer-accept/org-gone-at-issue"
	e, ctx := getEnv(t), cellContext(t)
	e.cleanup(t)
	org := e.seedOrg(t, ctx)
	owner, acceptor := e.seedPair(t, ctx)
	e.add(t, ctx, org, owner, "owner", nil)
	_, token := e.goInvite(t, ctx, org, owner, email(acceptor))

	g := e.holdIssueThenRun(t, ctx, cell, acceptor, token, "go-api delete organization", func() response { return e.deleteOrg(ctx, org) })

	wantRefusal(t, cell, g, http.StatusNotFound, "Organization not found")
	if got := e.scalar(t, ctx, `SELECT (SELECT count(*) FROM auth.sessions WHERE principal_id = $1)
  + (SELECT count(*) FROM public.memberships WHERE org_id = $2)`, acceptor, org); got != "0" {
		t.Fatalf("%s: %s session or membership rows left", cell, got)
	}
}

// The organization stays and only the membership is removed before the issue:
// auth-service refuses not_entitled as above, but the organization is there,
// so the accept keeps the issue route's 503.
func TestIssuerAcceptKeeps503WhenOnlyTheMembershipIsRemoved(t *testing.T) {
	const cell = "issuer-accept/membership-gone-at-issue"
	e, ctx := getEnv(t), cellContext(t)
	e.cleanup(t)
	org := e.seedOrg(t, ctx)
	owner, acceptor := e.seedPair(t, ctx)
	e.add(t, ctx, org, owner, "owner", nil)
	_, token := e.goInvite(t, ctx, org, owner, email(acceptor))

	path := "/api/v1/admin/orgs/" + org.String() + "/members/" + acceptor.String()
	g := e.holdIssueThenRun(t, ctx, cell, acceptor, token, "go-api remove member", func() response {
		return e.call(ctx, e.root, http.MethodDelete, path, nil)
	})

	wantRefusal(t, cell, g, http.StatusServiceUnavailable, sessionissue.UnavailableText)
	if got := e.scalar(t, ctx, `SELECT (SELECT count(*) FROM auth.sessions WHERE principal_id = $1)
  + (SELECT count(*) FROM public.memberships WHERE org_id = $2 AND user_id = $1)`, acceptor, org); got != "0" {
		t.Fatalf("%s: %s session or membership rows left", cell, got)
	}
}
