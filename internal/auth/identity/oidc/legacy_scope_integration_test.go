//go:build integration

package oidc

import (
	"net/http"
	"testing"

	"github.com/google/uuid"
)

// Switch off, end to end: a go-api account bound to another OIDC provider row of the provider organization carries
// the asserted subject. It is that provider's identity: the callback does not sign in as it, answers as for a new
// account, and writes nothing.
func TestSwitchOffSignInIgnoresAnAccountBoundToAnotherProvider(t *testing.T) {
	h := newHarness(t, harnessOpts{signup: false})
	other := h.seedProvider(t, h.orgID, map[string]any{"client_id": "client-" + uuid.NewString(), "issuer": h.issuer,
		"token_endpoint": h.idp.server.URL + "/token", "jwks_uri": h.idp.server.URL + "/jwks"}, true, nil)
	email := uniqueEmail("bound-elsewhere")
	user := h.seedPublicUser(t, email, "oidc", "sub-bound-elsewhere")
	h.bindSSO(t, user, other)
	h.seedMembership(t, user, h.orgID, "member")
	before := h.snapshot(t)
	s := h.start(t)
	login, failure := h.finish(t, s, h.sign(t, h.claims(s.nonce, "sub-bound-elsewhere", uniqueEmail("asserted"))))
	if failure == nil && login.PrincipalID == user {
		t.Fatal("the subject of an account bound to another provider row signed in as that account")
	}
	wantDenied(t, failure, http.StatusForbidden, ReasonMembershipRequired)
	wantUnchanged(t, h, before)
}

// Switch off, end to end: a member go-api account that carries the asserted subject but is bound to no provider row
// is no provider's identity. The callback answers 409 identity_conflict and writes nothing.
func TestSwitchOffSignInRefusesAnUnboundAccount(t *testing.T) {
	h := newHarness(t, harnessOpts{signup: false})
	email := uniqueEmail("unbound")
	user := h.seedPublicUser(t, email, "oidc", "sub-unbound")
	h.seedMembership(t, user, h.orgID, "member")
	before := h.snapshot(t)
	s := h.start(t)
	login, failure := h.finish(t, s, h.sign(t, h.claims(s.nonce, "sub-unbound", email)))
	if failure == nil && login.PrincipalID == user {
		t.Fatal("an unbound account signed in")
	}
	wantDenied(t, failure, http.StatusConflict, ReasonIdentityConflict)
	wantUnchanged(t, h, before)
}
