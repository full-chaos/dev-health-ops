//go:build integration

package oidc

import (
	"context"
	"net/http"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
)

// End to end: after the users cutover, a bound go-api account whose principal already holds another
// subject at this provider row (TestOwnedSSORelinkKeepsOneIdentityPerProvider at the store) signs in with its stored
// subject. The re-link refuses with identity.ErrProviderAlreadyLinked; the callback answers 409 identity_conflict and
// writes nothing.
func TestOwnedRelinkBesideAHeldSubjectAnswersIdentityConflict(t *testing.T) {
	h := newHarness(t, harnessOpts{signup: true})
	ctx := context.Background()
	email := uniqueEmail("relink-held")
	held := "sub-held-" + uuid.NewString()
	principal, err := h.ids.on.Store.ProvisionFederated(ctx, identity.ProvisionRequest{
		Key:   identity.ExternalKey{Issuer: h.issuer, Provider: identity.Provider(h.providerID.String()), Subject: held},
		Email: email, Actor: identity.SelfSignUp(nil), LegacyProvider: "oidc", LegacySubject: held,
	})
	if err != nil {
		t.Fatalf("provision the account with its held subject: %v", err)
	}
	second := "sub-second-" + uuid.NewString()
	if _, err := h.env.admin.Exec(ctx, `UPDATE public.users SET auth_provider_id = $2, sso_provider_id = $3 WHERE id = $1`,
		principal, second, h.providerID); err != nil {
		t.Fatalf("bind the go-api account to the provider row: %v", err)
	}
	h.seedMembership(t, principal, h.orgID, "member")
	before := h.snapshot(t)
	s := h.start(t)
	_, failure := h.finish(t, s, h.sign(t, h.claims(s.nonce, second, email)))
	wantDenied(t, failure, http.StatusConflict, ReasonIdentityConflict)
	wantUnchanged(t, h, before)
	if n := h.count(t, `SELECT count(*) FROM auth.external_identities WHERE principal_id = $1`, principal); n != 1 {
		t.Fatalf("identity rows = %d, want only the held one", n)
	}
}

// Switch off: a go-api account projected with one subject at this provider row, whose public row now carries another
// subject, signs in with the new one. The legacy projection refuses with identity.ErrProviderAlreadyLinked; the
// callback answers 409 identity_conflict and writes nothing.
func TestSwitchOffProjectionBesideAHeldSubjectAnswersIdentityConflict(t *testing.T) {
	h := newHarness(t, harnessOpts{signup: false})
	ctx := context.Background()
	email := uniqueEmail("projected-held")
	first := "sub-first-" + uuid.NewString()
	user := h.seedPublicUser(t, email, "oidc", first)
	h.seedMembership(t, user, h.orgID, "member")
	s := h.start(t)
	if login, failure := h.finish(t, s, h.sign(t, h.claims(s.nonce, first, email))); failure != nil || login.PrincipalID != user {
		t.Fatalf("first sign-in = %+v, %v; want the projected account %v", login, failure, user)
	}
	second := "sub-second-" + uuid.NewString()
	if _, err := h.env.admin.Exec(ctx, `UPDATE public.users SET auth_provider_id = $2 WHERE id = $1`, user, second); err != nil {
		t.Fatal(err)
	}
	before := h.snapshot(t)
	s2 := h.start(t)
	_, failure := h.finish(t, s2, h.sign(t, h.claims(s2.nonce, second, email)))
	wantDenied(t, failure, http.StatusConflict, ReasonIdentityConflict)
	wantUnchanged(t, h, before)
}
