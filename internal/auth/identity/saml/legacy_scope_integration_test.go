//go:build integration

package saml_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/identity/saml"
)

// Switch off, end to end: a member go-api account bound to another SAML provider row of the provider organization
// carries the asserted NameID. It is that provider's identity: the sign-in is not that account and writes nothing.
func TestSAMLSwitchOffSignInIgnoresAnAccountBoundToAnotherProvider(t *testing.T) {
	h := newHarness(t, true)
	h.withoutJoins()
	ctx := context.Background()
	other := uuid.New()
	if _, err := h.db.admin.Exec(ctx, `INSERT INTO public.sso_providers (id, org_id, name, protocol, status) VALUES ($1, $2, $3, 'saml', 'active')`,
		other, h.provider.OrgID, "idp-"+other.String()); err != nil {
		t.Fatal(err)
	}
	nameID := "bound-elsewhere-" + uuid.NewString()
	user := h.goAPIUser(uniqueEmail("bound-elsewhere"), "saml", &nameID, false)
	if _, err := h.db.admin.Exec(ctx, `UPDATE public.users SET sso_provider_id = $2 WHERE id = $1`, user, other); err != nil {
		t.Fatal(err)
	}
	h.member(user)
	before := h.writeCounts()
	signIn, err := h.consume(h.respond(resp{nameID: nid(nameID), email: uniqueEmail("asserted")}), nil)
	if err == nil || signIn.PrincipalID == user {
		t.Fatalf("sign-in %+v err %v: the NameID of an account bound to another provider row signed in", signIn, err)
	}
	if !errors.Is(err, saml.ErrNotOrgMember) {
		t.Fatalf("err = %v, want the new-account refusal ErrNotOrgMember", err)
	}
	if after := h.writeCounts(); after != before {
		t.Fatalf("users, memberships, principals, auth users, identities %v -> %v, want no write", before, after)
	}
}

// Switch off, end to end: a member go-api account that carries the asserted NameID but is bound to no provider row is
// no provider's identity: identity_conflict, nothing written.
func TestSAMLSwitchOffSignInRefusesAnUnboundAccount(t *testing.T) {
	h := newHarness(t, true)
	nameID := "unbound-" + uuid.NewString()
	email := uniqueEmail("unbound")
	h.member(h.goAPIUser(email, "saml", &nameID, false))
	before := h.writeCounts()
	_, err := h.consume(h.respond(resp{nameID: nid(nameID), email: email}), nil)
	if !errors.Is(err, saml.ErrIdentityAmbiguous) || saml.OutcomeReason(err) != "identity_conflict" {
		t.Fatalf("err = %v, want ErrIdentityAmbiguous (identity_conflict)", err)
	}
	if after := h.writeCounts(); after != before {
		t.Fatalf("users, memberships, principals, auth users, identities %v -> %v, want no write", before, after)
	}
}
