//go:build integration

package oidc

import (
	"context"
	"net/http"
	"testing"
)

// A linked key signs in only while the provider's allowed_domains holds the asserted domain: the list is read on
// every sign-in, so removing the domain refuses the next one with no session and no write (go-api oidc.go parity).
func TestLinkedIdentityIsRefusedOnceItsDomainLeavesTheList(t *testing.T) {
	h := newHarness(t, harnessOpts{signup: false, allowedDomains: []string{"example.com"}})
	key := ExternalKey{Issuer: h.issuer, Provider: h.providerID.String(), Subject: "sub-domain-linked"}
	email := uniqueEmail("linked")
	owner := h.link(t, key, email)
	h.seedMembership(t, owner, h.orgID, "member")

	s := h.start(t)
	if login, failure := h.finish(t, s, h.sign(t, h.claims(s.nonce, "sub-domain-linked", email))); failure != nil || login.PrincipalID != owner {
		t.Fatalf("control, domain on the list: login %+v failure %v", login, failure)
	}

	if _, err := h.env.admin.Exec(context.Background(), `UPDATE public.sso_providers SET allowed_domains = '["corp.example"]'::json WHERE id = $1`, h.providerID); err != nil {
		t.Fatal(err)
	}
	before := h.snapshot(t)
	s = h.start(t)
	_, failure := h.finish(t, s, h.sign(t, h.claims(s.nonce, "sub-domain-linked", email)))
	wantDenied(t, failure, http.StatusForbidden, ReasonDomainNotAllowed)
	wantUnchanged(t, h, before)
}

// A linked key with no asserted email is the 400 a new key gets; with a list, an address with no '@' is go-api's 400.
func TestLinkedIdentityWithNoUsableEmailIsRefused(t *testing.T) {
	h := newHarness(t, harnessOpts{signup: false, allowedDomains: []string{"example.com"}})
	key := ExternalKey{Issuer: h.issuer, Provider: h.providerID.String(), Subject: "sub-noemail-linked"}
	owner := h.link(t, key, uniqueEmail("linked"))
	h.seedMembership(t, owner, h.orgID, "member")
	for name, tc := range map[string]struct{ email, detail string }{
		"missing": {"", "Email address is missing from OIDC claims"},
		"no at":   {"linked.example.com", "Email address from OIDC claims is malformed"},
	} {
		t.Run(name, func(t *testing.T) {
			before := h.snapshot(t)
			s := h.start(t)
			_, failure := h.finish(t, s, h.sign(t, h.claims(s.nonce, "sub-noemail-linked", tc.email)))
			wantDenied(t, failure, http.StatusBadRequest, ReasonMalformed)
			if failure.Detail != tc.detail {
				t.Fatalf("detail %v, want %q", failure.Detail, tc.detail)
			}
			wantUnchanged(t, h, before)
		})
	}
}
