//go:build integration

package saml_test

import (
	"context"
	"errors"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/identity/saml"
)

// A linked NameID signs in only while the provider's allowed_domains holds the asserted domain, and only with an
// email (go-api saml.go parity): the list is read on every sign-in.
func TestSAMLLinkedIdentityIsRefusedOnceItsDomainLeavesTheList(t *testing.T) {
	h := newHarness(t, true)
	user := h.linkedUser(uniqueEmail("u"), "user-domain")
	email := "u-" + "linked" + "@corp.example.com"
	if _, err := h.db.admin.Exec(context.Background(), `UPDATE public.sso_providers SET allowed_domains = '["corp.example.com"]'::json WHERE id = $1`, h.provider.ID); err != nil {
		t.Fatal(err)
	}
	signIn, err := h.consume(h.respond(resp{nameID: nid("user-domain"), email: email}), nil)
	if err != nil || signIn.PrincipalID != user {
		t.Fatalf("control, domain on the list: sign-in %+v err %v", signIn, err)
	}
	if _, err := h.db.admin.Exec(context.Background(), `UPDATE public.sso_providers SET allowed_domains = '["other.example.org"]'::json WHERE id = $1`, h.provider.ID); err != nil {
		t.Fatal(err)
	}
	for name, tc := range map[string]struct {
		email string
		want  error
	}{
		"off the list": {email, saml.ErrDomainNotAllowed},
		"no at":        {"no-at-sign", saml.ErrEmailMalformed},
		"no email":     {"", saml.ErrEmailMissing},
	} {
		t.Run(name, func(t *testing.T) {
			signIn, err := h.consume(h.respond(resp{nameID: nid("user-domain"), email: tc.email}), nil)
			if !errors.Is(err, tc.want) || signIn.PrincipalID == user {
				t.Fatalf("sign-in %+v err %v, want %v", signIn, err, tc.want)
			}
		})
	}
}
