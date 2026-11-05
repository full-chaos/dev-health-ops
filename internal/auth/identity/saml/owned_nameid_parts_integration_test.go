//go:build integration

package saml_test

import (
	"context"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/identity/saml"
)

// Owned SAML provisioning writes the NameID Format and SPNameQualifier to public.users with the values and the
// normalisation go-api's SAML sign-up writes (internal/api/sso: the assertion's own attributes as asserted, an
// empty string stays an empty string, never NULL). A stored row keys to the subject the sign-in keyed with.
func TestOwnedSAMLProvisioningWritesTheNameIDParts(t *testing.T) {
	cases := []struct {
		name            string
		format, spnq    string
		wantFmt, wantSP string
	}{
		{"persistent with qualifier", persistent, "https://sp.example.test/q", persistent, "https://sp.example.test/q"},
		{"empty format and qualifier stay empty", "", "", "", ""},
		{"unspecified with qualifier", saml.NameIDFormatUnspecified, "q-1", saml.NameIDFormatUnspecified, "q-1"},
		{"surrounding space is kept as asserted", persistent, " q ", persistent, " q "},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t, true)
			ctx := context.Background()
			if _, err := h.db.admin.Exec(ctx, `UPDATE public.sso_providers SET auto_provision_users = true, allowed_domains = '["example.com"]'::json WHERE id = $1`, h.provider.ID); err != nil {
				t.Fatal(err)
			}
			h.ownUsers()
			h.svc.JIT = saml.PGJIT{Identities: h.db.owned}
			value, email := "nameid-"+uuid.NewString(), uniqueEmail("owned")
			n := saml.NameID{Format: tc.format, SPNameQualifier: tc.spnq, Value: value}
			if _, err := h.consume(h.respond(resp{nameID: n, email: email}), nil); err != nil {
				t.Fatalf("sign-in: %v", err)
			}
			var gotValue string
			var format, spnq *string
			if err := h.db.admin.QueryRow(ctx, `SELECT auth_provider_id, saml_nameid_format, saml_sp_name_qualifier FROM public.users WHERE email = $1`, email).
				Scan(&gotValue, &format, &spnq); err != nil {
				t.Fatal(err)
			}
			if gotValue != value || format == nil || *format != tc.wantFmt || spnq == nil || *spnq != tc.wantSP {
				t.Fatalf("stored value %q format %v qualifier %v, want %q %q %q", gotValue, format, spnq, value, tc.wantFmt, tc.wantSP)
			}
			want, err := saml.Subject(n)
			if err != nil {
				t.Fatal(err)
			}
			got, err := saml.SubjectFromStored(gotValue, format, spnq)
			if err != nil || got != want {
				t.Fatalf("SubjectFromStored = %q, %v; the sign-in key is %q", got, err, want)
			}
		})
	}
}
