package saml_test

import (
	"net/http"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/identity/saml"
)

// Legacy parity (go-api internal/api/sso/provision.go checkAllowedDomain): an empty or NULL allowed_domains is no
// domain filter, so with JIT on a new account is created, joined and signed in, as legacy answers 200.
func TestSAMLJITNewAccountWithNoDomainListMatchesLegacy(t *testing.T) {
	for name, list := range map[string][]string{"empty": {}, "null": nil} {
		t.Run(name, func(t *testing.T) {
			members := &memberSet{ids: map[uuid.UUID]bool{}}
			seam := &joinSeam{members: members}
			run := postACSRow(t, &lookupStore{}, members, seam, func(p *saml.Provider) { p.AllowedDomains = list })
			if run.status != http.StatusOK || len(seam.provisioned) != 1 || seam.joins != 0 {
				t.Fatalf("ACS = %d detail %q provisions %d joins %d, want the legacy 200, 1, 0",
					run.status, run.detail, len(seam.provisioned), seam.joins)
			}
			if len(run.sessions.got) != 1 || run.sessions.got[0].PrincipalID != seam.newID ||
				run.sessions.got[0].OrganizationID == nil || *run.sessions.got[0].OrganizationID != run.org {
				t.Fatalf("sessions %+v, want one in org %s for the new account", run.sessions.got, run.org)
			}
		})
	}
}
