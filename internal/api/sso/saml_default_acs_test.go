package sso

import (
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/identity/saml"
)

// go-api's own SAML routes (prod serves SAML sign-in from go-api) advertise the served ACS route by default, and the
// default is the same one auth-service computes: both serving paths name one ACS URL and one entity id.
func TestGoAPIsDefaultACSIsTheServedRouteAndEqualsAuthServices(t *testing.T) {
	id := uuid.MustParse("6f1c2a3b-4d5e-4f60-8a71-92b3c4d5e6f7")
	for _, base := range []string{"https://app.example.test", "http://localhost:13002"} {
		if got, want := spACSURLOf(samlConfigValues{}, id.String(), base), base+"/api/v1/auth/saml/"+id.String()+"/acs"; got != want {
			t.Errorf("spACSURLOf(%s) = %s, want %s", base, got, want)
		}
		p := saml.Provider{ID: id}
		if got, want := spACSURLOf(samlConfigValues{}, id.String(), base), p.ACSURL(base); got != want {
			t.Errorf("go-api ACS %s != auth-service ACS %s", got, want)
		}
		if got, want := spEntityIDOf(samlConfigValues{}, id.String(), base), p.SPEntityID(base); got != want {
			t.Errorf("go-api entity id %s != auth-service entity id %s", got, want)
		}
	}
}
