package saml

import (
	"strings"
	"testing"

	"github.com/google/uuid"
)

// A provider with no sp_acs_url advertises the ACS route that is served (the web ACS route and go-api's
// /api/v1/auth/saml/{provider_id}/acs), and the binding cookie is bound to that path. The entity id stays the legacy
// identifier (it is no URL any service answers).
func TestTheDefaultACSIsTheServedRoute(t *testing.T) {
	id := uuid.MustParse("6f1c2a3b-4d5e-4f60-8a71-92b3c4d5e6f7")
	p := Provider{ID: id}
	for _, base := range []string{"https://app.example.test", "http://localhost:13002"} {
		if got, want := p.ACSURL(base), base+"/api/v1/auth/saml/"+id.String()+"/acs"; got != want {
			t.Errorf("ACSURL(%s) = %s, want %s", base, got, want)
		}
		if got, want := ACSPath(p, base), "/api/v1/auth/saml/"+id.String()+"/acs"; got != want {
			t.Errorf("ACSPath(%s) = %s, want %s", base, got, want)
		}
		if got, want := p.SPEntityID(base), base+"/saml/"+id.String()+"/metadata"; got != want {
			t.Errorf("SPEntityID(%s) = %s, want the legacy %s", base, got, want)
		}
	}
	override := Provider{ID: id, Config: Config{SPACSURL: "https://sp.example.test/api/v1/auth/saml/" + id.String() + "/acs"}}
	if got := override.ACSURL("https://app.example.test"); got != override.Config.SPACSURL {
		t.Errorf("an sp_acs_url override is kept: got %s", got)
	}
}

// ValidateACSOverride admits exactly the served route of this provider: the bare /saml/<id>/acs form, another provider's
// route and a prefixed route are refused.
func TestValidateACSOverrideAdmitsExactlyTheServedRoute(t *testing.T) {
	id := uuid.MustParse("6f1c2a3b-4d5e-4f60-8a71-92b3c4d5e6f7")
	other := uuid.MustParse("0a1b2c3d-4e5f-4a6b-8c7d-9e0f1a2b3c4d")
	for url, ok := range map[string]bool{
		"https://sp.example.test/api/v1/auth/saml/" + id.String() + "/acs":       true,
		"https://sp.example.test/saml/" + id.String() + "/acs":                   false,
		"https://sp.example.test/api/v1/auth/saml/" + other.String() + "/acs":    false,
		"https://sp.example.test/proxy/api/v1/auth/saml/" + id.String() + "/acs": false,
		"https://sp.example.test/api/v1/auth/saml/" + id.String() + "/acs/extra": false,
	} {
		err := ValidateACSOverride(Config{SPACSURL: url}, id)
		if (err == nil) != ok || (err != nil && !strings.Contains(err.Error(), "sp_acs_url")) {
			t.Errorf("ValidateACSOverride(%s) = %v, want ok=%v", url, err, ok)
		}
	}
}
