package authapi

import (
	"net/http"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
)

func TestSwitchOrgRefusesAnAudDevHealthAPIBearerWith401(t *testing.T) {
	signer, _, _ := fileSigner(t)
	check, err := NewAccessCheck(AccessCheckConfig{IssuerURL: goldenIssuer, Keys: signer})
	if err != nil {
		t.Fatal(err)
	}
	post := func(t *testing.T, audience string) (*switchFixture, int, string) {
		t.Helper()
		f := newSwitchFixtureWith(t, newTestIssuer(t).WithAccessCheck(check))
		f.bearerIssuer = newEdDSAIssuer(t, signer, audience)
		r := f.post(orgBody(f.orgB), f.bearer())
		return f, r.Code, r.Body.String()
	}

	t.Run("aud auth-service is accepted", func(t *testing.T) {
		if _, code, body := post(t, contracts.SessionRouteAudience); code != http.StatusOK {
			t.Fatalf("%d %s, want 200", code, body)
		}
	})
	t.Run("aud dev-health-api is refused with 401", func(t *testing.T) {
		f, code, body := post(t, contracts.PlatformAPIAudience)
		if code != http.StatusUnauthorized {
			t.Fatalf("%d %s, want 401", code, body)
		}
		f.nothingMinted("aud dev-health-api bearer")
	})
}
