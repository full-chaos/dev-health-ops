package authapi

import (
	"net/http"
	"strings"
	"testing"

	authv1 "github.com/full-chaos/dev-health-ops/contracts/auth/v1"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/usertokens"
)

// The acr credential events route takes the acr-api workload token. Every kind of user token, as the Authorization
// bearer, is refused 401 invalid_credentials before any body byte is read: no key set, workload row or projection is
// read, and no token is echoed in the body.
func TestAcrEventsRefusesUserTokens(t *testing.T) {
	tokens := userTokensWithIssuer(t)
	for _, tok := range tokens {
		a := newAcrEventsFixture(t, authv1.AuthServiceEndpointProfiles)
		response, body := a.post(bearerOf(tok.Value), acrEventBody("cred_exampleaaaaaaaaaa", 1, "active"))
		if response.Code != http.StatusUnauthorized || !strings.Contains(response.Body.String(), reasonInvalidCredentials) {
			t.Errorf("%s: %d %s, want 401 %s", tok.Kind, response.Code, response.Body, reasonInvalidCredentials)
		}
		a.nothingRead(t, tok.Kind, body)
		if a.keys.reads != 0 {
			t.Errorf("%s: read the key set %d times", tok.Kind, a.keys.reads)
		}
		usertokens.NoLeak(t, tok.Kind+" body", response.Body.String(), tokens)
	}
}
