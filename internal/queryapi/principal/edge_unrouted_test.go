package principal

import "testing"

// TestEdgeVerifierAuthenticatorStaysUnrouted: NewEdgeVerifier alone builds an
// unrouted verifier, whatever the environment holds; only RouteAccess routes
// it (the server does, with AUTH_ACCESS_TOKEN_ISSUER in its lookup).
func TestEdgeVerifierAuthenticatorStaysUnrouted(t *testing.T) {
	t.Setenv("AUTH_ACCESS_TOKEN_ISSUER", "https://auth.example.test")
	v := mustEdgeVerifier(t, edgeTestSecret, edgeTestIssuer, edgeTestAudience)
	if v.auth == nil || v.auth.Routed() {
		t.Fatal("query-api's REST edge verifier authenticator is profile-routed")
	}
}
