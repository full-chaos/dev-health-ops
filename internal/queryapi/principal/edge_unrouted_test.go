package principal

import "testing"

// TestEdgeVerifierAuthenticatorStaysUnrouted: the REST edge verifier's
// Authenticator is never profile-routed (go-api only).
func TestEdgeVerifierAuthenticatorStaysUnrouted(t *testing.T) {
	t.Setenv("AUTH_ACCESS_TOKEN_ISSUER", "https://auth.example.test")
	v := mustEdgeVerifier(t, edgeTestSecret, edgeTestIssuer, edgeTestAudience)
	if v.auth == nil || v.auth.Routed() {
		t.Fatal("query-api's REST edge verifier authenticator is profile-routed")
	}
}
