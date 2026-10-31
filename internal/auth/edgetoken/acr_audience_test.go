package edgetoken

import (
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
)

// TestNoEdgeTokenVerifierOrSignerTakesAnACRAudience: every HS256 access-token
// verifier and signer in ops (go-api, query-api's edge verifier, auth-service's
// HS256 issuer and legacy verifier) is built here, so an ACR audience
// (CHAOS-12248) is refused here: no edge token is minted or accepted with one.
func TestNoEdgeTokenVerifierOrSignerTakesAnACRAudience(t *testing.T) {
	secret := strings.Repeat("s", MinSecretLength)
	if _, err := New(secret, "dev-health-ops", contracts.PlatformAPIAudience); err != nil {
		t.Fatalf("control verifier: %v", err)
	}
	if _, err := NewSigner(secret, "dev-health-ops", contracts.PlatformAPIAudience); err != nil {
		t.Fatalf("control signer: %v", err)
	}
	for _, audience := range []string{contracts.ACRAPIAudience, contracts.ACRMCPAudience} {
		if _, err := New(secret, "dev-health-ops", audience); err == nil {
			t.Errorf("a verifier was built for %s", audience)
		}
		if _, err := NewSigner(secret, "dev-health-ops", audience); err == nil {
			t.Errorf("a signer was built for %s", audience)
		}
	}
}
