package edgetoken

import (
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/acraudience"
)

// TestNoEdgeTokenVerifierTakesAnACRAudience: the one HS256 refresh-token
// verifier in ops (go-api logout) is built here, so an ACR audience
// (CHAOS-12248) is refused here: no legacy token is accepted with one.
func TestNoEdgeTokenVerifierTakesAnACRAudience(t *testing.T) {
	secret := strings.Repeat("s", MinSecretLength)
	if _, err := New(secret, "dev-health-ops", "dev-health-api"); err != nil {
		t.Fatalf("control verifier: %v", err)
	}
	for _, audience := range []string{acraudience.API, acraudience.MCP} {
		if _, err := New(secret, "dev-health-ops", audience); err == nil {
			t.Errorf("a verifier was built for %s", audience)
		}
	}
}
