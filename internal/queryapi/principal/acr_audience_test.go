package principal

import (
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/acraudience"
)

// TestNoEnvelopeVerifierTakesAnACRAudience: GO_API_ENVELOPE_AUDIENCE reaches
// NewVerifier from every query-api route that reads it, so an ACR audience
// (CHAOS-12248) is refused here: query-api never accepts an envelope with one.
func TestNoEnvelopeVerifierTakesAnACRAudience(t *testing.T) {
	if _, err := NewVerifier("jwks.json", "https://issuer.example.test", "query-api"); err != nil {
		t.Fatalf("control: %v", err)
	}
	for _, audience := range []string{acraudience.API, acraudience.MCP} {
		if _, err := NewVerifier("jwks.json", "https://issuer.example.test", audience); err == nil {
			t.Errorf("an envelope verifier was built for %s", audience)
		}
	}
}
