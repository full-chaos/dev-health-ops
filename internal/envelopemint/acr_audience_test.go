package envelopemint

import (
	"crypto/ed25519"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
)

// TestMintRefusesAnACRAudience: an effective-principal envelope never carries
// an ACR audience (CHAOS-12248), whether given as an option or read from
// GO_API_ENVELOPE_AUDIENCE.
func TestMintRefusesAnACRAudience(t *testing.T) {
	_, key, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	principal := ProveEnvelopePrincipal("00000000-0000-4000-8000-000000000001")
	if _, err := Mint(key, principal, Options{}); err != nil {
		t.Fatalf("control: %v", err)
	}
	for _, audience := range []string{contracts.ACRAPIAudience, contracts.ACRMCPAudience} {
		if _, err := Mint(key, principal, Options{Audience: audience}); err == nil {
			t.Errorf("minted with the option audience %s", audience)
		}
		t.Run("env "+audience, func(t *testing.T) {
			t.Setenv(AudienceEnvVar, audience)
			if _, err := Mint(key, principal, Options{}); err == nil {
				t.Errorf("minted with %s=%s", AudienceEnvVar, audience)
			}
		})
	}
}
