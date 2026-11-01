package edgetokenmint

import (
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/auth/acraudience"
)

// TestMintRefusesAnACRAudience: the proof edge token never carries an ACR
// audience (CHAOS-12248), whether given as an option or read from
// JWT_AUDIENCE.
func TestMintRefusesAnACRAudience(t *testing.T) {
	now := func() time.Time { return testNow }
	if _, err := Mint([]byte(testKey), testPrincipal(), Options{Now: now}); err != nil {
		t.Fatalf("control: %v", err)
	}
	for _, audience := range []string{acraudience.API, acraudience.MCP} {
		if _, err := Mint([]byte(testKey), testPrincipal(), Options{Audience: audience, Now: now}); err == nil {
			t.Errorf("minted with the option audience %s", audience)
		}
		t.Run("env "+audience, func(t *testing.T) {
			t.Setenv(AudienceEnvVar, audience)
			if _, err := Mint([]byte(testKey), testPrincipal(), Options{Now: now}); err == nil {
				t.Errorf("minted with %s=%s", AudienceEnvVar, audience)
			}
		})
	}
}
