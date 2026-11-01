package authruntime

import (
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
)

// The production issuer builder carries the configured eddsa lifetime; unset is the 60-minute ceiling.
func TestTheProductionIssuerTakesTheConfiguredLifetime(t *testing.T) {
	for value, want := range map[string]int{"": 3600, "10m": 600} {
		extra := map[string]string{}
		if value != "" {
			extra[authconfig.EnvAccessTokenLifetime] = value
		}
		issuer, err := newAccessIssuer(classConfigWith(t, authconfig.AccessClassEdDSA, extra), &stubSigner{})
		if err != nil {
			t.Fatal(err)
		}
		if got := issuer.ExpiresIn(); got != want {
			t.Errorf("lifetime %q: ExpiresIn %d, want %d", value, got, want)
		}
	}
}
