package apiservice

import (
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
)

// TestGoAPIRefusesToStartWithAnACRJWTAudience: JWT_AUDIENCE is the audience
// go-api signs and verifies its own access and refresh tokens with. With an
// ACR audience (CHAOS-12248) the protection that holds the verifier and the
// signer is not built, so go-api does not start.
func TestGoAPIRefusesToStartWithAnACRJWTAudience(t *testing.T) {
	if _, err := newProtection(apiTestConfig(), nil, false, quietLogger()); err != nil {
		t.Fatalf("control: %v", err)
	}
	for _, audience := range []string{contracts.ACRAPIAudience, contracts.ACRMCPAudience} {
		cfg := apiTestConfig()
		cfg.APIJWTAudience = audience
		if _, err := newProtection(cfg, nil, false, quietLogger()); err == nil {
			t.Errorf("go-api built its access-token verifier and signer for %s", audience)
		}
	}
}
