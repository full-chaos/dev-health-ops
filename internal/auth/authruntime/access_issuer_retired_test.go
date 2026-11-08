package authruntime

import (
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
)

func TestNewAccessIssuerRefusesTheRetiredClass(t *testing.T) {
	cfg := classConfig(t, authconfig.AccessClassEdDSA)
	cfg.Session.AccessClass = "hs256_compat"
	if issuer, err := newAccessIssuer(cfg, &stubSigner{}); err == nil {
		t.Fatalf("newAccessIssuer built %v for hs256_compat", issuer)
	}
	cfg.Session.AccessClass = ""
	if issuer, err := newAccessIssuer(cfg, &stubSigner{}); err == nil {
		t.Fatalf("newAccessIssuer built %v for an empty class", issuer)
	}
}
