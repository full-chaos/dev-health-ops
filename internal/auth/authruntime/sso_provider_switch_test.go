package authruntime

import (
	"log/slog"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// The SSO provider routes mount only with AUTH_SSO_PROVIDERS_OWNED on: with it off they are not mounted, with no error and
// no panic (the grants they write with are the grants pass's, behind the same switch).
func TestSSOProviderRoutesAreNotMountedWithTheSwitchOff(t *testing.T) {
	cfg := authconfig.Config{}
	cfg.Provisioning.Routes = true
	routes, err := ssoProviderRoutes(cfg, slog.Default(), nil, authschema.ValidatedIdentifier{}, nil, nil)
	if err != nil || len(routes) != 0 {
		t.Fatalf("switch off: %d routes, err %v; want none and no error", len(routes), err)
	}
	cfg.Provisioning.Routes = false
	cfg.SSOProvidersOwned = true
	routes, err = ssoProviderRoutes(cfg, slog.Default(), nil, authschema.ValidatedIdentifier{}, nil, nil)
	if err != nil || len(routes) != 0 {
		t.Fatalf("provisioning routes off: %d routes, err %v; want none and no error", len(routes), err)
	}
}
