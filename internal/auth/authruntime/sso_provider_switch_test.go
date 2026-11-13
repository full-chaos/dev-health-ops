package authruntime

import (
	"log/slog"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/authapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// The SSO provider routes mount only with AUTH_MEMBERSHIP_ROUTES and AUTH_SSO_PROVIDERS_OWNED both on (and a cipher that
// opens): with either off they are not mounted, with no error and no panic. The cipher key is valid in every case, so a
// dropped gate would mount the routes; the last case proves the cell can see them.
func TestSSOProviderRoutesMountOnlyWithBothSwitches(t *testing.T) {
	for name, c := range map[string]struct {
		membershipRoutes, ssoProvidersOwned bool
		want                                int
	}{
		"AUTH_SSO_PROVIDERS_OWNED off":   {true, false, 0},
		"AUTH_MEMBERSHIP_ROUTES off":     {false, true, 0},
		"both off":                       {false, false, 0},
		"both on (the positive control)": {true, true, authapi.SSOProviderRouteCount()},
	} {
		cfg := authconfig.Config{}
		cfg.SSO.EncryptionKey = secrets.NewValue(strings.Repeat("sso-fernet-test-key-", 2))
		cfg.Provisioning.Routes = c.membershipRoutes
		cfg.SSOProvidersOwned = c.ssoProvidersOwned
		routes, err := ssoProviderRoutes(cfg, slog.Default(), nil, authschema.ValidatedIdentifier{}, nil, nil)
		if err != nil || len(routes) != c.want {
			t.Errorf("%s: %d routes, err %v; want %d and no error", name, len(routes), err, c.want)
		}
	}
}
