package authruntime

import (
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	authv1 "github.com/full-chaos/dev-health-ops/contracts/auth/v1"
	"github.com/full-chaos/dev-health-ops/internal/auth/authapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// documentWithoutSSORows is the embedded endpoint-profile document without the seven SSO provider rows.
func documentWithoutSSORows(t *testing.T) []byte {
	t.Helper()
	var document map[string]any
	if err := json.Unmarshal(authv1.AuthServiceEndpointProfiles, &document); err != nil {
		t.Fatal(err)
	}
	drop := map[string]bool{}
	for _, route := range authapi.SSOProviderPatterns() {
		drop[route] = true
	}
	var kept []any
	for _, item := range document["rows"].([]any) {
		if route, _ := item.(map[string]any)["route"].(string); !drop[route] {
			kept = append(kept, item)
		}
	}
	document["rows"] = kept
	raw, err := json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func ssoSwitchConfig(membershipRoutes, ssoProvidersOwned bool) authconfig.Config {
	cfg := authconfig.Config{}
	cfg.SSO.EncryptionKey = secrets.NewValue(strings.Repeat("sso-fernet-test-key-", 2))
	cfg.Provisioning.Routes = membershipRoutes
	cfg.SSOProvidersOwned = ssoProvidersOwned
	return cfg
}

// The SSO provider routes mount only with AUTH_MEMBERSHIP_ROUTES, AUTH_SSO_PROVIDERS_OWNED and their endpoint-profile rows
// (and a cipher that opens): with any one missing they are not mounted, with no error and no panic. The cipher key is
// valid in every case, so a dropped gate mounts the routes; the last case proves the cell can see them.
func TestSSOProviderRoutesMountOnlyWithBothSwitchesAndTheirRows(t *testing.T) {
	previous := provisioningProfileRows
	t.Cleanup(func() { provisioningProfileRows = previous })
	provisioningProfileRows = nil
	for name, c := range map[string]struct {
		membershipRoutes, ssoProvidersOwned bool
		want                                int
	}{
		"AUTH_SSO_PROVIDERS_OWNED off":   {true, false, 0},
		"AUTH_MEMBERSHIP_ROUTES off":     {false, true, 0},
		"both off":                       {false, false, 0},
		"both on (the positive control)": {true, true, authapi.SSOProviderRouteCount()},
	} {
		routes, err := ssoProviderRoutes(ssoSwitchConfig(c.membershipRoutes, c.ssoProvidersOwned), slog.Default(), nil, authschema.ValidatedIdentifier{}, nil, nil)
		if err != nil || len(routes) != c.want {
			t.Errorf("%s: %d routes, err %v; want %d and no error", name, len(routes), err, c.want)
		}
	}
	provisioningProfileRows = documentWithoutSSORows(t)
	routes, err := ssoProviderRoutes(ssoSwitchConfig(true, true), slog.Default(), nil, authschema.ValidatedIdentifier{}, nil, nil)
	if err != nil || len(routes) != 0 {
		t.Errorf("both switches on, no rows: %d routes, err %v; want none and no error", len(routes), err)
	}
}
