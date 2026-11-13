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

// ssoRowsDocument is the embedded endpoint-profile document with the seven SSO provider rows planted: copies of the
// organization deprovision row (the embedded document holds none of them; they ship after the PR that adds the file they cite).
func ssoRowsDocument(t *testing.T) []byte {
	t.Helper()
	var document map[string]any
	if err := json.Unmarshal(authv1.AuthServiceEndpointProfiles, &document); err != nil {
		t.Fatal(err)
	}
	rows := document["rows"].([]any)
	var base map[string]any
	for _, item := range rows {
		if row := item.(map[string]any); row["route"] == authapi.PatternOrgDeprovision {
			base = row
		}
	}
	if base == nil {
		t.Fatal("no deprovision row in the embedded document")
	}
	for _, route := range authapi.SSOProviderPatterns() {
		clone := map[string]any{}
		for k, v := range base {
			clone[k] = v
		}
		clone["route"] = route
		clone["id"] = "POST " + route + " [dev-health-auth-service @api]"
		rows = append(rows, clone)
	}
	document["rows"] = rows
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
	provisioningProfileRows = ssoRowsDocument(t)
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
}

// The dark cell: both switches ON and the REAL embedded profile document (it holds no SSO provider rows) mount no route.
func TestSSOProviderRoutesAreDarkOnTheRealEmbeddedDocument(t *testing.T) {
	previous := provisioningProfileRows
	t.Cleanup(func() { provisioningProfileRows = previous })
	provisioningProfileRows = nil
	routes, err := ssoProviderRoutes(ssoSwitchConfig(true, true), slog.Default(), nil, authschema.ValidatedIdentifier{}, nil, nil)
	if err != nil || len(routes) != 0 {
		t.Fatalf("both switches on, embedded document: %d routes, err %v; want none and no error", len(routes), err)
	}
}
