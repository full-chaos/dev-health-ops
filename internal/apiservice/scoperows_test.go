package apiservice

import (
	"maps"
	"slices"
	"strings"
	"testing"

	authv1 "github.com/full-chaos/dev-health-ops/contracts/auth/v1"
)

// TestScopeRowsReadTheValidatorsClassField: the scope's access flag is the
// validator's row (accepts user_access_token or legacy HS256) for every go-api
// row, the webhook rows are exactly the signature-only routes, and the domain
// rows are the fcpush and svc_acr rows.
func TestScopeRowsReadTheValidatorsClassField(t *testing.T) {
	scopeRows, err := parseScopeRows(authv1.GoAPIEndpointProfiles)
	if err != nil {
		t.Fatal(err)
	}
	accessRows, err := goAPIAccessRows()
	if err != nil {
		t.Fatal(err)
	}
	if len(scopeRows) != len(accessRows) {
		t.Fatalf("%d scope rows, %d validator rows", len(scopeRows), len(accessRows))
	}
	for _, access := range accessRows {
		scope, ok := scopeRows[access.Profile]
		if !ok || scope.AccessToken != (access.AcceptsUser || access.LegacyHS256) {
			t.Errorf("%s: scope %+v, validator %+v", access.Profile, scope, access)
		}
	}
	var webhook, domain []string
	for profile, row := range scopeRows {
		if row.Webhook {
			webhook = append(webhook, profile)
		}
		if row.Domain {
			domain = append(domain, profile)
			if row.AccessToken {
				t.Errorf("%s declares a domain class and an access class", profile)
			}
		}
	}
	slices.Sort(webhook)
	if want := slices.Sorted(maps.Keys(signatureOnlyRoutes)); !slices.Equal(webhook, want) {
		t.Errorf("webhook rows %v, signature-only routes %v", webhook, want)
	}
	slices.Sort(domain)
	for _, profile := range domain {
		if !strings.Contains(profile, " /api/v1/external-ingest/") && !strings.Contains(profile, " /api/v1/internal/acr/") {
			t.Errorf("domain row %s is neither an external-ingest nor an internal acr row", profile)
		}
	}
	if len(domain) != 9 {
		t.Errorf("domain rows %v, want the 5 fcpush and 4 svc_acr rows", domain)
	}
}
