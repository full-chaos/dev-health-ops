package authruntime

import (
	"encoding/json"
	"testing"

	authv1 "github.com/full-chaos/dev-health-ops/contracts/auth/v1"
	"github.com/full-chaos/dev-health-ops/internal/auth/authapi"
)

var readRoutePatterns = []string{
	authapi.PatternPrincipalsGet, authapi.PatternPrincipalsList, authapi.PatternOrganizationsGet, authapi.PatternOrganizationsList,
	authapi.PatternMembershipsGet, authapi.PatternMembershipsList, authapi.PatternGrantsGet, authapi.PatternGrantsList,
}

// withReadRows plants the embedded endpoint-profile document plus one row per read route, each a copy of the suspend row
// (it lists both credential classes the read routes need).
func withReadRows(t *testing.T) {
	t.Helper()
	var document map[string]any
	if err := json.Unmarshal(authv1.AuthServiceEndpointProfiles, &document); err != nil {
		t.Fatal(err)
	}
	rows := document["rows"].([]any)
	var template map[string]any
	for _, item := range rows {
		if row := item.(map[string]any); row["route"] == authapi.PatternOrgSuspend && row["service"] == authapi.AccessService {
			template = row
		}
	}
	if template == nil {
		t.Fatal("no suspend row to copy")
	}
	for _, pattern := range readRoutePatterns {
		row := map[string]any{}
		for key, value := range template {
			row[key] = value
		}
		row["route"], row["id"] = pattern, "POST "+pattern+" [dev-health-auth-service @api]"
		rows = append(rows, row)
	}
	document["rows"] = rows
	raw, err := json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	provisioningProfileRows = raw
	t.Cleanup(func() { provisioningProfileRows = nil })
}

func TestAuthRuntimeServesTheReadRoutesOnlyWithTheirRows(t *testing.T) {
	dark := servedAuthRoutes(t)
	for _, pattern := range readRoutePatterns {
		if dark["POST "+pattern+"@api"] {
			t.Errorf("%s is served with no endpoint-profile row", pattern)
		}
	}
	withReadRows(t)
	lit := servedAuthRoutes(t)
	for _, pattern := range readRoutePatterns {
		if !lit["POST "+pattern+"@api"] {
			t.Errorf("%s is not served on the API listener with its row", pattern)
		}
		if lit["POST "+pattern+"@operator"] {
			t.Errorf("%s is served on the operator listener", pattern)
		}
	}
}
