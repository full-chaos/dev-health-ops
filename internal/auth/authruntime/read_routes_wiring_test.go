package authruntime

import (
	"encoding/json"
	"slices"
	"testing"

	authv1 "github.com/full-chaos/dev-health-ops/contracts/auth/v1"
	"github.com/full-chaos/dev-health-ops/internal/auth/authapi"
)

var readRoutePatterns = []string{
	authapi.PatternPrincipalsGet, authapi.PatternPrincipalsList, authapi.PatternOrganizationsGet, authapi.PatternOrganizationsList,
	authapi.PatternMembershipsGet, authapi.PatternMembershipsList, authapi.PatternGrantsGet, authapi.PatternGrantsList,
}

// withoutReadRows plants the embedded endpoint-profile document minus the read routes' rows.
func withoutReadRows(t *testing.T) {
	t.Helper()
	var document map[string]any
	if err := json.Unmarshal(authv1.AuthServiceEndpointProfiles, &document); err != nil {
		t.Fatal(err)
	}
	var kept []any
	for _, item := range document["rows"].([]any) {
		if !slices.Contains(readRoutePatterns, item.(map[string]any)["route"].(string)) {
			kept = append(kept, item)
		}
	}
	document["rows"] = kept
	raw, err := json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	provisioningProfileRows = raw
	t.Cleanup(func() { provisioningProfileRows = nil })
}

func TestAuthRuntimeServesTheReadRoutesOnlyWithTheirRows(t *testing.T) {
	withoutReadRows(t)
	dark := servedAuthRoutes(t)
	for _, pattern := range readRoutePatterns {
		if dark["POST "+pattern+"@api"] {
			t.Errorf("%s is served with no endpoint-profile row", pattern)
		}
	}
	provisioningProfileRows = nil
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

func TestAuthRuntimeServesTheReadRoutesFromTheEmbeddedProfile(t *testing.T) {
	provisioningProfileRows = nil
	served := servedAuthRoutes(t)
	for _, pattern := range readRoutePatterns {
		if !served["POST "+pattern+"@api"] {
			t.Errorf("%s is not served on the API listener from the embedded profile", pattern)
		}
		if served["POST "+pattern+"@operator"] {
			t.Errorf("%s is served on the operator listener", pattern)
		}
	}
}
