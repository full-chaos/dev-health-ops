package producttelemetry

import (
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/goapiproof"
)

// The prover's venue-proof class admits productTelemetryPlatformDashboard on
// the strength of a declared authorization requirement. This pins that
// declaration to the gate the route applies (RequirePlatformAdmin), over
// every role spelling and superuser value, for a principal outside an
// impersonation session: they cannot drift apart. The impersonation half of
// the gate is not expressible in the declaration (a venue run authenticates
// with a login token, never an impersonation session); the refusal itself is
// pinned by the principal table in producttelemetry_test.go.
func TestVenueAuthzRequirementMatchesRequirePlatformAdmin(t *testing.T) {
	requirement, ok := goapiproof.AuthzFor("productTelemetryPlatformDashboard")
	if !ok {
		t.Fatal("productTelemetryPlatformDashboard declares no requirement")
	}
	for _, role := range []string{"admin", "ADMIN", "Owner", "owner", "operator", "viewer", "member", "", "superuser", "platform_admin", " admin"} {
		for _, superuser := range []bool{false, true} {
			gate := RequirePlatformAdmin(Principal{Present: true, IsSuperuser: superuser}) == nil
			if declared := requirement.Satisfied(role, superuser); declared != gate {
				t.Errorf("role=%q superuser=%t: declared=%t gate=%t", role, superuser, declared, gate)
			}
		}
	}
}
