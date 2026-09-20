package datahealth

import (
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/goapiproof"
)

// The prover's venue-proof class admits the four Data Health operations on
// the strength of a declared authorization requirement. This pins that
// declaration to the gate the route actually applies, over every role
// spelling and superuser value: they cannot drift apart.
func TestVenueAuthzRequirementMatchesRequireOperator(t *testing.T) {
	for _, operation := range []string{"connectorsDataHealth", "dataHealthIdentity", "mappingCoverageHealth", "metricLineage"} {
		requirement, ok := goapiproof.AuthzFor(operation)
		if !ok {
			t.Fatalf("%s declares no requirement", operation)
		}
		for _, role := range []string{"admin", "ADMIN", "Owner", "owner", "operator", "viewer", "member", "", "administrator", "root", " admin"} {
			for _, superuser := range []bool{false, true} {
				gate := RequireOperator(Principal{Present: true, Role: role, IsSuperuser: superuser}) == nil
				if declared := requirement.Satisfied(role, superuser); declared != gate {
					t.Errorf("%s role=%q superuser=%t: declared=%t gate=%t", operation, role, superuser, declared, gate)
				}
			}
		}
	}
}
