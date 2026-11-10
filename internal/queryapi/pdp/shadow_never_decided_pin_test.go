package pdp

import (
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/policycatalog"
)

// The shadow's never-decided skip label copies the gate's orgScoped rule; the two must agree on every catalog action.
func TestShadowNeverDecidedAgreesWithOrgScopedOnEveryAction(t *testing.T) {
	checked := 0
	for _, action := range policycatalog.Default().Actions() {
		row, err := policy.ShadowRowFor(action.ID)
		if err != nil {
			continue
		}
		checked++
		if row.NeverDecided() == orgScoped(action.ID) {
			t.Errorf("%s: shadow NeverDecided %v, gate orgScoped %v", action.ID, row.NeverDecided(), orgScoped(action.ID))
		}
	}
	if checked == 0 {
		t.Fatal("no catalog action has a shadow row")
	}
}
