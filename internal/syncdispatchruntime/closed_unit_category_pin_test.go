package syncdispatchruntime

import (
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobs/providerunit"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
)

// The claim reads the category the writers stamp; the three literals must stay one value.
func TestClosedUnitFeatureDisabledCategoryIsTheWritersLiteral(t *testing.T) {
	for name, got := range map[string]string{
		"dispatch and discovery gates (featureDisabledErrorCategory)": featureDisabledErrorCategory,
		"worker entitlement re-check (providerunit)":                  providerunit.FeatureDisabledCategory,
	} {
		if got != providersync.FeatureDisabledCategory {
			t.Errorf("%s writes %q, the claim classifies %q", name, got, providersync.FeatureDisabledCategory)
		}
	}
}
