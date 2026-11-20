package syncreconciler

import (
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/providersync"
)

// The provider-unit claim closes a unit by the category this sweep stamps; the literal must stay one value.
func TestUnreclaimableSweepCategoryIsTheClosedUnitFeatureDisabledCategory(t *testing.T) {
	if unreclaimableErrorCategory != providersync.FeatureDisabledCategory {
		t.Fatalf("the sweep writes %q, the claim classifies %q", unreclaimableErrorCategory, providersync.FeatureDisabledCategory)
	}
}
