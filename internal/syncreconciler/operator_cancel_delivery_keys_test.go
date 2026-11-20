package syncreconciler

import (
	"slices"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/syncdispatchruntime"
)

// The operator cancel counts a unit's deliveries under exactly the dedupe keys this package can
// mint for it: the original key and every reclaim generation.
func TestOperatorCancelDeliveryKeysAreTheOrphanRepairKeys(t *testing.T) {
	unitID := "e784daf9-592c-5fca-b6f7-81d2c9de940e"
	got, want := syncdispatchruntime.ProviderUnitDeliveryKeys(unitID), providerUnitCandidateKeys(unitID)
	if !slices.Equal(got, want) || len(want) != maxProviderUnitReclaims+1 {
		t.Fatalf("operator cancel keys %q, orphan repair keys %q", got, want)
	}
}
