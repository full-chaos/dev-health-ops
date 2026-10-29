package syncdispatchruntime

import (
	"testing"

	schedulersync "github.com/full-chaos/dev-health-ops/internal/scheduler/sync"
)

// D6467: the terminal state of a run that planned zero units depends on WHY it planned none, and the planner (not the
// finalizer) says why: a provider with no such dataset is a no-op success; every other reason keeps the loud failure.
func TestZeroUnitTerminalIsASuccessOnlyForTheCapabilityReason(t *testing.T) {
	noDataset := zeroUnitTerminalFor(schedulersync.NoDatasetForProviderReason)
	if noDataset.status != syncRunStatusSuccess || !noDataset.clearError || noDataset.stampConfig || !noDataset.startNow {
		t.Errorf("no_dataset_for_provider = %+v, want success, no error, config not stamped, started_at set", noDataset)
	}
	for _, reason := range []string{zeroUnitGenericReason, "pagerduty_sync_disabled", "feature_disabled", "", "some_planner_error"} {
		other := zeroUnitTerminalFor(reason)
		if other.status != syncRunStatusFailed || other.clearError || !other.stampConfig || other.startNow {
			t.Errorf("reason %q = %+v, want the existing failure (status failed, error kept, config stamped, started_at untouched)", reason, other)
		}
	}
}
