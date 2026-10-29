package sync

import "strings"

// NoDatasetForProviderReason is the sync_runs.result reason of a run that planned zero units because the provider has
// no such dataset (D6467: GitHub has no incidents dataset). Such a run has nothing to sync and finalizes as a success.
const NoDatasetForProviderReason = "no_dataset_for_provider"

// ProviderSupportsDataset reports whether the planner knows the dataset for the provider. It reads the same
// capability table the planner plans against (supportedProviderDatasets), so the coverage projection and the finalizer
// cannot disagree with it.
func ProviderSupportsDataset(provider, dataset string) bool {
	_, ok := datasetSpecification(strings.ToLower(provider), dataset)
	return ok
}

// zeroUnitCapabilityReason returns NoDatasetForProviderReason when the provider is known, at least one dataset was
// requested, and the provider supports none of them; otherwise "". An unknown provider and an empty request are other
// problems and keep their own failure.
func zeroUnitCapabilityReason(provider string, datasets []PlanDataset) string {
	if len(datasets) == 0 {
		return ""
	}
	if _, known := supportedProviderDatasets[strings.ToLower(provider)]; !known {
		return ""
	}
	for _, dataset := range datasets {
		if ProviderSupportsDataset(provider, dataset.Key) {
			return ""
		}
	}
	return NoDatasetForProviderReason
}
