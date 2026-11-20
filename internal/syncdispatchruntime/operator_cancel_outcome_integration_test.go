//go:build integration

package syncdispatchruntime

// cancelDeliveries reads the provider-unit delivery counts off a cancel outcome.
func cancelDeliveries(outcome CancelSyncRunOutcome) (delivered, pending int) {
	return outcome.ProviderUnitDeliveries.Delivered, outcome.ProviderUnitDeliveries.Pending
}
