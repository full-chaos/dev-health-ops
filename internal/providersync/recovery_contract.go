package providersync

import (
	"slices"

	"github.com/full-chaos/dev-health-ops/internal/workitemcontract"
)

var clickHouseRetryProvenSafeSurfaces = func() []string {
	surfaces := append(
		workitemcontract.LinearExpiredLeaseRetryDestinations(),
		"manual_attribution_fallbacks",
	)
	slices.Sort(surfaces)
	return surfaces
}()

type ExpiredLeaseRetryDecision struct {
	ShouldRetry    bool
	RetryExhausted bool
	RetryCount     int
	NextRetryCount int
	RetrySurfaces  []string
	MaxRetries     int
}

// LinearExpiredLeaseRetryDecision preserves the production Python retry
// eligibility boundary for expired Linear backfill work-item leases. It is
// intentionally provider-, mode-, dataset-, and surface-specific: expanding
// any one of those dimensions requires a new parity oracle and idempotency
// proof before Go recovery may claim the unit.
func LinearExpiredLeaseRetryDecision(
	unit Unit,
	retryCount int,
	maxRetries int,
) ExpiredLeaseRetryDecision {
	if retryCount < 0 {
		retryCount = 0
	}
	if maxRetries < 0 {
		maxRetries = 0
	}
	surfaces := []string(nil)
	baseEligible := unit.Provider == "linear" &&
		unit.Mode == "backfill" &&
		slices.Contains(workitemcontract.LinearBackfillWorkItemDatasets(), unit.Dataset)
	if baseEligible {
		surfaces = workitemcontract.LinearExpiredLeaseRetryDestinations()
		baseEligible = len(surfaces) > 0
		for _, surface := range surfaces {
			if !slices.Contains(clickHouseRetryProvenSafeSurfaces, surface) {
				baseEligible = false
				break
			}
		}
	}
	exhausted := baseEligible && retryCount >= maxRetries
	return ExpiredLeaseRetryDecision{
		ShouldRetry:    baseEligible && !exhausted,
		RetryExhausted: exhausted,
		RetryCount:     retryCount,
		NextRetryCount: retryCount + 1,
		RetrySurfaces:  surfaces,
		MaxRetries:     maxRetries,
	}
}
