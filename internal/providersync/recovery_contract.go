package providersync

import "slices"

var linearBackfillWorkItemDatasets = []string{
	"work-item-comments",
	"work-item-history",
	"work-item-labels",
	"work-item-projects",
	"work-items",
}

var linearBackfillWorkItemRetrySurfaces = []string{
	"ai_attribution",
	"estimate_coverage_metrics_daily",
	"investment_classifications_daily",
	"investment_metrics_daily",
	"issue_type_metrics_daily",
	"sprints",
	"work_item_cycle_times",
	"work_item_dependencies",
	"work_item_interactions",
	"work_item_metrics_daily",
	"work_item_reopen_events",
	"work_item_state_durations_daily",
	"work_item_team_attributions",
	"work_item_transitions",
	"work_item_user_metrics_daily",
	"work_items",
}

var clickHouseRetryProvenSafeSurfaces = func() []string {
	surfaces := append(
		slices.Clone(linearBackfillWorkItemRetrySurfaces),
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
		slices.Contains(linearBackfillWorkItemDatasets, unit.Dataset)
	if baseEligible {
		surfaces = slices.Clone(linearBackfillWorkItemRetrySurfaces)
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
