package syncdispatchruntime

import "time"

// unitLogFields ports _unit_log_context verbatim, as the dict-shaped form
// every observation entry in this family embeds (unitLogAttrs is the same
// fields shaped for slog instead).
func unitLogFields(syncRunID string, unit budgetUnit) map[string]any {
	return map[string]any{
		"sync_run_id": syncRunID,
		"unit_id":     unit.id,
		"source_id":   unit.sourceID,
		"dataset_key": unit.datasetKey,
		"provider":    unit.provider,
		"cost_class":  unit.costClass,
	}
}

// mergeFields returns a NEW map holding base's entries overlaid with
// extra's -- every observeEstimate/admitSurplusRetries call site needs its
// own independent map (appended into a shared observations slice that
// outlives the loop), so entries are never written back into a shared
// logFields map across iterations.
func mergeFields(base, extra map[string]any) map[string]any {
	merged := make(map[string]any, len(base)+len(extra))
	for key, value := range base {
		merged[key] = value
	}
	for key, value := range extra {
		merged[key] = value
	}
	return merged
}

// observeEstimate ports _observe_estimate verbatim: the single-bucket
// fit-or-defer observation both observe_run's dry-run telemetry and
// enforce_run's real admission loop build from one estimate. consumedByBucket
// is a Go map, so a missing key already reads as 0 -- the same behavior
// Python gets from consumed_by_bucket being a defaultdict(int)
// (_active_budget_consumption / enforce_run both construct it that way).
//
// recordConsumption=false is observe_run's dry-run mode: look at what WOULD
// happen without actually charging the bucket, so a dry-run pass and the
// real admission loop can share this one function without the dry-run pass
// corrupting the real loop's running totals.
func observeEstimate(
	estimate budgetEstimate, logFields map[string]any, consumedByBucket map[string]int,
	limits map[string]int, defaultLimit int, observedAt time.Time, deferralSeconds int, recordConsumption bool,
) map[string]any {
	budgetKey := budgetKeyFor(estimate.Bucket, estimate.RouteFamily)
	limit := limitForBucket(estimate.Bucket, estimate.RouteFamily, limits, defaultLimit)
	previousUnits := consumedByBucket[budgetKey]
	projectedUnits := previousUnits + estimate.EstimatedUnits
	if recordConsumption {
		consumedByBucket[budgetKey] = projectedUnits
	}
	wouldDefer := projectedUnits > limit
	var suggestedAvailableAt any
	if wouldDefer {
		suggestedAvailableAt = observedAt.Add(time.Duration(deferralSeconds) * time.Second).Format(time.RFC3339Nano)
	}

	decision := "would_allow"
	if wouldDefer {
		decision = "would_defer"
	}
	return mergeFields(logFields, map[string]any{
		"decision":               decision,
		"bucket":                 bucketObservationFields(estimate.Bucket),
		"budget_key":             budgetKey,
		"estimated_units":        estimate.EstimatedUnits,
		"projected_units":        projectedUnits,
		"budget_limit":           limit,
		"confidence":             estimate.Confidence,
		"route_family":           estimate.RouteFamily,
		"suggested_available_at": suggestedAvailableAt,
	})
}
