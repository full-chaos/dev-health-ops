package synccoverage

import "sort"

// referenceBuildPayload is buildPayload as of ops a03ab785c (before CHAOS-14233), kept verbatim as the differential
// oracle; only the backfill pair bound is a parameter. Do not edit it to follow buildPayload.
func referenceBuildPayload(input payloadInput, backfillPairLimit int) (projectionPayload, error) {
	if len(input.Windows) > maxCompactWindows {
		return nil, newLimitError("compact_window", len(input.Windows), maxCompactWindows)
	}
	scheduleInterval := intervalForSchedule(input.Schedule, input.Now)
	paused := !input.Config.Active
	scheduled := input.Schedule != nil && input.HasSchedule

	byPair := make(map[string][]unitWindow)
	for _, window := range input.Windows {
		key := window.SourceID + "\x00" + window.DatasetKey
		byPair[key] = append(byPair[key], window)
	}
	scopeSourceIDs := make(map[string]struct{}, len(input.Scope.Sources))
	for _, item := range input.Scope.Sources {
		scopeSourceIDs[item.ID.String()] = struct{}{}
	}
	scopeDatasets := make(map[string]struct{}, len(input.Scope.DatasetKeys))
	for _, key := range input.Scope.DatasetKeys {
		scopeDatasets[key] = struct{}{}
	}
	backfillByPair := make(map[string][]coverageInterval)
	expanded := 0
	for _, interval := range input.Backfills {
		sourceIDs := interval.SourceIDs
		if len(sourceIDs) == 0 {
			for sourceID := range scopeSourceIDs {
				sourceIDs = append(sourceIDs, sourceID)
			}
			sort.Strings(sourceIDs)
		}
		datasetKeys := interval.DatasetKeys
		if len(datasetKeys) == 0 {
			datasetKeys = input.Scope.DatasetKeys
		}
		for _, sourceID := range sourceIDs {
			if _, ok := scopeSourceIDs[sourceID]; !ok {
				continue
			}
			for _, dataset := range datasetKeys {
				if _, ok := scopeDatasets[dataset]; !ok {
					continue
				}
				expanded++
				if expanded > backfillPairLimit {
					return nil, newLimitError("backfill_pair", expanded, backfillPairLimit)
				}
				key := sourceID + "\x00" + dataset
				backfillByPair[key] = append(backfillByPair[key], coverageInterval{
					Since: interval.Since, Before: interval.Before,
					SourceIDs: []string{sourceID}, RunIDs: interval.RunIDs,
				})
			}
		}
	}

	pairs := make([]pairCoverage, 0, len(input.Scope.Sources)*len(input.Scope.DatasetKeys))
	for _, sourceItem := range input.Scope.Sources {
		sourceID := sourceItem.ID.String()
		for _, dataset := range input.Scope.DatasetKeys {
			key := sourceID + "\x00" + dataset
			pairWindows := byPair[key]
			successes := filterWindows(pairWindows, "success")
			failures := filterWindows(pairWindows, "failed")
			closedRanges := closedRangesFromWindows(filterWindows(pairWindows, statusClosedWithReason))
			requested := append(intervalsFromWindows(pairWindows), backfillByPair[key]...)
			requested = mergeIntervals(requested)
			covered := mergeIntervals(intervalsFromWindows(successes))
			failedRanges := failedRangesNotSuperseded(failures, successes)
			gaps := subtractIntervals(requested, covered)
			coveredThrough := latestCoveredThrough(covered, input.Now)
			staleStatus := classifyStaleness(coveredThrough, input.Now, scheduleInterval, paused, scheduled)
			staleRanges := make([]coverageInterval, 0)
			if staleStatus == "stale" && coveredThrough != nil {
				staleRanges = append(staleRanges, coverageInterval{Since: *coveredThrough, Before: input.Now, SourceIDs: []string{sourceID}})
			}
			_, running := input.ActivePairs[key]
			pairs = append(pairs, pairCoverage{
				SourceID: sourceID, DatasetKey: dataset, Requested: requested, Covered: covered,
				Gaps: gaps, StaleRanges: staleRanges, FailedRanges: failedRanges, ClosedRanges: closedRanges,
				CoveredThrough: coveredThrough,
				Status: statusFromParts(statusParts{FailedCount: len(failedRanges), GapCount: len(gaps),
					StaleStatus: staleStatus, HasData: len(requested) > 0 || len(covered) > 0, Running: running}),
			})
		}
	}

	datasets := buildDatasetCoverage(input.Scope.DatasetKeys, pairs)
	sources := buildSourcePayload(input.Scope.Sources, pairs)
	failedCount, gapCount, staleCount := 0, 0, 0
	hasData := false
	for _, dataset := range datasets {
		failedCount += len(dataset.FailedRanges)
		gapCount += len(dataset.Gaps)
		if dataset.Status == "stale" {
			staleCount++
		}
		hasData = hasData || len(dataset.Requested) > 0 || len(dataset.Covered) > 0
	}
	overallHealth := "healthy"
	switch {
	case !hasData:
		overallHealth = "insufficient_data"
	case failedCount > 0:
		overallHealth = "failed"
	case gapCount > 0:
		overallHealth = "gaps"
	case staleCount > 0:
		overallHealth = "stale"
	}
	latestCovered := latestDatasetCoveredThrough(datasets)
	coverageSince, coverageThrough := coverageBounds(datasets)

	datasetPayload := make([]any, 0, len(datasets)+len(providerDatasets[input.Config.Provider]))
	for _, dataset := range datasets {
		datasetPayload = append(datasetPayload, map[string]any{
			"dataset_key": dataset.DatasetKey, "status": dataset.Status,
			"covered_through":  isoTimePointer(dataset.CoveredThrough),
			"requested_ranges": intervalPayloads(dataset.Requested),
			"covered_ranges":   intervalPayloads(dataset.Covered),
			"gaps":             intervalPayloads(dataset.Gaps),
			"stale_ranges":     intervalPayloads(dataset.StaleRanges),
			"failed_ranges":    intervalPayloads(dataset.FailedRanges),
			// CHAOS-13988: covered windows whose only lost reports were
			// closed with a reason. Additive: payloads stored before it lack
			// the key, so projectionVersion is unchanged.
			"closed_with_reason_ranges": closedRangePayloads(dataset.ClosedRanges),
		})
	}
	for _, key := range notEnabledDatasets(input.Config, input.Scope) {
		datasetPayload = append(datasetPayload, map[string]any{
			"dataset_key": key, "status": "not_enabled", "covered_through": nil,
			"requested_ranges": []any{}, "covered_ranges": []any{}, "gaps": []any{},
			"stale_ranges": []any{}, "failed_ranges": []any{}, "closed_with_reason_ranges": []any{},
		})
	}

	truncated := input.Now.AddDate(0, 0, -HistoryLookbackDays)
	return projectionPayload{
		"config_id": input.Config.ID.String(), "provider": input.Config.Provider,
		"generated_at": isoTime(input.Now), "data_basis": dataBasis(input.Config, input.Scope),
		"history_lookback_days": HistoryLookbackDays, "truncated_before": isoTime(truncated),
		"coverage_since": isoTimePointer(coverageSince), "coverage_through": isoTimePointer(coverageThrough),
		"is_truncated": input.IsTruncated, "truncation_reason": truncationReason(input.IsTruncated),
		"projection_version": projectionVersion, "projection_complete": true,
		"overall": map[string]any{
			"health": overallHealth, "latest_successful_run_at": isoTimePointer(input.LatestSuccessful),
			"latest_covered_through": isoTimePointer(latestCovered),
			"next_scheduled_run_at":  scheduleNextRun(input.Schedule),
			"gap_count":              gapCount, "stale_dataset_count": staleCount, "failed_range_count": failedCount,
		},
		"datasets": datasetPayload, "sources": sources,
		"backfill_windows": canonicalBackfillWindows(pairs),
	}, nil
}
