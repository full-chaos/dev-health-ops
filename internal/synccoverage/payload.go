package synccoverage

import (
	"fmt"
	"sort"
	"time"

	schedulersync "github.com/full-chaos/dev-health-ops/internal/scheduler/sync"
)

type payloadInput struct {
	Config           syncConfig
	Scope            effectiveScope
	Windows          []unitWindow
	Backfills        []coverageInterval
	ActivePairs      map[string]struct{}
	Schedule         *schedule
	HasSchedule      bool
	Now              time.Time
	LatestSuccessful *time.Time
	IsTruncated      bool
}

func buildPayload(input payloadInput) (projectionPayload, error) {
	if len(input.Windows) > maxCompactWindows {
		return nil, fmt.Errorf("sync coverage compact window limit exceeded: %d", maxCompactWindows)
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
				if expanded > maxBackfillPairs {
					return nil, fmt.Errorf("sync coverage backfill pair limit exceeded: %d", maxBackfillPairs)
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
				Gaps: gaps, StaleRanges: staleRanges, FailedRanges: failedRanges,
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
		})
	}
	for _, key := range notEnabledDatasets(input.Config, input.Scope) {
		datasetPayload = append(datasetPayload, map[string]any{
			"dataset_key": key, "status": "not_enabled", "covered_through": nil,
			"requested_ranges": []any{}, "covered_ranges": []any{}, "gaps": []any{},
			"stale_ranges": []any{}, "failed_ranges": []any{},
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

func intervalForSchedule(active *schedule, now time.Time) *time.Duration {
	if active == nil {
		return nil
	}
	first, _, err := schedulersync.NextOccurrence(active.Cron, now, "UTC")
	if err != nil {
		return nil
	}
	second, _, err := schedulersync.NextOccurrence(active.Cron, first, "UTC")
	if err != nil {
		return nil
	}
	interval := second.Sub(first)
	return &interval
}

func classifyStaleness(covered *time.Time, now time.Time, interval *time.Duration, paused, scheduled bool) string {
	if paused {
		return "paused"
	}
	if !scheduled {
		return "not_scheduled"
	}
	if covered == nil {
		return "insufficient_data"
	}
	grace := fallbackStaleGrace
	if interval != nil {
		grace = *interval * 2
		if grace < minimumStaleGrace {
			grace = minimumStaleGrace
		}
	}
	if covered.Add(grace).Before(now) {
		return "stale"
	}
	return "healthy"
}

func filterWindows(input []unitWindow, status string) []unitWindow {
	result := make([]unitWindow, 0)
	for _, window := range input {
		if window.Status == status {
			result = append(result, window)
		}
	}
	return result
}

func intervalsFromWindows(windows []unitWindow) []coverageInterval {
	result := make([]coverageInterval, 0, len(windows))
	for _, window := range windows {
		result = append(result, coverageInterval{Since: window.Since, Before: window.Before, SourceIDs: []string{window.SourceID}})
	}
	return result
}

func failedRangesNotSuperseded(failures, successes []unitWindow) []coverageInterval {
	result := make([]coverageInterval, 0)
	for _, failure := range failures {
		later := make([]coverageInterval, 0)
		for _, success := range successes {
			if success.SourceID == failure.SourceID && success.DatasetKey == failure.DatasetKey && !success.RunTime.Before(failure.RunTime) {
				later = append(later, coverageInterval{Since: success.Since, Before: success.Before, SourceIDs: []string{success.SourceID}})
			}
		}
		result = append(result, subtractIntervals([]coverageInterval{{Since: failure.Since, Before: failure.Before, SourceIDs: []string{failure.SourceID}}}, later)...)
	}
	return mergeIntervals(result)
}

func latestCoveredThrough(intervals []coverageInterval, now time.Time) *time.Time {
	var result *time.Time
	for _, interval := range intervals {
		before := minTime(interval.Before.UTC(), now.UTC())
		if interval.Since.UTC().Before(before) && (result == nil || before.After(*result)) {
			value := before
			result = &value
		}
	}
	return result
}

func buildDatasetCoverage(keys []string, pairs []pairCoverage) []datasetCoverage {
	result := make([]datasetCoverage, 0, len(keys))
	for _, key := range keys {
		item := datasetCoverage{DatasetKey: key}
		statuses := make([]string, 0)
		running, stale := false, false
		for _, pair := range pairs {
			if pair.DatasetKey != key {
				continue
			}
			item.Requested = append(item.Requested, pair.Requested...)
			item.Covered = append(item.Covered, pair.Covered...)
			item.Gaps = append(item.Gaps, pair.Gaps...)
			item.StaleRanges = append(item.StaleRanges, pair.StaleRanges...)
			item.FailedRanges = append(item.FailedRanges, pair.FailedRanges...)
			if pair.CoveredThrough != nil && (item.CoveredThrough == nil || pair.CoveredThrough.After(*item.CoveredThrough)) {
				value := *pair.CoveredThrough
				item.CoveredThrough = &value
			}
			statuses = append(statuses, pair.Status)
			running = running || pair.Status == "running"
			stale = stale || pair.Status == "stale"
		}
		item.Requested = mergeIntervals(item.Requested)
		item.Covered = mergeIntervals(item.Covered)
		item.Gaps = mergeIntervals(item.Gaps)
		item.StaleRanges = mergeIntervals(item.StaleRanges)
		item.FailedRanges = mergeIntervals(item.FailedRanges)
		item.Status = statusFromParts(statusParts{FailedCount: len(item.FailedRanges), GapCount: len(item.Gaps),
			StaleStatus: rollupStaleStatus(statuses), HasData: len(item.Requested) > 0 || len(item.Covered) > 0,
			Running: running && !stale})
		result = append(result, item)
	}
	return result
}

func buildSourcePayload(sources []source, pairs []pairCoverage) []any {
	result := make([]any, 0, len(sources))
	for _, sourceItem := range sources {
		id := sourceItem.ID.String()
		statuses := make([]string, 0)
		gapCount, failedCount := 0, 0
		hasData, running, stale := false, false, false
		var coveredThrough *time.Time
		for _, pair := range pairs {
			if pair.SourceID != id {
				continue
			}
			statuses = append(statuses, pair.Status)
			gapCount += len(pair.Gaps)
			failedCount += len(pair.FailedRanges)
			hasData = hasData || len(pair.Requested) > 0 || len(pair.Covered) > 0
			running = running || pair.Status == "running"
			stale = stale || pair.Status == "stale"
			if pair.CoveredThrough != nil && (coveredThrough == nil || pair.CoveredThrough.After(*coveredThrough)) {
				value := *pair.CoveredThrough
				coveredThrough = &value
			}
		}
		name := sourceItem.FullName
		if name == "" {
			name = sourceItem.Name
		}
		result = append(result, map[string]any{
			"source_id": id, "source_name": name,
			"status": statusFromParts(statusParts{FailedCount: failedCount, GapCount: gapCount,
				StaleStatus: rollupStaleStatus(statuses), HasData: hasData, Running: running && !stale}),
			"covered_through": isoTimePointer(coveredThrough), "gap_count": gapCount,
			"failed_range_count": failedCount,
		})
	}
	return result
}

func intervalPayloads(intervals []coverageInterval) []any {
	result := make([]any, 0, len(intervals))
	for _, interval := range intervals {
		item := map[string]any{"since": isoTime(interval.Since), "before": isoTime(interval.Before)}
		if len(interval.SourceIDs) > 0 {
			item["source_ids"] = interval.SourceIDs
		}
		if len(interval.RunIDs) > 0 {
			item["run_ids"] = interval.RunIDs
		}
		result = append(result, item)
	}
	return result
}

func latestDatasetCoveredThrough(datasets []datasetCoverage) *time.Time {
	var result *time.Time
	for _, dataset := range datasets {
		if dataset.CoveredThrough != nil && (result == nil || dataset.CoveredThrough.After(*result)) {
			value := *dataset.CoveredThrough
			result = &value
		}
	}
	return result
}

func coverageBounds(datasets []datasetCoverage) (*time.Time, *time.Time) {
	var since, through *time.Time
	for _, dataset := range datasets {
		for _, interval := range dataset.Covered {
			if since == nil || interval.Since.Before(*since) {
				value := interval.Since
				since = &value
			}
			if through == nil || interval.Before.After(*through) {
				value := interval.Before
				through = &value
			}
		}
	}
	return since, through
}

func notEnabledDatasets(config syncConfig, scope effectiveScope) []string {
	if scope.IntegrationID == nil || config.SourceID != nil {
		return nil
	}
	enabled := make(map[string]struct{}, len(scope.DatasetKeys))
	for _, key := range scope.DatasetKeys {
		enabled[key] = struct{}{}
	}
	result := make([]string, 0)
	for _, key := range providerDatasets[config.Provider] {
		if _, ok := enabled[key]; !ok {
			result = append(result, key)
		}
	}
	sort.Strings(result)
	return result
}

// canonicalBackfillWindows mirrors _canonical_backfill_windows in
// src/dev_health_ops/api/services/sync_coverage.py, which is authoritative:
// this payload feeds the same focused-backfill selector the Python API serves.
//
// Two rules carry the whole contract, and the previous implementation broke
// both.
//
// First, a window is emitted ONLY when the half-open range already sits on
// exact UTC-midnight boundaries, because that selector accepts nothing else.
// Python skips anything else outright, so an empty list means "not safely
// representable"; the old Go code instead COERCED a partial-day range onto day
// boundaries (nudging a midnight `before` back a microsecond, then truncating
// both ends), which manufactured windows for ranges the API would refuse --
// including a degenerate since == before empty window in the oracle fixture.
//
// Second, the scope is (since, before, source_id, dataset_key). A backfill
// window is only actionable against the source and dataset it came from. The
// old code iterated datasets alone, dropped source identity entirely, and then
// merged adjacent windows across BOTH -- so one source's gap could widen a
// different source's window. Python does no merging at all, deliberately.
func canonicalBackfillWindows(pairs []pairCoverage) []any {
	type scope struct {
		Since, Before        time.Time
		SourceID, DatasetKey string
	}
	reasonsByScope := make(map[scope]map[string]struct{})
	for _, pair := range pairs {
		for _, group := range []struct {
			Intervals []coverageInterval
			Reason    string
		}{{pair.Gaps, "gap"}, {pair.FailedRanges, "failed"}} {
			for _, interval := range group.Intervals {
				since, before := interval.Since.UTC(), interval.Before.UTC()
				// Boundaries are advertised verbatim. Gating on exact UTC
				// midnight matched 0 of 138 real intervals, because coverage
				// derives from sync run unit windows that start whenever a
				// sync ran (CHAOS-3915). The planner honours these instants:
				// it chunks on whole days but keeps the requested edges.
				//
				// Intervals no wider than the adjacency tolerance are not gaps:
				// they are the seam where one day-bounded window meets the next
				// (23:59:59.999999 -> 00:00:00). On real data 66 of 114
				// candidates were exactly one microsecond wide. This subsumes
				// the empty-interval case, which BackfillSelectorRequest would
				// reject with a 422. Mirrors _canonical_backfill_windows.
				if before.Sub(since) <= intervalAdjacencyTolerance {
					continue
				}
				key := scope{
					Since: since, Before: before,
					SourceID: pair.SourceID, DatasetKey: pair.DatasetKey,
				}
				if reasonsByScope[key] == nil {
					reasonsByScope[key] = make(map[string]struct{})
				}
				reasonsByScope[key][group.Reason] = struct{}{}
			}
		}
	}

	keys := make([]scope, 0, len(reasonsByScope))
	for key := range reasonsByScope {
		keys = append(keys, key)
	}
	// Python sorts by (since, before, dataset_keys, source_ids, reasons); the
	// first four already make the order total here, since reasons are a
	// function of the scope.
	sort.Slice(keys, func(i, j int) bool {
		if !keys[i].Since.Equal(keys[j].Since) {
			return keys[i].Since.Before(keys[j].Since)
		}
		if !keys[i].Before.Equal(keys[j].Before) {
			return keys[i].Before.Before(keys[j].Before)
		}
		if keys[i].DatasetKey != keys[j].DatasetKey {
			return keys[i].DatasetKey < keys[j].DatasetKey
		}
		return keys[i].SourceID < keys[j].SourceID
	})

	result := make([]any, 0, len(keys))
	for _, key := range keys {
		reasons := make([]string, 0, len(reasonsByScope[key]))
		for reason := range reasonsByScope[key] {
			reasons = append(reasons, reason)
		}
		sort.Strings(reasons)
		result = append(result, map[string]any{
			"since": isoTime(key.Since), "before": isoTime(key.Before),
			"source_ids": []string{key.SourceID}, "dataset_keys": []string{key.DatasetKey},
			"reasons": reasons,
		})
	}
	return result
}

func day(value time.Time) time.Time {
	value = value.UTC()
	return time.Date(value.Year(), value.Month(), value.Day(), 0, 0, 0, 0, time.UTC)
}

func isoTime(value time.Time) string {
	value = value.UTC().Truncate(time.Microsecond)
	if value.Nanosecond() == 0 {
		return value.Format("2006-01-02T15:04:05-07:00")
	}
	return value.Format("2006-01-02T15:04:05.000000-07:00")
}

func isoTimePointer(value *time.Time) any {
	if value == nil {
		return nil
	}
	return isoTime(*value)
}

func dataBasis(config syncConfig, scope effectiveScope) string {
	if config.IntegrationID != nil && scope.IntegrationID != nil {
		return "planner"
	}
	return "legacy"
}

func truncationReason(truncated bool) any {
	if truncated {
		return "lookback_limit"
	}
	return nil
}

func scheduleNextRun(active *schedule) any {
	if active == nil || active.NextRunAt == nil {
		return nil
	}
	return isoTime(*active.NextRunAt)
}
