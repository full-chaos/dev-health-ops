package synccoverage

import (
	"sort"
	"strings"
	"time"
)

const intervalAdjacencyTolerance = time.Microsecond

func mergeIntervals(input []coverageInterval) []coverageInterval {
	normalized := make([]coverageInterval, 0, len(input))
	for _, interval := range input {
		interval.Since = interval.Since.UTC()
		interval.Before = interval.Before.UTC()
		if !interval.Since.Before(interval.Before) {
			continue
		}
		interval.SourceIDs = uniqueSorted(interval.SourceIDs)
		interval.RunIDs = uniqueSorted(interval.RunIDs)
		normalized = append(normalized, interval)
	}
	sort.Slice(normalized, func(i, j int) bool {
		if normalized[i].Since.Equal(normalized[j].Since) {
			return normalized[i].Before.Before(normalized[j].Before)
		}
		return normalized[i].Since.Before(normalized[j].Since)
	})
	merged := make([]coverageInterval, 0, len(normalized))
	for _, interval := range normalized {
		if len(merged) == 0 {
			merged = append(merged, interval)
			continue
		}
		last := &merged[len(merged)-1]
		if !interval.Since.After(last.Before.Add(intervalAdjacencyTolerance)) {
			if interval.Before.After(last.Before) {
				last.Before = interval.Before
			}
			last.SourceIDs = unionSorted(last.SourceIDs, interval.SourceIDs)
			last.RunIDs = unionSorted(last.RunIDs, interval.RunIDs)
			continue
		}
		merged = append(merged, interval)
	}
	return merged
}

// mergeIntervalsBySourceScope merges intervals only when their source scopes
// match exactly.
//
// Dataset coverage is displayed as a union across sources, which remains
// useful for reporting, but a row-level backfill action must keep the source
// scope that produced the gap. Merging with plain mergeIntervals -- which
// unions SourceIDs across any time-adjacent interval regardless of which
// source(s) produced it -- turns a real, source-scoped gap (e.g. one repo
// with a unit still RUNNING) into a broad, wrongly-actionable range that also
// claims a fully-covered sibling source (CHAOS-4393). Mirrors
// “merge_intervals_by_source_scope“ in “api/services/sync_coverage.py“;
// use it for gap intervals ONLY -- Requested/Covered/FailedRanges rollups
// stay on plain mergeIntervals, matching the Python original.
func mergeIntervalsBySourceScope(input []coverageInterval) []coverageInterval {
	bySourceScope := make(map[string][]coverageInterval)
	for _, interval := range input {
		key := strings.Join(uniqueSorted(interval.SourceIDs), "\x00")
		bySourceScope[key] = append(bySourceScope[key], interval)
	}
	result := make([]coverageInterval, 0, len(input))
	for _, scoped := range bySourceScope {
		result = append(result, mergeIntervals(scoped)...)
	}
	sort.Slice(result, func(i, j int) bool {
		if !result[i].Since.Equal(result[j].Since) {
			return result[i].Since.Before(result[j].Since)
		}
		if !result[i].Before.Equal(result[j].Before) {
			return result[i].Before.Before(result[j].Before)
		}
		return strings.Join(result[i].SourceIDs, ",") < strings.Join(result[j].SourceIDs, ",")
	})
	return result
}

func subtractIntervals(requested, covered []coverageInterval) []coverageInterval {
	covered = mergeIntervals(covered)
	gaps := make([]coverageInterval, 0)
	for _, requestedInterval := range mergeIntervals(requested) {
		cursor := requestedInterval.Since
		for _, coveredInterval := range covered {
			if !coveredInterval.Before.After(cursor) {
				continue
			}
			if !coveredInterval.Since.Before(requestedInterval.Before) {
				break
			}
			if coveredInterval.Since.After(cursor) {
				end := minTime(coveredInterval.Since, requestedInterval.Before)
				gaps = append(gaps, coverageInterval{
					Since: cursor, Before: end,
					SourceIDs: append([]string(nil), requestedInterval.SourceIDs...),
					RunIDs:    append([]string(nil), requestedInterval.RunIDs...),
				})
			}
			if coveredInterval.Before.After(cursor) {
				cursor = coveredInterval.Before
			}
			if !cursor.Before(requestedInterval.Before) {
				break
			}
		}
		if cursor.Before(requestedInterval.Before) {
			gaps = append(gaps, coverageInterval{
				Since: cursor, Before: requestedInterval.Before,
				SourceIDs: append([]string(nil), requestedInterval.SourceIDs...),
				RunIDs:    append([]string(nil), requestedInterval.RunIDs...),
			})
		}
	}
	return mergeIntervals(gaps)
}

func statusFromParts(parts statusParts) string {
	switch {
	case parts.FailedCount > 0:
		return "failed"
	case parts.GapCount > 0:
		return "gaps"
	case !parts.HasData:
		return "insufficient_data"
	case parts.StaleStatus == "paused" || parts.StaleStatus == "not_scheduled":
		return parts.StaleStatus
	case parts.Running:
		return "running"
	case parts.StaleStatus == "stale":
		return "stale"
	default:
		return "healthy"
	}
}

func rollupStaleStatus(statuses []string) string {
	for _, target := range []string{"paused", "not_scheduled", "stale"} {
		for _, status := range statuses {
			if status == target {
				return target
			}
		}
	}
	return "healthy"
}

func uniqueSorted(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	set := make(map[string]struct{}, len(values))
	for _, value := range values {
		set[value] = struct{}{}
	}
	result := make([]string, 0, len(set))
	for value := range set {
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}

func unionSorted(left, right []string) []string {
	values := append(append([]string(nil), left...), right...)
	return uniqueSorted(values)
}

func minTime(left, right time.Time) time.Time {
	if left.Before(right) {
		return left
	}
	return right
}

func maxTime(left, right time.Time) time.Time {
	if left.After(right) {
		return left
	}
	return right
}
