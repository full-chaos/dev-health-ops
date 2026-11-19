package synccoverage

import (
	"slices"
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

// subtractIntervals returns the parts of requested that covered does not cover. A remainder no wider than
// intervalAdjacencyTolerance is dropped: it is the seam where one day-bounded window ends at 23:59:59.999999 and the
// next starts at 00:00:00 (mergeIntervals already treats those as adjacent), not a gap or a failed range.
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
				if end.Sub(cursor) <= intervalAdjacencyTolerance {
					// The seam between two day-bounded windows (23:59:59.999999 -> 00:00:00), not a remainder.
					cursor = coveredInterval.Before
					if !cursor.Before(requestedInterval.Before) {
						break
					}
					continue
				}
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
		if requestedInterval.Before.Sub(cursor) > intervalAdjacencyTolerance {
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

// subtractClosedRanges removes from each closed-with-reason range the parts a
// complete unit covered (complete wins, CHAOS-13988). Ranges with the same
// reasons are merged; ranges with different reasons are kept apart, so each
// range keeps exactly the reasons of the units that closed it.
func subtractClosedRanges(closed []closedRange, complete []coverageInterval) []closedRange {
	return groupClosedRanges(closed, func(intervals []coverageInterval) []coverageInterval {
		return subtractIntervals(intervals, complete)
	})
}

// mergeClosedRanges merges closed-with-reason ranges that share both their
// source scope and their reasons (the dataset rollup of the pair ranges).
func mergeClosedRanges(closed []closedRange) []closedRange {
	return groupClosedRanges(closed, func(intervals []coverageInterval) []coverageInterval {
		return mergeIntervals(intervals)
	})
}

func groupClosedRanges(closed []closedRange, combine func([]coverageInterval) []coverageInterval) []closedRange {
	type group struct {
		reasons   []string
		intervals []coverageInterval
	}
	groups := make(map[string]*group)
	for _, item := range closed {
		reasons := uniqueSorted(item.Reasons)
		key := strings.Join(uniqueSorted(item.Interval.SourceIDs), "\x00") + "\x01" + strings.Join(reasons, "\x00")
		if groups[key] == nil {
			groups[key] = &group{reasons: reasons}
		}
		groups[key].intervals = append(groups[key].intervals, item.Interval)
	}
	result := make([]closedRange, 0, len(closed))
	for _, item := range groups {
		for _, interval := range combine(item.intervals) {
			result = append(result, closedRange{Interval: interval, Reasons: append([]string(nil), item.reasons...)})
		}
	}
	sort.Slice(result, func(i, j int) bool {
		left, right := result[i].Interval, result[j].Interval
		if !left.Since.Equal(right.Since) {
			return left.Since.Before(right.Since)
		}
		if !left.Before.Equal(right.Before) {
			return left.Before.Before(right.Before)
		}
		if sources := strings.Join(left.SourceIDs, ","); sources != strings.Join(right.SourceIDs, ",") {
			return sources < strings.Join(right.SourceIDs, ",")
		}
		return strings.Join(result[i].Reasons, ",") < strings.Join(result[j].Reasons, ",")
	})
	return result
}

// subtractFailed is state.Failed = subtractIntervals(state.Failed, []coverageInterval{interval}) without a sort and
// copy of the whole list per row (CHAOS-14167). Failed is always mergeIntervals output (sorted, apart by more than
// intervalAdjacencyTolerance) and subtractIntervals cuts each failed interval on its own, so only the intervals that
// overlap interval change. The one other effect of subtractIntervals, dropping every interval no longer than
// intervalAdjacencyTolerance, is kept: only addFailed can leave one, and it sets failedHasShort.
func (state *pairState) subtractFailed(interval coverageInterval) {
	failed := state.Failed
	if state.failedHasShort {
		kept := make([]coverageInterval, 0, len(failed))
		for _, item := range failed {
			if item.Before.Sub(item.Since) > intervalAdjacencyTolerance {
				kept = append(kept, item)
			}
		}
		failed = kept
		state.failedHasShort = false
	}
	low := sort.Search(len(failed), func(index int) bool { return failed[index].Before.After(interval.Since) })
	high := low
	for high < len(failed) && failed[high].Since.Before(interval.Before) {
		high++
	}
	if low < high {
		failed = slices.Replace(failed, low, high, subtractIntervals(failed[low:high], []coverageInterval{interval})...)
	}
	state.Failed = failed
}

// addFailed is state.Failed = mergeIntervals(append(state.Failed, interval)) for the merged Failed list: only the
// run of failed intervals that mergeIntervals would join with interval is merged again.
func (state *pairState) addFailed(interval coverageInterval) {
	failed := state.Failed
	low := sort.Search(len(failed), func(index int) bool {
		return !interval.Since.After(failed[index].Before.Add(intervalAdjacencyTolerance))
	})
	end := interval.Before
	high := low
	for high < len(failed) && !failed[high].Since.After(end.Add(intervalAdjacencyTolerance)) {
		if failed[high].Before.After(end) {
			end = failed[high].Before
		}
		high++
	}
	merged := mergeIntervals(append(slices.Clone(failed[low:high]), interval))
	for _, item := range merged {
		if item.Before.Sub(item.Since) <= intervalAdjacencyTolerance {
			state.failedHasShort = true
		}
	}
	state.Failed = slices.Replace(failed, low, high, merged...)
}
