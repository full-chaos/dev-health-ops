package workitemmetrics

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// Percentile ports _percentile (compute_work_items.py:39). It returns nil for
// an empty input, mirroring how every caller guards the Python call with
// `if values else None`.
//
// Each intermediate below is bound to its own name and passed through an
// explicit float64 conversion, and the parenthesisation mirrors
// compute_work_items.py:48-52 exactly -- `(n-1) * (p/100)`, not `(n-1)*p/100`.
//
// This shape is not stylistic. Written as one expression, the Go compiler is
// free to contract `a*b + c*d` into fused operations that skip the intermediate
// rounding Python performs -- arm64 fuses, amd64 typically does not, and the
// production Go workers are arm64 (CHAOS-4818). Measured over 6000 percentile
// evaluations drawn from the real input class (hours derived from
// microsecond-resolution durations, n = 2..7, p = 50 and 90), the collapsed form
// landed one ulp away from the live `_percentile` on 319 of them -- 5.3% --
// while this form matched on all 6000.
//
// One ulp matters because these columns are compared for EQUALITY against the
// Python oracle and, in providersync's case, on effect readback: a percentile
// disagreeing in its last bit is a row that can never be confirmed, on every
// re-run, forever.
func Percentile(values []float64, percentile float64) *float64 {
	if len(values) == 0 {
		return nil
	}
	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)
	if percentile <= 0 {
		return floatPointer(sorted[0])
	}
	if percentile >= 100 {
		return floatPointer(sorted[len(sorted)-1])
	}
	ratio := float64(percentile / 100)
	rank := float64(float64(len(sorted)-1) * ratio)
	lo := int(rank)
	hi := lo + 1
	if hi >= len(sorted) {
		hi = len(sorted) - 1
	}
	frac := float64(rank - float64(lo))
	low := float64(sorted[lo] * float64(1-frac))
	high := float64(sorted[hi] * frac)
	return floatPointer(low + high)
}

// Seconds and Hours mirror how PYTHON reaches these quantities, which is not
// how Go's own Duration methods do.
//
// timedelta.total_seconds() is `(whole microseconds) / 10**6` -- one division of
// an exact integer. Duration.Seconds() instead splits into whole seconds plus a
// nanosecond remainder and adds them, and Duration.Hours() splits into whole
// hours plus a nanosecond remainder. Those are different roundings of the same
// interval, and they disagree in the last bit for ordinary values: a
// 12h31m08.107259s cycle is 12.518918683055555 hours through Python's path and
// 12.518918683055556 through Duration.Hours(). Both numbers are "right"; only
// one of them matches the producer, and these columns are compared for equality.
//
// Every datetime Python can hold is a whole number of microseconds, so
// Microseconds() loses nothing here.
func Seconds(value time.Duration) float64 {
	return float64(float64(value.Microseconds()) / 1e6)
}

// Hours is Seconds divided by 3600, matching Python's `.total_seconds() / 3600.0`.
func Hours(value time.Duration) float64 {
	return float64(Seconds(value) / 3600)
}

// FlowBreakdown ports _calculate_flow_breakdown (compute_work_items.py:969),
// returning (active_hours, wait_hours).
//
// Python takes the item and derives start/end from item.started_at/completed_at
// and returns (0, 0) when either is missing; this signature takes the two
// instants directly because every caller has already established both are
// present. The `start >= end` guard is kept.
//
// activeSeconds/waitSeconds accumulate with a plain `+=`, exactly as Python
// does (:1020-1035). CPython's Neumaier-compensated sum() is not involved --
// Python never calls sum() here -- so compensating in Go would be the
// divergence. The explicit float64() conversions prevent the compiler from
// contracting the accumulation.
func FlowBreakdown(startedAt, completedAt time.Time, transitions []Transition) (float64, float64) {
	start, end := startedAt.UTC(), completedAt.UTC()
	if !start.Before(end) {
		return 0, 0
	}
	sorted := append([]Transition(nil), transitions...)
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].OccurredAt.Before(sorted[j].OccurredAt) })

	// Python seeds current_status from the LAST transition at or before
	// started_at, then applies the unknown/todo/backlog -> in_progress default.
	currentStatus := "unknown"
	for _, transition := range sorted {
		occurred := transition.OccurredAt.UTC()
		if occurred.After(start) {
			break
		}
		currentStatus = transition.ToStatus
	}
	if currentStatus == "unknown" || currentStatus == "todo" || currentStatus == "backlog" {
		currentStatus = "in_progress"
	}

	last := start
	activeSeconds, waitSeconds := 0.0, 0.0
	for _, transition := range sorted {
		occurred := transition.OccurredAt.UTC()
		if !occurred.After(start) {
			continue
		}
		if !occurred.Before(end) {
			break
		}
		duration := Seconds(occurred.Sub(last))
		if _, waiting := waitStatuses[strings.ToLower(currentStatus)]; waiting {
			waitSeconds = float64(waitSeconds + duration)
		} else {
			activeSeconds = float64(activeSeconds + duration)
		}
		currentStatus, last = transition.ToStatus, occurred
	}
	if duration := Seconds(end.Sub(last)); duration > 0 {
		if _, waiting := waitStatuses[strings.ToLower(currentStatus)]; waiting {
			waitSeconds = float64(waitSeconds + duration)
		} else {
			activeSeconds = float64(activeSeconds + duration)
		}
	}
	return float64(activeSeconds / 3600), float64(waitSeconds / 3600)
}

// NormalizeTeamID ports normalize_team_id (providers/teams.py:37): a missing,
// empty, or whitespace-only team id becomes UNASSIGNED_TEAM_ID.
func NormalizeTeamID(value *string) string {
	if value == nil || strings.TrimSpace(*value) == "" {
		return UnassignedTeamID
	}
	return strings.TrimSpace(*value)
}

// NormalizeTeamName ports normalize_team_name (providers/teams.py:43).
func NormalizeTeamName(value *string) string {
	if value == nil || strings.TrimSpace(*value) == "" {
		return UnassignedTeamName
	}
	return strings.TrimSpace(*value)
}

// UnassignedTeamID/UnassignedTeamName port UNASSIGNED_TEAM_ID/
// UNASSIGNED_TEAM_NAME (providers/teams.py:33-34).
const (
	UnassignedTeamID   = "unassigned"
	UnassignedTeamName = "Unassigned"
)

// FirstAssignee is `item.assignees[0] if item.assignees else None`
// (compute_work_items.py:1126).
func FirstAssignee(values []string) *string {
	if len(values) == 0 {
		return nil
	}
	value := values[0]
	return &value
}

func utcTimePointer(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	utc := value.UTC()
	return &utc
}

// earliestTimePointer ports _earliest_utc (compute_work_items.py:56).
func earliestTimePointer(values ...*time.Time) *time.Time {
	var result *time.Time
	for _, value := range values {
		if value != nil && (result == nil || value.Before(*result)) {
			copied := *value
			result = &copied
		}
	}
	return result
}

func inHalfOpenDay(value *time.Time, start, end time.Time) bool {
	return value != nil && !value.Before(start) && value.Before(end)
}

func floatPointer(value float64) *float64 { return &value }

// maxFloat is Python's `max(1.0, float(throughput_7d))`
// (compute_work_items.py:1341). math.Max is avoided deliberately: it has NaN
// and signed-zero semantics this expression never needs, and neither input can
// be NaN here.
func maxFloat(left, right float64) float64 {
	if left > right {
		return left
	}
	return right
}

// AssertAligned pins the Resolver index contract: it panics unless each
// projected Item carries the SourceIndex matching its own position.
//
// It checks the INDEX, not the work_item_id. Comparing ids was the previous
// version's error and codex r2 caught its consequence: the contract chose
// indexes exactly so two source rows may share an id, so an id comparison
// accepts a reordering between two such rows -- equal length, equal ids at
// every position, and the resolver still reading the wrong row. Executed
// against that version: "AssertAligned duplicate reorder accepted: true".
//
// A panic is the right severity and deliberately not an error return.
// Misalignment means every attribution from the first divergence on is WRONG --
// items land in another item's team, and the output is plausible, non-empty and
// undetectable downstream. There is no partial-credit recovery from an
// already-mis-projected input, so failing loudly at construction converts a
// silent data-corruption class into an immediate crash in the caller's tests.
func AssertAligned(sourceRows int, items []Item, resolve Resolver) Resolver {
	if sourceRows != len(items) {
		panic(fmt.Sprintf(
			"workitemmetrics: Resolver index contract violated -- %d source rows "+
				"projected to %d items. The projection must be 1:1 and order-preserving.",
			sourceRows, len(items),
		))
	}
	for index := range items {
		if items[index].SourceIndex != index {
			panic(fmt.Sprintf(
				"workitemmetrics: Resolver index contract violated at position %d -- "+
					"the item there carries SourceIndex %d. The projection reordered or "+
					"substituted rows, so every resolver answer from here on attributes "+
					"one work item using another's ownership.",
				index, items[index].SourceIndex,
			))
		}
	}
	return resolve
}
