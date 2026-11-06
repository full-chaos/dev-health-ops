package workitemmetrics

import (
	"sort"
	"time"
)

// The work_item_state family's compute: one UTC day of status durations over loaded rows. It lives here, beside
// ComputeStoredItemsDay, for the same reason: the daily job writes work_item_state_durations_daily from it, and a
// request held to a repository ceiling recomputes the rows of the items linked to the allowed repositories from it
// (CHAOS-13088). A serving binary must not import the job package, so the pure compute is shared from this package;
// the I/O stays with each caller.

// StateItem is the column subset of a stored work item the state compute reads.
type StateItem struct {
	WorkItemID    string
	Provider      string
	Status        string // already-normalized WorkItemStatusCategory string
	ProjectKey    string
	ProjectID     string
	NativeTeamKey string
	ProjectName   string
	CreatedAt     time.Time
	CompletedAt   *time.Time
}

// StateTransition is one status transition of a work item, deduplicated as the loaders of both callers do.
type StateTransition struct {
	WorkItemID string
	OccurredAt time.Time
	FromStatus string // "" is a valid, meaningful value -- see SegmentStatuses
	ToStatus   string
}

// StateDurationRow is one aggregated output row, matching WorkItemStateDurationDailyRecord (schemas.py:598-609).
type StateDurationRow struct {
	Provider      string
	WorkScopeID   string
	TeamID        string
	TeamName      string
	Status        string
	DurationHours float64
	ItemsTouched  uint32
	AvgWIP        float64
}

// ItemBlockedDurationRow is one item's daily blocked-duration snapshot. Every item that contributes state time to the
// day has a row, including a zero duration.
type ItemBlockedDurationRow struct {
	Provider      string
	WorkScopeID   string
	TeamID        string
	TeamName      string
	WorkItemID    string
	DurationHours float64
}

// SegmentStatuses ports _segment_statuses (compute_work_item_state_durations.py:56-105) byte-for-byte.
//
// End of the final segment is completed_at if present, else computedAt (the Go port has no updated_at fallback tier
// because compute_work_item_state_durations_daily's caller never passes one either -- job_daily.py:1582 does not read
// updated_at for this family).
//
// createdAt/completedAt/computedAt must already be UTC; transitions are sorted here by occurred_at, matching Python's
// `sorted(transitions, key=...)`.
func SegmentStatuses(
	createdAt time.Time, completedAt *time.Time, itemStatus string, transitions []StateTransition, computedAt time.Time,
) []StatusSegment {
	endOfItem := computedAt
	if completedAt != nil {
		endOfItem = *completedAt
	}

	if len(transitions) == 0 {
		return nil
	}
	ordered := make([]StateTransition, len(transitions))
	copy(ordered, transitions)
	sort.SliceStable(ordered, func(i, j int) bool { return ordered[i].OccurredAt.Before(ordered[j].OccurredAt) })

	var segments []StatusSegment

	first := ordered[0]
	currentStatus := first.FromStatus
	if currentStatus == "" {
		currentStatus = itemStatus
	}
	currentStart := createdAt

	for _, transition := range ordered {
		transitionAt := transition.OccurredAt
		if !transitionAt.After(currentStart) {
			currentStatus = transition.ToStatus
			currentStart = transitionAt
			continue
		}
		segments = append(segments, StatusSegment{Status: currentStatus, Start: currentStart, End: transitionAt})
		currentStatus = transition.ToStatus
		currentStart = transitionAt
	}

	if endOfItem.After(currentStart) {
		segments = append(segments, StatusSegment{Status: currentStatus, Start: currentStart, End: endOfItem})
	}

	filtered := segments[:0]
	for _, segment := range segments {
		if segment.End.After(segment.Start) {
			filtered = append(filtered, segment)
		}
	}
	return filtered
}

// stateTotalKey mirrors the Python (provider, work_scope_id, team_id, status) aggregation key.
type stateTotalKey struct {
	provider    string
	workScopeID string
	teamID      string
	status      string
}

// ComputeStateDurationsDay ports compute_work_item_state_durations_daily's main body for one day's already-loaded
// items, transitions and primary attributions: the aggregate rows, one blocked-duration snapshot for each item that
// contributed to [start, end), and the number of items with no attribution row.
//
// Deterministic: items are processed in work_item_id order, so the output does not depend on the row order of the
// reads. An item with no transition is skipped (Python's `if not item_transitions: continue`). blocked holds, per item,
// the spans in which it has an open blocker (CHAOS-8493): the parts of a non-terminal segment inside one are
// "blocked" (OverlayBlocked); the hours of the item do not change.
func ComputeStateDurationsDay(
	start, end time.Time,
	items []StateItem,
	transitions []StateTransition,
	attributions map[string]PrimaryAttribution,
	computedAt time.Time,
	blocked map[string][]BlockedInterval,
) ([]StateDurationRow, []ItemBlockedDurationRow, int) {
	transitionsByItem := make(map[string][]StateTransition, len(transitions))
	for _, transition := range transitions {
		transitionsByItem[transition.WorkItemID] = append(transitionsByItem[transition.WorkItemID], transition)
	}

	sortedItems := make([]StateItem, len(items))
	copy(sortedItems, items)
	sort.SliceStable(sortedItems, func(i, j int) bool { return sortedItems[i].WorkItemID < sortedItems[j].WorkItemID })

	totals := make(map[stateTotalKey]float64)
	// keys is built alongside totals, from the same key values, so the key set is available without a later range
	// over totals.
	keys := make([]stateTotalKey, 0)
	keysSeen := make(map[stateTotalKey]struct{})
	itemsSeen := make(map[stateTotalKey]map[string]struct{})
	teamNameByKey := make(map[[3]string]string) // (provider, workScopeID, teamID) -> teamName
	itemRows := make([]ItemBlockedDurationRow, 0, len(sortedItems))
	missingAttribution := 0

	for _, item := range sortedItems {
		itemTransitions := transitionsByItem[item.WorkItemID]
		if len(itemTransitions) == 0 {
			continue
		}

		attribution, hasAttribution := attributions[item.WorkItemID]
		if !hasAttribution {
			missingAttribution++
		}
		teamID, teamName := NormalizeTeamID(&attribution.TeamID), NormalizeTeamName(&attribution.TeamName)
		workScopeID := WorkScopeID(item.Provider, item.ProjectKey, item.ProjectID, item.NativeTeamKey, item.ProjectName)
		teamNameByKey[[3]string{item.Provider, workScopeID, teamID}] = teamName

		segments := SegmentStatuses(item.CreatedAt, item.CompletedAt, item.Status, itemTransitions, computedAt)
		if len(segments) > 0 && len(blocked[item.WorkItemID]) > 0 {
			segments = OverlayBlocked(segments, blocked[item.WorkItemID])
		}
		blockedHours := 0.0
		contributed := false
		for _, segment := range segments {
			overlapStart := segment.Start
			if start.After(overlapStart) {
				overlapStart = start
			}
			overlapEnd := segment.End
			if end.Before(overlapEnd) {
				overlapEnd = end
			}
			if !overlapEnd.After(overlapStart) {
				continue
			}
			contributed = true
			hours := overlapEnd.Sub(overlapStart).Hours()
			if segment.Status == StatusBlocked {
				blockedHours += hours
			}
			key := stateTotalKey{provider: item.Provider, workScopeID: workScopeID, teamID: teamID, status: segment.Status}
			if _, ok := keysSeen[key]; !ok {
				keysSeen[key] = struct{}{}
				keys = append(keys, key)
			}
			totals[key] += hours
			seen := itemsSeen[key]
			if seen == nil {
				seen = make(map[string]struct{})
				itemsSeen[key] = seen
			}
			seen[item.WorkItemID] = struct{}{}
		}
		if contributed {
			itemRows = append(itemRows, ItemBlockedDurationRow{
				Provider:      item.Provider,
				WorkScopeID:   workScopeID,
				TeamID:        teamID,
				TeamName:      teamName,
				WorkItemID:    item.WorkItemID,
				DurationHours: blockedHours,
			})
		}
	}

	sort.Slice(keys, func(i, j int) bool {
		a, b := keys[i], keys[j]
		if a.provider != b.provider {
			return a.provider < b.provider
		}
		if a.workScopeID != b.workScopeID {
			return a.workScopeID < b.workScopeID
		}
		if a.teamID != b.teamID {
			return a.teamID < b.teamID
		}
		return a.status < b.status
	})

	rows := make([]StateDurationRow, 0, len(keys))
	for _, key := range keys {
		totalHours := totals[key]
		rows = append(rows, StateDurationRow{
			Provider:      key.provider,
			WorkScopeID:   key.workScopeID,
			TeamID:        key.teamID,
			TeamName:      teamNameByKey[[3]string{key.provider, key.workScopeID, key.teamID}],
			Status:        key.status,
			DurationHours: totalHours,
			// items_touched is the size of an in-memory set of distinct work-item IDs touched by this key on one day --
			// bounded by a day's work-item volume, never negative -- so it keeps a direct conversion.
			ItemsTouched: uint32(len(itemsSeen[key])),
			AvgWIP:       totalHours / 24.0,
		})
	}
	sort.Slice(itemRows, func(i, j int) bool {
		a, b := itemRows[i], itemRows[j]
		if a.Provider != b.Provider {
			return a.Provider < b.Provider
		}
		if a.WorkScopeID != b.WorkScopeID {
			return a.WorkScopeID < b.WorkScopeID
		}
		if a.TeamID != b.TeamID {
			return a.TeamID < b.TeamID
		}
		return a.WorkItemID < b.WorkItemID
	})

	return rows, itemRows, missingAttribution
}
