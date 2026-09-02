package daily

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
)

// unassignedTeamID/unassignedTeamName port UNASSIGNED_TEAM_ID/
// UNASSIGNED_TEAM_NAME (src/dev_health_ops/providers/teams.py:33-34).
const (
	unassignedTeamID   = "unassigned"
	unassignedTeamName = "Unassigned"
)

// WorkItemStateExecutor is the NATIVE implementation of the work_item_state
// metrics.daily family (CHAOS-4278) -- ports
// compute_work_item_state_durations_daily
// (src/dev_health_ops/metrics/compute_work_item_state_durations.py:108).
//
// # Team attribution: READ, not recompute (CHAOS-4278, team-lead ruling 2026-09-01)
//
// The Python function takes a full 9-source resolve_team_attribution
// cascade (team_resolver/project_key_resolver/linked_issue_resolver/
// attribution_context). That cascade is not ported to Go anywhere reusable
// (the only two Go implementations that exist -- the GitHub-only sync-time
// derivation context and the simpler team_repo_ownership arm set -- are
// both unexported and shaped for a different call site). Rather than port
// or partially-port it a third time, this executor reads the cascade's
// already-materialized output, `work_item_team_attributions.is_primary=1`,
// per work item -- see LoadWorkItemPrimaryTeamAttributions's doc comment for
// the measured-equivalence evidence this rests on. `work_item_attribution`
// (CHAOS-4283, still Python-bridged) keeps writing that table on the normal
// per-partition schedule; this executor only reads it.
//
// # Per-repo iteration (mirrors TeamWellbeingExecutor, CHAOS-4276)
//
// run_daily_metrics_job is invoked once PER repo_id by the compatibility
// bridge's fan-out loop (worker_metrics.py:1729 `for index, repo_id in
// enumerate(repo_ids)`), so every family it computes -- this one included --
// is scoped to one repo's rows per call. This executor mirrors that
// boundary explicitly: it loops the partition's repoIDs (in their own
// deterministic order) and calls the per-repo compute once per repo, exactly
// like computeWellbeingPerRepo.
type WorkItemStateExecutor struct {
	conn   driver.Conn
	nowUTC func() time.Time
	// missingAttributionObserver (CHAOS-4278) is optional -- set via
	// SetMissingAttributionObserver, mirroring
	// TeamWellbeingExecutor.SetRepoCountObserver's pattern. nil means no
	// observer wired -- ComputeFamily degrades to not recording, never
	// panics.
	missingAttributionObserver jobruntime.WorkItemStateMissingAttributionObserver
}

var errWorkItemStateUnavailable = errors.New("work_item_state native executor unavailable")

// NewWorkItemStateExecutor fails closed on a nil connection, matching every
// other native family executor's construction contract.
func NewWorkItemStateExecutor(conn driver.Conn) (*WorkItemStateExecutor, error) {
	if conn == nil {
		return nil, errWorkItemStateUnavailable
	}
	return &WorkItemStateExecutor{conn: conn, nowUTC: func() time.Time { return time.Now().UTC() }}, nil
}

// SetMissingAttributionObserver wires the optional CHAOS-4278
// missing-primary-attribution guard counter. Never required for
// construction: a nil observer (the default) simply means this deployment
// does not yet have the telemetry wired.
func (executor *WorkItemStateExecutor) SetMissingAttributionObserver(observer jobruntime.WorkItemStateMissingAttributionObserver) {
	if executor == nil {
		return
	}
	executor.missingAttributionObserver = observer
}

// ComputeFamily runs the work_item_state computation for one partition.
func (executor *WorkItemStateExecutor) ComputeFamily(
	ctx context.Context, run Run, partition Partition,
) (int, error) {
	if executor == nil || executor.conn == nil {
		return 0, errWorkItemStateUnavailable
	}
	if run.OrganizationID == "" || run.TargetDay.IsZero() {
		return 0, fmt.Errorf("%w: partition %s run has no organization or target day", ErrInvalidState, partition.ID)
	}

	repoIDs, err := parseRepositoryUUIDs(partition.RepoIDs)
	if err != nil {
		return 0, fmt.Errorf("%w: partition %s repo_ids: %v", ErrInvalidState, partition.ID, err)
	}

	day := time.Date(run.TargetDay.UTC().Year(), run.TargetDay.UTC().Month(), run.TargetDay.UTC().Day(), 0, 0, 0, 0, time.UTC)
	start := day
	end := start.Add(24 * time.Hour)

	total := 0
	for _, repoID := range repoIDs {
		items, err := LoadWorkItemStateWorkItems(ctx, executor.conn, run.OrganizationID, repoID, start, end)
		if err != nil {
			return total, err
		}
		if len(items) == 0 {
			continue
		}

		transitions, err := LoadWorkItemStateTransitions(ctx, executor.conn, run.OrganizationID, repoID, end)
		if err != nil {
			return total, err
		}
		if len(transitions) == 0 {
			// Mirrors Python's `if not item_transitions: continue` per
			// item -- with zero transitions for the whole repo, every
			// item is skipped, so there is nothing to load attribution
			// rows for either.
			continue
		}

		attributions, err := LoadWorkItemPrimaryTeamAttributions(ctx, executor.conn, run.OrganizationID, repoID)
		if err != nil {
			return total, err
		}

		// One honest, real-wall-clock timestamp per repo group -- see
		// WriteTeamMetricsDailyPerRepo's doc comment for why this
		// mirrors Python's real per-repo_id call cadence rather than
		// stamping the whole partition with one shared value.
		computedAt := executor.nowUTC()

		rows, missingAttribution := computeWorkItemStateDurationsForRepo(day, start, end, items, transitions, attributions, computedAt)

		// CHAOS-4278 (codex round-1 P2 finding): observe as soon as the
		// count is known, BEFORE attempting the write -- unlike
		// ObserveTeamMetricsDailyRepoCount (which describes properties of
		// rows that were actually written, so it must wait for the write to
		// durably land), missingAttribution describes something about the
		// INPUT this repo's items carried, independent of whether the
		// subsequent write succeeds. Observing only after a successful
		// write would silently undercount on every WriteWorkItemState
		// DurationsDaily failure -- exactly the gap that would make this
		// guard counter itself unreliable during an outage, the one time a
		// reader most needs to trust it. 0 is a valid, expected observation
		// (see ObserveWorkItemStateMissingAttribution's doc comment). A nil
		// observer (not yet wired) is a no-op, never a failure.
		if executor.missingAttributionObserver != nil {
			_ = executor.missingAttributionObserver.ObserveWorkItemStateMissingAttribution(missingAttribution)
		}

		if len(rows) == 0 {
			continue
		}
		written, err := WriteWorkItemStateDurationsDaily(ctx, executor.conn, run.OrganizationID, day, rows, computedAt)
		if err != nil {
			return total, err
		}
		total += written
	}
	return total, nil
}

// workItemStateSegment is one (status, start, end) span in a work item's
// status history, ported from _segment_statuses's return shape
// (compute_work_item_state_durations.py:56-105).
type workItemStateSegment struct {
	status string
	start  time.Time
	end    time.Time
}

// segmentWorkItemStatuses ports _segment_statuses byte-for-byte.
//
// End of the final segment is completed_at if present, else computedAt (the
// Go port has no updated_at fallback tier because
// compute_work_item_state_durations_daily's caller never passes one either
// -- job_daily.py:1582 does not read updated_at for this family; the
// docstring's three-tier fallback describes the function signature's
// generality, not this call site's actual behavior).
//
// created_at/completedAt/computedAt must already be UTC (both loaders and
// ComputeFamily guarantee this); transitions are sorted here by occurred_at,
// matching Python's `sorted(transitions, key=...)`.
func segmentWorkItemStatuses(
	createdAt time.Time, completedAt *time.Time, itemStatus string,
	transitions []workItemStateTransition, computedAt time.Time,
) []workItemStateSegment {
	endOfItem := computedAt
	if completedAt != nil {
		endOfItem = *completedAt
	}

	if len(transitions) == 0 {
		return nil
	}
	ordered := make([]workItemStateTransition, len(transitions))
	copy(ordered, transitions)
	sort.SliceStable(ordered, func(i, j int) bool { return ordered[i].OccurredAt.Before(ordered[j].OccurredAt) })

	var segments []workItemStateSegment

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
		segments = append(segments, workItemStateSegment{status: currentStatus, start: currentStart, end: transitionAt})
		currentStatus = transition.ToStatus
		currentStart = transitionAt
	}

	if endOfItem.After(currentStart) {
		segments = append(segments, workItemStateSegment{status: currentStatus, start: currentStart, end: endOfItem})
	}

	filtered := segments[:0]
	for _, segment := range segments {
		if segment.end.After(segment.start) {
			filtered = append(filtered, segment)
		}
	}
	return filtered
}

// workItemStateTotalKey mirrors the Python (provider, work_scope_id,
// team_id, status) aggregation key.
type workItemStateTotalKey struct {
	provider    string
	workScopeID string
	teamID      string
	status      string
}

// computeWorkItemStateDurationsForRepo ports
// compute_work_item_state_durations_daily's main body for one repo's
// already-loaded items/transitions/attributions. Deterministic: items are
// processed in work_item_id order (Python iterates whatever order its
// loader returned rows in, which is not itself guaranteed -- team_name is a
// property of team_id, not of iteration order, in every real case, so this
// ordering choice cannot change the OUTPUT, only makes it reproducible run
// to run regardless of ClickHouse row-return order, matching this package's
// established convention (see computeWellbeingPerRepo's doc comment)).
func computeWorkItemStateDurationsForRepo(
	day, start, end time.Time,
	items []workItemStateWorkItem,
	transitions []workItemStateTransition,
	attributions map[string]workItemPrimaryAttribution,
	computedAt time.Time,
) ([]workItemStateDailyRow, int) {
	transitionsByItem := make(map[string][]workItemStateTransition, len(transitions))
	for _, transition := range transitions {
		transitionsByItem[transition.WorkItemID] = append(transitionsByItem[transition.WorkItemID], transition)
	}

	sortedItems := make([]workItemStateWorkItem, len(items))
	copy(sortedItems, items)
	sort.SliceStable(sortedItems, func(i, j int) bool { return sortedItems[i].WorkItemID < sortedItems[j].WorkItemID })

	totals := make(map[workItemStateTotalKey]float64)
	itemsSeen := make(map[workItemStateTotalKey]map[string]struct{})
	teamNameByKey := make(map[[3]string]string) // (provider, workScopeID, teamID) -> teamName
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
		teamID, teamName := resolveWorkItemPrimaryTeam(attribution)
		workScopeID := item.workScopeID()
		teamNameByKey[[3]string{item.Provider, workScopeID, teamID}] = teamName

		for _, segment := range segmentWorkItemStatuses(item.CreatedAt, item.CompletedAt, item.Status, itemTransitions, computedAt) {
			overlapStart := segment.start
			if start.After(overlapStart) {
				overlapStart = start
			}
			overlapEnd := segment.end
			if end.Before(overlapEnd) {
				overlapEnd = end
			}
			if !overlapEnd.After(overlapStart) {
				continue
			}
			hours := overlapEnd.Sub(overlapStart).Hours()
			key := workItemStateTotalKey{provider: item.Provider, workScopeID: workScopeID, teamID: teamID, status: segment.status}
			totals[key] += hours
			seen := itemsSeen[key]
			if seen == nil {
				seen = make(map[string]struct{})
				itemsSeen[key] = seen
			}
			seen[item.WorkItemID] = struct{}{}
		}
	}

	keys := make([]workItemStateTotalKey, 0, len(totals))
	for key := range totals {
		keys = append(keys, key)
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

	rows := make([]workItemStateDailyRow, 0, len(keys))
	for _, key := range keys {
		totalHours := totals[key]
		rows = append(rows, workItemStateDailyRow{
			Provider:      key.provider,
			WorkScopeID:   key.workScopeID,
			TeamID:        key.teamID,
			TeamName:      teamNameByKey[[3]string{key.provider, key.workScopeID, key.teamID}],
			Status:        key.status,
			DurationHours: totalHours,
			ItemsTouched:  uint32(len(itemsSeen[key])),
			AvgWIP:        totalHours / 24.0,
		})
	}
	return rows, missingAttribution
}

// resolveWorkItemPrimaryTeam ports the normalize_team_id/normalize_team_name
// defaults (src/dev_health_ops/providers/teams.py:37-48) applied to a
// LoadWorkItemPrimaryTeamAttributions lookup miss or an empty team_id/name:
// None/empty -> "unassigned"/"Unassigned".
func resolveWorkItemPrimaryTeam(attribution workItemPrimaryAttribution) (teamID, teamName string) {
	teamID = attribution.TeamID
	if teamID == "" {
		teamID = unassignedTeamID
	}
	teamName = attribution.TeamName
	if teamName == "" {
		teamName = unassignedTeamName
	}
	return teamID, teamName
}

var _ NativeFamilyExecutor = (*WorkItemStateExecutor)(nil)
