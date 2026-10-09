package daily

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/workitemblockers"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/workitemmetrics"
)

// unassignedTeamID/unassignedTeamName are package-local names for
// UNASSIGNED_TEAM_ID/UNASSIGNED_TEAM_NAME (src/dev_health_ops/providers/
// teams.py:33-34). They ALIAS the workitemmetrics constants rather than
// restating the literals, so there is exactly one definition of the sentinel
// that resolveWorkItemPrimaryTeam returns and the golden compares against.
const (
	unassignedTeamID   = workitemmetrics.UnassignedTeamID
	unassignedTeamName = workitemmetrics.UnassignedTeamName
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
// # One compute for each work scope
//
// work_item_state_durations_daily replaces rows by a key that has
// work_scope_id and no repo_id, so the family computes a work scope once,
// over the items of every repository of the organization, and writes it once
// for the partition: see work_item_scope_read.go.
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

	// The version is taken before the reads (see WorkItemExecutor).
	computedAt := executor.nowUTC()
	read, err := loadWorkItemScopeRead(ctx, executor.conn, "work_item_state", run, partition,
		workItemPartitionScope{day: day, start: start, end: end, repoIDs: repoIDs}, true, false)
	if err != nil {
		return 0, err
	}
	// With no item, or no transition for any item, every item is skipped
	// (Python's `if not item_transitions: continue`), so there is nothing to
	// read the blocked spans for.
	//
	// The stale-key rule still applies (stale_team_keys.go): a key of the
	// read's work scopes that held a duration and that this compute does not
	// produce gets a row of zeros.
	supersede := func(total int, rows []workItemStateDailyRow) (int, error) {
		produced := make([]staleKey, 0, len(rows))
		for _, row := range rows {
			produced = append(produced, staleKey{row.Provider, row.WorkScopeID, row.TeamID, row.Status})
		}
		written, err := supersedeStaleTeamKeys(
			ctx, executor.conn, staleKeysWorkItemStateDurationsDaily, run.OrganizationID, day,
			read.staleKeyScope(), produced, computedAt,
		)
		total += written
		if err != nil {
			return wrapWorkItemScopePartialWrite("work_item_state", total, partition, err)
		}
		return total, nil
	}
	if len(read.Items) == 0 || len(read.Transitions) == 0 {
		return supersede(0, nil)
	}

	// The blocked spans (CHAOS-8493) are organization-wide -- a blocker lives
	// in any repository, or in none. A failed read fails the partition.
	// Computing without it would write full-length rows for the statuses the
	// blocked hours belong to, and those rows would read as a complete answer.
	blocked, ended, err := workitemblockers.LoadBlockedIntervals(ctx, executor.conn, run.OrganizationID)
	if err != nil {
		return 0, err
	}
	// One line per partition, no id: how many relations the end rule closed,
	// by provider, and how many of them are the named case of a github issue
	// on a Projects v2 board.
	counts := ended.Counts()
	slog.Info(workitemmetrics.EndedRelationsLogMessage,
		"writer", "daily_family",
		"relations", counts.Relations,
		"ended", counts.Ended,
		"ended_github", counts.EndedGitHub,
		"ended_gitlab", counts.EndedGitLab,
		"ended_jira", counts.EndedJira,
		"ended_linear", counts.EndedLinear,
		"ended_other", counts.EndedOther,
		"github_board_candidates", counts.GitHubBoardCandidates,
	)

	rows, itemRows, missingAttribution := computeWorkItemStateDurationRowsForRepo(
		day, start, end, read.stateItems(), read.Transitions, read.Attributions, computedAt, blocked,
	)

	// CHAOS-4278: observe as soon as the count is known, BEFORE the write.
	// missingAttribution describes the INPUT, independent of whether the
	// write succeeds; observing only after a successful write would
	// undercount on every write failure. 0 is a valid observation. A nil
	// observer is a no-op, never a failure.
	if executor.missingAttributionObserver != nil {
		_ = executor.missingAttributionObserver.ObserveWorkItemStateMissingAttribution(missingAttribution)
	}
	if len(rows) == 0 {
		return supersede(0, nil)
	}

	// Each writer reports its true row count on an ambiguous Send error, so
	// the count is added before the error check.
	total, err := WriteWorkItemStateDurationsDaily(ctx, executor.conn, run.OrganizationID, day, rows, computedAt)
	if err != nil {
		return wrapWorkItemScopePartialWrite("work_item_state", total, partition, err)
	}
	written, err := WriteWorkItemBlockedDurationsDaily(ctx, executor.conn, run.OrganizationID, day, itemRows, computedAt)
	total += written
	if err != nil {
		return wrapWorkItemScopePartialWrite("work_item_state", total, partition, err)
	}
	return supersede(total, rows)
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

// overlayWorkItemStateBlocked applies workitemmetrics.OverlayBlocked to this
// family's own segment type. With no interval the segments are returned as
// they are, so an item with no open blocker is computed exactly as before.
func overlayWorkItemStateBlocked(
	segments []workItemStateSegment, intervals []workitemmetrics.BlockedInterval,
) []workItemStateSegment {
	if len(segments) == 0 || len(intervals) == 0 {
		return segments
	}
	shared := make([]workitemmetrics.StatusSegment, 0, len(segments))
	for _, segment := range segments {
		shared = append(shared, workitemmetrics.StatusSegment{Status: segment.status, Start: segment.start, End: segment.end})
	}
	overlaid := workitemmetrics.OverlayBlocked(shared, intervals)
	result := make([]workItemStateSegment, 0, len(overlaid))
	for _, segment := range overlaid {
		result = append(result, workItemStateSegment{status: segment.Status, start: segment.Start, end: segment.End})
	}
	return result
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
	blocked map[string][]workitemmetrics.BlockedInterval,
) ([]workItemStateDailyRow, int) {
	rows, _, missingAttribution := computeWorkItemStateDurationRowsForRepo(
		day, start, end, items, transitions, attributions, computedAt, blocked,
	)
	return rows, missingAttribution
}

// computeWorkItemStateDurationRowsForRepo produces the established aggregate
// rows plus one blocked-duration snapshot for each item that contributed to the
// target day. A zero is deliberate: it supersedes an earlier positive snapshot
// when a recompute finds that the item no longer has blocked time.
func computeWorkItemStateDurationRowsForRepo(
	day, start, end time.Time,
	items []workItemStateWorkItem,
	transitions []workItemStateTransition,
	attributions map[string]workItemPrimaryAttribution,
	computedAt time.Time,
	blocked map[string][]workitemmetrics.BlockedInterval,
) ([]workItemStateDailyRow, []workItemBlockedDurationDailyRow, int) {
	transitionsByItem := make(map[string][]workItemStateTransition, len(transitions))
	for _, transition := range transitions {
		transitionsByItem[transition.WorkItemID] = append(transitionsByItem[transition.WorkItemID], transition)
	}

	sortedItems := make([]workItemStateWorkItem, len(items))
	copy(sortedItems, items)
	sort.SliceStable(sortedItems, func(i, j int) bool { return sortedItems[i].WorkItemID < sortedItems[j].WorkItemID })

	totals := make(map[workItemStateTotalKey]float64)
	// keys is built alongside totals, from the same key values, so the key
	// set is available without a later range over totals -- a map that a
	// zero-transition input leaves empty is never ranged over.
	keys := make([]workItemStateTotalKey, 0)
	keysSeen := make(map[workItemStateTotalKey]struct{})
	itemsSeen := make(map[workItemStateTotalKey]map[string]struct{})
	teamNameByKey := make(map[[3]string]string) // (provider, workScopeID, teamID) -> teamName
	itemRows := make([]workItemBlockedDurationDailyRow, 0, len(sortedItems))
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

		segments := segmentWorkItemStatuses(item.CreatedAt, item.CompletedAt, item.Status, itemTransitions, computedAt)
		// CHAOS-8493: the parts of a non-terminal segment in which the item
		// has an open blocker are "blocked". The hours of the item do not
		// change; see workitemmetrics.OverlayBlocked.
		segments = overlayWorkItemStateBlocked(segments, blocked[item.WorkItemID])
		blockedHours := 0.0
		contributed := false
		for _, segment := range segments {
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
			contributed = true
			hours := overlapEnd.Sub(overlapStart).Hours()
			if segment.status == workitemmetrics.StatusBlocked {
				blockedHours += hours
			}
			key := workItemStateTotalKey{provider: item.Provider, workScopeID: workScopeID, teamID: teamID, status: segment.status}
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
			itemRows = append(itemRows, workItemBlockedDurationDailyRow{
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
			// items_touched is the size of an in-memory set of distinct
			// work-item IDs touched by this (provider, work scope, team,
			// status) key on one day -- bounded by a day's work-item
			// volume, never negative -- so it keeps a direct conversion.
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

// resolveWorkItemPrimaryTeam applies normalize_team_id/normalize_team_name
// (src/dev_health_ops/providers/teams.py:37-48) to a
// LoadWorkItemPrimaryTeamAttributions lookup miss or an empty team_id/name.
//
// It DELEGATES to workitemmetrics rather than reimplementing the rule. The
// earlier body here tested only `== ""`, which is half of Python: Python is
// `if not team_id or not team_id.strip()` and then returns `team_id.strip()`,
// so it also maps a whitespace-only value to unassigned and STRIPS a padded
// one. Keeping a second, weaker copy of a normalizer beside the correct one is
// how the two drift; team_id is a grouping and sorting key, so a drift here
// splits one team's rows across two key values.
func resolveWorkItemPrimaryTeam(attribution workItemPrimaryAttribution) (teamID, teamName string) {
	return workitemmetrics.NormalizeTeamID(&attribution.TeamID),
		workitemmetrics.NormalizeTeamName(&attribution.TeamName)
}

var _ NativeFamilyExecutor = (*WorkItemStateExecutor)(nil)
