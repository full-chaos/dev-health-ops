package daily

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
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
	computed, err := computeWorkItemStateRows(ctx, executor.conn, run, partition,
		workItemPartitionScope{day: day, start: start, end: end, repoIDs: repoIDs}, computedAt)
	if err != nil {
		return 0, err
	}
	// With no item, or no transition for any item, every item is skipped
	// (Python's `if not item_transitions: continue`), so nothing was computed.
	if !computed.ran {
		return 0, nil
	}
	// One line per partition, no id: how many relations the end rule closed,
	// by provider, and how many of them are the named case of a github issue
	// on a Projects v2 board.
	counts := computed.ended.Counts()
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
	rows, itemRows, missingAttribution := computed.rows, computed.itemRows, computed.missingAttribution

	// CHAOS-4278: observe as soon as the count is known, BEFORE the write.
	// missingAttribution describes the INPUT, independent of whether the
	// write succeeds; observing only after a successful write would
	// undercount on every write failure. 0 is a valid observation. A nil
	// observer is a no-op, never a failure.
	if executor.missingAttributionObserver != nil {
		_ = executor.missingAttributionObserver.ObserveWorkItemStateMissingAttribution(missingAttribution)
	}
	if len(rows) == 0 {
		return 0, nil
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
	return total, nil
}

// workItemStateComputed is what computeWorkItemStateRows returns.
type workItemStateComputed struct {
	// ran is false when the read has no item or no transition: nothing was
	// computed and the blocked spans were not read.
	ran                bool
	read               workItemScopeRead
	rows               []workItemStateDailyRow
	itemRows           []workItemBlockedDurationDailyRow
	missingAttribution int
	ended              workitemmetrics.EndedRelationStats
}

// computeWorkItemStateRows reads the work scopes of the partition's
// repositories and the blocked spans, and computes the state-duration rows of
// the day. It writes nothing, logs nothing and observes nothing (see
// computeWorkItemTriplet for its two callers).
//
// The blocked spans (CHAOS-8493) are organization-wide -- a blocker lives in
// any repository, or in none. A failed read is returned. Computing without it
// would give full-length rows for the statuses the blocked hours belong to,
// and those rows would read as a complete answer.
func computeWorkItemStateRows(
	ctx context.Context, conn driver.Conn, run Run, partition Partition, scope workItemPartitionScope, computedAt time.Time,
) (workItemStateComputed, error) {
	read, err := loadWorkItemScopeRead(ctx, conn, "work_item_state", run, partition, scope, true, false, false)
	if err != nil {
		return workItemStateComputed{}, err
	}
	computed := workItemStateComputed{read: read}
	if len(read.Items) == 0 || len(read.Transitions) == 0 {
		return computed, nil
	}
	blocked, ended, err := workitemblockers.LoadBlockedIntervals(ctx, conn, run.OrganizationID)
	if err != nil {
		return workItemStateComputed{}, err
	}
	computed.ran, computed.ended = true, ended
	computed.rows, computed.itemRows, computed.missingAttribution = computeWorkItemStateDurationRowsForRepo(
		scope.day, scope.start, scope.end, read.stateItems(), read.Transitions, read.Attributions, computedAt, blocked,
	)
	return computed, nil
}

// workItemStateSegment is one (status, start, end) span in a work item's
// status history, ported from _segment_statuses's return shape
// (compute_work_item_state_durations.py:56-105).
type workItemStateSegment struct {
	status string
	start  time.Time
	end    time.Time
}

// segmentWorkItemStatuses is workitemmetrics.SegmentStatuses in this
// family's own segment type: the one port of _segment_statuses, shared with
// the request-time recompute of a repository's linked items (CHAOS-13088).
func segmentWorkItemStatuses(
	createdAt time.Time, completedAt *time.Time, itemStatus string,
	transitions []workItemStateTransition, computedAt time.Time,
) []workItemStateSegment {
	shared := workitemmetrics.SegmentStatuses(createdAt, completedAt, itemStatus, transitions, computedAt)
	if shared == nil {
		return nil
	}
	segments := make([]workItemStateSegment, 0, len(shared))
	for _, segment := range shared {
		segments = append(segments, workItemStateSegment{status: segment.Status, start: segment.Start, end: segment.End})
	}
	return segments
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

// computeWorkItemStateDurationsForRepo ports
// compute_work_item_state_durations_daily's main body for one repo's
// already-loaded items/transitions/attributions (see
// computeWorkItemStateDurationRowsForRepo).
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
//
// The compute is workitemmetrics.ComputeStateDurationsDay: one definition of
// the day's state durations for the daily job and for the request-time
// recompute of a repository's linked items (CHAOS-13088).
func computeWorkItemStateDurationRowsForRepo(
	_, start, end time.Time,
	items []workItemStateWorkItem,
	transitions []workItemStateTransition,
	attributions map[string]workItemPrimaryAttribution,
	computedAt time.Time,
	blocked map[string][]workitemmetrics.BlockedInterval,
) ([]workItemStateDailyRow, []workItemBlockedDurationDailyRow, int) {
	stateItems := make([]workitemmetrics.StateItem, 0, len(items))
	for _, item := range items {
		stateItems = append(stateItems, workitemmetrics.StateItem(item))
	}
	return workitemmetrics.ComputeStateDurationsDay(start, end, stateItems, transitions, attributions, computedAt, blocked)
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
