package daily

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
)

// workItemStateWorkItem is the narrow subset of the `work_items` ClickHouse
// table (src/dev_health_ops/migrations/clickhouse/009_raw_work_items.sql +
// 050_work_items_native_team_key_project_name.sql) that
// compute_work_item_state_durations_daily reads. Field names mirror
// WorkItem (src/dev_health_ops/models/work_items.py) so callers porting the
// Python logic can match fields 1:1.
type workItemStateWorkItem struct {
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

// workScopeID ports WorkItem.work_scope_id
// (src/dev_health_ops/models/work_items.py:88-119) byte-for-byte: jira uses
// project_key when present; every other provider prefers project_id, then
// project_name, then native_team_key (a team-only Linear issue keeps a
// non-empty scope key), then falls back to project_key too (covers a
// non-jira item whose project_key is set but the three fields above are
// all empty). May return "".
func (item workItemStateWorkItem) workScopeID() string {
	if item.Provider == "jira" && item.ProjectKey != "" {
		return item.ProjectKey
	}
	if item.ProjectID != "" {
		return item.ProjectID
	}
	if item.ProjectName != "" {
		return item.ProjectName
	}
	if item.NativeTeamKey != "" {
		return item.NativeTeamKey
	}
	if item.ProjectKey != "" {
		return item.ProjectKey
	}
	return ""
}

// workItemStateTransition is the narrow subset of `work_item_transitions`
// (same migration) compute_work_item_state_durations_daily reads.
type workItemStateTransition struct {
	WorkItemID string
	OccurredAt time.Time
	FromStatus string // "" is a valid, meaningful value -- see _segment_statuses
	ToStatus   string
}

// LoadWorkItemStateWorkItems ports the item half of ClickHouseMetricsLoader.
// load_work_items (src/dev_health_ops/metrics/loaders/clickhouse.py:454) --
// SAME predicate, scoped to one partition's (org, repo, day-window): a work
// item is included when it was created before the window ends AND it is
// either not-yet-done or completed no earlier than the window's start (so a
// long-lived open item and an item completed inside this window are both
// read; an item that finished before the window started is excluded, exactly
// like the Python filter). `work_items` is ReplacingMergeTree(last_synced)
// keyed on (repo_id, work_item_id) -- FINAL is a complete, correct dedup here
// (WORK_ITEMS_DEDUPED = "work_items FINAL",
// sinks/clickhouse/idempotency.py:5), unlike the transitions table below.
func LoadWorkItemStateWorkItems(
	ctx context.Context, conn repositoryRows, organizationID string, repoID uuid.UUID, start, end time.Time,
) ([]workItemStateWorkItem, error) {
	if conn == nil || strings.TrimSpace(organizationID) == "" || !start.Before(end) {
		return nil, ErrInvalidState
	}
	rows, err := conn.Query(ctx, `
SELECT work_item_id, provider, status, project_key, project_id, native_team_key, project_name, created_at, completed_at
FROM work_items FINAL
WHERE org_id = ? AND repo_id = ?
  AND created_at < ?
  AND (status != 'done' OR completed_at >= ?)`,
		organizationID, repoID.String(), end.UTC(), start.UTC(),
	)
	if err != nil {
		return nil, fmt.Errorf("load work_item_state work items: %w", err)
	}
	defer rows.Close()

	var items []workItemStateWorkItem
	for rows.Next() {
		var (
			item        workItemStateWorkItem
			completedAt *time.Time
		)
		if err := rows.Scan(
			&item.WorkItemID, &item.Provider, &item.Status, &item.ProjectKey, &item.ProjectID,
			&item.NativeTeamKey, &item.ProjectName, &item.CreatedAt, &completedAt,
		); err != nil {
			return nil, fmt.Errorf("scan work_item_state work item: %w", err)
		}
		item.CompletedAt = completedAt
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate work_item_state work items: %w", err)
	}
	return items, nil
}

// LoadWorkItemStateTransitions ports the transition half of load_work_items,
// including its SEMANTIC dedup contract
// (WORK_ITEM_TRANSITIONS_DEDUPED = semantic_deduped_subquery("work_item_transitions",
// WORK_ITEM_TRANSITION_SEMANTIC_COLUMNS), sinks/clickhouse/idempotency.py:60-63).
// `work_item_transitions` is ReplacingMergeTree(last_synced) ORDER BY
// (repo_id, work_item_id, occurred_at) -- that ORDER BY key does NOT include
// from_status/to_status/actor, so a plain FINAL would silently collapse two
// genuinely DIFFERENT transitions that happen to share one occurred_at down
// to whichever has the larger last_synced, losing a real transition. The
// Python semantic-dedup subquery instead GROUPs BY every semantic column
// (org_id, repo_id, work_item_id, occurred_at, provider, from_status,
// to_status, from_status_raw, to_status_raw, actor) and keeps
// max(last_synced) -- collapsing only re-synced COPIES of the identical
// event, never two distinct same-instant transitions. This query reproduces
// that GROUP BY exactly (only the columns this family reads are selected;
// from_status_raw/to_status_raw/actor still participate in the GROUP BY so
// the semantic identity matches Python's, even though their values are
// never read here).
func LoadWorkItemStateTransitions(
	ctx context.Context, conn repositoryRows, organizationID string, repoID uuid.UUID, end time.Time,
) ([]workItemStateTransition, error) {
	if conn == nil || strings.TrimSpace(organizationID) == "" {
		return nil, ErrInvalidState
	}
	rows, err := conn.Query(ctx, `
SELECT work_item_id, occurred_at, from_status, to_status
FROM (
	SELECT
		org_id, repo_id, work_item_id, occurred_at, provider,
		from_status, to_status, from_status_raw, to_status_raw, actor,
		max(last_synced) AS last_synced
	FROM work_item_transitions
	GROUP BY org_id, repo_id, work_item_id, occurred_at, provider,
		from_status, to_status, from_status_raw, to_status_raw, actor
)
WHERE org_id = ? AND repo_id = ? AND occurred_at < ?`,
		organizationID, repoID.String(), end.UTC(),
	)
	if err != nil {
		return nil, fmt.Errorf("load work_item_state transitions: %w", err)
	}
	defer rows.Close()

	var transitions []workItemStateTransition
	for rows.Next() {
		var transition workItemStateTransition
		if err := rows.Scan(&transition.WorkItemID, &transition.OccurredAt, &transition.FromStatus, &transition.ToStatus); err != nil {
			return nil, fmt.Errorf("scan work_item_state transition: %w", err)
		}
		transitions = append(transitions, transition)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate work_item_state transitions: %w", err)
	}
	return transitions, nil
}

// workItemPrimaryAttribution is one work item's winning
// (is_primary=1) row from `work_item_team_attributions`.
type workItemPrimaryAttribution struct {
	TeamID   string // "" when the primary row's team_id is NULL (e.g. source=unassigned)
	TeamName string
}

// LoadWorkItemPrimaryTeamAttributions reads the primary (is_primary=1) row
// per work item from `work_item_team_attributions` for one repo, INSTEAD OF
// recomputing the 9-source resolve_team_attribution cascade
// (compute_work_items.py:507) in Go.
//
// # Why a read, not a recompute (CHAOS-4278)
//
// Team attribution is sync-derived by rule -- the admin override is the
// only exception (docs/contribute/architecture/team-attribution.md §0) --
// and `work_item_team_attributions` is already the materialized output of
// that exact cascade, written every partition by
// compute_work_item_team_attributions (compute_work_items.py:1498), which
// is called from job_daily.py:1559 with the SAME team_resolver /
// project_key_resolver / linked_issue_resolver / attribution_context this
// family's own team resolution (compute_work_item_state_durations.py's
// `_resolve_team`) would otherwise need. Recomputing the cascade a second
// time in Go would duplicate ~1000 lines of admin-override/provider-layer/
// linked-issue-donor-walk logic with no independent value: both computations
// read the identical inputs and would either agree (redundant) or disagree
// (a bug in the duplicate, not evidence of anything). Reading the row
// instead makes `work_item_team_attributions` the single source of truth,
// matching how the Go read side already consumes it elsewhere
// (`build_unit_team_subquery`).
//
// Measured equivalence (local, org 70d529e0, 2026-09-01, one harness, both
// FINAL): work_item_cycle_times.team_id (written by the SAME
// resolve_team_attribution call, same job run, same normalize_team_id/
// normalize_team_name wrapper compute_work_item_state_durations.py's own
// _resolve_team applies) vs this table's is_primary=1 row --
// 528/528 match over the trailing 14 days, 1903/1903 over 90 days, 0
// mismatches, 0 missing. See CHAOS-4278 Linear comment (2026-09-01) for the
// full breakdown and its coverage caveat: this org's real data only ever
// resolves via 2 of the cascade's 9 sources (native_team, issue_project);
// the other 7 are equivalent by construction (identical function, identical
// run) but not independently measured.
//
// `work_item_team_attributions` is ReplacingMergeTree(computed_at) ORDER BY
// (org_id, repo_id, work_item_id, ifNull(team_id,”), source) -- FINAL alone
// does not collapse to one row per work item (the key includes team_id and
// source, so a losing candidate and the winning one are DIFFERENT keys and
// both survive FINAL). Codex round-1 P1 finding (CHAOS-4278, 2026-09-01),
// verified by reading the canonical Python readers of this exact table
// (job_work_items.py:295-301 `_load_prior_primary_attributions`,
// native_status_change.py:107-116): a plain `is_primary = 1` filter is NOT
// enough either -- `compute_work_item_team_attributions` appends every
// candidate of one compute run and never deletes prior ones, so an OLDER
// run's is_primary=1 candidate (a different team_id, hence a different
// ORDER BY key) survives FINAL as a stale EXTRA is_primary row alongside the
// current run's. Every documented reader of this table therefore adds the
// SAME `(work_item_id, computed_at) IN (SELECT work_item_id, max(computed_at)
// ... GROUP BY work_item_id)` latest-snapshot fence on top of `is_primary=1`
// -- this query now does too, verbatim, rather than a narrower Go-specific
// re-derivation. Without it, LoadWorkItemPrimaryTeamAttributions's map
// build (which last-write-wins on ClickHouse's returned row order for a
// given work_item_id) could pick either the current or a stale candidate
// nondeterministically.
//
// A work item with no matching row here (not yet attributed this run, or
// never synced) is simply absent from the returned map -- the caller
// applies the same normalize_team_id/normalize_team_name defaults
// ("unassigned"/"Unassigned") Python applies to a nil resolver result.
//
// KNOWN REMAINING GAP (codex round-1 P1, not yet resolved -- see
// WorkItemStateExecutor's doc comment and CHAOS-4278's handoff): this fence
// guarantees "the latest snapshot THIS QUERY CAN SEE", not "today's
// snapshot". `cmd/dev-health-worker/daily.go`'s PartitionHandler.Work runs
// every registered NativeFamilyExecutor (this one included) BEFORE the
// Python compatibility bridge call that recomputes `work_item_attribution`
// for the SAME partition (daily.go: computeNativeFamilies then
// compatibility.ComputePartition) -- so for an item whose attribution is
// new or has changed since the LAST successful `work_item_attribution` run,
// this family reads a stale (or, for a brand-new item, ABSENT) snapshot,
// not the one Python is about to compute moments later for this same day.
func LoadWorkItemPrimaryTeamAttributions(
	ctx context.Context, conn repositoryRows, organizationID string, repoID uuid.UUID,
) (map[string]workItemPrimaryAttribution, error) {
	if conn == nil || strings.TrimSpace(organizationID) == "" {
		return nil, ErrInvalidState
	}
	rows, err := conn.Query(ctx, `
SELECT work_item_id, ifNull(team_id, ''), ifNull(team_name, '')
FROM work_item_team_attributions FINAL
WHERE org_id = ? AND repo_id = ? AND is_primary = 1
  AND (work_item_id, computed_at) IN (
      SELECT work_item_id, max(computed_at)
      FROM work_item_team_attributions
      WHERE org_id = ? AND repo_id = ?
      GROUP BY work_item_id)`,
		organizationID, repoID.String(), organizationID, repoID.String(),
	)
	if err != nil {
		return nil, fmt.Errorf("load work_item_state primary attributions: %w", err)
	}
	defer rows.Close()

	attributions := make(map[string]workItemPrimaryAttribution)
	for rows.Next() {
		var (
			workItemID string
			attr       workItemPrimaryAttribution
		)
		if err := rows.Scan(&workItemID, &attr.TeamID, &attr.TeamName); err != nil {
			return nil, fmt.Errorf("scan work_item_state primary attribution: %w", err)
		}
		attributions[workItemID] = attr
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate work_item_state primary attributions: %w", err)
	}
	return attributions, nil
}

// workItemStateDailyRow is one aggregated output row, matching
// WorkItemStateDurationDailyRecord (schemas.py:598-609).
type workItemStateDailyRow struct {
	Provider      string
	WorkScopeID   string
	TeamID        string
	TeamName      string
	Status        string
	DurationHours float64
	ItemsTouched  uint32
	AvgWIP        float64
}

// workItemStateBatchConn is the narrow write capability
// WriteWorkItemStateDurationsDaily needs.
type workItemStateBatchConn interface {
	PrepareBatch(context.Context, string, ...driver.PrepareBatchOption) (driver.Batch, error)
}

// WriteWorkItemStateDurationsDaily ports the write side of
// write_work_item_state_durations (sinks/clickhouse/work_graph.py:312) --
// same table, same column list/order. `work_item_state_durations_daily` is a
// plain MergeTree (no ReplacingMergeTree/dedup key), matching Python's
// existing insert-only, no-idempotency contract for this table exactly: a
// partition recompute duplicates rows here today (in both languages) rather
// than replacing them, which is out of this port's scope to fix.
func WriteWorkItemStateDurationsDaily(
	ctx context.Context, conn workItemStateBatchConn, organizationID string, day time.Time,
	rows []workItemStateDailyRow, computedAt time.Time,
) (int, error) {
	if len(rows) == 0 {
		return 0, nil
	}
	if conn == nil || strings.TrimSpace(organizationID) == "" {
		return 0, ErrInvalidState
	}
	batch, err := conn.PrepareBatch(ctx, `INSERT INTO work_item_state_durations_daily (
		day, provider, work_scope_id, team_id, team_name, status,
		duration_hours, items_touched, avg_wip, computed_at, org_id)`)
	if err != nil {
		return 0, fmt.Errorf("prepare work_item_state_durations_daily batch: %w", err)
	}
	dayValue := time.Date(day.UTC().Year(), day.UTC().Month(), day.UTC().Day(), 0, 0, 0, 0, time.UTC)
	computedAtUTC := computedAt.UTC()
	for _, row := range rows {
		if err := batch.Append(
			dayValue, row.Provider, row.WorkScopeID, row.TeamID, row.TeamName, row.Status,
			row.DurationHours, row.ItemsTouched, row.AvgWIP, computedAtUTC, organizationID,
		); err != nil {
			return 0, fmt.Errorf("append work_item_state_durations_daily row: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return 0, fmt.Errorf("send work_item_state_durations_daily batch: %w", err)
	}
	return len(rows), nil
}
