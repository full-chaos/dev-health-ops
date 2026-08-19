package providersync

import (
	"context"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
)

// Concrete ClickHouse adapters for the six direct fact surfaces the Python
// composite work-item unit writes through metrics/sinks/clickhouse/work_graph.py.
// The seventh direct surface, ai_attribution, lives beside its own Python
// producer in github_work_items_ai_attribution_effects_clickhouse.go because it
// is the only partitioned destination and the only one with a JSON-encoded
// column.
//
// Every column list below is the Python sink's own column list, in the Python
// sink's own order, including its omissions. Under D16 an omission is behavior:
// `work_items` never writes description/priority_raw/service_class/due_at and
// `work_item_transitions` never writes provider, even though all five columns
// exist on the tables. Writing them here would diverge from the running system
// on a whole-row ReplacingMergeTree replacement.

// workItemReadbackVerdict folds a readback row count into a comparator answer.
//
// HONEST SCOPE: the `> 1` arm is defense-in-depth, NOT the duplicate protection.
// Every readback here fences the destination's full sorting key and then
// collapses (FINAL for the six unpartitioned tables, LIMIT 1 BY for
// ai_attribution), so more than one row cannot come back and this arm is
// unreachable in all seven adapters. It is kept because a future change to a
// WHERE clause or a collapse strategy would make it reachable again, and
// answering Absent there would rewrite forever -- the rewrite adds a row and
// cannot remove the duplicate that produced the verdict. Conflict is the only
// answer that terminates.
//
// The real same-key protection is dedupeBySortingKey, which collapses
// collisions inside the batch BEFORE they reach the write or the expected set.
// That is what a duplicate actually hits.
//
// The arms below are mutually exclusive, so their ORDER is not load-bearing:
// exactly one of {>1, <1, ==1} holds for every integer, making a swap
// equivalent over the whole input domain.
//
// The boolean reports whether the verdict is final; false means "exactly one
// row was found, the caller must now compare it".
func workItemReadbackVerdict(found int) (EffectInspection, bool) {
	switch {
	case found > 1:
		return EffectConflict, true
	case found < 1:
		return EffectAbsent, true
	default:
		return EffectExact, false
	}
}

// workItemVersionVerdict compares the ReplacingMergeTree version column before
// any field comparison. An older stored row means our write has not landed yet
// (replay); a newer one means somebody else owns the key (ambiguous).
func workItemVersionVerdict(actual, expected time.Time) (EffectInspection, bool) {
	switch {
	case actual.IsZero():
		return EffectAbsent, true
	case actual.UTC().Before(expected.UTC()):
		return EffectAbsent, true
	case actual.UTC().After(expected.UTC()):
		return EffectConflict, true
	default:
		return EffectExact, false
	}
}

// emptyEffectInspection mirrors the Python sink's `if not rows: return`. A
// destination that produced no rows was still evaluated; it has nothing to read
// back, so it reports Absent and the committer treats it as nothing to commit.
func emptyEffectInspection() EffectInspection { return EffectAbsent }

func workItemAdapterGuard(
	ctx context.Context,
	identity GitHubWorkItemEffectIdentity,
	effect EffectBatch,
	destination string,
	conn driver.Conn,
	rowCount int,
) error {
	if ctx == nil || identity.Destination != destination ||
		effect.Destination != destination || identity.OrgID == "" ||
		identity.RowCount != len(effect.Rows) {
		return ErrInvalidConfiguration
	}
	if rowCount > 0 && conn == nil {
		return ErrInvalidConfiguration
	}
	return nil
}

// -----------------------------------------------------------------------------
// work_items
// -----------------------------------------------------------------------------

// GitHubWorkItemsClickHouseAdapter mirrors write_work_items
// (metrics/sinks/clickhouse/work_graph.py:656).
type GitHubWorkItemsClickHouseAdapter struct{ Conn driver.Conn }

const gitHubWorkItemsInsert = `INSERT INTO work_items (repo_id, work_item_id, provider, title, type, status, status_raw, project_key, project_id, native_team_key, project_name, assignees, reporter, created_at, updated_at, started_at, completed_at, closed_at, labels, story_points, sprint_id, sprint_name, parent_id, epic_id, url, last_synced, org_id, source_id)`

const gitHubWorkItemsSelect = `SELECT repo_id, work_item_id, provider, title, type, status, status_raw, project_key, project_id, native_team_key, project_name, assignees, reporter, created_at, updated_at, started_at, completed_at, closed_at, labels, story_points, sprint_id, sprint_name, parent_id, epic_id, url, last_synced, org_id, source_id FROM work_items FINAL WHERE org_id = ? AND repo_id = ? AND work_item_id = ?`

// workItemStoredRow is the stored projection of the columns this unit owns. It
// is deliberately not githubWorkItemRow: the semantic row carries fields this
// unit never writes, and comparing those would assert ownership Python does not
// take.
type workItemStoredRow struct {
	RepoID        uuid.UUID
	WorkItemID    string
	Provider      string
	Title         string
	Type          string
	Status        string
	StatusRaw     string
	ProjectKey    string
	ProjectID     string
	NativeTeamKey string
	ProjectName   string
	Assignees     []string
	Reporter      string
	CreatedAt     time.Time
	UpdatedAt     time.Time
	StartedAt     *time.Time
	CompletedAt   *time.Time
	ClosedAt      *time.Time
	Labels        []string
	StoryPoints   *float64
	SprintID      string
	SprintName    string
	ParentID      string
	EpicID        string
	URL           string
	LastSynced    time.Time
	OrgID         string
	SourceID      *uuid.UUID
}

// projectWorkItem applies exactly the coercions write_work_items applies.
func projectWorkItem(row githubWorkItemRow) workItemStoredRow {
	repoID := uuid.Nil
	if row.RepoID != nil {
		repoID = *row.RepoID
	}
	assignees, labels := row.Assignees, row.Labels
	if assignees == nil {
		assignees = []string{}
	}
	if labels == nil {
		labels = []string{}
	}
	return workItemStoredRow{
		RepoID: repoID, WorkItemID: row.WorkItemID, Provider: row.Provider,
		Title: row.Title, Type: row.Type, Status: row.Status,
		StatusRaw:     derefString(row.StatusRaw),
		ProjectKey:    derefString(row.ProjectKey),
		ProjectID:     derefString(row.ProjectID),
		NativeTeamKey: derefString(row.NativeTeamKey),
		ProjectName:   derefString(row.ProjectName),
		Assignees:     assignees, Reporter: derefString(row.Reporter),
		CreatedAt: clickHouseMillis(row.CreatedAt), UpdatedAt: clickHouseMillis(row.UpdatedAt),
		StartedAt: utcPointer(row.StartedAt), CompletedAt: utcPointer(row.CompletedAt),
		ClosedAt: utcPointer(row.ClosedAt), Labels: labels,
		StoryPoints: row.StoryPoints, SprintID: derefString(row.SprintID),
		SprintName: derefString(row.SprintName), ParentID: derefString(row.ParentID),
		EpicID: derefString(row.EpicID), URL: derefString(row.URL),
		LastSynced: clickHouseMillis(row.LastSynced), OrgID: row.OrgID,
		// source_id is NULL for native sync; only the external-ingest sink
		// stamps it (CHAOS-2698 D1), and this unit is never that writer.
		SourceID: nil,
	}
}

func (adapter GitHubWorkItemsClickHouseAdapter) WriteGitHubWorkItemEffect(
	ctx context.Context,
	identity GitHubWorkItemEffectIdentity,
	effect EffectBatch,
) error {
	rows, err := decodeEffectRows[githubWorkItemRow](effect)
	if err != nil {
		return err
	}
	if err := workItemAdapterGuard(
		ctx, identity, effect, "work_items", adapter.Conn, len(rows),
	); err != nil {
		return err
	}
	for _, row := range rows {
		if row.OrgID != identity.OrgID {
			return ErrInvalidConfiguration
		}
	}
	rows = dedupeBySortingKey(rows, workItemSortingKey)
	if len(rows) == 0 {
		return nil
	}
	batch, err := adapter.Conn.PrepareBatch(ctx, gitHubWorkItemsInsert)
	if err != nil {
		return err
	}
	defer batch.Abort()
	for _, row := range rows {
		if err := batch.Append(projectWorkItem(row).values()...); err != nil {
			return err
		}
	}
	return batch.Send()
}

func (adapter GitHubWorkItemsClickHouseAdapter) InspectGitHubWorkItemEffect(
	ctx context.Context,
	identity GitHubWorkItemEffectIdentity,
	effect EffectBatch,
) (EffectInspection, error) {
	rows, err := decodeEffectRows[githubWorkItemRow](effect)
	if err != nil {
		return EffectConflict, err
	}
	if err := workItemAdapterGuard(
		ctx, identity, effect, "work_items", adapter.Conn, len(rows),
	); err != nil {
		return EffectConflict, err
	}
	for _, row := range rows {
		if row.OrgID != identity.OrgID {
			return EffectConflict, ErrInvalidConfiguration
		}
	}
	rows = dedupeBySortingKey(rows, workItemSortingKey)
	if len(rows) == 0 {
		return emptyEffectInspection(), nil
	}
	return foldWorkItemInspections(rows, func(row githubWorkItemRow) (EffectInspection, error) {
		expected := projectWorkItem(row)
		result, err := adapter.Conn.Query(
			ctx, gitHubWorkItemsSelect,
			expected.OrgID, expected.RepoID, expected.WorkItemID,
		)
		if err != nil {
			return EffectConflict, err
		}
		defer result.Close()
		var actual workItemStoredRow
		found := 0
		for result.Next() {
			if err := result.Scan(
				&actual.RepoID, &actual.WorkItemID, &actual.Provider, &actual.Title,
				&actual.Type, &actual.Status, &actual.StatusRaw, &actual.ProjectKey,
				&actual.ProjectID, &actual.NativeTeamKey, &actual.ProjectName,
				&actual.Assignees, &actual.Reporter, &actual.CreatedAt,
				&actual.UpdatedAt, &actual.StartedAt, &actual.CompletedAt,
				&actual.ClosedAt, &actual.Labels, &actual.StoryPoints,
				&actual.SprintID, &actual.SprintName, &actual.ParentID,
				&actual.EpicID, &actual.URL, &actual.LastSynced, &actual.OrgID,
				&actual.SourceID,
			); err != nil {
				return EffectConflict, err
			}
			found++
		}
		if err := result.Err(); err != nil {
			return EffectConflict, err
		}
		if verdict, final := workItemReadbackVerdict(found); final {
			return verdict, nil
		}
		if verdict, final := workItemVersionVerdict(actual.LastSynced, expected.LastSynced); final {
			return verdict, nil
		}
		return workItemStoredRowsEqual(expected, actual), nil
	})
}

func workItemStoredRowsEqual(expected, actual workItemStoredRow) EffectInspection {
	if expected.RepoID != actual.RepoID ||
		expected.WorkItemID != actual.WorkItemID ||
		expected.Provider != actual.Provider || expected.Title != actual.Title ||
		expected.Type != actual.Type || expected.Status != actual.Status ||
		expected.StatusRaw != actual.StatusRaw ||
		expected.ProjectKey != actual.ProjectKey ||
		expected.ProjectID != actual.ProjectID ||
		expected.NativeTeamKey != actual.NativeTeamKey ||
		expected.ProjectName != actual.ProjectName ||
		!stringSlicesEqual(expected.Assignees, actual.Assignees) ||
		expected.Reporter != actual.Reporter ||
		!expected.CreatedAt.Equal(actual.CreatedAt) ||
		!expected.UpdatedAt.Equal(actual.UpdatedAt) ||
		!timePointersEqual(expected.StartedAt, actual.StartedAt) ||
		!timePointersEqual(expected.CompletedAt, actual.CompletedAt) ||
		!timePointersEqual(expected.ClosedAt, actual.ClosedAt) ||
		!stringSlicesEqual(expected.Labels, actual.Labels) ||
		!float64PointersEqual(expected.StoryPoints, actual.StoryPoints) ||
		expected.SprintID != actual.SprintID ||
		expected.SprintName != actual.SprintName ||
		expected.ParentID != actual.ParentID || expected.EpicID != actual.EpicID ||
		expected.URL != actual.URL || expected.OrgID != actual.OrgID ||
		!uuidPointersEqual(expected.SourceID, actual.SourceID) {
		return EffectConflict
	}
	return EffectExact
}

// -----------------------------------------------------------------------------
// work_item_transitions
// -----------------------------------------------------------------------------

// GitHubWorkItemTransitionsClickHouseAdapter mirrors write_work_item_transitions
// (metrics/sinks/clickhouse/work_graph.py:744). Note the absent `provider`
// column: the Python sink builds no provider value for this table, so the stored
// row carries the column default.
type GitHubWorkItemTransitionsClickHouseAdapter struct{ Conn driver.Conn }

const gitHubWorkItemTransitionsInsert = `INSERT INTO work_item_transitions (repo_id, work_item_id, occurred_at, from_status, to_status, from_status_raw, to_status_raw, actor, last_synced, org_id, source_id)`

const gitHubWorkItemTransitionsSelect = `SELECT repo_id, work_item_id, occurred_at, from_status, to_status, from_status_raw, to_status_raw, actor, last_synced, org_id, source_id FROM work_item_transitions FINAL WHERE org_id = ? AND repo_id = ? AND work_item_id = ? AND occurred_at = toDateTime64(?, 3, 'UTC')`

type workItemTransitionStoredRow struct {
	RepoID        uuid.UUID
	WorkItemID    string
	OccurredAt    time.Time
	FromStatus    string
	ToStatus      string
	FromStatusRaw string
	ToStatusRaw   string
	Actor         string
	LastSynced    time.Time
	OrgID         string
	SourceID      *uuid.UUID
}

func projectWorkItemTransition(row githubWorkItemTransitionRow) workItemTransitionStoredRow {
	return workItemTransitionStoredRow{
		// The semantic transition row carries no repo_id, so Python's
		// `if repo_id_val: ... else uuid.UUID(int=0)` always takes the else
		// branch for this unit.
		RepoID: uuid.Nil, WorkItemID: row.WorkItemID,
		OccurredAt: clickHouseMillis(row.OccurredAt), FromStatus: row.FromStatus,
		ToStatus:      row.ToStatus,
		FromStatusRaw: derefString(row.FromStatusRaw),
		ToStatusRaw:   derefString(row.ToStatusRaw),
		Actor:         derefString(row.Actor),
		LastSynced:    clickHouseMillis(row.LastSynced), OrgID: row.OrgID, SourceID: nil,
	}
}

func (adapter GitHubWorkItemTransitionsClickHouseAdapter) WriteGitHubWorkItemEffect(
	ctx context.Context,
	identity GitHubWorkItemEffectIdentity,
	effect EffectBatch,
) error {
	rows, err := decodeEffectRows[githubWorkItemTransitionRow](effect)
	if err != nil {
		return err
	}
	if err := workItemAdapterGuard(
		ctx, identity, effect, "work_item_transitions", adapter.Conn, len(rows),
	); err != nil {
		return err
	}
	for _, row := range rows {
		if row.OrgID != identity.OrgID {
			return ErrInvalidConfiguration
		}
	}
	rows = dedupeBySortingKey(rows, workItemTransitionSortingKey)
	if len(rows) == 0 {
		return nil
	}
	batch, err := adapter.Conn.PrepareBatch(ctx, gitHubWorkItemTransitionsInsert)
	if err != nil {
		return err
	}
	defer batch.Abort()
	for _, row := range rows {
		if err := batch.Append(projectWorkItemTransition(row).values()...); err != nil {
			return err
		}
	}
	return batch.Send()
}

func (adapter GitHubWorkItemTransitionsClickHouseAdapter) InspectGitHubWorkItemEffect(
	ctx context.Context,
	identity GitHubWorkItemEffectIdentity,
	effect EffectBatch,
) (EffectInspection, error) {
	rows, err := decodeEffectRows[githubWorkItemTransitionRow](effect)
	if err != nil {
		return EffectConflict, err
	}
	if err := workItemAdapterGuard(
		ctx, identity, effect, "work_item_transitions", adapter.Conn, len(rows),
	); err != nil {
		return EffectConflict, err
	}
	for _, row := range rows {
		if row.OrgID != identity.OrgID {
			return EffectConflict, ErrInvalidConfiguration
		}
	}
	rows = dedupeBySortingKey(rows, workItemTransitionSortingKey)
	if len(rows) == 0 {
		return emptyEffectInspection(), nil
	}
	return foldWorkItemInspections(rows, func(row githubWorkItemTransitionRow) (EffectInspection, error) {
		expected := projectWorkItemTransition(row)
		result, err := adapter.Conn.Query(
			ctx, gitHubWorkItemTransitionsSelect,
			expected.OrgID, expected.RepoID, expected.WorkItemID,
			clickHouseMillisParam(expected.OccurredAt),
		)
		if err != nil {
			return EffectConflict, err
		}
		defer result.Close()
		var actual workItemTransitionStoredRow
		found := 0
		for result.Next() {
			if err := result.Scan(
				&actual.RepoID, &actual.WorkItemID, &actual.OccurredAt,
				&actual.FromStatus, &actual.ToStatus, &actual.FromStatusRaw,
				&actual.ToStatusRaw, &actual.Actor, &actual.LastSynced,
				&actual.OrgID, &actual.SourceID,
			); err != nil {
				return EffectConflict, err
			}
			found++
		}
		if err := result.Err(); err != nil {
			return EffectConflict, err
		}
		if verdict, final := workItemReadbackVerdict(found); final {
			return verdict, nil
		}
		if verdict, final := workItemVersionVerdict(actual.LastSynced, expected.LastSynced); final {
			return verdict, nil
		}
		if expected.RepoID != actual.RepoID ||
			expected.WorkItemID != actual.WorkItemID ||
			!expected.OccurredAt.Equal(actual.OccurredAt) ||
			expected.FromStatus != actual.FromStatus ||
			expected.ToStatus != actual.ToStatus ||
			expected.FromStatusRaw != actual.FromStatusRaw ||
			expected.ToStatusRaw != actual.ToStatusRaw ||
			expected.Actor != actual.Actor || expected.OrgID != actual.OrgID ||
			!uuidPointersEqual(expected.SourceID, actual.SourceID) {
			return EffectConflict, nil
		}
		return EffectExact, nil
	})
}

// -----------------------------------------------------------------------------
// work_item_dependencies
// -----------------------------------------------------------------------------

// GitHubWorkItemDependenciesClickHouseAdapter mirrors
// write_work_item_dependencies (metrics/sinks/clickhouse/work_graph.py:334),
// which routes through core._insert_rows: dataclass fields pass through
// unchanged apart from datetime normalisation, so the Nullable columns keep
// their NULLs rather than collapsing to "".
type GitHubWorkItemDependenciesClickHouseAdapter struct{ Conn driver.Conn }

const gitHubWorkItemDependenciesInsert = `INSERT INTO work_item_dependencies (source_work_item_id, target_work_item_id, relationship_type, relationship_type_raw, relationship_semantics_version, last_synced, org_id, source_id)`

const gitHubWorkItemDependenciesSelect = `SELECT source_work_item_id, target_work_item_id, relationship_type, relationship_type_raw, relationship_semantics_version, last_synced, org_id, source_id FROM work_item_dependencies FINAL WHERE org_id = ? AND source_work_item_id = ? AND target_work_item_id = ? AND relationship_type = ?`

func (adapter GitHubWorkItemDependenciesClickHouseAdapter) WriteGitHubWorkItemEffect(
	ctx context.Context,
	identity GitHubWorkItemEffectIdentity,
	effect EffectBatch,
) error {
	rows, err := decodeEffectRows[githubWorkItemDependencyRow](effect)
	if err != nil {
		return err
	}
	if err := workItemAdapterGuard(
		ctx, identity, effect, "work_item_dependencies", adapter.Conn, len(rows),
	); err != nil {
		return err
	}
	for _, row := range rows {
		if row.OrgID != identity.OrgID {
			return ErrInvalidConfiguration
		}
	}
	rows = dedupeBySortingKey(rows, workItemDependencySortingKey)
	if len(rows) == 0 {
		return nil
	}
	batch, err := adapter.Conn.PrepareBatch(ctx, gitHubWorkItemDependenciesInsert)
	if err != nil {
		return err
	}
	defer batch.Abort()
	for _, row := range rows {
		if err := batch.Append(workItemDependencyValues(row)...); err != nil {
			return err
		}
	}
	return batch.Send()
}

func (adapter GitHubWorkItemDependenciesClickHouseAdapter) InspectGitHubWorkItemEffect(
	ctx context.Context,
	identity GitHubWorkItemEffectIdentity,
	effect EffectBatch,
) (EffectInspection, error) {
	rows, err := decodeEffectRows[githubWorkItemDependencyRow](effect)
	if err != nil {
		return EffectConflict, err
	}
	if err := workItemAdapterGuard(
		ctx, identity, effect, "work_item_dependencies", adapter.Conn, len(rows),
	); err != nil {
		return EffectConflict, err
	}
	for _, row := range rows {
		if row.OrgID != identity.OrgID {
			return EffectConflict, ErrInvalidConfiguration
		}
	}
	rows = dedupeBySortingKey(rows, workItemDependencySortingKey)
	if len(rows) == 0 {
		return emptyEffectInspection(), nil
	}
	return foldWorkItemInspections(rows, func(row githubWorkItemDependencyRow) (EffectInspection, error) {
		result, err := adapter.Conn.Query(
			ctx, gitHubWorkItemDependenciesSelect,
			row.OrgID, row.SourceWorkItemID, row.TargetWorkItemID, row.RelationshipType,
		)
		if err != nil {
			return EffectConflict, err
		}
		defer result.Close()
		var actual githubWorkItemDependencyRow
		found := 0
		for result.Next() {
			if err := result.Scan(
				&actual.SourceWorkItemID, &actual.TargetWorkItemID,
				&actual.RelationshipType, &actual.RelationshipTypeRaw,
				&actual.RelationshipSemanticsVersion, &actual.LastSynced,
				&actual.OrgID, &actual.SourceID,
			); err != nil {
				return EffectConflict, err
			}
			found++
		}
		if err := result.Err(); err != nil {
			return EffectConflict, err
		}
		if verdict, final := workItemReadbackVerdict(found); final {
			return verdict, nil
		}
		if verdict, final := workItemVersionVerdict(actual.LastSynced, clickHouseMillis(row.LastSynced)); final {
			return verdict, nil
		}
		if actual.SourceWorkItemID != row.SourceWorkItemID ||
			actual.TargetWorkItemID != row.TargetWorkItemID ||
			actual.RelationshipType != row.RelationshipType ||
			actual.RelationshipTypeRaw != row.RelationshipTypeRaw ||
			actual.RelationshipSemanticsVersion != row.RelationshipSemanticsVersion ||
			actual.OrgID != row.OrgID ||
			!uuidPointersEqual(actual.SourceID, row.SourceID) {
			return EffectConflict, nil
		}
		return EffectExact, nil
	})
}

// -----------------------------------------------------------------------------
// work_item_reopen_events
// -----------------------------------------------------------------------------

// GitHubWorkItemReopenEventsClickHouseAdapter mirrors
// write_work_item_reopen_events (metrics/sinks/clickhouse/work_graph.py:352).
type GitHubWorkItemReopenEventsClickHouseAdapter struct{ Conn driver.Conn }

const gitHubWorkItemReopenEventsInsert = `INSERT INTO work_item_reopen_events (work_item_id, occurred_at, from_status, to_status, from_status_raw, to_status_raw, actor, last_synced, org_id)`

const gitHubWorkItemReopenEventsSelect = `SELECT work_item_id, occurred_at, from_status, to_status, from_status_raw, to_status_raw, actor, last_synced, org_id FROM work_item_reopen_events FINAL WHERE org_id = ? AND work_item_id = ? AND occurred_at = toDateTime64(?, 3, 'UTC')`

func (adapter GitHubWorkItemReopenEventsClickHouseAdapter) WriteGitHubWorkItemEffect(
	ctx context.Context,
	identity GitHubWorkItemEffectIdentity,
	effect EffectBatch,
) error {
	rows, err := decodeEffectRows[githubWorkItemReopenRow](effect)
	if err != nil {
		return err
	}
	if err := workItemAdapterGuard(
		ctx, identity, effect, "work_item_reopen_events", adapter.Conn, len(rows),
	); err != nil {
		return err
	}
	for _, row := range rows {
		if row.OrgID != identity.OrgID {
			return ErrInvalidConfiguration
		}
	}
	rows = dedupeBySortingKey(rows, workItemReopenSortingKey)
	if len(rows) == 0 {
		return nil
	}
	batch, err := adapter.Conn.PrepareBatch(ctx, gitHubWorkItemReopenEventsInsert)
	if err != nil {
		return err
	}
	defer batch.Abort()
	for _, row := range rows {
		if err := batch.Append(workItemReopenValues(row)...); err != nil {
			return err
		}
	}
	return batch.Send()
}

func (adapter GitHubWorkItemReopenEventsClickHouseAdapter) InspectGitHubWorkItemEffect(
	ctx context.Context,
	identity GitHubWorkItemEffectIdentity,
	effect EffectBatch,
) (EffectInspection, error) {
	rows, err := decodeEffectRows[githubWorkItemReopenRow](effect)
	if err != nil {
		return EffectConflict, err
	}
	if err := workItemAdapterGuard(
		ctx, identity, effect, "work_item_reopen_events", adapter.Conn, len(rows),
	); err != nil {
		return EffectConflict, err
	}
	for _, row := range rows {
		if row.OrgID != identity.OrgID {
			return EffectConflict, ErrInvalidConfiguration
		}
	}
	rows = dedupeBySortingKey(rows, workItemReopenSortingKey)
	if len(rows) == 0 {
		return emptyEffectInspection(), nil
	}
	return foldWorkItemInspections(rows, func(row githubWorkItemReopenRow) (EffectInspection, error) {
		result, err := adapter.Conn.Query(
			ctx, gitHubWorkItemReopenEventsSelect,
			row.OrgID, row.WorkItemID, clickHouseMillisParam(row.OccurredAt),
		)
		if err != nil {
			return EffectConflict, err
		}
		defer result.Close()
		var actual githubWorkItemReopenRow
		found := 0
		for result.Next() {
			if err := result.Scan(
				&actual.WorkItemID, &actual.OccurredAt, &actual.FromStatus,
				&actual.ToStatus, &actual.FromStatusRaw, &actual.ToStatusRaw,
				&actual.Actor, &actual.LastSynced, &actual.OrgID,
			); err != nil {
				return EffectConflict, err
			}
			found++
		}
		if err := result.Err(); err != nil {
			return EffectConflict, err
		}
		if verdict, final := workItemReadbackVerdict(found); final {
			return verdict, nil
		}
		if verdict, final := workItemVersionVerdict(actual.LastSynced, clickHouseMillis(row.LastSynced)); final {
			return verdict, nil
		}
		if actual.WorkItemID != row.WorkItemID ||
			!actual.OccurredAt.Equal(clickHouseMillis(row.OccurredAt)) ||
			actual.FromStatus != row.FromStatus || actual.ToStatus != row.ToStatus ||
			!stringPointersEqual(actual.FromStatusRaw, row.FromStatusRaw) ||
			!stringPointersEqual(actual.ToStatusRaw, row.ToStatusRaw) ||
			!stringPointersEqual(actual.Actor, row.Actor) ||
			actual.OrgID != row.OrgID {
			return EffectConflict, nil
		}
		return EffectExact, nil
	})
}

// -----------------------------------------------------------------------------
// work_item_interactions
// -----------------------------------------------------------------------------

// GitHubWorkItemInteractionsClickHouseAdapter mirrors
// write_work_item_interactions (metrics/sinks/clickhouse/work_graph.py:373).
type GitHubWorkItemInteractionsClickHouseAdapter struct{ Conn driver.Conn }

const gitHubWorkItemInteractionsInsert = `INSERT INTO work_item_interactions (work_item_id, provider, interaction_type, occurred_at, actor, body_length, last_synced, org_id)`

const gitHubWorkItemInteractionsSelect = `SELECT work_item_id, provider, interaction_type, occurred_at, actor, body_length, last_synced, org_id FROM work_item_interactions FINAL WHERE org_id = ? AND work_item_id = ? AND occurred_at = toDateTime64(?, 3, 'UTC') AND interaction_type = ?`

func (adapter GitHubWorkItemInteractionsClickHouseAdapter) WriteGitHubWorkItemEffect(
	ctx context.Context,
	identity GitHubWorkItemEffectIdentity,
	effect EffectBatch,
) error {
	rows, err := decodeEffectRows[githubWorkItemInteractionRow](effect)
	if err != nil {
		return err
	}
	if err := workItemAdapterGuard(
		ctx, identity, effect, "work_item_interactions", adapter.Conn, len(rows),
	); err != nil {
		return err
	}
	for _, row := range rows {
		// body_length is UInt32; a negative length cannot round-trip and must
		// never reach the driver as a silent wrap.
		if row.OrgID != identity.OrgID || row.BodyLength < 0 {
			return ErrInvalidConfiguration
		}
	}
	rows = dedupeBySortingKey(rows, workItemInteractionSortingKey)
	if len(rows) == 0 {
		return nil
	}
	batch, err := adapter.Conn.PrepareBatch(ctx, gitHubWorkItemInteractionsInsert)
	if err != nil {
		return err
	}
	defer batch.Abort()
	for _, row := range rows {
		if err := batch.Append(workItemInteractionValues(row)...); err != nil {
			return err
		}
	}
	return batch.Send()
}

func (adapter GitHubWorkItemInteractionsClickHouseAdapter) InspectGitHubWorkItemEffect(
	ctx context.Context,
	identity GitHubWorkItemEffectIdentity,
	effect EffectBatch,
) (EffectInspection, error) {
	rows, err := decodeEffectRows[githubWorkItemInteractionRow](effect)
	if err != nil {
		return EffectConflict, err
	}
	if err := workItemAdapterGuard(
		ctx, identity, effect, "work_item_interactions", adapter.Conn, len(rows),
	); err != nil {
		return EffectConflict, err
	}
	for _, row := range rows {
		if row.OrgID != identity.OrgID || row.BodyLength < 0 {
			return EffectConflict, ErrInvalidConfiguration
		}
	}
	rows = dedupeBySortingKey(rows, workItemInteractionSortingKey)
	if len(rows) == 0 {
		return emptyEffectInspection(), nil
	}
	return foldWorkItemInspections(rows, func(row githubWorkItemInteractionRow) (EffectInspection, error) {
		result, err := adapter.Conn.Query(
			ctx, gitHubWorkItemInteractionsSelect,
			row.OrgID, row.WorkItemID, clickHouseMillisParam(row.OccurredAt), row.InteractionType,
		)
		if err != nil {
			return EffectConflict, err
		}
		defer result.Close()
		var actual githubWorkItemInteractionRow
		var actualBodyLength uint32
		found := 0
		for result.Next() {
			if err := result.Scan(
				&actual.WorkItemID, &actual.Provider, &actual.InteractionType,
				&actual.OccurredAt, &actual.Actor, &actualBodyLength,
				&actual.LastSynced, &actual.OrgID,
			); err != nil {
				return EffectConflict, err
			}
			found++
		}
		if err := result.Err(); err != nil {
			return EffectConflict, err
		}
		if verdict, final := workItemReadbackVerdict(found); final {
			return verdict, nil
		}
		if verdict, final := workItemVersionVerdict(actual.LastSynced, clickHouseMillis(row.LastSynced)); final {
			return verdict, nil
		}
		if actual.WorkItemID != row.WorkItemID || actual.Provider != row.Provider ||
			actual.InteractionType != row.InteractionType ||
			!actual.OccurredAt.Equal(clickHouseMillis(row.OccurredAt)) ||
			!stringPointersEqual(actual.Actor, row.Actor) ||
			actualBodyLength != uint32(row.BodyLength) ||
			actual.OrgID != row.OrgID {
			return EffectConflict, nil
		}
		return EffectExact, nil
	})
}

// -----------------------------------------------------------------------------
// sprints
// -----------------------------------------------------------------------------

// GitHubSprintsClickHouseAdapter mirrors write_sprints
// (metrics/sinks/clickhouse/work_graph.py:393), including its pre-insert
// `replace(native_team_key or "")`: the column is a non-nullable String, so a
// missing native team key is stored as the empty string, not NULL.
type GitHubSprintsClickHouseAdapter struct{ Conn driver.Conn }

const gitHubSprintsInsert = `INSERT INTO sprints (provider, sprint_id, native_team_key, name, state, started_at, ended_at, completed_at, last_synced, org_id)`

const gitHubSprintsSelect = `SELECT provider, sprint_id, native_team_key, name, state, started_at, ended_at, completed_at, last_synced, org_id FROM sprints FINAL WHERE org_id = ? AND provider = ? AND sprint_id = ?`

func (adapter GitHubSprintsClickHouseAdapter) WriteGitHubWorkItemEffect(
	ctx context.Context,
	identity GitHubWorkItemEffectIdentity,
	effect EffectBatch,
) error {
	rows, err := decodeEffectRows[githubSprintRow](effect)
	if err != nil {
		return err
	}
	if err := workItemAdapterGuard(
		ctx, identity, effect, "sprints", adapter.Conn, len(rows),
	); err != nil {
		return err
	}
	for _, row := range rows {
		if row.OrgID != identity.OrgID {
			return ErrInvalidConfiguration
		}
	}
	rows = dedupeBySortingKey(rows, sprintSortingKey)
	if len(rows) == 0 {
		return nil
	}
	batch, err := adapter.Conn.PrepareBatch(ctx, gitHubSprintsInsert)
	if err != nil {
		return err
	}
	defer batch.Abort()
	for _, row := range rows {
		if err := batch.Append(sprintValues(row)...); err != nil {
			return err
		}
	}
	return batch.Send()
}

func (adapter GitHubSprintsClickHouseAdapter) InspectGitHubWorkItemEffect(
	ctx context.Context,
	identity GitHubWorkItemEffectIdentity,
	effect EffectBatch,
) (EffectInspection, error) {
	rows, err := decodeEffectRows[githubSprintRow](effect)
	if err != nil {
		return EffectConflict, err
	}
	if err := workItemAdapterGuard(
		ctx, identity, effect, "sprints", adapter.Conn, len(rows),
	); err != nil {
		return EffectConflict, err
	}
	for _, row := range rows {
		if row.OrgID != identity.OrgID {
			return EffectConflict, ErrInvalidConfiguration
		}
	}
	rows = dedupeBySortingKey(rows, sprintSortingKey)
	if len(rows) == 0 {
		return emptyEffectInspection(), nil
	}
	return foldWorkItemInspections(rows, func(row githubSprintRow) (EffectInspection, error) {
		result, err := adapter.Conn.Query(
			ctx, gitHubSprintsSelect, row.OrgID, row.Provider, row.SprintID,
		)
		if err != nil {
			return EffectConflict, err
		}
		defer result.Close()
		var actual githubSprintRow
		var actualNativeTeamKey string
		found := 0
		for result.Next() {
			if err := result.Scan(
				&actual.Provider, &actual.SprintID, &actualNativeTeamKey,
				&actual.Name, &actual.State, &actual.StartedAt, &actual.EndedAt,
				&actual.CompletedAt, &actual.LastSynced, &actual.OrgID,
			); err != nil {
				return EffectConflict, err
			}
			found++
		}
		if err := result.Err(); err != nil {
			return EffectConflict, err
		}
		if verdict, final := workItemReadbackVerdict(found); final {
			return verdict, nil
		}
		if verdict, final := workItemVersionVerdict(actual.LastSynced, clickHouseMillis(row.LastSynced)); final {
			return verdict, nil
		}
		if actual.Provider != row.Provider || actual.SprintID != row.SprintID ||
			actualNativeTeamKey != derefString(row.NativeTeamKey) ||
			!stringPointersEqual(actual.Name, row.Name) ||
			!stringPointersEqual(actual.State, row.State) ||
			!timePointersEqual(actual.StartedAt, utcPointer(row.StartedAt)) ||
			!timePointersEqual(actual.EndedAt, utcPointer(row.EndedAt)) ||
			!timePointersEqual(actual.CompletedAt, utcPointer(row.CompletedAt)) ||
			actual.OrgID != row.OrgID {
			return EffectConflict, nil
		}
		return EffectExact, nil
	})
}

// -----------------------------------------------------------------------------
// shared folding and value helpers
// -----------------------------------------------------------------------------

// foldWorkItemInspections reduces per-row verdicts to one batch verdict. A
// mixed batch is a conflict rather than a replay: replaying it would rewrite
// the rows that already landed, and the committer cannot express "replay these
// three of five".
func foldWorkItemInspections[T any](
	rows []T,
	inspect func(T) (EffectInspection, error),
) (EffectInspection, error) {
	exact, absent := 0, 0
	for _, row := range rows {
		inspection, err := inspect(row)
		if err != nil {
			return EffectConflict, err
		}
		switch inspection {
		case EffectExact:
			exact++
		case EffectAbsent:
			absent++
		default:
			return EffectConflict, nil
		}
	}
	switch {
	case exact == len(rows):
		return EffectExact, nil
	case absent == len(rows):
		return EffectAbsent, nil
	default:
		return EffectConflict, nil
	}
}

func derefString(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func utcPointer(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	utc := clickHouseMillis(*value)
	return &utc
}

// clickHouseMillis renders an instant at the precision every destination column
// here actually stores, DateTime64(3).
//
// The projection is the answer to "what will ClickHouse hold after this write",
// so it must round-trip: an untruncated expectation compares .123456789 against
// a stored .123, the version verdict reads Absent for a row that landed, and
// the committer rewrites it every pass. The route truncates normalizedAt at
// entry as well; this makes the adapter correct for ANY caller rather than only
// a well-behaved one, since a sub-millisecond expectation is never satisfiable.
func clickHouseMillis(value time.Time) time.Time {
	return value.UTC().Truncate(workItemColumnPrecision)
}

// workItemColumnPrecision is the stored precision EVERY timestamp column across
// these seven destinations currently has: DateTime64(3), milliseconds.
//
// It is a single constant rather than a per-destination table because the seven
// tables agree today -- but that agreement is an assumption about the schema,
// not a property of it, and the assumption is load-bearing in both directions:
// truncating coarser than the column stores makes the comparator reject a row
// it wrote, and truncating finer leaves a value the column cannot hold, which
// is the same permanent-Absent rewrite loop either way.
//
// So it is ASSERTED, not assumed:
// TestWorkItemTimestampColumnsAllHaveTheAssumedPrecision reads the real
// migrated schema and fails if any timestamp column disagrees. A sibling lane
// hit exactly this shape one table over -- a version column that was plain
// DateTime (seconds) while the shared helper truncated to milliseconds -- so if
// that test ever fails, the fix is to make the truncation per-destination
// rather than to widen the constant.
const workItemColumnPrecision = time.Millisecond

// clickHouseMillisParam renders an instant for use as an EQUALITY PARAMETER
// against a DateTime64(3) column.
//
// A bound time.Time does not work: measured against a real server, a row whose
// stored occurred_at scans back Equal() to the expectation still matched ZERO
// rows under `occurred_at = ?`, because the driver binds the parameter without
// the sub-second component. The readback then reports Absent for a row that is
// present and the committer rewrites it forever.
//
// Every fixture in this suite previously used a whole second, so the truncated
// parameter happened to equal the stored value and the defect was invisible.
//
// The value is formatted to millisecond precision and interpreted explicitly as
// UTC, which keeps the predicate independent of the server timezone; the column
// itself is naive DateTime64(3). The column stays bare on the left-hand side so
// the primary-key index can still prune.
func clickHouseMillisParam(value time.Time) string {
	return clickHouseMillis(value).Format("2006-01-02 15:04:05.000")
}

func float64PointersEqual(left, right *float64) bool {
	if left == nil || right == nil {
		return left == right
	}
	return *left == *right
}

func uuidPointersEqual(left, right *uuid.UUID) bool {
	if left == nil || right == nil {
		return left == right
	}
	return *left == *right
}

func stringSlicesEqual(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

var _ GitHubWorkItemEffectAdapter = GitHubWorkItemsClickHouseAdapter{}
var _ GitHubWorkItemEffectAdapter = GitHubWorkItemTransitionsClickHouseAdapter{}
var _ GitHubWorkItemEffectAdapter = GitHubWorkItemDependenciesClickHouseAdapter{}
var _ GitHubWorkItemEffectAdapter = GitHubWorkItemReopenEventsClickHouseAdapter{}
var _ GitHubWorkItemEffectAdapter = GitHubWorkItemInteractionsClickHouseAdapter{}
var _ GitHubWorkItemEffectAdapter = GitHubSprintsClickHouseAdapter{}

// -----------------------------------------------------------------------------
// ordered insert values
// -----------------------------------------------------------------------------
//
// One definition of each destination's value order, used by the writer above
// AND by the differential oracle that compares it against the real Python
// sink's insert matrix. Duplicating the order in the test would let the two
// drift together and agree while both were wrong.

func (row workItemStoredRow) values() []any {
	return []any{
		row.RepoID, row.WorkItemID, row.Provider, row.Title, row.Type,
		row.Status, row.StatusRaw, row.ProjectKey, row.ProjectID,
		row.NativeTeamKey, row.ProjectName, row.Assignees, row.Reporter,
		row.CreatedAt, row.UpdatedAt, row.StartedAt, row.CompletedAt,
		row.ClosedAt, row.Labels, row.StoryPoints, row.SprintID, row.SprintName,
		row.ParentID, row.EpicID, row.URL, row.LastSynced, row.OrgID,
		row.SourceID,
	}
}

func (row workItemTransitionStoredRow) values() []any {
	return []any{
		row.RepoID, row.WorkItemID, row.OccurredAt, row.FromStatus, row.ToStatus,
		row.FromStatusRaw, row.ToStatusRaw, row.Actor, row.LastSynced, row.OrgID,
		row.SourceID,
	}
}

func workItemDependencyValues(row githubWorkItemDependencyRow) []any {
	return []any{
		row.SourceWorkItemID, row.TargetWorkItemID, row.RelationshipType,
		row.RelationshipTypeRaw, row.RelationshipSemanticsVersion,
		clickHouseMillis(row.LastSynced), row.OrgID, row.SourceID,
	}
}

func workItemReopenValues(row githubWorkItemReopenRow) []any {
	return []any{
		row.WorkItemID, clickHouseMillis(row.OccurredAt), row.FromStatus, row.ToStatus,
		row.FromStatusRaw, row.ToStatusRaw, row.Actor, clickHouseMillis(row.LastSynced),
		row.OrgID,
	}
}

func workItemInteractionValues(row githubWorkItemInteractionRow) []any {
	return []any{
		row.WorkItemID, row.Provider, row.InteractionType, clickHouseMillis(row.OccurredAt),
		row.Actor, uint32(row.BodyLength), clickHouseMillis(row.LastSynced), row.OrgID,
	}
}

func sprintValues(row githubSprintRow) []any {
	return []any{
		row.Provider, row.SprintID, derefString(row.NativeTeamKey), row.Name,
		row.State, utcPointer(row.StartedAt), utcPointer(row.EndedAt),
		utcPointer(row.CompletedAt), clickHouseMillis(row.LastSynced), row.OrgID,
	}
}

// -----------------------------------------------------------------------------
// same-key collision inside one batch
// -----------------------------------------------------------------------------

// dedupeBySortingKey collapses rows that share a destination's ReplacingMergeTree
// sorting key, keeping the LAST occurrence in the batch's order.
//
// Why this exists: a single batch can legitimately carry two rows at one sorting
// key. Two issue events in the same second produce two transitions at the same
// (org_id, repo_id, work_item_id, occurred_at); the parse-failure fallback to the
// item's createdAt makes it reachable deterministically; and
// github_work_items_projects_v2.go:574 concatenates the repository and
// projects-v2 transition slices with no dedup of its own.
//
// Without this, ClickHouse collapses the pair on write, the readback finds ONE
// row, and the comparator sees a field mismatch against whichever expectation
// lost -- a permanent Conflict, which reaches the committer as
// ErrEffectRecoveryAmbiguous and wedges the unit with no way to self-heal.
// Python does not fail there: its sink hands both rows to ClickHouse and simply
// loses one. Turning that silent loss into a hard stop would be an undeclared
// D16 liveness divergence, so the port mirrors the loss.
//
// keep-LAST is not a guess. Measured against a real ReplacingMergeTree with two
// rows at one key and EQUAL versions:
//
//	one INSERT block, payloads (A, B) -> retains B, raw count 1
//	one INSERT block, payloads (B, A) -> retains A, raw count 1
//	two INSERT blocks, A then B       -> FINAL retains B, raw count 2
//
// The reversed-order control rules out "greatest value wins": it is the last row
// in insertion order that survives. Keeping the last occurrence therefore makes
// the expected set agree with what the table actually holds.
func dedupeBySortingKey[T any](rows []T, key func(T) string) []T {
	if len(rows) < 2 {
		return rows
	}
	position := make(map[string]int, len(rows))
	deduped := make([]T, 0, len(rows))
	for _, row := range rows {
		identity := key(row)
		if index, seen := position[identity]; seen {
			deduped[index] = row // last occurrence wins, as the engine does
			continue
		}
		position[identity] = len(deduped)
		deduped = append(deduped, row)
	}
	return deduped
}

// The sorting key of each destination, rendered as a comparable string. Unit
// separators keep two different field splits from colliding into one identity.
const workItemKeySeparator = "\x1f"

func workItemSortingKey(row githubWorkItemRow) string {
	repoID := uuid.Nil
	if row.RepoID != nil {
		repoID = *row.RepoID
	}
	return strings.Join([]string{row.OrgID, repoID.String(), row.WorkItemID}, workItemKeySeparator)
}

func workItemTransitionSortingKey(row githubWorkItemTransitionRow) string {
	// repo_id is always the nil UUID for this unit; see projectWorkItemTransition.
	return strings.Join([]string{
		row.OrgID, uuid.Nil.String(), row.WorkItemID,
		clickHouseMillis(row.OccurredAt).Format(time.RFC3339Nano),
	}, workItemKeySeparator)
}

func workItemDependencySortingKey(row githubWorkItemDependencyRow) string {
	return strings.Join([]string{
		row.OrgID, row.SourceWorkItemID, row.TargetWorkItemID, row.RelationshipType,
	}, workItemKeySeparator)
}

func workItemReopenSortingKey(row githubWorkItemReopenRow) string {
	return strings.Join([]string{
		row.OrgID, row.WorkItemID,
		clickHouseMillis(row.OccurredAt).Format(time.RFC3339Nano),
	}, workItemKeySeparator)
}

func workItemInteractionSortingKey(row githubWorkItemInteractionRow) string {
	return strings.Join([]string{
		row.OrgID, row.WorkItemID,
		clickHouseMillis(row.OccurredAt).Format(time.RFC3339Nano), row.InteractionType,
	}, workItemKeySeparator)
}

func sprintSortingKey(row githubSprintRow) string {
	return strings.Join([]string{row.OrgID, row.Provider, row.SprintID}, workItemKeySeparator)
}
