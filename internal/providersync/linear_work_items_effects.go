package providersync

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/google/uuid"
)

var linearWorkItemEffectDestinations = []string{
	"work_items", "work_item_transitions", "work_item_dependencies",
	"work_item_reopen_events", "work_item_interactions", "sprints",
}

// LinearWorkItemEffectRows is the provider-owned projection for the first
// canonical vertical slice. Empty projections remain explicit effects: the
// recovery ledger must distinguish an evaluated destination with no rows from
// a route that silently forgot the destination.
type LinearWorkItemEffectRows struct {
	WorkItems         []json.RawMessage
	StatusTransitions []json.RawMessage
	Dependencies      []json.RawMessage
	ReopenEvents      []json.RawMessage
	Interactions      []json.RawMessage
	Sprints           []json.RawMessage
}

func BuildLinearWorkItemEffects(rows LinearWorkItemEffectRows) ([]EffectBatch, error) {
	projections := map[string][]json.RawMessage{
		"work_items":              rows.WorkItems,
		"work_item_transitions":   rows.StatusTransitions,
		"work_item_dependencies":  rows.Dependencies,
		"work_item_reopen_events": rows.ReopenEvents,
		"work_item_interactions":  rows.Interactions,
		"sprints":                 rows.Sprints,
	}
	effects := make([]EffectBatch, 0, len(linearWorkItemEffectDestinations))
	seen := make(map[string]struct{}, len(linearWorkItemEffectDestinations))
	for _, destination := range linearWorkItemEffectDestinations {
		if _, duplicate := seen[destination]; duplicate {
			return nil, ErrInvalidConfiguration
		}
		seen[destination] = struct{}{}
		effect, err := BuildEffectBatch(
			destination, EffectReadbackRequired, projections[destination],
		)
		if err != nil {
			return nil, err
		}
		effects = append(effects, effect)
	}
	return effects, nil
}

func buildLinearWorkItemEffectsFromRows(
	rows linearWorkItemRows,
) ([]EffectBatch, error) {
	workItems, err := effectRowsFromValues(rows.WorkItems)
	if err != nil {
		return nil, err
	}
	transitions, err := effectRowsFromValues(rows.StatusTransitions)
	if err != nil {
		return nil, err
	}
	dependencies, err := effectRowsFromValues(rows.Dependencies)
	if err != nil {
		return nil, err
	}
	reopens, err := effectRowsFromValues(rows.ReopenEvents)
	if err != nil {
		return nil, err
	}
	interactions, err := effectRowsFromValues(rows.Interactions)
	if err != nil {
		return nil, err
	}
	sprints, err := effectRowsFromValues(rows.Sprints)
	if err != nil {
		return nil, err
	}
	return BuildLinearWorkItemEffects(LinearWorkItemEffectRows{
		WorkItems: workItems, StatusTransitions: transitions,
		Dependencies: dependencies, ReopenEvents: reopens,
		Interactions: interactions, Sprints: sprints,
	})
}

func effectRowsFromValues[T any](rows []T) ([]json.RawMessage, error) {
	encoded := make([]json.RawMessage, 0, len(rows))
	for _, row := range rows {
		value, err := json.Marshal(row)
		if err != nil {
			return nil, err
		}
		encoded = append(encoded, value)
	}
	return encoded, nil
}

// LinearWorkItemEffectIdentity is the frozen tenant/generation namespace used
// for both writes and readback. It never trusts org_id from an effect row.
type LinearWorkItemEffectIdentity struct {
	OrgID         string
	Provider      string
	Dataset       string
	Generation    string
	Destination   string
	ContentDigest string
	RowCount      int
}

func newLinearWorkItemEffectIdentity(
	claim Claim,
	effect EffectBatch,
) (LinearWorkItemEffectIdentity, error) {
	if claim.Validate() != nil || claim.Provider != "linear" ||
		claim.Dataset != "work-items" ||
		!linearWorkItemDestination(effect.Destination) ||
		effect.Recovery != EffectReadbackRequired ||
		!validDigest(effect.ContentDigest) || effect.PayloadBytes < 0 {
		return LinearWorkItemEffectIdentity{}, ErrInvalidConfiguration
	}
	rebuilt, err := BuildEffectBatch(effect.Destination, effect.Recovery, effect.Rows)
	if err != nil || rebuilt.ContentDigest != effect.ContentDigest ||
		rebuilt.PayloadBytes != effect.PayloadBytes {
		return LinearWorkItemEffectIdentity{}, ErrInvalidConfiguration
	}
	identity := LinearWorkItemEffectIdentity{
		OrgID: claim.OrgID, Provider: claim.Provider, Dataset: claim.Dataset,
		Generation: claim.GenerationKey(), Destination: effect.Destination,
		ContentDigest: effect.ContentDigest, RowCount: len(effect.Rows),
	}
	if strings.TrimSpace(identity.OrgID) == "" ||
		strings.TrimSpace(identity.Generation) == "" {
		return LinearWorkItemEffectIdentity{}, ErrInvalidConfiguration
	}
	return identity, nil
}

func linearWorkItemDestination(destination string) bool {
	for _, candidate := range linearWorkItemEffectDestinations {
		if candidate == destination {
			return true
		}
	}
	return false
}

type LinearWorkItemEffectAdapter interface {
	WriteLinearWorkItemEffect(
		context.Context, LinearWorkItemEffectIdentity, EffectBatch,
	) error
	InspectLinearWorkItemEffect(
		context.Context, LinearWorkItemEffectIdentity, EffectBatch,
	) (EffectInspection, error)
}

// LinearWorkItemClickHouseEffects is a provider-only, six-destination effect
// dispatcher. Its adapters are injected by future worker construction; this
// file intentionally does not register or activate that construction.
type LinearWorkItemClickHouseEffects struct {
	Lease             providerfoundation.LeaseGuard
	WorkItems         LinearWorkItemEffectAdapter
	StatusTransitions LinearWorkItemEffectAdapter
	Dependencies      LinearWorkItemEffectAdapter
	ReopenEvents      LinearWorkItemEffectAdapter
	Interactions      LinearWorkItemEffectAdapter
	Sprints           LinearWorkItemEffectAdapter
}

func (sink LinearWorkItemClickHouseEffects) WriteEffect(
	ctx context.Context, claim Claim, effect EffectBatch,
) error {
	identity, adapter, err := sink.resolve(claim, effect)
	if err != nil || ctx == nil || sink.Lease == nil {
		return ErrInvalidConfiguration
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return err
	}
	if err := adapter.WriteLinearWorkItemEffect(ctx, identity, effect); err != nil {
		return err
	}
	return sink.Lease.Assert(ctx)
}

func (sink LinearWorkItemClickHouseEffects) InspectEffect(
	ctx context.Context, claim Claim, effect EffectBatch,
) (EffectInspection, error) {
	identity, adapter, err := sink.resolve(claim, effect)
	if err != nil || ctx == nil || sink.Lease == nil {
		return EffectConflict, ErrInvalidConfiguration
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return EffectConflict, err
	}
	inspection, err := adapter.InspectLinearWorkItemEffect(ctx, identity, effect)
	if err != nil {
		return EffectConflict, err
	}
	switch inspection {
	case EffectExact, EffectAbsent, EffectConflict:
		return inspection, nil
	default:
		return EffectConflict, ErrInvalidConfiguration
	}
}

func (sink LinearWorkItemClickHouseEffects) resolve(
	claim Claim, effect EffectBatch,
) (LinearWorkItemEffectIdentity, LinearWorkItemEffectAdapter, error) {
	identity, err := newLinearWorkItemEffectIdentity(claim, effect)
	if err != nil || !sink.complete() {
		return LinearWorkItemEffectIdentity{}, nil, ErrInvalidConfiguration
	}
	adapter, known := sink.adapterForDestination(effect.Destination)
	if !known || adapter == nil {
		return LinearWorkItemEffectIdentity{}, nil, ErrInvalidConfiguration
	}
	return identity, adapter, nil
}

func (sink LinearWorkItemClickHouseEffects) adapterForDestination(
	destination string,
) (LinearWorkItemEffectAdapter, bool) {
	switch destination {
	case "work_items":
		return sink.WorkItems, true
	case "work_item_transitions":
		return sink.StatusTransitions, true
	case "work_item_dependencies":
		return sink.Dependencies, true
	case "work_item_reopen_events":
		return sink.ReopenEvents, true
	case "work_item_interactions":
		return sink.Interactions, true
	case "sprints":
		return sink.Sprints, true
	default:
		return nil, false
	}
}

func (sink LinearWorkItemClickHouseEffects) MissingDestinations() []string {
	missing := make([]string, 0, len(linearWorkItemEffectDestinations))
	for _, destination := range linearWorkItemEffectDestinations {
		adapter, known := sink.adapterForDestination(destination)
		if !known || adapter == nil {
			missing = append(missing, destination)
		}
	}
	return missing
}

func (sink LinearWorkItemClickHouseEffects) complete() bool {
	return len(sink.MissingDestinations()) == 0
}

var _ EffectSink = LinearWorkItemClickHouseEffects{}
var _ EffectReadback = LinearWorkItemClickHouseEffects{}

func linearWorkItemAdapterGuard(
	ctx context.Context,
	identity LinearWorkItemEffectIdentity,
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

const linearWorkItemsInsert = `INSERT INTO work_items (repo_id, work_item_id, provider, title, type, status, status_raw, project_key, project_id, native_team_key, project_name, assignees, reporter, created_at, updated_at, started_at, completed_at, closed_at, labels, story_points, sprint_id, sprint_name, parent_id, epic_id, url, last_synced, org_id, source_id)`

const linearWorkItemsSelect = `SELECT repo_id, work_item_id, provider, title, type, status, status_raw, project_key, project_id, native_team_key, project_name, assignees, reporter, created_at, updated_at, started_at, completed_at, closed_at, labels, story_points, sprint_id, sprint_name, parent_id, epic_id, url, last_synced, org_id, source_id FROM work_items FINAL WHERE org_id = ? AND repo_id = ? AND work_item_id = ?`

func projectLinearWorkItem(row linearWorkItemRow) workItemStoredRow {
	assignees, labels := row.Assignees, row.Labels
	if assignees == nil {
		assignees = []string{}
	}
	if labels == nil {
		labels = []string{}
	}
	return workItemStoredRow{
		RepoID: uuid.Nil, WorkItemID: row.WorkItemID, Provider: row.Provider,
		Title: row.Title, Type: row.Type, Status: row.Status,
		StatusRaw: derefString(row.StatusRaw), ProjectKey: derefString(row.ProjectKey),
		ProjectID: derefString(row.ProjectID), NativeTeamKey: derefString(row.NativeTeamKey),
		ProjectName: derefString(row.ProjectName), Assignees: assignees,
		Reporter: derefString(row.Reporter), CreatedAt: clickHouseMillis(row.CreatedAt),
		UpdatedAt: clickHouseMillis(row.UpdatedAt), StartedAt: utcPointer(row.StartedAt),
		CompletedAt: utcPointer(row.CompletedAt), ClosedAt: utcPointer(row.ClosedAt),
		Labels: labels, StoryPoints: row.StoryPoints,
		SprintID: derefString(row.SprintID), SprintName: derefString(row.SprintName),
		ParentID: derefString(row.ParentID), EpicID: derefString(row.EpicID),
		URL: derefString(row.URL), LastSynced: clickHouseMillis(row.LastSynced),
		OrgID: row.OrgID, SourceID: nil,
	}
}

type LinearWorkItemsClickHouseAdapter struct{ Conn driver.Conn }

func (adapter LinearWorkItemsClickHouseAdapter) WriteLinearWorkItemEffect(
	ctx context.Context, identity LinearWorkItemEffectIdentity, effect EffectBatch,
) error {
	rows, err := decodeEffectRows[linearWorkItemRow](effect)
	if err != nil {
		return err
	}
	if err := linearWorkItemAdapterGuard(
		ctx, identity, effect, "work_items", adapter.Conn, len(rows),
	); err != nil {
		return err
	}
	for _, row := range rows {
		if row.OrgID != identity.OrgID || row.Provider != "linear" {
			return ErrInvalidConfiguration
		}
	}
	rows = dedupeBySortingKey(rows, linearWorkItemSortingKey)
	if len(rows) == 0 {
		return nil
	}
	batch, err := adapter.Conn.PrepareBatch(ctx, linearWorkItemsInsert)
	if err != nil {
		return err
	}
	defer batch.Abort()
	for _, row := range rows {
		if err := batch.Append(projectLinearWorkItem(row).values()...); err != nil {
			return err
		}
	}
	return batch.Send()
}

func (adapter LinearWorkItemsClickHouseAdapter) InspectLinearWorkItemEffect(
	ctx context.Context, identity LinearWorkItemEffectIdentity, effect EffectBatch,
) (EffectInspection, error) {
	rows, err := decodeEffectRows[linearWorkItemRow](effect)
	if err != nil {
		return EffectConflict, err
	}
	if err := linearWorkItemAdapterGuard(
		ctx, identity, effect, "work_items", adapter.Conn, len(rows),
	); err != nil {
		return EffectConflict, err
	}
	for _, row := range rows {
		if row.OrgID != identity.OrgID || row.Provider != "linear" {
			return EffectConflict, ErrInvalidConfiguration
		}
	}
	rows = dedupeBySortingKey(rows, linearWorkItemSortingKey)
	if len(rows) == 0 {
		return EffectAbsent, nil
	}
	return foldWorkItemInspections(rows, func(row linearWorkItemRow) (EffectInspection, error) {
		expected := projectLinearWorkItem(row)
		result, err := adapter.Conn.Query(
			ctx, linearWorkItemsSelect, expected.OrgID, expected.RepoID, expected.WorkItemID,
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
				&actual.SprintID, &actual.SprintName, &actual.ParentID, &actual.EpicID,
				&actual.URL, &actual.LastSynced, &actual.OrgID, &actual.SourceID,
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

const linearWorkItemTransitionsInsert = `INSERT INTO work_item_transitions (repo_id, work_item_id, occurred_at, from_status, to_status, from_status_raw, to_status_raw, actor, last_synced, org_id, source_id)`

const linearWorkItemTransitionsSelect = `SELECT repo_id, work_item_id, occurred_at, from_status, to_status, from_status_raw, to_status_raw, actor, last_synced, org_id, source_id FROM work_item_transitions FINAL WHERE org_id = ? AND repo_id = ? AND work_item_id = ? AND occurred_at = toDateTime64(?, 3, 'UTC')`

func projectLinearTransition(row linearWorkItemTransitionRow) workItemTransitionStoredRow {
	return workItemTransitionStoredRow{
		RepoID: uuid.Nil, WorkItemID: row.WorkItemID,
		OccurredAt: clickHouseMillis(row.OccurredAt), FromStatus: row.FromStatus,
		ToStatus: row.ToStatus, FromStatusRaw: derefString(row.FromStatusRaw),
		ToStatusRaw: derefString(row.ToStatusRaw), Actor: derefString(row.Actor),
		LastSynced: clickHouseMillis(row.LastSynced), OrgID: row.OrgID, SourceID: nil,
	}
}

type LinearWorkItemTransitionsClickHouseAdapter struct{ Conn driver.Conn }

func (adapter LinearWorkItemTransitionsClickHouseAdapter) WriteLinearWorkItemEffect(
	ctx context.Context, identity LinearWorkItemEffectIdentity, effect EffectBatch,
) error {
	rows, err := decodeEffectRows[linearWorkItemTransitionRow](effect)
	if err != nil {
		return err
	}
	if err := linearWorkItemAdapterGuard(
		ctx, identity, effect, "work_item_transitions", adapter.Conn, len(rows),
	); err != nil {
		return err
	}
	for _, row := range rows {
		if row.OrgID != identity.OrgID || row.Provider != "linear" {
			return ErrInvalidConfiguration
		}
	}
	rows = dedupeBySortingKey(rows, linearTransitionSortingKey)
	if len(rows) == 0 {
		return nil
	}
	batch, err := adapter.Conn.PrepareBatch(ctx, linearWorkItemTransitionsInsert)
	if err != nil {
		return err
	}
	defer batch.Abort()
	for _, row := range rows {
		if err := batch.Append(projectLinearTransition(row).values()...); err != nil {
			return err
		}
	}
	return batch.Send()
}

func (adapter LinearWorkItemTransitionsClickHouseAdapter) InspectLinearWorkItemEffect(
	ctx context.Context, identity LinearWorkItemEffectIdentity, effect EffectBatch,
) (EffectInspection, error) {
	rows, err := decodeEffectRows[linearWorkItemTransitionRow](effect)
	if err != nil {
		return EffectConflict, err
	}
	if err := linearWorkItemAdapterGuard(
		ctx, identity, effect, "work_item_transitions", adapter.Conn, len(rows),
	); err != nil {
		return EffectConflict, err
	}
	for _, row := range rows {
		if row.OrgID != identity.OrgID || row.Provider != "linear" {
			return EffectConflict, ErrInvalidConfiguration
		}
	}
	rows = dedupeBySortingKey(rows, linearTransitionSortingKey)
	if len(rows) == 0 {
		return EffectAbsent, nil
	}
	return foldWorkItemInspections(rows, func(row linearWorkItemTransitionRow) (EffectInspection, error) {
		expected := projectLinearTransition(row)
		result, err := adapter.Conn.Query(
			ctx, linearWorkItemTransitionsSelect, expected.OrgID, expected.RepoID,
			expected.WorkItemID, clickHouseMillisParam(expected.OccurredAt),
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
		if expected.RepoID != actual.RepoID || expected.WorkItemID != actual.WorkItemID ||
			!expected.OccurredAt.Equal(actual.OccurredAt) ||
			expected.FromStatus != actual.FromStatus || expected.ToStatus != actual.ToStatus ||
			expected.FromStatusRaw != actual.FromStatusRaw ||
			expected.ToStatusRaw != actual.ToStatusRaw || expected.Actor != actual.Actor ||
			expected.OrgID != actual.OrgID || !uuidPointersEqual(expected.SourceID, actual.SourceID) {
			return EffectConflict, nil
		}
		return EffectExact, nil
	})
}

func linearWorkItemSortingKey(row linearWorkItemRow) string {
	return strings.Join([]string{row.OrgID, uuid.Nil.String(), row.WorkItemID}, workItemKeySeparator)
}

func linearTransitionSortingKey(row linearWorkItemTransitionRow) string {
	return strings.Join([]string{
		row.OrgID, uuid.Nil.String(), row.WorkItemID,
		clickHouseMillis(row.OccurredAt).Format(time.RFC3339Nano),
	}, workItemKeySeparator)
}

var _ LinearWorkItemEffectAdapter = LinearWorkItemsClickHouseAdapter{}
var _ LinearWorkItemEffectAdapter = LinearWorkItemTransitionsClickHouseAdapter{}
