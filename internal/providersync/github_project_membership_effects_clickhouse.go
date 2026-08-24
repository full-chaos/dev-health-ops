package providersync

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/projectmembership"
)

// The two CHAOS-4194 effect destinations.
//
// These exist because the Projects V2 producer's rows previously reached the
// route and stopped there: buildGitHubWorkItemsRouteEffects serialized seven
// families and neither of these was among them, so board memberships were built
// and then discarded one layer past the normalizer that used to discard them.
// Adding the destinations is what makes the producer's output reachable at all.
//
// Both write through projectmembership rather than spelling their own SQL. The
// external-ingest sink writes the SAME two tables from a different entry point,
// and two independently maintained column lists agree only until someone edits
// one of them -- at which point every row is well-formed and every value is in
// the wrong column.

// -----------------------------------------------------------------------------
// project_membership_transitions
// -----------------------------------------------------------------------------

// GitHubProjectMembershipClickHouseAdapter writes board-membership rows.
//
// Unlike its sibling adapters there is no Python counterpart to mirror: Python
// drops PullRequest board items outright (normalize.py:514), so this
// destination is Go-only by construction rather than by porting order.
type GitHubProjectMembershipClickHouseAdapter struct{ Conn driver.Conn }

// The readback selects on the FULL sorting key. A narrower predicate would
// match a different row of the same subject's history -- the table is
// append-only and one subject legitimately holds many rows -- and the readback
// would then compare this effect's row against an unrelated earlier event and
// call a correct write a conflict.
const gitHubProjectMembershipSelect = `SELECT org_id, source_id, repo_id, subject_kind, subject_id, provider, ` +
	`from_project_id, to_project_id, from_project_key, to_project_key, actor, occurred_at, last_synced, event_id ` +
	`FROM project_membership_transitions FINAL WHERE org_id = ? AND subject_kind = ? AND repo_id = ? ` +
	`AND subject_id = ? AND occurred_at = toDateTime64(?, 3, 'UTC') AND event_id = ?`

func projectMembershipStored(row projectmembership.Row) projectmembership.Row {
	row.OccurredAt = clickHouseMillis(row.OccurredAt)
	row.LastSynced = clickHouseMillis(row.LastSynced)
	return row
}

func (adapter GitHubProjectMembershipClickHouseAdapter) WriteGitHubWorkItemEffect(
	ctx context.Context,
	identity GitHubWorkItemEffectIdentity,
	effect EffectBatch,
) error {
	rows, err := decodeEffectRows[projectmembership.Row](effect)
	if err != nil {
		return err
	}
	if err := workItemAdapterGuard(
		ctx, identity, effect, "project_membership_transitions", adapter.Conn, len(rows),
	); err != nil {
		return err
	}
	for _, row := range rows {
		if row.OrgID != identity.OrgID {
			return ErrInvalidConfiguration
		}
	}
	rows = dedupeBySortingKey(rows, projectMembershipSortingKey)
	if len(rows) == 0 {
		return nil
	}
	batch, err := adapter.Conn.PrepareBatch(ctx, projectmembership.TransitionsInsert)
	if err != nil {
		return err
	}
	defer batch.Abort()
	for _, row := range rows {
		if err := batch.Append(projectMembershipStored(row).Values()...); err != nil {
			return err
		}
	}
	return batch.Send()
}

func (adapter GitHubProjectMembershipClickHouseAdapter) InspectGitHubWorkItemEffect(
	ctx context.Context,
	identity GitHubWorkItemEffectIdentity,
	effect EffectBatch,
) (EffectInspection, error) {
	rows, err := decodeEffectRows[projectmembership.Row](effect)
	if err != nil {
		return EffectConflict, err
	}
	if err := workItemAdapterGuard(
		ctx, identity, effect, "project_membership_transitions", adapter.Conn, len(rows),
	); err != nil {
		return EffectConflict, err
	}
	for _, row := range rows {
		if row.OrgID != identity.OrgID {
			return EffectConflict, ErrInvalidConfiguration
		}
	}
	rows = dedupeBySortingKey(rows, projectMembershipSortingKey)
	if len(rows) == 0 {
		return emptyEffectInspection(), nil
	}
	return foldWorkItemInspections(rows, func(row projectmembership.Row) (EffectInspection, error) {
		expected := projectMembershipStored(row)
		result, err := adapter.Conn.Query(
			ctx, gitHubProjectMembershipSelect,
			expected.OrgID, expected.SubjectKind, expected.RepoID, expected.SubjectID,
			clickHouseMillisParam(expected.OccurredAt), expected.EventID,
		)
		if err != nil {
			return EffectConflict, err
		}
		defer result.Close()
		var actual projectmembership.Row
		found := 0
		for result.Next() {
			if err := result.Scan(
				&actual.OrgID, &actual.SourceID, &actual.RepoID, &actual.SubjectKind,
				&actual.SubjectID, &actual.Provider, &actual.FromProjectID,
				&actual.ToProjectID, &actual.FromProjectKey, &actual.ToProjectKey,
				&actual.Actor, &actual.OccurredAt, &actual.LastSynced, &actual.EventID,
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
		// Every non-key column is compared, not just the ones this producer
		// happens to populate. A column left out here is a column a future
		// producer could get wrong while the readback still answered EXACT.
		if expected.Provider != actual.Provider ||
			expected.FromProjectID != actual.FromProjectID ||
			expected.ToProjectID != actual.ToProjectID ||
			expected.FromProjectKey != actual.FromProjectKey ||
			expected.ToProjectKey != actual.ToProjectKey ||
			expected.Actor != actual.Actor ||
			!uuidPointersEqual(expected.SourceID, actual.SourceID) {
			return EffectConflict, nil
		}
		return EffectExact, nil
	})
}

func projectMembershipSortingKey(row projectmembership.Row) string {
	return projectMembershipStored(row).SortingKey()
}

// -----------------------------------------------------------------------------
// projects
// -----------------------------------------------------------------------------

// GitHubProjectCatalogClickHouseAdapter writes the `projects` row that makes a
// membership row's destination resolvable.
//
// It writes the BASE 051 columns only. 073/074 added linear-specific lifecycle
// columns, and this adapter leaves them at their defaults because it has no
// honest value for them -- see projectmembership.EnsureProjectsRow.
//
// A note on the readback's one real interaction: `projects` is a
// ReplacingMergeTree keyed (org_id, provider, id) versioned on updated_at, and
// the Linear reference catalogue writes the same table. A newer Linear row for
// the same key would win under FINAL and make this readback report a conflict
// for a write that was correct when it happened. That cannot arise today --
// this adapter only ever writes provider `github` with a `ghprojv2:` id, and
// Linear writes provider `linear` -- so the two never share a key. Recorded
// because the guarantee is about the ID SPACES being disjoint, not about
// anything this code enforces.
type GitHubProjectCatalogClickHouseAdapter struct{ Conn driver.Conn }

const gitHubProjectCatalogSelect = `SELECT id, org_id, provider, project_key, name, is_active, updated_at, last_synced ` +
	`FROM projects FINAL WHERE org_id = ? AND provider = ? AND id = ?`

func projectCatalogStored(row projectmembership.CatalogRow) projectmembership.CatalogRow {
	row.UpdatedAt = clickHouseMillis(row.UpdatedAt)
	row.LastSynced = clickHouseMillis(row.LastSynced)
	return row
}

func (adapter GitHubProjectCatalogClickHouseAdapter) WriteGitHubWorkItemEffect(
	ctx context.Context,
	identity GitHubWorkItemEffectIdentity,
	effect EffectBatch,
) error {
	rows, err := decodeEffectRows[projectmembership.CatalogRow](effect)
	if err != nil {
		return err
	}
	if err := workItemAdapterGuard(
		ctx, identity, effect, "projects", adapter.Conn, len(rows),
	); err != nil {
		return err
	}
	for _, row := range rows {
		if row.OrgID != identity.OrgID {
			return ErrInvalidConfiguration
		}
	}
	rows = dedupeBySortingKey(rows, projectCatalogSortingKey)
	if len(rows) == 0 {
		return nil
	}
	batch, err := adapter.Conn.PrepareBatch(ctx, projectmembership.ProjectsInsert)
	if err != nil {
		return err
	}
	defer batch.Abort()
	for _, row := range rows {
		if err := batch.Append(projectCatalogStored(row).Values()...); err != nil {
			return err
		}
	}
	return batch.Send()
}

func (adapter GitHubProjectCatalogClickHouseAdapter) InspectGitHubWorkItemEffect(
	ctx context.Context,
	identity GitHubWorkItemEffectIdentity,
	effect EffectBatch,
) (EffectInspection, error) {
	rows, err := decodeEffectRows[projectmembership.CatalogRow](effect)
	if err != nil {
		return EffectConflict, err
	}
	if err := workItemAdapterGuard(
		ctx, identity, effect, "projects", adapter.Conn, len(rows),
	); err != nil {
		return EffectConflict, err
	}
	for _, row := range rows {
		if row.OrgID != identity.OrgID {
			return EffectConflict, ErrInvalidConfiguration
		}
	}
	rows = dedupeBySortingKey(rows, projectCatalogSortingKey)
	if len(rows) == 0 {
		return emptyEffectInspection(), nil
	}
	return foldWorkItemInspections(rows, func(row projectmembership.CatalogRow) (EffectInspection, error) {
		expected := projectCatalogStored(row)
		result, err := adapter.Conn.Query(
			ctx, gitHubProjectCatalogSelect, expected.OrgID, expected.Provider, expected.ID,
		)
		if err != nil {
			return EffectConflict, err
		}
		defer result.Close()
		var actual projectmembership.CatalogRow
		var projectKey *string
		found := 0
		for result.Next() {
			if err := result.Scan(
				&actual.ID, &actual.OrgID, &actual.Provider, &projectKey,
				&actual.Name, &actual.IsActive, &actual.UpdatedAt, &actual.LastSynced,
			); err != nil {
				return EffectConflict, err
			}
			found++
		}
		if err := result.Err(); err != nil {
			return EffectConflict, err
		}
		// project_key is Nullable(String) in 051 while the shared row carries a
		// bare string, so a stored NULL reads back as "" -- which is exactly
		// what a github board writes, since a Projects V2 board has no key.
		if projectKey != nil {
			actual.ProjectKey = *projectKey
		}
		if verdict, final := workItemReadbackVerdict(found); final {
			return verdict, nil
		}
		// updated_at is this table's version column, not last_synced.
		if verdict, final := workItemVersionVerdict(actual.UpdatedAt, expected.UpdatedAt); final {
			return verdict, nil
		}
		if expected.ProjectKey != actual.ProjectKey || expected.Name != actual.Name ||
			expected.IsActive != actual.IsActive {
			return EffectConflict, nil
		}
		return EffectExact, nil
	})
}

func projectCatalogSortingKey(row projectmembership.CatalogRow) string {
	return projectCatalogStored(row).SortingKey()
}

// existenceOnlyProjectCatalogReadback answers "does the projects table already
// carry a resolvable row at this row's key", independent of which writer's
// version currently wins there. An ensure-only catalog writer -- Jira's and
// Linear's project-membership producers, unlike this file's own
// GitHubProjectCatalogClickHouseAdapter -- must use this instead of
// InspectGitHubWorkItemEffect's version-comparing readback above: its whole
// job is making a foreign key resolvable, and a newer row from ANY writer at
// that same key already satisfies that job just as well as its own row
// would. Comparing versions instead reports EffectConflict on a perfectly
// fine convergence and permanently wedges recovery the moment a second
// writer instance (a later sync unit, a richer reference-catalog sync) ever
// touches the same key first (codex review finding, CHAOS-4193).
func existenceOnlyProjectCatalogReadback(
	ctx context.Context, conn driver.Conn, rows []projectmembership.CatalogRow,
) (EffectInspection, error) {
	rows = dedupeBySortingKey(rows, projectCatalogSortingKey)
	if len(rows) == 0 {
		return emptyEffectInspection(), nil
	}
	return foldWorkItemInspections(rows, func(row projectmembership.CatalogRow) (EffectInspection, error) {
		result, err := conn.Query(ctx, gitHubProjectCatalogSelect, row.OrgID, row.Provider, row.ID)
		if err != nil {
			return EffectConflict, err
		}
		defer result.Close()
		found := 0
		for result.Next() {
			var discard projectmembership.CatalogRow
			var projectKey *string
			if err := result.Scan(
				&discard.ID, &discard.OrgID, &discard.Provider, &projectKey,
				&discard.Name, &discard.IsActive, &discard.UpdatedAt, &discard.LastSynced,
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
		return EffectExact, nil
	})
}

var _ GitHubWorkItemEffectAdapter = GitHubProjectMembershipClickHouseAdapter{}
var _ GitHubWorkItemEffectAdapter = GitHubProjectCatalogClickHouseAdapter{}
