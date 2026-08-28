package providersync

import (
	"context"
	"errors"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
)

// team_repo_ownership_derivation_clickhouse.go loads the already-synced
// inputs deriveTeamRepoOwnership needs and writes its output, for one org,
// against ClickHouse. Never fetches from a provider -- CHAOS-4365 item 1b.
// Triggered as a post-sync River job (see native_post_sync.go /
// sync_dispatch.go), not from within any provider's own sync route.

var ErrTeamRepoOwnershipDerivationUnavailable = errors.New("team_repo_ownership derivation is unavailable")

// TeamRepoOwnershipDerivationService reads already-synced team_project_ownership,
// work_items, work_item_dependencies and work_graph_issue_pr rows for one org,
// runs the pure derivation, and writes team_repo_ownership (source='inferred').
type TeamRepoOwnershipDerivationService struct {
	Conn driver.Conn
}

// teamRepoOwnershipRepoInfo is one org's repos-table row, keyed by repo_id --
// needed to stamp the REPO's own provider (not the work item's tracker
// provider) and repo_full_name onto every written row, since
// team_repo_ownership.source=='provider_access' rows (team_autoimport_github.py)
// are read back by a (org_id, provider, repo_full_name) NAME join
// (providers/teams.py::load_team_repo_ownership_map) and repo_full_name is
// part of this table's ReplacingMergeTree ORDER BY key -- leaving it blank
// would collide two different repos owned by the same team onto the same
// sort key and silently drop one on merge.
type teamRepoOwnershipRepoInfo struct {
	FullName string
	Provider string
}

// Derive loads orgID's already-synced ownership/linkage rows, runs the pure
// derivation, and writes the result to team_repo_ownership. Returns the
// number of rows written. A zero result with a nil error is the normal,
// expected outcome for an org with no team_project_ownership rows yet (a
// GitHub-only org, or an org with no team auto-import configured at all) --
// never an error.
func (service TeamRepoOwnershipDerivationService) Derive(ctx context.Context, orgID string) (int, error) {
	if service.Conn == nil || ctx == nil || orgID == "" {
		return 0, ErrTeamRepoOwnershipDerivationUnavailable
	}
	now := time.Now().UTC()

	projectLinks, err := loadTeamRepoOwnershipProjectLinks(ctx, service.Conn, orgID, now)
	if err != nil {
		return 0, err
	}
	if len(projectLinks) == 0 {
		// No project ownership synced for this org yet -- nothing to derive,
		// and no reason to touch work_items/repos at all.
		return 0, nil
	}

	repos, err := loadTeamRepoOwnershipRepos(ctx, service.Conn, orgID)
	if err != nil {
		return 0, err
	}
	if len(repos) == 0 {
		return 0, nil
	}
	repoIDs := make([]uuid.UUID, 0, len(repos))
	for id := range repos {
		repoIDs = append(repoIDs, id)
	}

	workItems, err := loadTeamRepoOwnershipWorkItems(ctx, service.Conn, orgID)
	if err != nil {
		return 0, err
	}

	dependencyEdges, err := loadTeamRepoOwnershipDependencyEdges(ctx, service.Conn, orgID)
	if err != nil {
		return 0, err
	}

	issuePRLinks, err := loadTeamRepoOwnershipIssuePRLinks(ctx, service.Conn, repoIDs)
	if err != nil {
		return 0, err
	}

	derived := deriveTeamRepoOwnership(projectLinks, workItems, dependencyEdges, issuePRLinks)
	if len(derived) == 0 {
		return 0, nil
	}
	written, err := writeTeamRepoOwnershipRows(ctx, service.Conn, orgID, now, derived, repos)
	if err != nil {
		return 0, err
	}
	return written, nil
}

func loadTeamRepoOwnershipProjectLinks(
	ctx context.Context, conn driver.Conn, orgID string, asOf time.Time,
) ([]TeamRepoOwnershipProjectLink, error) {
	rows, err := conn.Query(ctx, `
SELECT project_id, team_id, is_primary
FROM team_project_ownership FINAL
WHERE org_id = ?
  AND project_id != ''
  AND team_id != ''
  AND valid_from <= ?
  AND (valid_to IS NULL OR valid_to > ?)`,
		orgID, asOf, asOf)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []TeamRepoOwnershipProjectLink
	for rows.Next() {
		var link TeamRepoOwnershipProjectLink
		var isPrimary uint8
		if err := rows.Scan(&link.ProjectID, &link.TeamID, &isPrimary); err != nil {
			return nil, err
		}
		link.IsPrimary = isPrimary != 0
		out = append(out, link)
	}
	return out, rows.Err()
}

func loadTeamRepoOwnershipRepos(
	ctx context.Context, conn driver.Conn, orgID string,
) (map[uuid.UUID]teamRepoOwnershipRepoInfo, error) {
	rows, err := conn.Query(ctx, `
SELECT id, provider, repo
FROM repos FINAL
WHERE org_id = ?`,
		orgID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[uuid.UUID]teamRepoOwnershipRepoInfo{}
	for rows.Next() {
		var id uuid.UUID
		var info teamRepoOwnershipRepoInfo
		if err := rows.Scan(&id, &info.Provider, &info.FullName); err != nil {
			return nil, err
		}
		if id == uuid.Nil {
			continue
		}
		out[id] = info
	}
	return out, rows.Err()
}

func loadTeamRepoOwnershipWorkItems(
	ctx context.Context, conn driver.Conn, orgID string,
) ([]TeamRepoOwnershipWorkItem, error) {
	rows, err := conn.Query(ctx, `
SELECT work_item_id, provider, repo_id, project_id
FROM work_items FINAL
WHERE org_id = ?`,
		orgID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []TeamRepoOwnershipWorkItem
	for rows.Next() {
		var item TeamRepoOwnershipWorkItem
		var repoID uuid.UUID
		if err := rows.Scan(&item.WorkItemID, &item.Provider, &repoID, &item.ProjectID); err != nil {
			return nil, err
		}
		if repoID != uuid.Nil {
			item.RepoID = repoID.String()
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

func loadTeamRepoOwnershipDependencyEdges(
	ctx context.Context, conn driver.Conn, orgID string,
) ([]TeamRepoOwnershipDependencyEdge, error) {
	// work_item_dependencies (011_work_item_extras.sql) carries no org_id
	// column, so scoping goes through work_items -- only edges whose SOURCE
	// belongs to this org are useful anyway (a donor walk only ever needs to
	// resolve a source in this org; the target may legitimately be a
	// cross-provider extkey: form that never appears in work_items at all).
	rows, err := conn.Query(ctx, `
SELECT d.source_work_item_id, d.target_work_item_id, d.relationship_type, d.last_synced
FROM work_item_dependencies AS d FINAL
INNER JOIN (
    SELECT DISTINCT work_item_id FROM work_items FINAL WHERE org_id = ?
) AS source_scope
    ON source_scope.work_item_id = d.source_work_item_id`,
		orgID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []TeamRepoOwnershipDependencyEdge
	for rows.Next() {
		var edge TeamRepoOwnershipDependencyEdge
		if err := rows.Scan(
			&edge.SourceWorkItemID, &edge.TargetWorkItemID,
			&edge.RelationshipType, &edge.LastSynced,
		); err != nil {
			return nil, err
		}
		out = append(out, edge)
	}
	return out, rows.Err()
}

func loadTeamRepoOwnershipIssuePRLinks(
	ctx context.Context, conn driver.Conn, repoIDs []uuid.UUID,
) ([]TeamRepoOwnershipIssuePRLink, error) {
	if len(repoIDs) == 0 {
		return nil, nil
	}
	// work_graph_issue_pr (014_work_graph.sql) carries no org_id column
	// either; scope by this org's own repo_id set instead.
	rows, err := conn.Query(ctx, `
SELECT repo_id, work_item_id, pr_number
FROM work_graph_issue_pr FINAL
WHERE repo_id IN (?)`,
		repoIDs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []TeamRepoOwnershipIssuePRLink
	for rows.Next() {
		var link TeamRepoOwnershipIssuePRLink
		var repoID uuid.UUID
		if err := rows.Scan(&repoID, &link.WorkItemID, &link.PRNumber); err != nil {
			return nil, err
		}
		if repoID == uuid.Nil {
			continue
		}
		link.RepoID = repoID.String()
		out = append(out, link)
	}
	return out, rows.Err()
}

const teamRepoOwnershipInsert = `INSERT INTO team_repo_ownership (org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, priority, valid_from, valid_to, updated_at)`

// writeTeamRepoOwnershipRows batch-inserts the derived rows. A derived row
// whose repo_id has no matching repos entry (should not happen -- every
// RepoID came from either work_items.repo_id or work_graph_issue_pr.repo_id,
// both scoped to repos already loaded for this org -- but a race between the
// repos snapshot and the work_items/work_graph_issue_pr snapshot is possible
// under concurrent sync) is skipped rather than written with a blank
// repo_full_name, which would collide this table's ORDER BY key across
// unrelated repos owned by the same team.
func writeTeamRepoOwnershipRows(
	ctx context.Context,
	conn driver.Conn,
	orgID string,
	now time.Time,
	derived []DerivedTeamRepoOwnershipRow,
	repos map[uuid.UUID]teamRepoOwnershipRepoInfo,
) (int, error) {
	batch, err := conn.PrepareBatch(ctx, teamRepoOwnershipInsert)
	if err != nil {
		return 0, err
	}
	written := 0
	for _, row := range derived {
		repoID, err := uuid.Parse(row.RepoID)
		if err != nil || repoID == uuid.Nil {
			continue
		}
		info, ok := repos[repoID]
		if !ok || info.FullName == "" {
			continue
		}
		if err := batch.Append(
			orgID,
			info.Provider,
			row.TeamID,
			repoID,
			info.FullName,
			"exact",
			"inferred",
			uint8(1),
			row.Specificity,
			int32(0),
			now,
			nil,
			now,
		); err != nil {
			return written, err
		}
		written++
	}
	if written == 0 {
		return 0, nil
	}
	if err := batch.Send(); err != nil {
		return 0, err
	}
	return written, nil
}
