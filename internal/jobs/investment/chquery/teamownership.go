package chquery

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// TeamOwnedRepo is one work item's owning team and the single repository that
// team owns most specifically (CHAOS-5459).
//
// This has NO Python counterpart: `_allocate_repo_effort`
// (materialize.py:1011-1069) and its Go port (units.AllocateRepoEffort) both
// terminate at a NULL repo_id for a component with no commit/PR churn, and
// `resolve_repo_ids_for_teams` (queries.py:324-348) -- the only team->repo
// read Python's materializer ever did -- resolves the run's SCOPE from
// user_metrics_daily, never a per-unit attribution, and never touches
// team_repo_ownership at all.
type TeamOwnedRepo struct {
	// TeamID is team_repo_ownership.team_id / teams.id (e.g. "CHAOS"), not the
	// native key -- it is what the ownership row is keyed on, so it is the
	// honest provenance string for repo_source.
	TeamID string
	// RepoID is the winning repository, already a canonical UUID string.
	RepoID string
}

// FetchTeamOwnedRepos returns, for each requested work item, the repository
// owned by that item's native team -- or no entry at all when the item has no
// native team, the team is inactive, or the team owns no live repository.
//
// # WHY THE WHOLE RESOLUTION IS PUSHED INTO CLICKHOUSE
//
// team_repo_ownership is a validity-interval table: this org alone carries
// 2305 live rows for ONE team (one per derivation pass), and a 1000-org
// fleet multiplies that. Slicing it into Go to pick a winner per team is
// exactly the whole-partition-into-memory shape chris ruled out
// (2026-09-05 07:04/07:05 PDT). The join, the per-(team,repo) collapse, the
// ordering and the one-row-per-team pick all happen server-side; what
// crosses the wire is one row per REQUESTED WORK ITEM and nothing else.
//
// # THE WINNER IS DETERMINISTIC, AND IT IS ONE REPO, NOT A FAN-OUT
//
// A team owning six repositories does not mean an issue-only work unit
// touched six repositories. Splitting its effort across all of them would
// fabricate per-repo churn the data never observed and would inflate every
// downstream per-repo total. So exactly one repository is chosen, by
// `(is_primary DESC, specificity DESC, priority DESC, repo_id ASC)` -- an
// explicitly-declared primary beats a broad pattern beats a low-priority
// match, and the repo_id tail makes the choice stable across runs when every
// preceding key ties. `max()` per (team, repo) takes the STRONGEST live
// signal for that pair: a repo carrying both an `inferred` row and a `manual`
// primary row is judged on the manual one.
//
// An empty id list returns an empty map WITHOUT querying, matching the rest
// of this package -- an unguarded `IN ()` is both wasteful and a different
// query.
func (reader *Reader) FetchTeamOwnedRepos(
	ctx context.Context, workItemIDs []string, organizationID string,
) (map[string]TeamOwnedRepo, error) {
	if reader == nil || reader.conn == nil {
		return nil, ErrUnavailable
	}
	ids := dedupeStrings(workItemIDs)
	if len(ids) == 0 {
		return map[string]TeamOwnedRepo{}, nil
	}
	// An unscoped read here would fuse tenants: team_repo_ownership and teams
	// are both org-partitioned and `native_team_key` is NOT globally unique
	// (two orgs can both call a team "CORE"). scope.go already refuses an
	// empty org for the run as a whole; refuse it here too rather than
	// depending on that check staying upstream.
	if organizationID == "" {
		return nil, fmt.Errorf("%w: team-owned repo lookup requires an organization", ErrUnavailable)
	}

	const query = `
        WITH ownership AS (
            SELECT provider, native_team_key, team_id, repo_id
            FROM (
                SELECT
                    t.provider AS provider,
                    t.native_team_key AS native_team_key,
                    t.id AS team_id,
                    o.repo_id AS repo_id,
                    max(o.is_primary) AS is_primary,
                    max(o.specificity) AS specificity,
                    max(o.priority) AS priority
                FROM (
                    SELECT id, provider, native_team_key
                    FROM teams FINAL
                    WHERE org_id = {org_id:String}
                      AND is_active = 1
                      AND ifNull(native_team_key, '') != ''
                ) AS t
                INNER JOIN (
                    SELECT team_id, repo_id, is_primary, specificity, priority
                    FROM team_repo_ownership
                    WHERE org_id = {org_id:String}
                      AND valid_to IS NULL
                      AND repo_id IS NOT NULL
                ) AS o ON o.team_id = t.id
                GROUP BY provider, native_team_key, team_id, repo_id
                ORDER BY
                    provider ASC,
                    native_team_key ASC,
                    is_primary DESC,
                    specificity DESC,
                    priority DESC,
                    repo_id ASC
                LIMIT 1 BY provider, native_team_key
            )
        )
        SELECT
            w.work_item_id AS work_item_id,
            ownership.team_id AS team_id,
            toString(ownership.repo_id) AS repo_id
        FROM (
            SELECT work_item_id, provider, ifNull(native_team_key, '') AS native_team_key
            FROM ` + workItemsDeduped + `
            WHERE org_id = {org_id:String}
              AND work_item_id IN {work_item_ids:Array(String)}
        ) AS w
        INNER JOIN ownership
            ON ownership.provider = w.provider
            AND ownership.native_team_key = w.native_team_key
    `

	rows, err := reader.conn.Query(ctx, query,
		clickhouse.Named("org_id", organizationID),
		clickhouse.Named("work_item_ids", ids),
	)
	if err != nil {
		return nil, fmt.Errorf("query team_repo_ownership: %w", err)
	}
	defer func() { _ = rows.Close() }()

	owned := make(map[string]TeamOwnedRepo, len(ids))
	for rows.Next() {
		var workItemID, teamID, repoID string
		if err := rows.Scan(&workItemID, &teamID, &repoID); err != nil {
			return nil, fmt.Errorf("scan team_repo_ownership row: %w", err)
		}
		// Same hex-substitution discipline FetchWorkItems applies: work_item_id
		// feeds work_unit_id, so a value the two planes spell differently
		// re-addresses the row.
		workItemID = pythonparity.DecodeClickHouseStringValue(workItemID)
		teamID = pythonparity.DecodeClickHouseStringValue(teamID)
		repoID = pythonparity.DecodeClickHouseStringValue(repoID)
		if workItemID == "" || repoID == "" {
			continue
		}
		owned[workItemID] = TeamOwnedRepo{TeamID: teamID, RepoID: repoID}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate team_repo_ownership rows: %w", err)
	}
	return owned, nil
}
