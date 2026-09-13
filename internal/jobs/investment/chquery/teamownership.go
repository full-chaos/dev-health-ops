package chquery

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
	"github.com/google/uuid"
)

// TeamRepoDonor is persisted ownership evidence for one member issue. A repo
// can have multiple donor teams; callers must union repo IDs before allocation.
type TeamRepoDonor struct {
	WorkItemID string
	TeamID     string
	RepoID     uuid.UUID
}

// teamRepoDonorsQuery joins repos to ownership on plain equality keys: an
// ownership row names its repository either by id or, when repo_id is unset,
// by provider+name, so the two cases are two equi-join branches unioned
// together rather than one join with an OR predicate. An OR across two
// unrelated columns is not a hash-join key, so ClickHouse previously fell
// back to a filtered cross product between repos and ownership.
const teamRepoDonorsQuery = `
WITH attributions AS (
    SELECT work_item_id, team_id, source, is_primary
    FROM work_item_team_attributions FINAL
    WHERE org_id = {org_id:String} AND work_item_id IN {work_item_ids:Array(String)}
      AND (work_item_id, computed_at) IN (
          SELECT work_item_id, max(computed_at)
          FROM work_item_team_attributions
          WHERE org_id = {org_id:String} AND work_item_id IN {work_item_ids:Array(String)}
          GROUP BY work_item_id
      )
),
active_teams AS (
    SELECT id FROM teams FINAL WHERE org_id = {org_id:String} AND is_active = 1
),
ownership AS (
    SELECT team_id, provider, repo_id, lower(repo_full_name) AS repo_name_lower
    FROM team_repo_ownership FINAL
    WHERE org_id = {org_id:String}
      AND source IN ('native', 'jira_legacy', 'provider_access', 'inferred')
      AND valid_from <= {as_of:DateTime64(3, 'UTC')}
      AND (valid_to IS NULL OR valid_to > {as_of:DateTime64(3, 'UTC')})
),
scoped_repos AS (
    SELECT id, provider, lower(repo) AS repo_lower FROM repos FINAL WHERE org_id = {org_id:String}
),
donors AS (
    SELECT a.work_item_id AS work_item_id, a.team_id AS team_id, a.source AS source,
           a.is_primary AS is_primary, r.id AS repo_id
    FROM attributions AS a
    INNER JOIN active_teams AS t ON t.id = a.team_id
    INNER JOIN ownership AS o ON o.team_id = a.team_id AND o.repo_id IS NOT NULL
    INNER JOIN scoped_repos AS r ON r.provider = o.provider AND r.id = o.repo_id

    UNION ALL

    SELECT a.work_item_id AS work_item_id, a.team_id AS team_id, a.source AS source,
           a.is_primary AS is_primary, r.id AS repo_id
    FROM attributions AS a
    INNER JOIN active_teams AS t ON t.id = a.team_id
    INNER JOIN ownership AS o ON o.team_id = a.team_id AND o.repo_id IS NULL
    INNER JOIN scoped_repos AS r ON r.provider = o.provider AND r.repo_lower = o.repo_name_lower
)
SELECT DISTINCT work_item_id, assumeNotNull(team_id), repo_id
FROM donors
WHERE is_primary = 1 AND source IN ('native_team', 'issue_project', 'project_ownership', 'repo_ownership')
  AND team_id IS NOT NULL AND team_id != ''
  AND repo_id != toUUID('00000000-0000-0000-0000-000000000000')
ORDER BY work_item_id, repo_id, team_id`

// FetchTeamRepoDonors reads only the latest primary attribution snapshot and
// live, sync-derived repository ownership. It never resolves person membership
// or accepts a manual mapping. linked_issue is excluded because its persisted
// evidence does not establish whether the donor was itself membership-derived.
// See docs/reference/data-models/investment.md for the allocation boundary.
func (reader *Reader) FetchTeamRepoDonors(ctx context.Context, workItemIDs []string, organizationID string, asOf time.Time) ([]TeamRepoDonor, error) {
	if reader == nil || reader.conn == nil || strings.TrimSpace(organizationID) == "" || asOf.IsZero() {
		return nil, ErrUnavailable
	}
	ids := dedupeStrings(workItemIDs)
	if len(ids) == 0 {
		return []TeamRepoDonor{}, nil
	}
	rows, err := reader.conn.Query(ctx, teamRepoDonorsQuery,
		clickhouse.Named("org_id", organizationID), clickhouse.Named("work_item_ids", ids),
		clickhouse.Named("as_of", asOf.UTC().Format("2006-01-02 15:04:05.000")))
	if err != nil {
		return nil, fmt.Errorf("query team repository donors: %w", err)
	}
	defer func() { _ = rows.Close() }()
	donors := make([]TeamRepoDonor, 0)
	for rows.Next() {
		var donor TeamRepoDonor
		if err := rows.Scan(&donor.WorkItemID, &donor.TeamID, &donor.RepoID); err != nil {
			return nil, fmt.Errorf("scan team repository donor: %w", err)
		}
		// Same decode every other fetcher in this package applies. The donor's
		// work_item_id is matched against component node ids that arrive from
		// the edge fetcher already hexed, so an undecoded donor spells the same
		// bytes differently, matches nothing, and the unit loses its allocation
		// with no error raised anywhere.
		donor.WorkItemID = pythonparity.DecodeClickHouseStringValue(donor.WorkItemID)
		donor.TeamID = pythonparity.DecodeClickHouseStringValue(donor.TeamID)
		donors = append(donors, donor)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate team repository donors: %w", err)
	}
	return donors, nil
}
