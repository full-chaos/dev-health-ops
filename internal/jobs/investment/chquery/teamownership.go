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
	rows, err := reader.conn.Query(ctx, `
SELECT DISTINCT a.work_item_id, assumeNotNull(a.team_id), r.id
FROM (
    SELECT work_item_id, team_id, source, is_primary
    FROM work_item_team_attributions FINAL
    WHERE org_id = {org_id:String} AND work_item_id IN {work_item_ids:Array(String)}
      AND (work_item_id, computed_at) IN (
          SELECT work_item_id, max(computed_at)
          FROM work_item_team_attributions
          WHERE org_id = {org_id:String} AND work_item_id IN {work_item_ids:Array(String)}
          GROUP BY work_item_id
      )
) AS a
INNER JOIN (SELECT id FROM teams FINAL WHERE org_id = {org_id:String} AND is_active = 1) AS t
    ON t.id = a.team_id
INNER JOIN (
    SELECT team_id, provider, repo_id, repo_full_name
    FROM team_repo_ownership FINAL
    WHERE org_id = {org_id:String}
      AND source IN ('native', 'jira_legacy', 'provider_access', 'inferred')
      AND valid_from <= {as_of:DateTime64(3, 'UTC')}
      AND (valid_to IS NULL OR valid_to > {as_of:DateTime64(3, 'UTC')})
) AS o ON o.team_id = a.team_id
INNER JOIN (SELECT id, provider, repo FROM repos FINAL WHERE org_id = {org_id:String}) AS r
    ON r.provider = o.provider AND
       (r.id = o.repo_id OR (o.repo_id IS NULL AND lower(r.repo) = lower(o.repo_full_name)))
WHERE a.is_primary = 1 AND a.source IN ('native_team', 'issue_project', 'project_ownership', 'repo_ownership')
  AND a.team_id IS NOT NULL AND a.team_id != ''
  AND r.id != toUUID('00000000-0000-0000-0000-000000000000')
ORDER BY a.work_item_id, r.id, a.team_id`,
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
