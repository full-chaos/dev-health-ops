package chquery

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/ext"
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
//
// Every dedup here is load-bearing, because the outer DISTINCT removes
// duplicate tuples but cannot remove a tuple that only a stale version
// produces: a newer teams version deactivates a team, a newer ownership
// version retracts it, a newer repos version renames the repository away from
// a name-only ownership row, a same-instant attributions correction ties on
// computed_at, and an older attribution generation lives under a different
// sorting key. So the query does not shed FINALs; it evaluates each one once.
// ClickHouse inlines a WITH subquery at every reference, so only the ownership
// and repository resolution -- small, and genuinely needed once per branch --
// sits inside the UNION ALL. The attributions and teams reads are joined once
// to its result.
//
// The latest generation is picked with a window over the FINAL rows instead
// of a second scan feeding an IN set. The two agree: FINAL keeps every sorting
// key's highest computed_at, so the highest computed_at among the FINAL rows
// of a work item is the highest among all its physical rows.
//
// Both sides of the team_id join carry legitimate duplicates: a work item can
// hold several eligible primary rows for one team, and a team can reach one
// repository through several ownership rows (sources, validity windows, the id
// arm and the name arm). Collapsing them only after the join made the join
// emit the product of the two multiplicities, hundreds of millions of rows in
// production for a far smaller result. So each side is reduced to its distinct
// join keys first: (work item, team) after the team filter, and (team,
// repository) after both arms. A key is then unique on each side, every joined
// tuple is already distinct, and no DISTINCT is needed after the join. The
// ownership rows are also collapsed to their distinct matching columns before
// the arms join repositories, for the same reason one level down.
//
// The requested work item ids arrive as an external table, not an
// Array(String) parameter. The server substitutes a parameter into the query
// text before logging it, so hundreds of long ids made the logged query about
// 100 kB, cut at log_queries_cut_to_length before its first CTE ended: every
// form of this query then shared one normalized_query_hash and no text match
// could find it. An external table keeps the logged text to the query itself.
const teamRepoDonorsQuery = `
WITH attributions AS (
    SELECT work_item_id, team_id, source, is_primary
    FROM (
        SELECT work_item_id, team_id, source, is_primary, computed_at,
               max(computed_at) OVER (PARTITION BY work_item_id) AS latest_computed_at
        FROM work_item_team_attributions FINAL
        WHERE org_id = {org_id:String} AND work_item_id IN (SELECT work_item_id FROM donor_work_items)
    )
    WHERE computed_at = latest_computed_at
),
active_teams AS (
    SELECT id FROM teams FINAL WHERE org_id = {org_id:String} AND is_active = 1
),
donor_teams AS (
    SELECT DISTINCT a.work_item_id AS work_item_id, assumeNotNull(a.team_id) AS team_id
    FROM attributions AS a
    INNER JOIN active_teams AS t ON t.id = a.team_id
    WHERE a.is_primary = 1 AND a.source IN ('native_team', 'issue_project', 'project_ownership', 'repo_ownership')
      AND a.team_id IS NOT NULL AND a.team_id != ''
),
ownership AS (
    SELECT DISTINCT team_id, provider, repo_id, lower(repo_full_name) AS repo_name_lower
    FROM team_repo_ownership FINAL
    WHERE org_id = {org_id:String}
      AND source IN ('native', 'jira_legacy', 'provider_access', 'inferred')
      AND valid_from <= {as_of:DateTime64(3, 'UTC')}
      AND (valid_to IS NULL OR valid_to > {as_of:DateTime64(3, 'UTC')})
),
scoped_repos AS (
    SELECT id, provider, lower(repo) AS repo_lower FROM repos FINAL WHERE org_id = {org_id:String}
),
team_repos AS (
    SELECT DISTINCT team_id, repo_id
    FROM (
        SELECT o.team_id AS team_id, r.id AS repo_id
        FROM ownership AS o
        INNER JOIN scoped_repos AS r ON r.provider = o.provider AND r.id = o.repo_id
        WHERE o.repo_id IS NOT NULL

        UNION ALL

        SELECT o.team_id AS team_id, r.id AS repo_id
        FROM ownership AS o
        INNER JOIN scoped_repos AS r ON r.provider = o.provider AND r.repo_lower = o.repo_name_lower
        WHERE o.repo_id IS NULL
    )
    WHERE repo_id != toUUID('00000000-0000-0000-0000-000000000000')
)
SELECT d.work_item_id, d.team_id, tr.repo_id
FROM donor_teams AS d
INNER JOIN team_repos AS tr ON tr.team_id = d.team_id
ORDER BY d.work_item_id, tr.repo_id, d.team_id`

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
	queryCtx, err := withDonorWorkItems(ctx, ids)
	if err != nil {
		return nil, err
	}
	rows, err := reader.conn.Query(queryCtx, teamRepoDonorsQuery,
		clickhouse.Named("org_id", organizationID),
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

// withDonorWorkItems attaches the work item ids the donor query reads as its
// donor_work_items external table.
func withDonorWorkItems(ctx context.Context, workItemIDs []string) (context.Context, error) {
	table, err := ext.NewTable("donor_work_items", ext.Column("work_item_id", "String"))
	if err != nil {
		return nil, fmt.Errorf("build donor work item table: %w", err)
	}
	for _, id := range workItemIDs {
		if err := table.Append(id); err != nil {
			return nil, fmt.Errorf("build donor work item table: %w", err)
		}
	}
	return clickhouse.Context(ctx, clickhouse.WithExternalTable(table)), nil
}
