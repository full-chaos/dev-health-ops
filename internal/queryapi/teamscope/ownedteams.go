package teamscope

import (
	"context"
	"fmt"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
)

// QueryClient is the read OwnedTeams makes of ClickHouse.
type QueryClient interface {
	Query(ctx context.Context, statement string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error)
}

// OwnedTeamsMarker is a fragment of the ownership read's SQL that appears in no other statement in this binary,
// for a test double to recognise it by.
const OwnedTeamsMarker = "SELECT DISTINCT o.team_id AS owning_team_id"

// ownedTeamsSQL asks which of the requested teams own at least one repository at one instant, in one
// organization. It is RepoCondition's membership test, answered per team: the org predicate, the validity
// window and FINAL, and the repository-catalog join -- an ownership row whose repository is not in the
// catalog (an orphan, or a name the catalog does not hold) is not ownership, exactly as it is not for a
// repo-keyed read. The zero-UUID guard is the `1 AS matched` sentinel, for the reason RepoCondition gives.
const ownedTeamsSQL = `
        SELECT DISTINCT o.team_id AS owning_team_id
        FROM team_repo_ownership AS o FINAL
        LEFT JOIN (
            SELECT org_id, provider, id, repo, 1 AS matched
            FROM repos FINAL
            WHERE org_id = {team_scope_org_id:String}
        ) AS r
            ON r.org_id = o.org_id
               AND r.provider = o.provider
               AND lower(r.repo) = lower(o.repo_full_name)
        WHERE o.org_id = {team_scope_org_id:String}
          AND o.team_id IN {team_scope_ids:Array(String)}
          AND (o.repo_id IS NOT NULL OR r.matched = 1)
          AND o.valid_from <= {team_scope_as_of:DateTime64(3, 'UTC')}
          AND (o.valid_to IS NULL OR o.valid_to > {team_scope_as_of:DateTime64(3, 'UTC')})
          AND coalesce(toString(o.repo_id), toString(r.id)) IN (
              SELECT toString(id) AS id
              FROM repos FINAL
              WHERE org_id = {team_scope_org_id:String}
          )
    `

// DistinctTeamIDs returns the non-blank ids once each, in the order first seen.
func DistinctTeamIDs(teamIDs []string) []string {
	seen := make(map[string]struct{}, len(teamIDs))
	var out []string
	for _, id := range teamIDs {
		if id == "" {
			continue
		}
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}

// OwnedTeams returns, in the order requested, the teams that have an open ownership row in orgID at asOf,
// on a repository the catalog holds. Blank and repeated ids are dropped first. A team with none has no team
// answer: a caller reports it missing and never reads a team_id-keyed table for it. An empty request returns
// nil without a read; the caller decides what a supplied-but-empty list means.
func OwnedTeams(ctx context.Context, client QueryClient, orgID string, teamIDs []string, asOf time.Time) ([]string, error) {
	requested := DistinctTeamIDs(teamIDs)
	if len(requested) == 0 {
		return nil, nil
	}
	rows, err := client.Query(ctx, ownedTeamsSQL, []dhclickhouse.Binding{
		{Name: BindingOrgID, Value: orgID},
		{Name: BindingTeamIDs, Value: requested},
		{Name: BindingAsOf, Value: asOf.UTC()},
	})
	if err != nil {
		return nil, fmt.Errorf("teamscope: team ownership query: %w", err)
	}
	defer func() { _ = rows.Close() }()
	owned := make(map[string]struct{}, len(requested))
	for rows.Next() {
		var teamID string
		if scanErr := rows.Scan(&teamID); scanErr != nil {
			return nil, fmt.Errorf("teamscope: team ownership scan: %w", scanErr)
		}
		owned[teamID] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("teamscope: team ownership rows: %w", err)
	}
	var out []string
	for _, id := range requested {
		if _, ok := owned[id]; ok {
			out = append(out, id)
		}
	}
	return out, nil
}
