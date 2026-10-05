package capacityforecast

import (
	"context"
	"fmt"
	"time"

	"github.com/full-chaos/dev-health-go/clickhouse"
)

// ownedTeamsSQL asks which of the requested teams own at least one repository at one instant: a team
// has an answer here only through team_repo_ownership, never through the team_id a metrics row happens
// to carry (internal/queryapi/teamscope: team means repository ownership). The validity window and FINAL
// are teamscope.RepoCondition's, for the reasons its doc comment gives; the repository-catalog join is
// not needed because the question is whether the team owns anything, not which repositories.
const ownedTeamsSQL = `
        SELECT DISTINCT team_id
        FROM team_repo_ownership FINAL
        WHERE org_id = {org_id:String}
          AND team_id IN {team_ids:Array(String)}
          AND valid_from <= {as_of:DateTime64(3, 'UTC')}
          AND (valid_to IS NULL OR valid_to > {as_of:DateTime64(3, 'UTC')})
    `

// ownedTeams returns, in the order requested, the teams that have an ownership row open at asOf.
// A team with none has no team answer: the caller reports it missing and never reads org data for it.
func ownedTeams(ctx context.Context, client QueryClient, orgID string, teamIDs []string, asOf time.Time) ([]string, error) {
	rows, err := client.Query(ctx, ownedTeamsSQL, []clickhouse.Binding{
		{Name: "org_id", Value: orgID},
		{Name: "team_ids", Value: teamIDs},
		{Name: "as_of", Value: asOf.UTC()},
	})
	if err != nil {
		return nil, fmt.Errorf("capacityForecast: team ownership query: %w", err)
	}
	defer func() { _ = rows.Close() }()
	owned := make(map[string]struct{}, len(teamIDs))
	for rows.Next() {
		var teamID string
		if scanErr := rows.Scan(&teamID); scanErr != nil {
			return nil, fmt.Errorf("capacityForecast: team ownership scan: %w", scanErr)
		}
		owned[teamID] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("capacityForecast: team ownership rows: %w", err)
	}
	var out []string
	for _, id := range teamIDs {
		if _, ok := owned[id]; ok {
			out = append(out, id)
		}
	}
	return out, nil
}
