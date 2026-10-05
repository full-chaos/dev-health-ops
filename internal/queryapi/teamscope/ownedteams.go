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

// OwnedTeamsMarker is a fragment of the ownership read's SQL that appears in no other statement in this binary, for a
// test double to recognise it by.
const OwnedTeamsMarker = "SELECT DISTINCT team_id\n        FROM team_repo_ownership FINAL"

// ownedTeamsSQL asks which of the requested teams have at least one ownership row open at one instant,
// in one organization. The org predicate is part of the statement: a team that owns a repository only in
// another organization owns nothing here. The validity window and FINAL are RepoCondition's, for the
// reasons its doc comment gives; the repository-catalog join is not part of this question (does the team
// own anything), only of RepoCondition's (which repositories).
const ownedTeamsSQL = `
        SELECT DISTINCT team_id
        FROM team_repo_ownership FINAL
        WHERE org_id = {team_scope_org_id:String}
          AND team_id IN {team_scope_ids:Array(String)}
          AND valid_from <= {team_scope_as_of:DateTime64(3, 'UTC')}
          AND (valid_to IS NULL OR valid_to > {team_scope_as_of:DateTime64(3, 'UTC')})
    `

// OwnedTeams returns, in the order requested, the teams that have an open ownership row in orgID at asOf.
// A team with none has no team answer: a caller reports it missing and never reads a team_id-keyed table
// for it. Empty and blank ids are dropped. An empty request returns nil without a read.
func OwnedTeams(ctx context.Context, client QueryClient, orgID string, teamIDs []string, asOf time.Time) ([]string, error) {
	var requested []string
	for _, id := range teamIDs {
		if id != "" {
			requested = append(requested, id)
		}
	}
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
