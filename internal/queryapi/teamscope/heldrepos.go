package teamscope

import (
	"context"
	"fmt"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
)

// HeldRepoIDs are the given repository ids (already resolved to repositories of the
// organization) that the selected teams hold at asOf, by the one ownership rule of
// RepoCondition. It is the narrowing the repository-keyed reads get from
// NarrowRepoScope, for a read keyed by something else (the work items linked to a
// repository, CHAOS-9191): with a team AND named repositories, only the repositories
// the team holds are kept. No team ids = the ids unchanged.
func HeldRepoIDs(ctx context.Context, client RowQuerier, orgID string, repoIDs, teamIDs []string, asOf time.Time) ([]string, error) {
	var teams []string
	for _, id := range teamIDs {
		if id != "" {
			teams = append(teams, id)
		}
	}
	if len(teams) == 0 || len(repoIDs) == 0 {
		return repoIDs, nil
	}
	condition, bindings := RepoCondition(orgID, "toString(id)", teams, asOf)
	statement := fmt.Sprintf(`
SELECT DISTINCT toString(id) AS repo_id
FROM repos FINAL
WHERE org_id = {%s:String}
  AND toString(id) IN {named_repo_ids:Array(String)}
  AND %s
SETTINGS max_execution_time = 30
`, BindingOrgID, condition)
	rows, err := client.Query(ctx, statement, append([]dhclickhouse.Binding{{Name: "named_repo_ids", Value: repoIDs}}, bindings...))
	if err != nil {
		return nil, fmt.Errorf("teamscope: team-held repositories: %w", err)
	}
	defer rows.Close()
	var held []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("teamscope: scan team-held repository: %w", err)
		}
		held = append(held, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("teamscope: iterate team-held repositories: %w", err)
	}
	return held, nil
}
