package quadrant

import (
	"context"
	"log/slog"
	"sort"
	"strings"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/activeteams"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/teamscope"
)

// ownerSharesQuery reads, for the named repositories, each active team that owns one with the share it holds
// (teamscope.OwnerShares, D5988: shares of one repository sum to 1 over ALL its owners; a team scope removes rows
// afterwards and never re-weighs those that stay).
func ownerSharesQuery(orgID string, repoNames, selectedTeams []string, asOf time.Time) (string, []dhclickhouse.Binding) {
	shares, bindings := teamscope.OwnerShares(orgID, selectedTeams, asOf)
	query := `
SELECT repos.repo AS repo_name, o.owner_team_id AS team_id, o.owner_team_label AS team_label, o.owner_share AS share
FROM repos FINAL
INNER JOIN ` + shares + ` AS o ON o.owner_repo_id = toString(repos.id)
WHERE repos.org_id = {org_id:String}
  AND repos.repo IN {owner_repo_names:Array(String)}
  AND o.owner_team_id IN (` + activeteams.IDsSubquery + `)
ORDER BY repo_name, team_id`
	bindings = append(bindings,
		dhclickhouse.Binding{Name: "org_id", Value: orgID},
		dhclickhouse.Binding{Name: "owner_repo_names", Value: repoNames},
	)
	return query, bindings
}

// attachOwnerShares sets OwnerShares on each repository point. A failed lookup degrades to points without owners
// (a consumer reads that as no team churn), it never fails the quadrant.
func attachOwnerShares(ctx context.Context, client QueryClient, orgID string, points []Point, selectedTeams []string) {
	if len(points) == 0 {
		return
	}
	names := make([]string, 0, len(points))
	for _, point := range points {
		names = append(names, point.EntityID)
	}
	sort.Strings(names)
	query, bindings := ownerSharesQuery(orgID, names, selectedTeams, time.Now().UTC())
	rows, err := client.Query(ctx, query, bindings)
	if err != nil {
		slog.WarnContext(ctx, "quadrant: could not read the repository owner shares", "org_id", orgID, "repos", len(names), "error", err)
		return
	}
	defer rows.Close()

	byRepo := map[string][]OwnerShare{}
	for rows.Next() {
		var repoName string
		var owner OwnerShare
		if err := rows.Scan(&repoName, &owner.TeamID, &owner.TeamLabel, &owner.Share); err != nil {
			slog.WarnContext(ctx, "quadrant: could not read the repository owner shares", "org_id", orgID, "error", err)
			return
		}
		repoName = strings.TrimSpace(repoName)
		if repoName == "" || owner.TeamID == "" {
			continue
		}
		byRepo[repoName] = append(byRepo[repoName], owner)
	}
	if err := rows.Err(); err != nil {
		slog.WarnContext(ctx, "quadrant: could not read the repository owner shares", "org_id", orgID, "error", err)
		return
	}
	owned := 0
	for i := range points {
		points[i].OwnerShares = byRepo[points[i].EntityID]
		if len(points[i].OwnerShares) > 0 {
			owned++
		}
	}
	slog.DebugContext(ctx, "quadrant: repository owner shares attached", "org_id", orgID, "repos", len(points), "owned", owned, "selected_teams", len(selectedTeams))
}
