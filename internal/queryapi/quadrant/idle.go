package quadrant

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/activeteams"
)

type catalogTeam struct {
	ID   string
	Name string
}

// listTeamsInScope reads the teams a team-grain quadrant answers for: the
// ACTIVE team rows of the org (activeteams), narrowed to the selected teams
// when the request names some. A selected id that is no active team is not
// invented here.
func listTeamsInScope(ctx context.Context, client QueryClient, orgID string, selectedTeams []string) ([]catalogTeam, error) {
	query := `
            SELECT toString(id) AS team_id, name AS team_name
            FROM teams FINAL
            WHERE org_id = {org_id:String}
              AND id IN (` + activeteams.IDsSubquery + `)`
	bindings := []dhclickhouse.Binding{{Name: "org_id", Value: orgID}}
	if len(selectedTeams) > 0 {
		query += `
              AND toString(id) IN {scope_team_ids:Array(String)}`
		bindings = append(bindings, dhclickhouse.Binding{Name: "scope_team_ids", Value: selectedTeams})
	}
	rows, err := client.Query(ctx, query, bindings)
	if err != nil {
		return nil, fmt.Errorf("quadrant: list teams in scope query: %w", err)
	}
	defer rows.Close()
	var out []catalogTeam
	for rows.Next() {
		var team catalogTeam
		if err := rows.Scan(&team.ID, &team.Name); err != nil {
			return nil, fmt.Errorf("quadrant: list teams in scope scan: %w", err)
		}
		if team.ID != "" {
			out = append(out, team)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("quadrant: list teams in scope rows: %w", err)
	}
	return out, nil
}

// idleTeamPoints is one no-data point for every team in scope that has no
// plotted point: a team with no work in the window is a distinct "no data"
// answer, never an absent point and never a zero (check 12). The points follow
// the plotted ones, ordered by label then id.
func idleTeamPoints(ctx context.Context, client QueryClient, orgID string, selectedTeams []string, plotted map[string]bool, startDay, endDay time.Time, evidence string) ([]Point, int, error) {
	teams, err := listTeamsInScope(ctx, client, orgID, selectedTeams)
	if err != nil {
		return nil, 0, err
	}
	label := func(team catalogTeam) string {
		if team.Name == "" {
			return team.ID
		}
		return team.Name
	}
	sort.SliceStable(teams, func(i, j int) bool {
		if li, lj := label(teams[i]), label(teams[j]); li != lj {
			return li < lj
		}
		return teams[i].ID < teams[j].ID
	})
	var idle []Point
	for _, team := range teams {
		if plotted[team.ID] {
			continue
		}
		idle = append(idle, Point{
			EntityID:     team.ID,
			EntityLabel:  label(team),
			WindowStart:  startDay.Format("2006-01-02"),
			WindowEnd:    endDay.Format("2006-01-02"),
			EvidenceLink: evidence,
			NoData:       true,
		})
	}
	slog.Debug("quadrant: teams in scope without a point are sent as no-data points",
		"org_id", orgID, "teams_total", len(teams), "teams_idle", len(idle), "teams_plotted", len(teams)-len(idle),
		"teams_selected", len(selectedTeams))
	return idle, len(teams), nil
}
