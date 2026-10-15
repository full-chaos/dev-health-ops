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

// withCatalogTeams makes the team points the teams of the catalog: a plotted
// point whose id is no active team (a retired id that only survives in a
// metrics table, activeteams) is dropped, and every active team in scope without
// a plotted point is added as a no-data point. A team with no work in the window
// is a distinct "no data" answer, never an absent point and never a zero (check
// 12). No-data points follow the plotted ones, ordered by label then id.
func withCatalogTeams(ctx context.Context, client QueryClient, orgID string, selectedTeams []string, points []Point, startDay, endDay time.Time, evidence string) ([]Point, error) {
	teams, err := listTeamsInScope(ctx, client, orgID, selectedTeams)
	if err != nil {
		return nil, err
	}
	inCatalog := make(map[string]bool, len(teams))
	for _, team := range teams {
		inCatalog[team.ID] = true
	}
	plotted := make(map[string]bool, len(points))
	kept := make([]Point, 0, len(points))
	for _, point := range points {
		if !inCatalog[point.EntityID] && point.EntityID != activeteams.UnassignedID {
			continue
		}
		plotted[point.EntityID] = true
		kept = append(kept, point)
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
	idle := 0
	for _, team := range teams {
		if plotted[team.ID] {
			continue
		}
		idle++
		kept = append(kept, Point{
			EntityID:     team.ID,
			EntityLabel:  label(team),
			WindowStart:  startDay.Format("2006-01-02"),
			WindowEnd:    endDay.Format("2006-01-02"),
			EvidenceLink: evidence,
			NoData:       true,
		})
	}
	slog.Debug("quadrant: team points follow the active team catalog",
		"org_id", orgID, "teams_total", len(teams), "teams_idle", idle, "points_dropped_inactive", len(points)-len(plotted),
		"teams_selected", len(selectedTeams))
	return kept, nil
}
