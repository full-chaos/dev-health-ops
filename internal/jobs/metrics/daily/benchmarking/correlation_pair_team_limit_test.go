package benchmarking

import (
	"context"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/teamscope"
)

type seriesByMetric map[string]map[string][]MetricPoint

func (series seriesByMetric) FetchMetricSeriesByScope(_ context.Context, metric string, _, _ time.Time, _ string) (map[string][]MetricPoint, error) {
	return series[metric], nil
}

// A team correlation pair takes the TestOps owner team at the run on one side and the stored work-item team on the
// other, and gives a row only where both are the same team id: a main owner whose items are its own has a row, a
// co-owner team and a team that only has work items have none.
func TestCorrelationPairNeedsTheSameTeamOnBothSides(t *testing.T) {
	asOf := time.Date(2026, 11, 15, 0, 0, 0, 0, time.UTC)
	var teamPairs [][3]string
	for _, pair := range DefaultCorrelationPairs {
		if pair[2] != ScopeTeam {
			continue
		}
		teamPairs = append(teamPairs, pair)
		for side, metric := range []string{pair[0], pair[1]} {
			statement, _ := recordTeamStatement(t, metric, asOf)
			ownership := strings.Contains(statement, teamscope.Marker) && strings.Contains(statement, "own.owner_team_id AS scope_key")
			stored := strings.Contains(statement, "argMax(tuple(team_id), computed_at)")
			if side == 0 && (!ownership || stored) {
				t.Errorf("%s: the TestOps side must take the owners at the run, not the stored team:\n%s", metric, statement)
			}
			if side == 1 && (ownership || !stored) {
				t.Errorf("%s: the work-item side must take the stored work-item team, not the owners:\n%s", metric, statement)
			}
		}
	}
	if len(teamPairs) != 2 {
		t.Fatalf("team correlation pairs = %v, want the 2 TestOps x work-item pairs", teamPairs)
	}

	points := func(values ...float64) []MetricPoint {
		series := make([]MetricPoint, len(values))
		for index, value := range values {
			series[index] = MetricPoint{Day: asOf.AddDate(0, 0, index-len(values)+1), Value: value}
		}
		return series
	}
	// Repository R: main owner team-main, co-owner team-co; its items are attributed to team-main.
	// team-items owns no repository and has work items only.
	repoSeries := points(0.10, 0.12, 0.15, 0.11, 0.20, 0.18)
	fetcher := seriesByMetric{}
	for _, pair := range teamPairs {
		fetcher[pair[0]] = map[string][]MetricPoint{"team-main": repoSeries, "team-co": repoSeries}
		fetcher[pair[1]] = map[string][]MetricPoint{
			"team-main":  points(10, 12, 16, 11, 22, 19),
			"team-items": points(5, 6, 7, 8, 9, 10),
		}
	}
	outputs, err := ComputeBenchmarkingForDay(context.Background(), fetcher, asOf, asOf, "org-pair-limit", nil, teamPairs, nil)
	if err != nil {
		t.Fatal(err)
	}
	rows := map[string][]string{}
	for _, record := range outputs.Correlations {
		rows[record.MetricName] = append(rows[record.MetricName], record.ScopeKey)
	}
	for _, pair := range teamPairs {
		keys := rows[pair[0]]
		sort.Strings(keys)
		if strings.Join(keys, ",") != "team-main" {
			t.Errorf("%s ~ %s: pair rows for %v, want team-main only (team-co and team-items have no pair row)", pair[0], pair[1], keys)
		}
	}
}
