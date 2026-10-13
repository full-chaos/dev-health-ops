package daily

import (
	"context"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/numerical"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/testops"
)

// A repository with several main owners (a full ownership tie, D5990) counts
// whole in each of them in every family whose daily table keys a row by team,
// and TestOps gets one row per main owner.
func TestARepositoryWithSeveralMainOwnersCountsWholeInEach(t *testing.T) {
	tied, single := uuid.New(), uuid.New()
	owners := map[string][]string{tied.String(): {"team-b", "team-a"}, single.String(): {"team-c"}}

	t.Run("team cognitive load", func(t *testing.T) {
		userRows := []userMetricsCognitiveLoadInput{
			{RepoID: tied, AuthorEmail: "a@example.com", PRInterruptionLoad: 3, ReviewRequestLoad: 1},
			{RepoID: single, AuthorEmail: "c@example.com", PRInterruptionLoad: 5, ReviewRequestLoad: 2},
		}
		got := map[string][2]float64{}
		for _, row := range buildTeamCognitiveLoadRows("acme", cognitiveLoadTestDay, userRows, nil, owners, cognitiveLoadTestComputedAt) {
			got[row.TeamID] = [2]float64{row.PRInterruptionLoad, row.ReviewRequestLoad}
		}
		want := map[string][2]float64{"team-a": {3, 1}, "team-b": {3, 1}, "team-c": {5, 2}}
		if len(got) != len(want) || got["team-a"] != want["team-a"] || got["team-b"] != want["team-b"] || got["team-c"] != want["team-c"] {
			t.Errorf("rows %v, want %v", got, want)
		}
	})

	t.Run("team complexity", func(t *testing.T) {
		rows := []repoComplexityInput{{RepoID: tied, LOCTotal: 2000, CyclomaticTotal: 100}, {RepoID: single, LOCTotal: 50, CyclomaticTotal: 5}}
		got := map[string]int64{}
		for _, row := range buildTeamComplexityRows("acme", teamComplexityTestDay, rows, owners, teamComplexityTestComputedAt) {
			got[row.TeamID] = int64(row.LOCTotal)
		}
		if len(got) != 3 || got["team-a"] != 2000 || got["team-b"] != 2000 || got["team-c"] != 50 {
			t.Errorf("loc by team %v, want team-a and team-b 2000, team-c 50", got)
		}
	})

	t.Run("testops gets one row per main owner", func(t *testing.T) {
		metrics := []testops.CoverageMetric{{RepoID: tied}, {RepoID: single}}
		got := fanOutOwners(context.Background(), "testops_coverage", metrics,
			func(m testops.CoverageMetric) uuid.UUID { return m.RepoID },
			func(m testops.CoverageMetric) *string { return m.TeamID },
			func(m *testops.CoverageMetric, team *string) { m.TeamID = team }, owners)
		teams := map[uuid.UUID][]string{}
		for _, metric := range got {
			teams[metric.RepoID] = append(teams[metric.RepoID], *metric.TeamID)
		}
		if len(got) != 3 || len(teams[tied]) != 2 || teams[tied][0] != "team-b" || teams[tied][1] != "team-a" || len(teams[single]) != 1 || teams[single][0] != "team-c" {
			t.Errorf("teams by repository %v, want team-b and team-a for the tied repo and team-c", teams)
		}
	})

	t.Run("testops keeps a team of the row and an unowned repository", func(t *testing.T) {
		own, none := "team-own", ""
		unowned := uuid.New()
		metrics := []testops.CoverageMetric{{RepoID: tied, TeamID: &own}, {RepoID: unowned, TeamID: &none}}
		got := fanOutOwners(context.Background(), "testops_coverage", metrics,
			func(m testops.CoverageMetric) uuid.UUID { return m.RepoID },
			func(m testops.CoverageMetric) *string { return m.TeamID },
			func(m *testops.CoverageMetric, team *string) { m.TeamID = team }, owners)
		if len(got) != 2 || *got[0].TeamID != "team-own" || *got[1].TeamID != "" {
			t.Errorf("rows %+v, want both rows unchanged", got)
		}
	})

	t.Run("wellbeing", func(t *testing.T) {
		resolver, _ := newOwnershipRepoResolver(owners, []WellbeingTeam{{ID: "team-a", Name: "A"}, {ID: "team-b", Name: "B"}})
		multi, ok := resolver.(numerical.RepoOwnersResolver)
		if !ok {
			t.Fatal("the ownership resolver does not name several owners")
		}
		if got := multi.ResolveRepoOwners(tied.String()); len(got) != 2 || got[0] != (numerical.TeamRef{ID: "team-b", Name: "B"}) || got[1] != (numerical.TeamRef{ID: "team-a", Name: "A"}) {
			t.Errorf("owners of the tied repo %v, want team-b then team-a with their names", got)
		}
		if id, name := resolver.ResolveRepo(tied.String()); id != "team-b" || name != "B" {
			t.Errorf("ResolveRepo of the tied repo = %q %q, want the first owner team-b B", id, name)
		}
	})
}
