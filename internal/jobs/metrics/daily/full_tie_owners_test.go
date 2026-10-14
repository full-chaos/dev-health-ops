package daily

import (
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/numerical"
	"github.com/full-chaos/dev-health-ops/internal/teamownership"
)

// A repository with several main owners (a full ownership tie, D5990) is in
// each of them in every family whose daily table keys a row by team (whole for
// the families not yet on the owner-shares contract, at its equal share for team
// cognitive load);
// a TestOps table holds one row per repository, so it takes the first owner by
// rank.
func TestARepositoryWithSeveralMainOwnersCountsWholeInEach(t *testing.T) {
	tied, single := uuid.New(), uuid.New()
	owners := map[string][]string{tied.String(): {"team-b", "team-a"}, single.String(): {"team-c"}}

	t.Run("team cognitive load takes each owner at its share", func(t *testing.T) {
		shares := map[string][]teamownership.OwnerShare{
			tied.String():   {{TeamID: "team-b", Main: true, Share: 0.5}, {TeamID: "team-a", Main: true, Share: 0.5}},
			single.String(): {{TeamID: "team-c", Main: true, Share: 1}},
		}
		userRows := []userMetricsCognitiveLoadInput{
			{RepoID: tied, AuthorEmail: "a@example.com", PRInterruptionLoad: 3, ReviewRequestLoad: 1},
			{RepoID: single, AuthorEmail: "c@example.com", PRInterruptionLoad: 5, ReviewRequestLoad: 2},
		}
		got := map[string][2]float64{}
		for _, row := range buildTeamCognitiveLoadRows("acme", cognitiveLoadTestDay, userRows, nil, shares, cognitiveLoadTestComputedAt) {
			got[row.TeamID] = [2]float64{row.PRInterruptionLoad, row.ReviewRequestLoad}
		}
		want := map[string][2]float64{"team-a": {1.5, 0.5}, "team-b": {1.5, 0.5}, "team-c": {5, 2}}
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

	t.Run("testops takes the first owner", func(t *testing.T) {
		got := firstOwnerOfEach(owners)
		if len(got) != 2 || got[tied.String()] != "team-b" || got[single.String()] != "team-c" {
			t.Errorf("first owners %v, want team-b for the tied repo and team-c", got)
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
