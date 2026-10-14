package compoundingrisk

import (
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/teamownership"
)

// A repository with several main owners (a full ownership tie, D5990) is in
// each of them: each owner's team row has the repository's own raw inputs.
func TestBuildTeamRowsCountsARepositoryWholeInEachMainOwner(t *testing.T) {
	repoInputs := []RepoInputs{
		{RepoID: "repo-tied", Inputs: Inputs{ReworkChurn: ptr(0.5)}},
		{RepoID: "repo-single", Inputs: Inputs{ReworkChurn: ptr(0.1)}},
	}
	records := BuildTeamRows(
		goldenDay(), teamGoldenOrgID, repoInputs,
		map[string][]teamownership.OwnerShare{
			"repo-tied":   {{TeamID: "team-b", Main: true, Share: 0.5}, {TeamID: "team-a", Main: true, Share: 0.5}},
			"repo-single": {{TeamID: "team-a", Main: true, Share: 1}},
		}, goldenStamp(),
		DefaultWeights, DefaultThresholds, DefaultReferences,
	)
	byTeam := map[string]Record{}
	for _, record := range records {
		byTeam[record.ScopeID] = record
	}
	if len(byTeam) != 2 {
		t.Fatalf("team rows %v, want team-a and team-b", byTeam)
	}
	if _, ok := byTeam["team-b"]; !ok {
		t.Fatalf("no row for team-b, a main owner of repo-tied: %v", byTeam)
	}
	// team-b holds repo-tied only (0.5); team-a holds both, repo-tied at its
	// share 0.5 and repo-single whole: (0.5 x 0.5 + 0.1 x 1) / 1.5 (D5998).
	if a, b := byTeam["team-a"], byTeam["team-b"]; a.ReworkChurn == nil || b.ReworkChurn == nil || *b.ReworkChurn != 0.5 || *a.ReworkChurn < 0.23333 || *a.ReworkChurn > 0.23334 {
		t.Errorf("rework churn team-a %v team-b %v, want 0.2333 and 0.5", a.ReworkChurn, b.ReworkChurn)
	}
}
