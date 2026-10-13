package compoundingrisk

import "testing"

// A repository with several main owners (a full ownership tie, D5990) counts
// whole in each of them: each owner's team row has the repository's inputs.
func TestBuildTeamRowsCountsARepositoryWholeInEachMainOwner(t *testing.T) {
	repoInputs := []RepoInputs{
		{RepoID: "repo-tied", Inputs: Inputs{ReworkChurn: ptr(0.5)}},
		{RepoID: "repo-single", Inputs: Inputs{ReworkChurn: ptr(0.1)}},
	}
	records := BuildTeamRows(
		goldenDay(), teamGoldenOrgID, repoInputs,
		map[string][]string{"repo-tied": {"team-b", "team-a"}, "repo-single": {"team-a"}}, goldenStamp(),
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
	// team-b holds repo-tied only (0.5); team-a holds both (mean 0.3).
	if a, b := byTeam["team-a"], byTeam["team-b"]; a.ReworkChurn == nil || b.ReworkChurn == nil || *b.ReworkChurn != 0.5 || *a.ReworkChurn < 0.2999 || *a.ReworkChurn > 0.3001 {
		t.Errorf("rework churn team-a %v team-b %v, want 0.3 and 0.5", a.ReworkChurn, b.ReworkChurn)
	}
}
