package numerical

import (
	"testing"
	"time"
)

type ownersResolver map[string][]TeamRef

func (resolver ownersResolver) ResolveRepo(repo string) (string, string) {
	if owners := resolver[repo]; len(owners) > 0 {
		return owners[0].ID, owners[0].Name
	}
	return "", ""
}

func (resolver ownersResolver) ResolveRepoOwners(repo string) []TeamRef { return resolver[repo] }

// A repository with several main owners (a full ownership tie, D5990) counts
// whole in each of them: every commit is one commit of each owner.
func TestComputeTeamWellbeingCountsARepositoryWholeInEachMainOwner(t *testing.T) {
	day := time.Date(2026, 3, 3, 0, 0, 0, 0, time.UTC)
	commits := []Commit{
		{RepoID: "repo-1", AuthorEmail: "a@example.com", CommitterWhen: day.Add(10 * time.Hour)},
		{RepoID: "repo-1", AuthorEmail: "b@example.com", CommitterWhen: day.Add(22 * time.Hour)},
	}
	resolver := ownersResolver{"acme/repo": {{ID: "team-b", Name: "B"}, {ID: "team-a", Name: "A"}}}
	metrics := ComputeTeamWellbeing(day, commits, map[string]string{"repo-1": "acme/repo"}, resolver, nil, time.UTC, 9, 18)
	got := map[string][2]int{}
	for _, metric := range metrics {
		got[metric.TeamID+"/"+metric.TeamName] = [2]int{metric.CommitsCount, metric.AfterHoursCommitsCount}
	}
	want := map[string][2]int{"team-a/A": {2, 1}, "team-b/B": {2, 1}}
	if len(got) != 2 || got["team-a/A"] != want["team-a/A"] || got["team-b/B"] != want["team-b/B"] {
		t.Errorf("metrics %v, want %v", got, want)
	}
}
