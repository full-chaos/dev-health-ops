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

// A resolver that only names its owners (no shares) counts the repository whole
// in each of them: every commit is one commit of each owner.
func TestComputeTeamWellbeingCountsARepositoryWholeInEachOwnerWithoutShares(t *testing.T) {
	day := time.Date(2026, 3, 3, 0, 0, 0, 0, time.UTC)
	commits := []Commit{
		{RepoID: "repo-1", AuthorEmail: "a@example.com", CommitterWhen: day.Add(10 * time.Hour)},
		{RepoID: "repo-1", AuthorEmail: "b@example.com", CommitterWhen: day.Add(22 * time.Hour)},
	}
	resolver := ownersResolver{"acme/repo": {{ID: "team-b", Name: "B"}, {ID: "team-a", Name: "A"}}}
	metrics := ComputeTeamWellbeing(day, commits, map[string]string{"repo-1": "acme/repo"}, resolver, nil, time.UTC, 9, 18)
	got := map[string][2]float64{}
	for _, metric := range metrics {
		got[metric.TeamID+"/"+metric.TeamName] = [2]float64{metric.CommitsCount, metric.AfterHoursCommitsCount}
	}
	want := map[string][2]float64{"team-a/A": {2, 1}, "team-b/B": {2, 1}}
	if len(got) != 2 || got["team-a/A"] != want["team-a/A"] || got["team-b/B"] != want["team-b/B"] {
		t.Errorf("metrics %v, want %v", got, want)
	}
}

type shareOwnersResolver map[string][]ShareTeamRef

func (resolver shareOwnersResolver) ResolveRepo(repo string) (string, string) {
	if owners := resolver[repo]; len(owners) > 0 {
		return owners[0].ID, owners[0].Name
	}
	return "", ""
}

func (resolver shareOwnersResolver) ResolveRepoOwnerShares(repo string) []ShareTeamRef {
	return resolver[repo]
}

// CHAOS-9601, D5988: the main owner and every lesser owner take the commits of
// a repository at their own linked share; the ratios are those of the repository;
// nothing is rounded (D5991).
func TestComputeTeamWellbeingTakesEachOwnerAtItsShare(t *testing.T) {
	day := time.Date(2026, 3, 3, 0, 0, 0, 0, time.UTC)
	commits := []Commit{
		{RepoID: "repo-1", AuthorEmail: "a@example.com", CommitterWhen: day.Add(10 * time.Hour)},
		{RepoID: "repo-1", AuthorEmail: "b@example.com", CommitterWhen: day.Add(22 * time.Hour)},
		{RepoID: "repo-1", AuthorEmail: "c@example.com", CommitterWhen: day.Add(23 * time.Hour)},
		{RepoID: "repo-1", AuthorEmail: "d@example.com", CommitterWhen: day.Add(11 * time.Hour)},
	}
	resolver := shareOwnersResolver{"acme/repo": {
		{TeamRef: TeamRef{ID: "main", Name: "Main"}, Share: 0.7},
		{TeamRef: TeamRef{ID: "lesser", Name: "Lesser"}, Share: 0.3},
	}}
	metrics := ComputeTeamWellbeing(day, commits, map[string]string{"repo-1": "acme/repo"}, resolver, nil, time.UTC, 9, 18)
	got := map[string]TeamWellbeingMetric{}
	for _, metric := range metrics {
		got[metric.TeamID] = metric
	}
	near := func(a, b float64) bool { return a-b < 1e-9 && b-a < 1e-9 }
	main, lesser := got["main"], got["lesser"]
	if len(got) != 2 || !near(main.CommitsCount, 2.8) || !near(main.AfterHoursCommitsCount, 1.4) ||
		!near(lesser.CommitsCount, 1.2) || !near(lesser.AfterHoursCommitsCount, 0.6) {
		t.Fatalf("metrics %+v, want main 2.8 commits / 1.4 after hours, lesser 1.2 / 0.6", got)
	}
	if !near(main.AfterHoursCommitRatio, 0.5) || !near(lesser.AfterHoursCommitRatio, 0.5) {
		t.Errorf("ratios %v %v, want the repository's own 0.5 for both owners", main.AfterHoursCommitRatio, lesser.AfterHoursCommitRatio)
	}
}
