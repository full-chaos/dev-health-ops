package providersync

import (
	"testing"

	"github.com/google/uuid"
)

// Co-owner share floor (chris, 2026-10-15): "Drop them if they're < 1%". A
// co-owner whose linked share of a repository is below 0.01 gets no row. The
// main-owner rule is unchanged: the main owner keeps its row whatever its share.

// TestACoOwnerBelowOnePercentGetsNoRow is the prod dev-health-acr shape: one
// main owner with many native links and teams with one text link each. A
// co-owner at exactly 1% keeps its row.
func TestACoOwnerBelowOnePercentGetsNoRow(t *testing.T) {
	for _, pair := range rankedOwnerProviderPairs {
		t.Run(pair[0]+"_over_"+pair[1], func(t *testing.T) {
			got := deriveRankedOwner(map[rankedOwnerTeam]map[string]int{
				{pair[0], "owner"}:    {"native": 293},
				{pair[1], "mobile"}:   {"explicit_text": 1},
				{pair[1], "payments"}: {"explicit_text": 1},
				{pair[0], "pandas"}:   {"heuristic": 2},
				{pair[1], "edge"}:     {"explicit_text": 3},
			})
			assertRepoOwners(t, got, []repoOwnerWant{
				{team: "owner", primary: teamRepoOwnershipMainOwner, share: 293.0 / 300},
				{team: "edge", primary: teamRepoOwnershipCoOwner, share: 3.0 / 300},
			}, nil)
		})
	}
}

// TestAMainOwnerBelowOnePercentKeepsItsRow: one native link outranks any
// number of text links, so the main owner can hold a share below 1%. The
// floor never removes a main owner.
func TestAMainOwnerBelowOnePercentKeepsItsRow(t *testing.T) {
	for _, pair := range rankedOwnerProviderPairs {
		t.Run(pair[0]+"_over_"+pair[1], func(t *testing.T) {
			got := deriveRankedOwner(map[rankedOwnerTeam]map[string]int{
				{pair[0], "owner"}: {"native": 1},
				{pair[1], "rival"}: {"explicit_text": 150},
			})
			assertRepoOwners(t, got, []repoOwnerWant{
				{team: "owner", primary: teamRepoOwnershipMainOwner, share: 1.0 / 151},
				{team: "rival", primary: teamRepoOwnershipCoOwner, share: 150.0 / 151},
			}, nil)
		})
	}
}

// TestTiedMainOwnersBelowOnePercentKeepTheirRows: every tied team is a main
// owner (D5990), so the floor does not apply to any of them.
func TestTiedMainOwnersBelowOnePercentKeepTheirRows(t *testing.T) {
	got := deriveRankedOwner(map[rankedOwnerTeam]map[string]int{
		{"linear", "a"}: {"native": 1},
		{"jira", "b"}:   {"native": 1},
		{"github", "c"}: {"explicit_text": 198},
	})
	assertRepoOwners(t, got, []repoOwnerWant{
		{team: "a", primary: teamRepoOwnershipMainOwner, share: 1.0 / 200},
		{team: "b", primary: teamRepoOwnershipMainOwner, share: 1.0 / 200},
		{team: "c", primary: teamRepoOwnershipCoOwner, share: 198.0 / 200},
	}, []TeamRepoOwnershipTie{{RepoID: "repo-1", TeamIDs: []string{"a", "b"}}})
}

// TestAnOpenCoOwnerRowBelowTheFloorIsRetracted: the co-owner rows written
// before the floor are retracted by the run's own diff, because the run no
// longer derives them.
func TestAnOpenCoOwnerRowBelowTheFloorIsRetracted(t *testing.T) {
	repo := uuid.New()
	repos := map[uuid.UUID]teamRepoOwnershipRepoInfo{repo: {FullName: "full-chaos/dev-health-acr", Provider: "github"}}
	projectLinks, workItems, issuePRLinks := rankedOwnerInputs(map[rankedOwnerTeam]map[string]int{
		{"linear", "owner"}: {"native": 199},
		{"jira", "mobile"}:  {"explicit_text": 1},
	})
	for index := range issuePRLinks {
		issuePRLinks[index].RepoID = repo.String()
	}
	derived := deriveTeamRepoOwnership("org-1", projectLinks, workItems, nil, issuePRLinks, nil).Rows
	active := []teamRepoOwnershipActiveRow{
		{Provider: "github", TeamID: "owner", RepoID: repo, RepoFullName: "full-chaos/dev-health-acr"},
		{Provider: "github", TeamID: "mobile", RepoID: repo, RepoFullName: "full-chaos/dev-health-acr"},
	}
	got := diffTeamRepoOwnershipRetractions(active, derived, repos)
	if len(got) != 1 || got[0].TeamID != "mobile" {
		t.Fatalf("retracted %+v, want only mobile's co-owner row", got)
	}
}

// TestAnOpenRowNoCurrentLinkSupportsIsRetracted: a co-owner whose links are
// gone (here the text links the native rule removed) is not derived, and its
// open row is retracted.
func TestAnOpenRowNoCurrentLinkSupportsIsRetracted(t *testing.T) {
	repo := uuid.New()
	repos := map[uuid.UUID]teamRepoOwnershipRepoInfo{repo: {FullName: "full-chaos/dev-health-deploy", Provider: "github"}}
	projectLinks, workItems, issuePRLinks := rankedOwnerInputs(map[rankedOwnerTeam]map[string]int{
		{"linear", "owner"}: {"native": 5},
	})
	projectLinks = append(projectLinks, TeamRepoOwnershipProjectLink{Provider: "jira", ProjectID: "web-proj", TeamID: "web", IsPrimary: true})
	for index := range issuePRLinks {
		issuePRLinks[index].RepoID = repo.String()
	}
	derived := deriveTeamRepoOwnership("org-1", projectLinks, workItems, nil, issuePRLinks, nil).Rows
	active := []teamRepoOwnershipActiveRow{
		{Provider: "github", TeamID: "owner", RepoID: repo, RepoFullName: "full-chaos/dev-health-deploy"},
		{Provider: "github", TeamID: "web", RepoID: repo, RepoFullName: "full-chaos/dev-health-deploy"},
	}
	got := diffTeamRepoOwnershipRetractions(active, derived, repos)
	if len(got) != 1 || got[0].TeamID != "web" {
		t.Fatalf("retracted %+v, want only web's row", got)
	}
}
