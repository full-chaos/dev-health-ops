package teamattribution

import "testing"

// A provider team id carries a prefix (teamid.Of), so the native team key a
// work item carries is not the team's id. The cascade reaches the team
// through its native_team_key, for every provider: as the native_team row of
// an item that carries the key, and as the issue_project row of an item whose
// scope is the key. A team with the same native key of another provider is
// not reached.
func TestNativeTeamResolvesThroughTheNativeTeamKey(t *testing.T) {
	for _, provider := range coOwnerProviders {
		t.Run(provider, func(t *testing.T) {
			native := "ENG"
			own := provider + ":ENG"
			teams := []GithubWorkItemDerivationTeamFact{{Provider: provider, TeamID: own, NativeTeamKey: native}}
			for _, other := range otherProviders(provider) {
				teams = append(teams, GithubWorkItemDerivationTeamFact{Provider: other, TeamID: other + ":ENG", NativeTeamKey: native})
			}
			derived := NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{Teams: catalogOrder(teams)})

			subject := GithubWorkItemDerivationSubject{WorkItemID: provider + ":ENG-1", Provider: provider, NativeTeamKey: &native, OrgID: "org"}
			teamID, _, candidates := derived.Resolve(subject)
			team, source, count := primaryRow(candidates)
			if GithubWorkItemDerivationStringValue(teamID) != own || team != own || source != "native_team" || count != 1 {
				t.Errorf("native key: primary = %q/%s (%d rows), want %s/native_team", team, source, count, own)
			}

			scoped := GithubWorkItemDerivationSubject{WorkItemID: provider + ":ENG-2", Provider: provider, ProjectKey: &native, OrgID: "org"}
			found := false
			for _, candidate := range derived.IssueProjectCandidates(scoped) {
				if got := GithubWorkItemDerivationStringValue(candidate.TeamID); got != own {
					t.Errorf("issue_project candidate %q, want only %s", got, own)
				}
				found = true
			}
			if !found {
				t.Errorf("issue_project: no candidate for scope key %q, want %s", native, own)
			}

			bare := NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{Teams: []GithubWorkItemDerivationTeamFact{{Provider: provider, TeamID: own}}})
			if candidate := bare.NativeTeamCandidate(subject); candidate != nil {
				t.Errorf("a team without native_team_key is reached by key %q: %+v", native, *candidate)
			}
		})
	}
}
