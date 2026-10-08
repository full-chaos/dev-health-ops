package teamattribution

import (
	"fmt"
	"sort"
	"testing"
)

// catalogOrder sorts teams the way LoadTeams reads them: (provider, id). A
// team with no provider sorts first.
func catalogOrder(teams []GithubWorkItemDerivationTeamFact) []GithubWorkItemDerivationTeamFact {
	sort.SliceStable(teams, func(left, right int) bool {
		if teams[left].Provider != teams[right].Provider {
			return teams[left].Provider < teams[right].Provider
		}
		return teams[left].TeamID < teams[right].TeamID
	})
	return teams
}

func otherProviders(provider string) []string {
	var result []string
	for _, candidate := range coOwnerProviders {
		if candidate != provider {
			result = append(result, candidate)
		}
	}
	return result
}

func primaryRow(candidates []GithubWorkItemDerivationCandidate) (team, source string, count int) {
	for _, candidate := range candidates {
		if candidate.IsPrimary == AttributionPrimary {
			team, source = GithubWorkItemDerivationStringValue(candidate.TeamID), candidate.Source
			count++
		}
	}
	return team, source, count
}

// The issue_project primary is a team of the item's provider, also when a
// team of another provider, or a team with no provider, holds the same key
// and sorts first in the catalog. The other holders of the item's provider
// are co-owners; the holders of another provider or of no provider have no
// row. Every provider of the item against every other provider.
func TestIssueProjectPrimaryIsATeamOfTheItemsProvider(t *testing.T) {
	for _, provider := range coOwnerProviders {
		for _, other := range append(otherProviders(provider), "") {
			t.Run(provider+"/other="+other, func(t *testing.T) {
				key := "SHARED"
				derived := NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{
					Teams: catalogOrder([]GithubWorkItemDerivationTeamFact{
						{Provider: other, TeamID: "a-first-other", ProjectKeys: []string{key}},
						{Provider: provider, TeamID: "m-team-a", ProjectKeys: []string{key}},
						{Provider: provider, TeamID: "m-team-b", ProjectKeys: []string{key}},
						{Provider: other, TeamID: "z-last-other", ProjectKeys: []string{key}},
					}),
				})
				teamID, _, candidates := derived.Resolve(GithubWorkItemDerivationSubject{
					WorkItemID: provider + ":SHARED-1", Provider: provider, ProjectKey: &key, OrgID: "org",
				})
				if got := GithubWorkItemDerivationStringValue(teamID); got != "m-team-a" {
					t.Fatalf("Resolve team = %q, want m-team-a (first holder of the item's provider)", got)
				}
				byTeam := rowsByTeam(candidates)
				if got := marks(byTeam["m-team-a"]); fmt.Sprint(got) != "map[issue_project=1:1]" {
					t.Errorf("m-team-a marks = %v, want one issue_project primary row", got)
				}
				if got := marks(byTeam["m-team-b"]); fmt.Sprint(got) != "map[issue_project=2:1]" {
					t.Errorf("m-team-b marks = %v, want one issue_project co-owner row", got)
				}
				for _, team := range []string{"a-first-other", "z-last-other"} {
					if rows, present := byTeam[team]; present {
						t.Errorf("team of provider %q has rows %v on a %s item, want none", other, rows, provider)
					}
				}
				if _, _, count := primaryRow(candidates); count != 1 {
					t.Errorf("primary rows = %d, want 1", count)
				}
				assertEveryRowHasProvenance(t, candidates)
			})
		}
	}
}

// When only teams of other providers (and a team with no provider) hold the
// item's key, the issue_project tier gives no candidate and the cascade goes
// on to its next source: the project ownership of a team of the item's
// provider when there is one, else no team.
func TestAKeyHeldOnlyByOtherProvidersGivesNoIssueProjectRow(t *testing.T) {
	for _, provider := range coOwnerProviders {
		key := "ONLY"
		teams := []GithubWorkItemDerivationTeamFact{{Provider: "", TeamID: "admin-team", ProjectKeys: []string{key}}}
		for _, other := range otherProviders(provider) {
			teams = append(teams, GithubWorkItemDerivationTeamFact{Provider: other, TeamID: other + "-team", ProjectKeys: []string{key}})
		}
		subject := GithubWorkItemDerivationSubject{WorkItemID: provider + ":ONLY-1", Provider: provider, ProjectKey: &key, OrgID: "org"}
		assertNoIssueProjectRow := func(t *testing.T, candidates []GithubWorkItemDerivationCandidate) {
			t.Helper()
			for _, candidate := range candidates {
				if candidate.Source == "issue_project" {
					t.Errorf("issue_project row of team %q on a %s item, want none", GithubWorkItemDerivationStringValue(candidate.TeamID), provider)
				}
			}
		}
		t.Run(provider+"/next tier is project ownership", func(t *testing.T) {
			derived := NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{
				Teams: catalogOrder(append(append([]GithubWorkItemDerivationTeamFact{}, teams...),
					GithubWorkItemDerivationTeamFact{Provider: provider, TeamID: "owner-team"})),
				Projects: []GithubWorkItemDerivationProjectFact{
					{Provider: provider, TeamID: "owner-team", TeamName: "Owner", ProjectKey: &key, IsPrimary: 1, Specificity: 110, Priority: 10},
				},
			})
			teamID, _, candidates := derived.Resolve(subject)
			assertNoIssueProjectRow(t, candidates)
			team, source, count := primaryRow(candidates)
			if GithubWorkItemDerivationStringValue(teamID) != "owner-team" || team != "owner-team" || source != "project_ownership" || count != 1 {
				t.Errorf("primary = %q/%s (%d rows), want owner-team/project_ownership", team, source, count)
			}
			assertEveryRowHasProvenance(t, candidates)
		})
		t.Run(provider+"/no next tier", func(t *testing.T) {
			derived := NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{Teams: catalogOrder(append([]GithubWorkItemDerivationTeamFact{}, teams...))})
			teamID, _, candidates := derived.Resolve(subject)
			assertNoIssueProjectRow(t, candidates)
			if teamID != nil {
				t.Errorf("Resolve team = %q, want none", *teamID)
			}
			if _, source, _ := primaryRow(candidates); source != "unassigned" {
				t.Errorf("primary source = %q, want unassigned", source)
			}
		})
	}
}

// A team with no provider is not a holder of any provider's key, and an item
// with no provider matches no team, also not a team with no provider. The
// provider is compared trimmed, as AttributionMapKey compares it.
func TestIssueProjectProviderMatchIsExactAndNeverEmpty(t *testing.T) {
	key := "K"
	t.Run("item and team with no provider", func(t *testing.T) {
		derived := NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{
			Teams: []GithubWorkItemDerivationTeamFact{{Provider: "", TeamID: "admin-team", ProjectKeys: []string{key}}},
		})
		if got := derived.IssueProjectCandidates(GithubWorkItemDerivationSubject{WorkItemID: "K-1", Provider: "", ProjectKey: &key}); len(got) != 0 {
			t.Errorf("candidates = %+v, want none", got)
		}
		if got := derived.IssueProjectCandidates(GithubWorkItemDerivationSubject{WorkItemID: "K-1", Provider: "  ", ProjectKey: &key}); len(got) != 0 {
			t.Errorf("blank provider: candidates = %+v, want none", got)
		}
	})
	for _, provider := range coOwnerProviders {
		t.Run(provider+"/padded provider", func(t *testing.T) {
			derived := NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{
				Teams: []GithubWorkItemDerivationTeamFact{{Provider: " " + provider + " ", TeamID: "padded-team", ProjectKeys: []string{key}}},
			})
			got := derived.IssueProjectCandidates(GithubWorkItemDerivationSubject{WorkItemID: provider + ":K-1", Provider: provider + " ", ProjectKey: &key})
			if len(got) != 1 || GithubWorkItemDerivationStringValue(got[0].TeamID) != "padded-team" {
				t.Errorf("candidates = %+v, want padded-team", got)
			}
		})
	}
}

// An item looks up its scope key first and its project key second. A key
// held only by teams of another provider is not held for the item, so the
// lookup goes on to the next key. (A jira item has one key: its scope is its
// project key.)
func TestTheFirstKeyHeldByATeamOfTheItemsProviderDecides(t *testing.T) {
	for _, provider := range []string{"gitlab", "github", "linear"} {
		t.Run(provider, func(t *testing.T) {
			projectID, projectKey := "PID", "PKEY"
			teams := []GithubWorkItemDerivationTeamFact{{Provider: provider, TeamID: "key-team", ProjectKeys: []string{projectKey}}}
			for _, other := range otherProviders(provider) {
				teams = append(teams, GithubWorkItemDerivationTeamFact{Provider: other, TeamID: other + "-scope-team", ProjectKeys: []string{projectID}})
			}
			derived := NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{Teams: catalogOrder(teams)})
			got := derived.IssueProjectCandidates(GithubWorkItemDerivationSubject{
				WorkItemID: provider + ":PKEY-1", Provider: provider, ProjectID: &projectID, ProjectKey: &projectKey,
			})
			if len(got) != 1 || GithubWorkItemDerivationStringValue(got[0].TeamID) != "key-team" || got[0].Evidence != "issue_project_key=PKEY" {
				t.Errorf("candidates = %+v, want key-team with evidence issue_project_key=PKEY", got)
			}
		})
	}
}

// Teams are (provider, id): a team of another provider with the same id that
// holds the same key does not hide the item's own team from the key.
func TestATeamIDOfAnotherProviderDoesNotHideTheItemsTeam(t *testing.T) {
	for _, provider := range coOwnerProviders {
		for _, other := range otherProviders(provider) {
			t.Run(provider+"/other="+other, func(t *testing.T) {
				key := "ENG"
				derived := NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{
					Teams: catalogOrder([]GithubWorkItemDerivationTeamFact{
						{Provider: other, TeamID: "ENG", ProjectKeys: []string{key}},
						{Provider: provider, TeamID: "ENG", ProjectKeys: []string{key}},
					}),
				})
				got := derived.IssueProjectCandidates(GithubWorkItemDerivationSubject{WorkItemID: provider + ":ENG-1", Provider: provider, ProjectKey: &key})
				if len(got) != 1 || GithubWorkItemDerivationStringValue(got[0].TeamID) != "ENG" {
					t.Errorf("candidates = %+v, want the %s team ENG", got, provider)
				}
			})
		}
	}
}

// The native team key follows the same rule: the native_team row is the
// first team of the item's provider that holds the key. A team of another
// provider, or with no provider, that holds the key and sorts first is not
// the item's native team; when only such teams hold it, there is no
// native_team row. (Only linear items carry a native team key today; the
// rule is the same for every provider.)
func TestNativeTeamIsATeamOfTheItemsProvider(t *testing.T) {
	for _, provider := range coOwnerProviders {
		for _, other := range append(otherProviders(provider), "") {
			t.Run(provider+"/other="+other, func(t *testing.T) {
				native := "ENG"
				subject := GithubWorkItemDerivationSubject{WorkItemID: provider + ":ENG-1", Provider: provider, NativeTeamKey: &native, OrgID: "org"}
				holders := []GithubWorkItemDerivationTeamFact{{Provider: other, TeamID: "a-first-other", ProjectKeys: []string{native}}}
				derived := NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{Teams: catalogOrder(append(append([]GithubWorkItemDerivationTeamFact{}, holders...),
					GithubWorkItemDerivationTeamFact{Provider: provider, TeamID: "m-own-team", ProjectKeys: []string{native}}))})
				teamID, _, candidates := derived.Resolve(subject)
				team, source, count := primaryRow(candidates)
				if GithubWorkItemDerivationStringValue(teamID) != "m-own-team" || team != "m-own-team" || source != "native_team" || count != 1 {
					t.Errorf("primary = %q/%s (%d rows), want m-own-team/native_team", team, source, count)
				}
				if rows, present := rowsByTeam(candidates)["a-first-other"]; present {
					t.Errorf("team of provider %q has rows %v, want none", other, rows)
				}

				onlyOther := NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{Teams: catalogOrder(append([]GithubWorkItemDerivationTeamFact{}, holders...))})
				if candidate := onlyOther.NativeTeamCandidate(subject); candidate != nil {
					t.Errorf("native candidate = %+v, want none", *candidate)
				}
				teamID, _, candidates = onlyOther.Resolve(subject)
				if teamID != nil {
					t.Errorf("Resolve team = %q, want none", *teamID)
				}
				for _, candidate := range candidates {
					if candidate.Source == "native_team" || candidate.Source == "issue_project" {
						t.Errorf("%s row of team %q, want none", candidate.Source, GithubWorkItemDerivationStringValue(candidate.TeamID))
					}
				}
			})
		}
	}
}
