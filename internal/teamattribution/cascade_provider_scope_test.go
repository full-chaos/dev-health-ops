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

// When only teams of other providers hold the item's key, the native_team and
// issue_project tiers give no candidate and the cascade goes on to its next
// source: the project ownership of a team of the item's provider when there is
// one, else no team. Both tiers: an item with a project key, and an item with
// the same string as its native team key too.
func TestAKeyHeldOnlyByOtherProvidersGivesNoKeyTierRow(t *testing.T) {
	for _, provider := range coOwnerProviders {
		for _, tier := range []string{"issue_project", "native_team"} {
			key := "ONLY"
			var teams []GithubWorkItemDerivationTeamFact
			for _, other := range otherProviders(provider) {
				teams = append(teams, GithubWorkItemDerivationTeamFact{Provider: other, TeamID: other + "-team", ProjectKeys: []string{key}})
			}
			subject := GithubWorkItemDerivationSubject{WorkItemID: provider + ":ONLY-1", Provider: provider, ProjectKey: &key, OrgID: "org"}
			if tier == "native_team" {
				subject.NativeTeamKey = &key
			}
			assertNoKeyTierRow := func(t *testing.T, candidates []GithubWorkItemDerivationCandidate) {
				t.Helper()
				for _, candidate := range candidates {
					if candidate.Source == "issue_project" || candidate.Source == "native_team" {
						t.Errorf("%s row of team %q on a %s item, want none", candidate.Source, GithubWorkItemDerivationStringValue(candidate.TeamID), provider)
					}
				}
			}
			t.Run(provider+"/"+tier+"/next tier is project ownership", func(t *testing.T) {
				derived := NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{
					Teams: catalogOrder(append(append([]GithubWorkItemDerivationTeamFact{}, teams...),
						GithubWorkItemDerivationTeamFact{Provider: provider, TeamID: "owner-team"})),
					Projects: []GithubWorkItemDerivationProjectFact{
						{Provider: provider, TeamID: "owner-team", TeamName: "Owner", ProjectKey: &key, IsPrimary: 1, Specificity: 110, Priority: 10},
					},
				})
				teamID, _, candidates := derived.Resolve(subject)
				assertNoKeyTierRow(t, candidates)
				team, source, count := primaryRow(candidates)
				if GithubWorkItemDerivationStringValue(teamID) != "owner-team" || team != "owner-team" || source != "project_ownership" || count != 1 {
					t.Errorf("primary = %q/%s (%d rows), want owner-team/project_ownership", team, source, count)
				}
				assertEveryRowHasProvenance(t, candidates)
			})
			t.Run(provider+"/"+tier+"/no next tier", func(t *testing.T) {
				derived := NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{Teams: catalogOrder(append([]GithubWorkItemDerivationTeamFact{}, teams...))})
				teamID, _, candidates := derived.Resolve(subject)
				assertNoKeyTierRow(t, candidates)
				if teamID != nil {
					t.Errorf("Resolve team = %q, want none", *teamID)
				}
				if _, source, _ := primaryRow(candidates); source != "unassigned" {
					t.Errorf("primary source = %q, want unassigned", source)
				}
			})
		}
	}
}

// An admin team (provider "": admin create and admin import write it so) that
// holds the item's key takes the item when no team of the item's provider
// holds the key, also when teams of other providers hold it and sort first.
// It is a key-tier row, so it outranks the project ownership of a team of the
// item's provider (as on main). It loses the key to a team of the item's
// provider, and a team of another provider never takes it. Every provider,
// both tiers.
func TestAnAdminTeamHoldsTheKeyOnlyWhenNoTeamOfTheItemsProviderDoes(t *testing.T) {
	for _, provider := range coOwnerProviders {
		for _, tier := range []string{"issue_project", "native_team"} {
			key := "ENG"
			subject := GithubWorkItemDerivationSubject{WorkItemID: provider + ":ENG-1", Provider: provider, OrgID: "org"}
			if tier == "native_team" {
				subject.NativeTeamKey = &key
			} else {
				subject.ProjectKey = &key
			}
			holders := []GithubWorkItemDerivationTeamFact{
				{Provider: "", TeamID: "z-admin-team", TeamName: "Admin", ProjectKeys: []string{key}},
				{Provider: "", TeamID: "zz-admin-team", TeamName: "Admin 2", ProjectKeys: []string{key}},
			}
			for _, other := range otherProviders(provider) {
				holders = append(holders, GithubWorkItemDerivationTeamFact{Provider: other, TeamID: "a-" + other + "-team", ProjectKeys: []string{key}})
			}
			t.Run(provider+"/"+tier+"/admin team takes the item", func(t *testing.T) {
				derived := NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{
					Teams: catalogOrder(append(append([]GithubWorkItemDerivationTeamFact{}, holders...),
						GithubWorkItemDerivationTeamFact{Provider: provider, TeamID: "owner-team"})),
					Projects: []GithubWorkItemDerivationProjectFact{
						{Provider: provider, TeamID: "owner-team", TeamName: "Owner", ProjectKey: &key, IsPrimary: 1, Specificity: 110, Priority: 10},
					},
				})
				teamID, _, candidates := derived.Resolve(subject)
				team, source, count := primaryRow(candidates)
				if GithubWorkItemDerivationStringValue(teamID) != "z-admin-team" || team != "z-admin-team" || source != tier || count != 1 {
					t.Errorf("primary = %q/%s (%d rows), want z-admin-team/%s", team, source, count, tier)
				}
				byTeam := rowsByTeam(candidates)
				for _, other := range otherProviders(provider) {
					if rows, present := byTeam["a-"+other+"-team"]; present {
						t.Errorf("team of provider %s has rows %v on a %s item, want none", other, rows, provider)
					}
				}
				wantSecond := map[string]string{"issue_project": "map[issue_project=2:1]", "native_team": "map[]"}[tier]
				if got := marks(byTeam["zz-admin-team"]); fmt.Sprint(got) != wantSecond {
					t.Errorf("second admin team marks = %v, want %s", got, wantSecond)
				}
				assertEveryRowHasProvenance(t, candidates)
			})
			t.Run(provider+"/"+tier+"/admin team loses to a team of the item's provider", func(t *testing.T) {
				derived := NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{
					Teams: catalogOrder(append(append([]GithubWorkItemDerivationTeamFact{}, holders...),
						GithubWorkItemDerivationTeamFact{Provider: provider, TeamID: "zzz-own-team", ProjectKeys: []string{key}})),
				})
				teamID, _, candidates := derived.Resolve(subject)
				team, source, count := primaryRow(candidates)
				if GithubWorkItemDerivationStringValue(teamID) != "zzz-own-team" || team != "zzz-own-team" || source != tier || count != 1 {
					t.Errorf("primary = %q/%s (%d rows), want zzz-own-team/%s", team, source, count, tier)
				}
				for id, rows := range rowsByTeam(candidates) {
					if id != "zzz-own-team" {
						t.Errorf("team %s has rows %v, want none", id, rows)
					}
				}
			})
		}
	}
}

// An item with no provider takes the teams with no provider that hold its key,
// never a team of a provider. The provider is compared trimmed, as
// AttributionMapKey compares it. Both tiers.
func TestTheProviderMatchIsTrimmedAndAnItemWithNoProviderTakesAnAdminTeam(t *testing.T) {
	key := "K"
	t.Run("item with no provider", func(t *testing.T) {
		teams := []GithubWorkItemDerivationTeamFact{{Provider: "", TeamID: "admin-team", ProjectKeys: []string{key}}}
		for _, provider := range coOwnerProviders {
			teams = append(teams, GithubWorkItemDerivationTeamFact{Provider: provider, TeamID: "a-" + provider, ProjectKeys: []string{key}})
		}
		derived := NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{Teams: catalogOrder(teams)})
		for _, provider := range []string{"", "  "} {
			got := derived.IssueProjectCandidates(GithubWorkItemDerivationSubject{WorkItemID: "K-1", Provider: provider, ProjectKey: &key})
			if len(got) != 1 || GithubWorkItemDerivationStringValue(got[0].TeamID) != "admin-team" {
				t.Errorf("provider %q: candidates = %+v, want admin-team only", provider, got)
			}
			native := derived.NativeTeamCandidate(GithubWorkItemDerivationSubject{WorkItemID: "K-1", Provider: provider, NativeTeamKey: &key})
			if native == nil || GithubWorkItemDerivationStringValue(native.TeamID) != "admin-team" {
				t.Errorf("provider %q: native candidate = %+v, want admin-team", provider, native)
			}
		}
		onlyProviders := NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{Teams: catalogOrder(teams[1:])})
		if got := onlyProviders.IssueProjectCandidates(GithubWorkItemDerivationSubject{WorkItemID: "K-1", ProjectKey: &key}); len(got) != 0 {
			t.Errorf("no admin team: candidates = %+v, want none", got)
		}
		if got := onlyProviders.NativeTeamCandidate(GithubWorkItemDerivationSubject{WorkItemID: "K-1", NativeTeamKey: &key}); got != nil {
			t.Errorf("no admin team: native candidate = %+v, want none", *got)
		}
	})
	for _, provider := range coOwnerProviders {
		t.Run(provider+"/padded provider", func(t *testing.T) {
			derived := NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{
				Teams: []GithubWorkItemDerivationTeamFact{
					{Provider: "", TeamID: "admin-team", ProjectKeys: []string{key}},
					{Provider: " " + provider + " ", TeamID: "padded-team", ProjectKeys: []string{key}},
				},
			})
			got := derived.IssueProjectCandidates(GithubWorkItemDerivationSubject{WorkItemID: provider + ":K-1", Provider: provider + " ", ProjectKey: &key})
			if len(got) != 1 || GithubWorkItemDerivationStringValue(got[0].TeamID) != "padded-team" {
				t.Errorf("candidates = %+v, want padded-team", got)
			}
			native := derived.NativeTeamCandidate(GithubWorkItemDerivationSubject{WorkItemID: provider + ":K-1", Provider: provider + " ", NativeTeamKey: &key})
			if native == nil || GithubWorkItemDerivationStringValue(native.TeamID) != "padded-team" {
				t.Errorf("native candidate = %+v, want padded-team", native)
			}
		})
		t.Run(provider+"/one team listed twice with a padded provider holds the key once", func(t *testing.T) {
			derived := NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{
				Teams: []GithubWorkItemDerivationTeamFact{
					{Provider: provider, TeamID: "team", ProjectKeys: []string{key}},
					{Provider: " " + provider + " ", TeamID: "team", ProjectKeys: []string{key}},
				},
			})
			got := derived.IssueProjectCandidates(GithubWorkItemDerivationSubject{WorkItemID: provider + ":K-1", Provider: provider, ProjectKey: &key})
			if len(got) != 1 {
				t.Errorf("candidates = %+v, want one for team", got)
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

// The admin fallback is per key: when no team of the item's provider holds the
// scope key and an admin team holds it, that key has a holder and decides, as
// on main; the project key is not looked up.
func TestAnAdminHolderOfTheFirstKeyDecides(t *testing.T) {
	for _, provider := range []string{"gitlab", "github", "linear"} {
		t.Run(provider, func(t *testing.T) {
			projectID, projectKey := "PID", "PKEY"
			derived := NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{Teams: catalogOrder([]GithubWorkItemDerivationTeamFact{
				{Provider: provider, TeamID: "key-team", ProjectKeys: []string{projectKey}},
				{Provider: "", TeamID: "admin-scope-team", ProjectKeys: []string{projectID}},
			})})
			got := derived.IssueProjectCandidates(GithubWorkItemDerivationSubject{
				WorkItemID: provider + ":PKEY-1", Provider: provider, ProjectID: &projectID, ProjectKey: &projectKey,
			})
			if len(got) != 1 || GithubWorkItemDerivationStringValue(got[0].TeamID) != "admin-scope-team" || got[0].Evidence != "issue_project_key=PID" {
				t.Errorf("candidates = %+v, want admin-scope-team with evidence issue_project_key=PID", got)
			}
		})
	}
}

// Teams are (provider, id): a team of another provider with the same id that
// holds the same key does not hide the item's own team from the key, in
// either tier.
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
				if native := derived.NativeTeamCandidate(GithubWorkItemDerivationSubject{WorkItemID: provider + ":ENG-1", Provider: provider, NativeTeamKey: &key}); native == nil {
					t.Errorf("native candidate = nil, want the %s team ENG", provider)
				}
			})
		}
	}
}

// The native team key follows the same rule: the native_team row is the
// first team of the item's provider that holds the key. A team of another
// provider, or with no provider, that holds the key and sorts first is not
// the item's native team. When only teams of other providers hold it, there
// is no native_team row; when an admin team (no provider) holds it, the admin
// team is the native team. (Only linear items carry a native team key today;
// the rule is the same for every provider.)
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
				if other == "" {
					// An admin team is the native team when no team of the
					// item's provider holds the key.
					teamID, _, candidates = onlyOther.Resolve(subject)
					team, source, count := primaryRow(candidates)
					if GithubWorkItemDerivationStringValue(teamID) != "a-first-other" || team != "a-first-other" || source != "native_team" || count != 1 {
						t.Errorf("only an admin team: primary = %q/%s (%d rows), want a-first-other/native_team", team, source, count)
					}
					return
				}
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

// Teams are (provider, id) for the active-team rule too. A retired Jira
// project-as-team row has id = the project key ("ENG"); a Linear team id is
// the team key ("ENG"). The inactive team of one provider does not drop the
// active team of another provider with the same id, in either order of
// providers and in either catalog order; the inactive team of the item's own
// provider still drops its candidate, unless an active admin team has the id. Every path: native team key, project
// key and project ownership.
func TestAnInactiveTeamOfAnotherProviderWithTheSameIDDoesNotDropTheItemsTeam(t *testing.T) {
	key := "ENG"
	for _, provider := range coOwnerProviders {
		for _, other := range append(otherProviders(provider), "") {
			for _, reversed := range []bool{false, true} {
				name := fmt.Sprintf("%s/inactive=%q/reversed=%v", provider, other, reversed)
				t.Run(name, func(t *testing.T) {
					teams := []GithubWorkItemDerivationTeamFact{
						{Provider: other, TeamID: "ENG", ProjectKeys: []string{key}, Inactive: true},
						{Provider: provider, TeamID: "ENG", TeamName: "Eng", ProjectKeys: []string{key}},
					}
					if reversed {
						teams[0], teams[1] = teams[1], teams[0]
					}
					derived := NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{
						Teams: teams,
						Projects: []GithubWorkItemDerivationProjectFact{
							{Provider: provider, TeamID: "ENG", TeamName: "Eng", ProjectID: "PROJ", IsPrimary: 1, Specificity: 110, Priority: 10},
						},
					})
					projectID := "PROJ"
					for path, subject := range map[string]GithubWorkItemDerivationSubject{
						"native_team":       {WorkItemID: provider + ":ENG-1", Provider: provider, NativeTeamKey: &key, OrgID: "org"},
						"issue_project":     {WorkItemID: provider + ":ENG-2", Provider: provider, ProjectKey: &key, OrgID: "org"},
						"project_ownership": {WorkItemID: provider + ":ENG-3", Provider: provider, ProjectID: &projectID, OrgID: "org"},
					} {
						teamID, _, candidates := derived.Resolve(subject)
						team, source, _ := primaryRow(candidates)
						if GithubWorkItemDerivationStringValue(teamID) != "ENG" || team != "ENG" || source != path {
							t.Errorf("%s: primary = %q/%s, want ENG/%s (candidates %+v)", path, team, source, path, candidates)
						}
					}
				})
				t.Run(name+"/own team inactive", func(t *testing.T) {
					teams := []GithubWorkItemDerivationTeamFact{
						{Provider: other, TeamID: "ENG", ProjectKeys: []string{key}},
						{Provider: provider, TeamID: "ENG", ProjectKeys: []string{key}, Inactive: true},
					}
					if reversed {
						teams[0], teams[1] = teams[1], teams[0]
					}
					derived := NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{
						Teams: teams,
						Projects: []GithubWorkItemDerivationProjectFact{
							{Provider: provider, TeamID: "ENG", ProjectID: "PROJ", IsPrimary: 1, Specificity: 110, Priority: 10},
						},
					})
					projectID := "PROJ"
					subject := GithubWorkItemDerivationSubject{WorkItemID: provider + ":ENG-3", Provider: provider, ProjectID: &projectID, OrgID: "org"}
					teamID, _, candidates := derived.Resolve(subject)
					if other == "" {
						// The active admin team ENG is the team the id means
						// when the item's own team ENG is inactive.
						team, source, _ := primaryRow(candidates)
						if GithubWorkItemDerivationStringValue(teamID) != "ENG" || team != "ENG" || source != "project_ownership" {
							t.Errorf("primary = %q/%s, want the active admin team ENG/project_ownership (candidates %+v)", team, source, candidates)
						}
						for _, candidate := range candidates {
							if candidate.IsPrimary == AttributionPrimary && (candidate.TeamProvider != "" || !candidate.TeamResolved) {
								t.Errorf("primary bound to %q (resolved %v), want the admin team", candidate.TeamProvider, candidate.TeamResolved)
							}
						}
						return
					}
					if teamID != nil {
						t.Errorf("Resolve team = %q, want none: the item's own team ENG is inactive (candidates %+v)", *teamID, candidates)
					}
					// The other provider's active ENG is that provider's team:
					// its own item resolves to it.
					otherKey := key
					teamID, _, _ = derived.Resolve(GithubWorkItemDerivationSubject{WorkItemID: other + ":ENG-9", Provider: other, NativeTeamKey: &otherKey, OrgID: "org"})
					if GithubWorkItemDerivationStringValue(teamID) != "ENG" {
						t.Errorf("%s item: Resolve team = %q, want ENG of %s", other, GithubWorkItemDerivationStringValue(teamID), other)
					}
				})
			}
		}
	}
}

// A candidate id that names only teams of other providers (an ownership or
// membership fact can name such a team) binds to the first ACTIVE one of them
// and is dropped when none is active: the id can only mean those teams.
func TestAnIDOfOnlyOtherProvidersFollowsTheirActiveFlag(t *testing.T) {
	for _, provider := range coOwnerProviders {
		for _, other := range otherProviders(provider) {
			for _, inactive := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/team of %s inactive=%v and an active team of a later provider", provider, other, inactive), func(t *testing.T) {
					later := "zz-" + other
					projectID := "PROJ"
					derived := NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{
						Teams: catalogOrder([]GithubWorkItemDerivationTeamFact{
							{Provider: other, TeamID: "far-team", Inactive: inactive},
							{Provider: later, TeamID: "far-team", TeamName: "Far " + later},
						}),
						Projects: []GithubWorkItemDerivationProjectFact{
							{Provider: provider, TeamID: "far-team", ProjectID: projectID, IsPrimary: 1, Specificity: 110, Priority: 10},
						},
					})
					teamID, _, candidates := derived.Resolve(GithubWorkItemDerivationSubject{WorkItemID: provider + ":P-1", Provider: provider, ProjectID: &projectID, OrgID: "org"})
					if GithubWorkItemDerivationStringValue(teamID) != "far-team" {
						t.Errorf("Resolve team = %q, want far-team: an active team has the id (candidates %+v)", GithubWorkItemDerivationStringValue(teamID), candidates)
					}
				})
				t.Run(fmt.Sprintf("%s/team of %s inactive=%v", provider, other, inactive), func(t *testing.T) {
					projectID := "PROJ"
					derived := NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{
						Teams: []GithubWorkItemDerivationTeamFact{{Provider: other, TeamID: "far-team", Inactive: inactive}},
						Projects: []GithubWorkItemDerivationProjectFact{
							{Provider: provider, TeamID: "far-team", ProjectID: projectID, IsPrimary: 1, Specificity: 110, Priority: 10},
						},
					})
					teamID, _, _ := derived.Resolve(GithubWorkItemDerivationSubject{WorkItemID: provider + ":P-1", Provider: provider, ProjectID: &projectID, OrgID: "org"})
					if got, want := GithubWorkItemDerivationStringValue(teamID), map[bool]string{false: "far-team", true: ""}[inactive]; got != want {
						t.Errorf("Resolve team = %q, want %q", got, want)
					}
				})
			}
		}
	}
}

// An admin team (no provider) is the team a candidate id means when no team
// of the item's provider has that id; when that admin team is inactive, the
// candidate is dropped, also when an active team of another provider has the
// same id.
func TestAnInactiveAdminTeamIsDroppedWhenNoTeamOfTheItemsProviderHasItsID(t *testing.T) {
	for _, provider := range coOwnerProviders {
		for _, other := range otherProviders(provider) {
			t.Run(provider+"/active team of "+other, func(t *testing.T) {
				projectID := "PROJ"
				derived := NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{
					Teams: catalogOrder([]GithubWorkItemDerivationTeamFact{
						{Provider: "", TeamID: "ENG", Inactive: true},
						{Provider: other, TeamID: "ENG"},
					}),
					Projects: []GithubWorkItemDerivationProjectFact{
						{Provider: provider, TeamID: "ENG", ProjectID: projectID, IsPrimary: 1, Specificity: 110, Priority: 10},
					},
				})
				teamID, _, candidates := derived.Resolve(GithubWorkItemDerivationSubject{WorkItemID: provider + ":P-1", Provider: provider, ProjectID: &projectID, OrgID: "org"})
				if teamID != nil {
					t.Errorf("Resolve team = %q, want none: the admin team ENG is inactive (candidates %+v)", *teamID, candidates)
				}
			})
		}
	}
}

// A manual fallback is provider-neutral: it applies to items of every
// provider, and the team id it names is bound for the item like a key holder:
// the active team of the item's provider with that id, else the active admin
// team with that id, never a team of another provider. The bound team gives
// the row its name. An inactive team never takes the item: an inactive admin
// team ENG with the same id as the item provider's active team ENG neither
// names the row nor keeps it alive when it is the only team ENG.
func TestAManualFallbackIsBoundToAnActiveTeamOfTheItemsProvider(t *testing.T) {
	key := "ENG"
	rule := GithubWorkItemDerivationManualFallback{ScopeType: "issue_key_prefix", ScopeID: key, TeamID: "ENG", TeamName: "Inactive admin ENG", Priority: 5}
	for _, provider := range coOwnerProviders {
		subject := GithubWorkItemDerivationSubject{WorkItemID: provider + ":ENG-3", Provider: provider, OrgID: "org"}
		resolveWith := func(teams ...GithubWorkItemDerivationTeamFact) (string, string, string, []GithubWorkItemDerivationCandidate) {
			derived := NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{
				Teams: catalogOrder(teams), ManualFallbacks: []GithubWorkItemDerivationManualFallback{rule},
			})
			teamID, teamName, candidates := derived.Resolve(subject)
			_, source, _ := primaryRow(candidates)
			return GithubWorkItemDerivationStringValue(teamID), GithubWorkItemDerivationStringValue(teamName), source, candidates
		}
		t.Run(provider+"/inactive admin and active own team share the id", func(t *testing.T) {
			team, name, source, candidates := resolveWith(
				GithubWorkItemDerivationTeamFact{Provider: provider, TeamID: "ENG", TeamName: "Eng " + provider},
				GithubWorkItemDerivationTeamFact{Provider: "", TeamID: "ENG", TeamName: "Inactive admin ENG", Inactive: true},
			)
			if team != "ENG" || name != "Eng "+provider || source != "manual_fallback" {
				t.Errorf("primary = %q %q/%s, want ENG %q/manual_fallback: the active %s team, never the inactive admin team (candidates %+v)", team, name, source, "Eng "+provider, provider, candidates)
			}
		})
		t.Run(provider+"/only the inactive admin team has the id", func(t *testing.T) {
			team, _, source, candidates := resolveWith(GithubWorkItemDerivationTeamFact{Provider: "", TeamID: "ENG", TeamName: "Inactive admin ENG", Inactive: true})
			if team != "" || source != "unassigned" {
				t.Errorf("primary = %q/%s, want unassigned (candidates %+v)", team, source, candidates)
			}
		})
		t.Run(provider+"/only the inactive own team has the id", func(t *testing.T) {
			team, _, source, _ := resolveWith(GithubWorkItemDerivationTeamFact{Provider: provider, TeamID: "ENG", Inactive: true})
			if team != "" || source != "unassigned" {
				t.Errorf("primary = %q/%s, want unassigned", team, source)
			}
		})
		t.Run(provider+"/active admin team has the id", func(t *testing.T) {
			team, name, source, _ := resolveWith(GithubWorkItemDerivationTeamFact{Provider: "", TeamID: "ENG", TeamName: "Admin ENG"})
			if team != "ENG" || name != "Admin ENG" || source != "manual_fallback" {
				t.Errorf("primary = %q %q/%s, want the admin team ENG", team, name, source)
			}
		})
		for _, other := range otherProviders(provider) {
			t.Run(provider+"/only a team of "+other+" has the id", func(t *testing.T) {
				team, _, source, _ := resolveWith(GithubWorkItemDerivationTeamFact{Provider: other, TeamID: "ENG", TeamName: "Eng " + other})
				if team != "" || source != "unassigned" {
					t.Errorf("primary = %q/%s, want unassigned: a team of another provider is never bound", team, source)
				}
			})
		}
		t.Run(provider+"/an id no catalog row has stays as named", func(t *testing.T) {
			team, name, source, _ := resolveWith()
			if team != "ENG" || name != "Inactive admin ENG" || source != "manual_fallback" {
				t.Errorf("primary = %q %q/%s, want ENG as the rule names it", team, name, source)
			}
		})
	}
}

// A linked_issue candidate carries the donor's bound team: the PR of one
// provider inherits the team of an issue of another provider, and that team is
// the donor provider's team. An inactive team of the PR's provider, or an
// inactive admin team, with the same id does not drop it. Every pair of
// providers.
func TestALinkedIssueKeepsTheDonorsTeamIdentity(t *testing.T) {
	for _, donorProvider := range []string{"jira", "linear"} {
		for _, itemProvider := range coOwnerProviders {
			if itemProvider == donorProvider {
				continue
			}
			for _, inactive := range []string{"", itemProvider} {
				t.Run(fmt.Sprintf("%s item, %s donor, inactive %q ENG", itemProvider, donorProvider, inactive), func(t *testing.T) {
					donorProject := "PROJ"
					derived := NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{
						Teams: catalogOrder([]GithubWorkItemDerivationTeamFact{
							{Provider: donorProvider, TeamID: "ENG", TeamName: "Eng " + donorProvider},
							{Provider: inactive, TeamID: "ENG", TeamName: "Retired ENG", Inactive: true},
						}),
						Projects: []GithubWorkItemDerivationProjectFact{
							{Provider: donorProvider, TeamID: "ENG", ProjectID: donorProject, IsPrimary: 1, Specificity: 80, Priority: 10},
						},
					})
					donor := GithubWorkItemDerivationSubject{WorkItemID: donorProvider + ":ENG-42", Provider: donorProvider, ProjectID: &donorProject, OrgID: "org"}
					dependent := GithubWorkItemDerivationSubject{WorkItemID: itemProvider + ":item-7", Provider: itemProvider, OrgID: "org"}
					linked, _, _ := derived.BuildLinkedIssueIndex(itemProvider,
						map[string]GithubWorkItemDerivationSubject{donor.WorkItemID: donor, dependent.WorkItemID: dependent},
						[]GithubWorkItemDerivationDependencyEdge{{SourceWorkItemID: dependent.WorkItemID, TargetWorkItemID: donor.WorkItemID, RelationshipType: "relates_to"}},
						nil)
					derived.LinkedIssue = linked
					teamID, teamName, candidates := derived.Resolve(dependent)
					_, source, _ := primaryRow(candidates)
					if GithubWorkItemDerivationStringValue(teamID) != "ENG" || GithubWorkItemDerivationStringValue(teamName) != "Eng "+donorProvider || source != "linked_issue" {
						t.Errorf("primary = %q %q/%s, want ENG of %s via linked_issue (candidates %+v)",
							GithubWorkItemDerivationStringValue(teamID), GithubWorkItemDerivationStringValue(teamName), source, donorProvider, candidates)
					}
				})
			}
		}
	}
}
