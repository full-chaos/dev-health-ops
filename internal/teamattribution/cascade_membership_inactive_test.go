package teamattribution

import (
	"fmt"
	"strings"
	"testing"
)

func membershipTeams(provider string, inactive ...string) []GithubWorkItemDerivationTeamFact {
	down := map[string]bool{}
	for _, id := range inactive {
		down[id] = true
	}
	var teams []GithubWorkItemDerivationTeamFact
	for _, id := range []string{"t1", "t2", "t3"} {
		teams = append(teams, GithubWorkItemDerivationTeamFact{
			Provider: provider, TeamID: id, TeamName: id, Inactive: down[id], ProjectKeys: []string{"K-" + id},
		})
	}
	return teams
}

func memberOf(provider string, teams ...string) []GithubWorkItemDerivationMemberFact {
	var facts []GithubWorkItemDerivationMemberFact
	for _, id := range teams {
		facts = append(facts, GithubWorkItemDerivationMemberFact{
			Provider: provider, TeamID: id, TeamName: id, MemberID: "alice", IsPrimary: 1, Specificity: 60,
		})
	}
	return facts
}

func untypedOf(teams ...string) []GithubWorkItemDerivationUntypedMemberFact {
	var facts []GithubWorkItemDerivationUntypedMemberFact
	for _, id := range teams {
		facts = append(facts, GithubWorkItemDerivationUntypedMemberFact{TeamID: id, TeamName: id, Facet: "alice"})
	}
	return facts
}

func candidateTeams(candidates []GithubWorkItemDerivationCandidate) string {
	seen := map[string]struct{}{}
	for _, candidate := range candidates {
		seen[GithubWorkItemDerivationStringValue(candidate.TeamID)] = struct{}{}
	}
	return GithubWorkItemDerivationSortedTeamIDs(seen)
}

// An inactive team takes no work item, so it is not a team of the person
// either: the exactly-one-team gate counts the ACTIVE teams only, in the admin
// layer and in the provider layer, typed and untyped, for every provider.
func TestMembershipGateCountsOnlyActiveTeams(t *testing.T) {
	type layer struct {
		name  string
		facts func(provider string, teams ...string) GithubWorkItemDerivationFacts
		ambig string
	}
	layers := []layer{
		{"admin", func(p string, teams ...string) GithubWorkItemDerivationFacts {
			return GithubWorkItemDerivationFacts{Teams: membershipTeams(p, "t1"), Members: memberOf(p, teams...)}
		}, "ambiguous_admin_membership:"},
		{"admin-untyped", func(p string, teams ...string) GithubWorkItemDerivationFacts {
			return GithubWorkItemDerivationFacts{Teams: membershipTeams(p, "t1"), UntypedMembers: untypedOf(teams...)}
		}, "ambiguous_admin_membership:"},
		{"provider", func(p string, teams ...string) GithubWorkItemDerivationFacts {
			return GithubWorkItemDerivationFacts{Teams: membershipTeams(p, "t1"), ProviderMembers: memberOf(p, teams...)}
		}, "ambiguous_provider_membership:"},
	}
	for _, provider := range coOwnerProviders {
		for _, l := range layers {
			t.Run(provider+"/"+l.name, func(t *testing.T) {
				// t1 is inactive in every case.
				derived := NewGitHubWorkItemDerivationContext(l.facts(provider, "t1", "t2"))
				candidates, reason := derived.ResolveMembership(provider, "alice")
				if reason != "" || candidateTeams(candidates) != "t2" {
					t.Errorf("inactive t1 + active t2: teams %q reason %q, want t2 and no reason", candidateTeams(candidates), reason)
				}

				derived = NewGitHubWorkItemDerivationContext(l.facts(provider, "t1"))
				candidates, reason = derived.ResolveMembership(provider, "alice")
				if reason != "no_membership" || candidates != nil {
					t.Errorf("only inactive t1: candidates %v reason %q, want none and no_membership", candidates, reason)
				}

				derived = NewGitHubWorkItemDerivationContext(l.facts(provider, "t2", "t3"))
				_, reason = derived.ResolveMembership(provider, "alice")
				if reason != l.ambig+"t2,t3" && !strings.HasPrefix(reason, l.ambig) {
					t.Errorf("two active teams: reason %q, want %s...", reason, l.ambig)
				}
			})
		}
	}
}

// An admin layer whose teams are all inactive has no candidate left, so the
// person falls through to the provider layer. An admin layer with an active
// team stays authoritative: the provider layer is not consulted.
func TestAnInactiveOnlyAdminLayerFallsThroughToTheProviderLayer(t *testing.T) {
	for _, provider := range coOwnerProviders {
		t.Run(provider, func(t *testing.T) {
			derived := NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{
				Teams:           membershipTeams(provider, "t1"),
				Members:         memberOf(provider, "t1"),
				ProviderMembers: memberOf(provider, "t2"),
			})
			candidates, reason := derived.ResolveMembership(provider, "alice")
			if reason != "" || candidateTeams(candidates) != "t2" {
				t.Errorf("inactive-only admin layer: teams %q reason %q, want the provider team t2", candidateTeams(candidates), reason)
			}

			derived = NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{
				Teams:           membershipTeams(provider, "t1"),
				Members:         memberOf(provider, "t3"),
				ProviderMembers: memberOf(provider, "t2"),
			})
			candidates, reason = derived.ResolveMembership(provider, "alice")
			if reason != "" || candidateTeams(candidates) != "t3" {
				t.Errorf("active admin team: teams %q reason %q, want the admin team t3", candidateTeams(candidates), reason)
			}

			derived = NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{
				Teams:           membershipTeams(provider, "t1"),
				Members:         memberOf(provider, "t2", "t3"),
				ProviderMembers: memberOf(provider, "t2"),
			})
			if _, reason = derived.ResolveMembership(provider, "alice"); !strings.HasPrefix(reason, "ambiguous_admin_membership:") {
				t.Errorf("two active admin teams: reason %q, want ambiguous_admin_membership", reason)
			}
		})
	}
}

// The whole cascade: the person of inactive t1 and active t2 is attributed to
// t2 through membership, and the item is not left unassigned as ambiguous.
func TestAPersonOfAnInactiveAndAnActiveTeamResolvesToTheActiveTeam(t *testing.T) {
	for _, provider := range coOwnerProviders {
		t.Run(provider, func(t *testing.T) {
			derived := NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{
				Teams:           membershipTeams(provider, "t1"),
				ProviderMembers: memberOf(provider, "t1", "t2"),
			})
			team, _, candidates := derived.Resolve(GithubWorkItemDerivationSubject{
				WorkItemID: fmt.Sprintf("%s:x-1", provider), Provider: provider, Type: "issue",
				Assignees: []string{"alice"}, OrgID: "org",
			})
			if got := GithubWorkItemDerivationStringValue(team); got != "t2" {
				t.Errorf("resolved team = %q, want t2 (candidates %+v)", got, candidates)
			}
		})
	}
}

// An unbound candidate that names a team has no known provider team: it takes
// no part, as in the drop before the cascade picks a team.
func TestCandidateNamesInactiveTeamTreatsAnUnboundCandidateAsInactive(t *testing.T) {
	derived := NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{})
	id := "t1"
	if !derived.candidateNamesInactiveTeam(GithubWorkItemDerivationCandidate{TeamID: &id}) {
		t.Error("unbound candidate with a team id: want it dropped")
	}
	if derived.candidateNamesInactiveTeam(GithubWorkItemDerivationCandidate{}) {
		t.Error("candidate with no team: want it kept")
	}
}
