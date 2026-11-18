package teamattribution

import (
	"testing"
	"time"
)

// CHAOS-14068 cells: the PR / MR membership ownership gate (teamOwnsSubjectRepo)
// and a repo whose owner team is inactive.

const gateCellRepo = "c7198fbc-1945-3717-05d8-eb78866b4e79"

var gateCellNow = time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)

func gateCellItemType(provider string) string {
	if provider == "gitlab" {
		return "merge_request"
	}
	return "pr"
}

func gateCellRepoFact(provider, team, repoID string) GithubWorkItemDerivationRepoFact {
	id := repoID
	return GithubWorkItemDerivationRepoFact{
		Provider: provider, TeamID: team, TeamName: team, RepoID: &id,
		RepoFullName: "acme/" + repoID[:4], IsPrimary: 1, Specificity: 70, UpdatedAt: gateCellNow,
	}
}

// gateCellFacts: catalog t1 t2 t3 (inactive as listed), the subject repo owned by
// owners, t2 and t3 each own one other repo (so neither is null-carrying), alice a
// member of member in the admin layer.
func gateCellFacts(provider string, owners []string, member string, inactive ...string) GithubWorkItemDerivationFacts {
	repos := []GithubWorkItemDerivationRepoFact{
		gateCellRepoFact(provider, "t2", "aaaa0000-0000-0000-0000-000000000002"),
		gateCellRepoFact(provider, "t3", "aaaa0000-0000-0000-0000-000000000003"),
	}
	for _, owner := range owners {
		repos = append(repos, gateCellRepoFact(provider, owner, gateCellRepo))
	}
	return GithubWorkItemDerivationFacts{
		Teams: membershipTeams(provider, inactive...),
		Repos: repos,
		Members: []GithubWorkItemDerivationMemberFact{{
			Provider: provider, TeamID: member, TeamName: member, MemberID: "alice",
			IsPrimary: 1, Specificity: 60, UpdatedAt: gateCellNow,
		}},
	}
}

func gateCellSubject(provider string, author bool) GithubWorkItemDerivationSubject {
	repoID := gateCellRepo
	subject := GithubWorkItemDerivationSubject{
		WorkItemID: "cell#1", Provider: provider, Type: gateCellItemType(provider), RepoID: &repoID, OrgID: "org-cell",
	}
	if author {
		reporter := "alice"
		subject.Reporter = &reporter
	} else {
		subject.Assignees = []string{"alice"}
	}
	return subject
}

// gateCellOutcome: the gate reason for the member's team, and the membership row
// the cascade writes (source, ownership label) or "none".
func gateCellOutcome(t *testing.T, facts GithubWorkItemDerivationFacts, provider string, author bool) (string, bool, string) {
	t.Helper()
	derived := NewGitHubWorkItemDerivationContext(facts)
	subject := gateCellSubject(provider, author)
	source := "assignee_membership"
	if author {
		source = "author_membership"
	}
	reason, owns := "-", false
	if candidates, _ := derived.ResolveMembership(provider, "alice"); len(candidates) > 0 {
		reason, owns = derived.teamOwnsSubjectRepo(subject, GithubWorkItemDerivationStringValue(candidates[0].TeamID))
	} else {
		reason = "membership_dropped_before_gate"
	}
	row := "none"
	_, _, rows := derived.Resolve(subject)
	for _, candidate := range rows {
		if candidate.Source == source {
			row = GithubWorkItemDerivationStringValue(candidate.TeamID) + "/" + candidate.OwnershipReason
		}
	}
	return reason, owns, row
}

// A repo owned ONLY by inactive teams gives the gate the same result as a repo
// with no ownership fact at all (before the fix it answered repo_not_owned).
func TestMembershipGateInactiveOnlyOwnerIsNoOwnershipFact(t *testing.T) {
	for _, provider := range []string{"github", "gitlab"} {
		for _, author := range []bool{false, true} {
			name := provider + map[bool]string{false: "/assignee", true: "/author"}[author]
			t.Run(name, func(t *testing.T) {
				noFactReason, noFactOwns, noFactRow := gateCellOutcome(t, gateCellFacts(provider, nil, "t2", "t1"), provider, author)
				reason, owns, row := gateCellOutcome(t, gateCellFacts(provider, []string{"t1"}, "t2", "t1"), provider, author)
				t.Logf("CELL %s no-fact: reason=%q owns=%v row=%s | owner inactive t1 only: reason=%q owns=%v row=%s", name, noFactReason, noFactOwns, noFactRow, reason, owns, row)
				if reason != noFactReason || owns != noFactOwns || row != noFactRow {
					t.Errorf("owner = inactive t1 only: (%q, %v, %s), want the no-ownership-fact result (%q, %v, %s)", reason, owns, row, noFactReason, noFactOwns, noFactRow)
				}
			})
		}
	}
}

// The Done-when shape: alice is a member of the inactive owner team itself. On main
// her membership is dropped before the gate (ResolveMembership counts active teams
// only), in every layer and binding, so no row passes the gate on an inactive owner.
func TestMembershipGateMemberOfTheInactiveOwnerNeverReachesTheGate(t *testing.T) {
	for _, provider := range []string{"github", "gitlab"} {
		for _, author := range []bool{false, true} {
			name := provider + map[bool]string{false: "/assignee", true: "/author"}[author]
			shapes := map[string]GithubWorkItemDerivationFacts{
				"admin": gateCellFacts(provider, []string{"t1"}, "t1", "t1"),
				"provider": func() GithubWorkItemDerivationFacts {
					f := gateCellFacts(provider, []string{"t1"}, "t1", "t1")
					f.ProviderMembers, f.Members = f.Members, nil
					return f
				}(),
				"untyped": func() GithubWorkItemDerivationFacts {
					f := gateCellFacts(provider, []string{"t1"}, "t1", "t1")
					f.Members, f.UntypedMembers = nil, untypedOf("t1")
					return f
				}(),
				// t1 also exists, active, as a team of another provider and as a providerless team.
				"other-provider-active-t1": func() GithubWorkItemDerivationFacts {
					f := gateCellFacts(provider, []string{"t1"}, "t1", "t1")
					f.Teams = append(f.Teams, GithubWorkItemDerivationTeamFact{Provider: "jira", TeamID: "t1", TeamName: "t1"})
					return f
				}(),
				"providerless-active-t1": func() GithubWorkItemDerivationFacts {
					f := gateCellFacts(provider, []string{"t1"}, "t1", "t1")
					f.Members, f.UntypedMembers = nil, untypedOf("t1")
					f.Teams = append(f.Teams, GithubWorkItemDerivationTeamFact{Provider: "", TeamID: "t1", TeamName: "t1"})
					return f
				}(),
			}
			for shape, facts := range shapes {
				t.Run(name+"/"+shape, func(t *testing.T) {
					derived := NewGitHubWorkItemDerivationContext(facts)
					reason, owns, row := gateCellOutcome(t, facts, provider, author)
					ownerInactive := false
					for _, owner := range derived.repoByID[AttributionMapKey(provider, gateCellRepo)] {
						ownerInactive = ownerInactive || derived.candidateNamesInactiveTeam(owner)
					}
					t.Logf("CELL %s/%s: owner inactive=%v reason=%q owns=%v row=%s", name, shape, ownerInactive, reason, owns, row)
					if ownerInactive && row != "none" {
						t.Errorf("a membership row %s passed the gate on an inactive owner (reason %q)", row, reason)
					}
				})
			}
		}
	}
}

// Controls (unchanged by the fix): no owner fact, an active owner = the member's
// team, an active owner != the member's team, an inactive + an active owner.
func TestMembershipGateControls(t *testing.T) {
	for _, provider := range []string{"github", "gitlab"} {
		for _, author := range []bool{false, true} {
			name := provider + map[bool]string{false: "/assignee", true: "/author"}[author]
			for _, c := range []struct {
				shape   string
				owners  []string
				member  string
				reason  string
				owns    bool
				wantRow string
			}{
				{"no-fact", nil, "t2", MembershipOwnershipReasonUnknown, true, "t2/" + MembershipOwnershipReasonUnknown},
				{"active-owner-is-member", []string{"t2"}, "t2", "", true, "t2/owned"},
				{"active-owner-not-member", []string{"t3"}, "t2", MembershipOwnershipReasonNotOwned, false, "none"},
				{"inactive-and-active-owner-member", []string{"t1", "t2"}, "t2", "", true, "t2/owned"},
				{"inactive-and-active-owner-not-member", []string{"t1", "t3"}, "t2", MembershipOwnershipReasonNotOwned, false, "none"},
			} {
				t.Run(name+"/"+c.shape, func(t *testing.T) {
					reason, owns, row := gateCellOutcome(t, gateCellFacts(provider, c.owners, c.member, "t1"), provider, author)
					t.Logf("CELL %s/%s: reason=%q owns=%v row=%s", name, c.shape, reason, owns, row)
					if reason != c.reason || owns != c.owns || row != c.wantRow {
						t.Errorf("got (%q, %v, %s), want (%q, %v, %s)", reason, owns, row, c.reason, c.owns, c.wantRow)
					}
				})
			}
		}
	}
}
