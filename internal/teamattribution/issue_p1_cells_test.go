package teamattribution

import (
	"strings"
	"testing"
)

// P1 2: the evidence suffix is applied after ranking and dedup, so the writer's last-wins tie keeps the assignee main keeps.
func TestOwnershipSuffixDoesNotChangeWhichAssigneeTheWriterKeeps(t *testing.T) {
	repo := "22222222-2222-4222-8222-222222222222"
	members := []GithubWorkItemDerivationMemberFact{
		{Provider: "github", TeamID: "team-m", TeamName: "Member", MemberID: "alice", IsPrimary: 1, Specificity: 60},
		{Provider: "github", TeamID: "team-m", TeamName: "Member", MemberID: "alice1", IsPrimary: 1, Specificity: 60},
	}
	for _, c := range []struct{ name, itemType string }{{"pr no owner fact", "pr"}, {"issue", "issue"}} {
		t.Run(c.name, func(t *testing.T) {
			derived := NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{Members: members})
			_, _, candidates := derived.Resolve(GithubWorkItemDerivationSubject{WorkItemID: "gh:x#1", Provider: "github", Type: c.itemType, RepoID: &repo, Assignees: []string{"alice", "alice1"}, OrgID: "o"})
			var last string
			for _, candidate := range candidates {
				if candidate.Source == "assignee_membership" {
					last = candidate.Evidence
				}
			}
			if !strings.HasPrefix(last, "assignee_membership=alice1") {
				t.Fatalf("the row the writer keeps (the last of the team) is %q, want the alice1 row as on main", last)
			}
		})
	}
}

// P1 3: an issue's resolved membership is kept even when the member's team has no ownership anywhere (the team-level null-carrying check
// is a gate on a PR/MR; the issue exception comes first). The PR/MR control is refused as on main.
func TestIssueMembershipIsKeptForANullCarryingTeam(t *testing.T) {
	repo := "22222222-2222-4222-8222-222222222222"
	for _, c := range []struct {
		name, itemType string
		wantRow        bool
	}{{"issue", "issue", true}, {"pr control", "pr", false}} {
		t.Run(c.name, func(t *testing.T) {
			derived := NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{
				Teams:   []GithubWorkItemDerivationTeamFact{{Provider: "github", TeamID: "team-m", TeamName: "Member"}},
				Members: []GithubWorkItemDerivationMemberFact{{Provider: "github", TeamID: "team-m", TeamName: "Member", MemberID: "alice", IsPrimary: 1, Specificity: 60}},
			})
			_, _, candidates := derived.Resolve(GithubWorkItemDerivationSubject{WorkItemID: "gh:x#1", Provider: "github", Type: c.itemType, RepoID: &repo, Assignees: []string{"alice"}, OrgID: "o"})
			var row string
			for _, candidate := range candidates {
				if candidate.Source == "assignee_membership" {
					row = candidate.Evidence
				}
			}
			if c.wantRow && row != "assignee_membership=alice;ownership=ownership_gate_not_applied_issue" {
				t.Fatalf("issue membership row %q, want it kept with the gate-not-applied suffix", row)
			}
			if !c.wantRow && row != "" {
				t.Fatalf("PR/MR membership row %q, want none (null-carrying team refused, as on main)", row)
			}
		})
	}
}

// P1 1 (acp: in scope): an issue's repo_ownership row is no donor row, so a PR/MR linked to it copies no team from it. Production shape:
// the donor row is the issue's own primary row (OwnDonorRows), copied by BuildLinkedIssueIndexFromRows. Each case pins the end state
// (primary source, team, and whether a linked_issue provenance row stays); main gives the opposite in every case.
func TestPRLinkedToAnIssueWhoseOnlyDonorRowWasRepoOwnership(t *testing.T) {
	issueRepo := "11111111-1111-4111-8111-111111111111"
	prRepo := "33333333-3333-4333-8333-333333333333"
	for _, kind := range []struct{ provider, prType, prID string }{{"github", "pr", "ghpr:a/b#7"}, {"gitlab", "merge_request", "a/b!7"}} {
		issueID := kind.provider + ":a/b#9"
		base := func(repos []GithubWorkItemDerivationRepoFact, teams []GithubWorkItemDerivationTeamFact, members []GithubWorkItemDerivationMemberFact, extra ...GithubWorkItemDerivationSubject) (GithubWorkItemDerivationContext, map[string]GithubWorkItemDerivationSubject) {
			derived := NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{Repos: repos, Teams: teams, Members: members})
			subjects := map[string]GithubWorkItemDerivationSubject{
				kind.prID: {WorkItemID: kind.prID, Provider: kind.provider, Type: kind.prType, OrgID: "org"},
				issueID:   {WorkItemID: issueID, Provider: kind.provider, Type: "issue", RepoID: &issueRepo, OrgID: "org"},
			}
			for _, subject := range extra {
				subjects[subject.WorkItemID] = subject
			}
			return derived, subjects
		}
		resolve := func(t *testing.T, derived GithubWorkItemDerivationContext, subjects map[string]GithubWorkItemDerivationSubject, edges []GithubWorkItemDerivationDependencyEdge) []GithubWorkItemDerivationCandidate {
			all := map[string]struct{}{}
			for id := range subjects {
				all[id] = struct{}{}
			}
			teams, evidence, _, _ := derived.BuildLinkedIssueIndexFromRows(kind.provider, subjects, derived.OwnDonorRows(subjects, all), edges, nil)
			derived.LinkedIssue, derived.LinkedIssueEvidence = teams, evidence
			_, _, candidates := derived.Resolve(subjects[kind.prID])
			return candidates
		}
		issueOwner := []GithubWorkItemDerivationRepoFact{{Provider: kind.provider, TeamID: "team-repo-owner", TeamName: "Repo Owner", RepoID: &issueRepo, IsPrimary: 1, Specificity: 100}}
		edge := func(target string) GithubWorkItemDerivationDependencyEdge {
			return GithubWorkItemDerivationDependencyEdge{SourceWorkItemID: kind.prID, TargetWorkItemID: target, RelationshipType: "relates_to"}
		}
		linkedRow := func(candidates []GithubWorkItemDerivationCandidate) bool {
			for _, candidate := range candidates {
				if candidate.Source == "linked_issue" {
					return true
				}
			}
			return false
		}
		t.Run(kind.provider+"/repo-only donor, no other primary: the PR/MR loses the team", func(t *testing.T) {
			derived, subjects := base(issueOwner, nil, nil)
			candidates := resolve(t, derived, subjects, []GithubWorkItemDerivationDependencyEdge{edge(issueID)})
			row, _ := primaryCandidate14010(candidates)
			if row.Source != "unassigned" || linkedRow(candidates) {
				t.Fatalf("primary %s team %q linked_issue row %v, want unassigned and no linked_issue row", row.Source, GithubWorkItemDerivationStringValue(row.TeamID), linkedRow(candidates))
			}
		})
		t.Run(kind.provider+"/repo-only donor and another eligible donor: the PR/MR switches to the other donor", func(t *testing.T) {
			second := GithubWorkItemDerivationSubject{WorkItemID: "jira:ENG-2", Provider: "jira", Type: "story", OrgID: "org"}
			ek := "ENG"
			second.ProjectKey = &ek
			derived, subjects := base(issueOwner, []GithubWorkItemDerivationTeamFact{{Provider: "jira", TeamID: "other-team", TeamName: "Other", ProjectKeys: []string{"ENG"}}}, nil, second)
			candidates := resolve(t, derived, subjects, []GithubWorkItemDerivationDependencyEdge{edge(issueID), edge("jira:ENG-2")})
			row, _ := primaryCandidate14010(candidates)
			if row.Source != "linked_issue" || GithubWorkItemDerivationStringValue(row.TeamID) != "other-team" {
				t.Fatalf("primary %s team %q, want linked_issue other-team (main: team-repo-owner)", row.Source, GithubWorkItemDerivationStringValue(row.TeamID))
			}
		})
		t.Run(kind.provider+"/PR/MR has its own repo owner: it keeps the primary and loses the linked_issue row", func(t *testing.T) {
			repos := append(append([]GithubWorkItemDerivationRepoFact(nil), issueOwner...),
				GithubWorkItemDerivationRepoFact{Provider: kind.provider, TeamID: "team-pr-owner", TeamName: "PR Owner", RepoID: &prRepo, IsPrimary: 1, Specificity: 100})
			derived, subjects := base(repos, nil, nil)
			pr := subjects[kind.prID]
			pr.RepoID = &prRepo
			subjects[kind.prID] = pr
			candidates := resolve(t, derived, subjects, []GithubWorkItemDerivationDependencyEdge{edge(issueID)})
			row, _ := primaryCandidate14010(candidates)
			if row.Source != "repo_ownership" || GithubWorkItemDerivationStringValue(row.TeamID) != "team-pr-owner" || linkedRow(candidates) {
				t.Fatalf("primary %s team %q linked_issue row %v, want repo_ownership team-pr-owner and no linked_issue row", row.Source, GithubWorkItemDerivationStringValue(row.TeamID), linkedRow(candidates))
			}
		})
		t.Run(kind.provider+"/issue shares the PR/MR repo: the linked_issue row goes", func(t *testing.T) {
			derived, subjects := base(issueOwner, nil, nil)
			pr := subjects[kind.prID]
			pr.RepoID = &issueRepo
			subjects[kind.prID] = pr
			candidates := resolve(t, derived, subjects, []GithubWorkItemDerivationDependencyEdge{edge(issueID)})
			row, _ := primaryCandidate14010(candidates)
			if row.Source != "repo_ownership" || GithubWorkItemDerivationStringValue(row.TeamID) != "team-repo-owner" || linkedRow(candidates) {
				t.Fatalf("primary %s team %q linked_issue row %v, want repo_ownership team-repo-owner and no linked_issue row", row.Source, GithubWorkItemDerivationStringValue(row.TeamID), linkedRow(candidates))
			}
		})
		t.Run(kind.provider+"/PR/MR reporter resolves to another team: unassigned with author provenance", func(t *testing.T) {
			members := []GithubWorkItemDerivationMemberFact{{Provider: kind.provider, TeamID: "team-author", TeamName: "Author Team", MemberID: "dana", IsPrimary: 1, Specificity: 60}}
			derived, subjects := base(issueOwner, nil, members)
			pr := subjects[kind.prID]
			pr.Reporter = strPtr("dana")
			subjects[kind.prID] = pr
			candidates := resolve(t, derived, subjects, []GithubWorkItemDerivationDependencyEdge{edge(issueID)})
			row, _ := primaryCandidate14010(candidates)
			var author bool
			for _, candidate := range candidates {
				author = author || (candidate.Source == "author_membership" && candidate.IsPrimary == AttributionNotPrimary)
			}
			if row.Source != "unassigned" || !author {
				t.Fatalf("primary %s, author provenance row %v, want unassigned and an author_membership row", row.Source, author)
			}
		})
	}
}
