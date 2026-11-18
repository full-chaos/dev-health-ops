package teamattribution

import (
	"fmt"
	"strings"
	"testing"
)

func primaryCandidate14010(candidates []GithubWorkItemDerivationCandidate) (GithubWorkItemDerivationCandidate, bool) {
	for _, candidate := range candidates {
		if candidate.IsPrimary == AttributionPrimary {
			return candidate, true
		}
	}
	return GithubWorkItemDerivationCandidate{}, false
}

// P1 2: a repo-scoped manual rule never matches an issue through the issue's own repo field; a PR keeps it.
func TestRepoScopedManualRuleNeverMatchesAnIssueThroughItsOwnRepo(t *testing.T) {
	repo := "11111111-1111-4111-8111-111111111111"
	for _, c := range []struct {
		provider, itemType string
		isPR               bool
		byName             bool
	}{
		{"github", "issue", false, false}, {"github", "bug", false, false}, {"gitlab", "issue", false, false}, {"gitlab", "issue", false, true},
		{"jira", "story", false, false}, {"linear", "issue", false, false},
		{"github", "pr", true, false}, {"gitlab", "merge_request", true, false}, {"gitlab", "merge_request", true, true},
	} {
		name := fmt.Sprintf("%s/%s/byName=%v", c.provider, c.itemType, c.byName)
		t.Run(name, func(t *testing.T) {
			scope := repo
			subject := GithubWorkItemDerivationSubject{WorkItemID: c.provider + ":" + c.itemType + "#1", Provider: c.provider, Type: c.itemType, OrgID: "o"}
			if c.byName {
				scope = "platform/api"
				subject.GitLabPaths = []string{"platform/api"}
			} else {
				subject.RepoID = &repo
			}
			derived := NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{
				ManualFallbacks: []GithubWorkItemDerivationManualFallback{{Provider: c.provider, ScopeType: "repo", ScopeID: scope, TeamID: "team-rule", Priority: 1}},
			})
			_, _, candidates := derived.Resolve(subject)
			row, _ := primaryCandidate14010(candidates)
			var manual bool
			for _, candidate := range candidates {
				manual = manual || candidate.Source == "manual_fallback"
			}
			if c.isPR {
				if row.Source != "manual_fallback" || GithubWorkItemDerivationStringValue(row.TeamID) != "team-rule" {
					t.Fatalf("PR control: primary %s team %q, want manual_fallback team-rule", row.Source, GithubWorkItemDerivationStringValue(row.TeamID))
				}
				return
			}
			if manual || row.Source != "unassigned" || GithubWorkItemDerivationStringValue(row.TeamID) != "" || row.Evidence != "no_candidate" {
				t.Fatalf("issue: manual_fallback row %v, primary %s team %q evidence %q, want no manual row, unassigned, no team, no_candidate", manual, row.Source, GithubWorkItemDerivationStringValue(row.TeamID), row.Evidence)
			}
		})
	}
}

// P1 5: a PR whose ownership fact yields no primary (an inactive owner, or a co-owner only) keeps the evidence it has on main.
func TestPRUnassignedEvidenceIsUnchangedWhenOwnershipFactYieldsNoPrimary(t *testing.T) {
	repo := "22222222-2222-4222-8222-222222222222"
	member := []GithubWorkItemDerivationMemberFact{{Provider: "github", TeamID: "team-m", TeamName: "Member", MemberID: "alice", IsPrimary: 1, Specificity: 60}}
	for _, c := range []struct {
		name  string
		teams []GithubWorkItemDerivationTeamFact
		repos []GithubWorkItemDerivationRepoFact
	}{
		{"inactive owner",
			[]GithubWorkItemDerivationTeamFact{{Provider: "github", TeamID: "team-x", Inactive: true}},
			[]GithubWorkItemDerivationRepoFact{{Provider: "github", TeamID: "team-x", TeamName: "X", RepoID: &repo, IsPrimary: 1, Specificity: 100}}},
		{"co-owner only", nil,
			[]GithubWorkItemDerivationRepoFact{{Provider: "github", TeamID: "team-x", TeamName: "X", RepoID: &repo, IsPrimary: 2, Specificity: 100}}},
	} {
		t.Run(c.name, func(t *testing.T) {
			derived := NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{Teams: c.teams, Members: member, Repos: c.repos})
			_, _, candidates := derived.Resolve(GithubWorkItemDerivationSubject{WorkItemID: "ghpr:a/b#1", Provider: "github", Type: "pr", RepoID: &repo, Assignees: []string{"alice"}, OrgID: "o"})
			row, _ := primaryCandidate14010(candidates)
			if row.Source != "unassigned" || row.Evidence != "no_candidate" {
				t.Fatalf("PR primary %s evidence %q, want unassigned no_candidate (as on main)", row.Source, row.Evidence)
			}
		})
	}
}

// A1: the ownership gate is not applied to an issue (no own repo field, no linked-PR repo read): the membership row is kept as
// provenance (is_primary 0) and its evidence names ownership_gate_not_applied_issue, never ownership_unknown (that means no ownership row exists).
func TestIssueMembershipRowNamesTheGateNotApplied(t *testing.T) {
	repo := "11111111-1111-4111-8111-111111111111"
	for _, provider := range []string{"github", "gitlab", "jira", "linear"} {
		t.Run(provider, func(t *testing.T) {
			derived := NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{
				Members: []GithubWorkItemDerivationMemberFact{{Provider: provider, TeamID: "team-m", TeamName: "Member", MemberID: "alice", IsPrimary: 1, Specificity: 60}},
				Repos:   []GithubWorkItemDerivationRepoFact{{Provider: provider, TeamID: "team-other", TeamName: "Other", RepoID: &repo, IsPrimary: 1, Specificity: 100}},
			})
			_, _, candidates := derived.Resolve(GithubWorkItemDerivationSubject{WorkItemID: provider + ":i#1", Provider: provider, Type: "issue", RepoID: &repo, Assignees: []string{"alice"}, OrgID: "o"})
			row, _ := primaryCandidate14010(candidates)
			if row.Source != "unassigned" || row.Evidence != "no_candidate" {
				t.Errorf("primary %s evidence %q, want unassigned no_candidate", row.Source, row.Evidence)
			}
			var found bool
			for _, candidate := range candidates {
				if candidate.Source != "assignee_membership" {
					continue
				}
				found = true
				if candidate.IsPrimary != AttributionNotPrimary || candidate.Evidence != "assignee_membership=alice;ownership=ownership_gate_not_applied_issue" || strings.Contains(candidate.Evidence, "ownership_unknown") {
					t.Errorf("membership row is_primary %d evidence %q, want 0 and the gate-not-applied suffix", candidate.IsPrimary, candidate.Evidence)
				}
			}
			if !found {
				t.Errorf("no assignee_membership provenance row in %+v", candidates)
			}
		})
	}
}
