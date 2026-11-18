package teamattribution

import (
	"fmt"
	"strings"
	"testing"
)

// Issue kinds and PR kinds per provider; the end state is asserted: primary source, team, evidence.
func TestIssueTakesNoTeamFromItsOwnRepoField(t *testing.T) {
	repo := "11111111-1111-4111-8111-111111111111"
	type cell struct{ provider, itemType string }
	issues := []cell{{"github", "issue"}, {"github", "bug"}, {"gitlab", "issue"}, {"jira", "story"}, {"linear", "issue"}}
	prs := []cell{{"github", "pr"}, {"gitlab", "merge_request"}}
	for _, byName := range []bool{false, true} {
		mode := "id"
		if byName {
			mode = "name"
		}
		for _, c := range append(append([]cell{}, issues...), prs...) {
			isPR := c.itemType == "pr" || c.itemType == "merge_request"
			if byName && c.provider != "gitlab" {
				continue
			}
			t.Run(mode+"/"+c.provider+"/"+c.itemType, func(t *testing.T) {
				fact := GithubWorkItemDerivationRepoFact{Provider: c.provider, TeamID: "team-repo-owner", TeamName: "Repo Owner", IsPrimary: 1, Specificity: 100}
				subject := GithubWorkItemDerivationSubject{WorkItemID: c.provider + ":" + c.itemType + "#1", Provider: c.provider, Type: c.itemType, OrgID: "org"}
				if byName {
					fact.RepoFullName = "platform/api"
					subject.GitLabPaths = []string{"platform/api"}
				} else {
					fact.RepoID = &repo
					subject.RepoID = &repo
				}
				derived := NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{Repos: []GithubWorkItemDerivationRepoFact{fact}})
				_, _, candidates := derived.Resolve(subject)
				row, ok := primaryCandidate14010(candidates)
				if !ok {
					t.Fatalf("no primary row")
				}
				team := GithubWorkItemDerivationStringValue(row.TeamID)
				if isPR {
					if row.Source != "repo_ownership" || team != "team-repo-owner" {
						t.Fatalf("PR control: primary %s team %q evidence %q, want repo_ownership team-repo-owner", row.Source, team, row.Evidence)
					}
					return
				}
				if row.Source != "unassigned" || team != "" || row.Evidence != "no_candidate" {
					t.Fatalf("issue: primary source %s team %q evidence %q, want unassigned, no team, no_candidate", row.Source, team, row.Evidence)
				}
				for _, candidate := range candidates {
					if candidate.Source == "repo_ownership" {
						t.Fatalf("issue carries a repo_ownership row: %+v", candidate)
					}
				}
			})
		}
	}
}

// An issue linked to a donor (a pull request whose row is repo_ownership) keeps the donor's team as a linked_issue
// provenance row (is_primary 0: a link over an issue edge is never the primary, CHAOS-13917); its own repo owner's team is not a row at all.
func TestIssueLinkedToADonorKeepsLinkedIssueProvenanceNotItsOwnRepoOwner(t *testing.T) {
	ownRepo := "11111111-1111-4111-8111-111111111111"
	for _, c := range []struct{ provider, itemType string }{{"github", "issue"}, {"gitlab", "issue"}, {"jira", "story"}, {"linear", "issue"}} {
		t.Run(c.provider, func(t *testing.T) {
			derived := NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{
				Repos: []GithubWorkItemDerivationRepoFact{{Provider: c.provider, TeamID: "team-own-repo", TeamName: "Own Repo", RepoID: &ownRepo, IsPrimary: 1, Specificity: 100}},
			})
			derived.LinkedIssue = map[string][3]string{"item-1": {"team-pr-repo", "PR Repo", ""}}
			subject := GithubWorkItemDerivationSubject{WorkItemID: "item-1", Provider: c.provider, Type: c.itemType, RepoID: &ownRepo, OrgID: "org"}
			_, _, candidates := derived.Resolve(subject)
			row, ok := primaryCandidate14010(candidates)
			if !ok || row.Source != "unassigned" || GithubWorkItemDerivationStringValue(row.TeamID) != "" {
				t.Errorf("primary %s team %q, want unassigned with no team", row.Source, GithubWorkItemDerivationStringValue(row.TeamID))
			}
			var linked bool
			for _, candidate := range candidates {
				switch {
				case candidate.Source == "repo_ownership":
					t.Errorf("issue carries a repo_ownership row: %+v", candidate)
				case candidate.Source == "linked_issue":
					linked = true
					if GithubWorkItemDerivationStringValue(candidate.TeamID) != "team-pr-repo" || candidate.IsPrimary != AttributionNotPrimary {
						t.Errorf("linked_issue row %+v, want team-pr-repo, is_primary 0", candidate)
					}
				}
			}
			if !linked {
				t.Errorf("no linked_issue provenance row in %+v", candidates)
			}
		})
	}
}

// A PR whose repo has no ownership fact reaches the pass-through; its membership rows must name ownership_unknown (missing is not healthy).
func TestPRMembershipPassThroughWithoutOwnershipFactNamesOwnershipUnknown(t *testing.T) {
	repo := "22222222-2222-4222-8222-222222222222"
	derived := NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{
		Members: []GithubWorkItemDerivationMemberFact{{Provider: "github", TeamID: "team-m", TeamName: "Member Team", MemberID: "alice", IsPrimary: 1, Specificity: 60}},
	})
	_, _, candidates := derived.Resolve(GithubWorkItemDerivationSubject{WorkItemID: "ghpr:a/b#1", Provider: "github", Type: "pr", RepoID: &repo, Assignees: []string{"alice"}, Reporter: strPtr("alice"), OrgID: "o"})
	got := map[string]string{}
	for _, candidate := range candidates {
		got[candidate.Source] = candidate.Evidence + "|" + fmt.Sprint(candidate.IsPrimary)
	}
	t.Logf("PR rows: %v", got)
	for _, source := range []string{"assignee_membership", "author_membership"} {
		if !strings.Contains(got[source], "ownership_unknown") || !strings.HasSuffix(got[source], "|0") {
			t.Errorf("%s row %q, want evidence naming ownership_unknown and is_primary 0", source, got[source])
		}
	}
	if got["unassigned"] != "no_candidate|1" {
		t.Errorf("unassigned row %q, want no_candidate|1 (the item stays unassigned)", got["unassigned"])
	}
}

// PR control: a PR WITH an ownership fact keeps its rows byte-equal to main (the owning team's membership row).
func TestPRMembershipWithOwnershipFactEvidenceUnchanged(t *testing.T) {
	repo := "22222222-2222-4222-8222-222222222222"
	derived := NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{
		Members: []GithubWorkItemDerivationMemberFact{{Provider: "github", TeamID: "team-m", TeamName: "Member Team", MemberID: "alice", IsPrimary: 1, Specificity: 60}},
		Repos:   []GithubWorkItemDerivationRepoFact{{Provider: "github", TeamID: "team-m", TeamName: "Member Team", RepoID: &repo, IsPrimary: 1, Specificity: 100}},
	})
	_, _, candidates := derived.Resolve(GithubWorkItemDerivationSubject{WorkItemID: "ghpr:a/b#1", Provider: "github", Type: "pr", RepoID: &repo, Assignees: []string{"alice"}, Reporter: strPtr("alice"), OrgID: "o"})
	got := map[string]string{}
	for _, candidate := range candidates {
		got[candidate.Source] = candidate.Evidence + "|" + fmt.Sprint(candidate.IsPrimary)
	}
	t.Logf("PR rows with fact: %v", got)
	want := map[string]string{"assignee_membership": "assignee_membership=alice|0", "author_membership": "reporter=alice|0", "repo_ownership": "repo_ownership=22222222-2222-4222-8222-222222222222|1"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("PR rows %v, want %v", got, want)
	}
}
