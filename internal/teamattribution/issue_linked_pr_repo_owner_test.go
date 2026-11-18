package teamattribution

import (
	"fmt"
	"sort"
	"strings"
	"testing"
)

const (
	linkedRepoA = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	linkedRepoB = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
	linkedRepoN = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
)

// linkedRepoFact is a repository ownership fact of a GitHub or GitLab repository.
func linkedRepoFact(provider, repoID, teamID string, isPrimary, specificity int) GithubWorkItemDerivationRepoFact {
	repo := repoID
	return GithubWorkItemDerivationRepoFact{
		Provider: provider, TeamID: teamID, TeamName: "Team " + teamID, RepoID: &repo,
		RepoFullName: "acme/" + teamID, IsPrimary: isPrimary, Specificity: specificity,
	}
}

func linkedPR(repoID string, number uint32, tier string) GithubWorkItemDerivationLinkedPullRequest {
	return GithubWorkItemDerivationLinkedPullRequest{RepoID: repoID, PRNumber: number, Tier: tier}
}

// linkedRows resolves the subject and returns its rows as source|team|is_primary, sorted, and the evidence of each
// repo_ownership row by team.
func linkedRows(t *testing.T, derived GithubWorkItemDerivationContext, subject GithubWorkItemDerivationSubject) ([]string, map[string]string) {
	t.Helper()
	_, _, candidates := derived.Resolve(subject)
	var rows []string
	evidence := map[string]string{}
	for _, candidate := range candidates {
		team := GithubWorkItemDerivationStringValue(candidate.TeamID)
		rows = append(rows, fmt.Sprintf("%s|%s|%d", candidate.Source, team, candidate.IsPrimary))
		if candidate.Source == "repo_ownership" {
			evidence[team] = candidate.Evidence
		}
	}
	sort.Strings(rows)
	return rows, evidence
}

func wantLinkedRows(t *testing.T, got []string, want ...string) {
	t.Helper()
	sort.Strings(want)
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("rows %v, want %v", got, want)
	}
}

// Every issue kind of every provider takes the owners of the repositories of its linked pull requests
// (work_graph_issue_pr, the issue's best tier only); the repositories are GitHub's and GitLab's.
func TestAnIssueTakesTheOwnerOfItsLinkedPullRequestsRepository(t *testing.T) {
	issues := []struct{ provider, itemType string }{{"github", "issue"}, {"gitlab", "issue"}, {"jira", "story"}, {"linear", "issue"}}
	for _, repoProvider := range []string{"github", "gitlab"} {
		for _, kind := range issues {
			name := repoProvider + "/" + kind.provider + "/" + kind.itemType
			subject := GithubWorkItemDerivationSubject{WorkItemID: kind.provider + ":ISS-1", Provider: kind.provider, Type: kind.itemType, OrgID: "org"}
			resolve := func(t *testing.T, facts GithubWorkItemDerivationFacts, links ...GithubWorkItemDerivationLinkedPullRequest) ([]string, map[string]string) {
				t.Helper()
				derived := NewGitHubWorkItemDerivationContext(facts)
				derived.LinkedPullRequests = map[string][]GithubWorkItemDerivationLinkedPullRequest{subject.WorkItemID: links}
				return linkedRows(t, derived, subject)
			}
			ownerA, ownerB := linkedRepoFact(repoProvider, linkedRepoA, "team-a", 1, 100), linkedRepoFact(repoProvider, linkedRepoB, "team-b", 1, 100)

			t.Run(name+"/one link: the repository owner", func(t *testing.T) {
				rows, evidence := resolve(t, GithubWorkItemDerivationFacts{Repos: []GithubWorkItemDerivationRepoFact{ownerA}}, linkedPR(linkedRepoA, 7, "native"))
				wantLinkedRows(t, rows, "repo_ownership|team-a|1")
				if want := "repo_ownership=" + linkedRepoA + ";work_graph_issue_pr=7;tier=native"; evidence["team-a"] != want {
					t.Errorf("evidence %q, want %q", evidence["team-a"], want)
				}
			})
			t.Run(name+"/two repositories of one owner: one row", func(t *testing.T) {
				sameOwner := linkedRepoFact(repoProvider, linkedRepoB, "team-a", 1, 100)
				rows, _ := resolve(t, GithubWorkItemDerivationFacts{Repos: []GithubWorkItemDerivationRepoFact{ownerA, sameOwner}},
					linkedPR(linkedRepoA, 1, "native"), linkedPR(linkedRepoB, 2, "native"))
				wantLinkedRows(t, rows, "repo_ownership|team-a|1")
			})
			t.Run(name+"/the most links make the main owner, the other owner a co-owner", func(t *testing.T) {
				// team-b's fact outranks team-a's (specificity), but team-a holds 2 of the 3 links.
				strongB := linkedRepoFact(repoProvider, linkedRepoB, "team-b", 1, 200)
				rows, evidence := resolve(t, GithubWorkItemDerivationFacts{Repos: []GithubWorkItemDerivationRepoFact{ownerA, strongB}},
					linkedPR(linkedRepoA, 1, "native"), linkedPR(linkedRepoA, 2, "native"), linkedPR(linkedRepoB, 3, "native"))
				wantLinkedRows(t, rows, "repo_ownership|team-a|1", "repo_ownership|team-b|2")
				if want := "repo_ownership=" + linkedRepoB + ";work_graph_issue_pr=3;tier=native"; evidence["team-b"] != want {
					t.Errorf("co-owner evidence %q, want %q", evidence["team-b"], want)
				}
			})
			t.Run(name+"/a tie of links: the ranked fact, then the lowest team id", func(t *testing.T) {
				strongB := linkedRepoFact(repoProvider, linkedRepoB, "team-b", 1, 200)
				rows, _ := resolve(t, GithubWorkItemDerivationFacts{Repos: []GithubWorkItemDerivationRepoFact{ownerA, strongB}},
					linkedPR(linkedRepoA, 1, "native"), linkedPR(linkedRepoB, 2, "native"))
				wantLinkedRows(t, rows, "repo_ownership|team-a|2", "repo_ownership|team-b|1")
				rows, _ = resolve(t, GithubWorkItemDerivationFacts{Repos: []GithubWorkItemDerivationRepoFact{ownerB, ownerA}},
					linkedPR(linkedRepoB, 1, "native"), linkedPR(linkedRepoA, 2, "native"))
				wantLinkedRows(t, rows, "repo_ownership|team-a|1", "repo_ownership|team-b|2")
			})
			t.Run(name+"/best tier only: a native link to an unowned repository shadows a heuristic link", func(t *testing.T) {
				rows, _ := resolve(t, GithubWorkItemDerivationFacts{Repos: []GithubWorkItemDerivationRepoFact{ownerA}},
					linkedPR(linkedRepoN, 1, "native"), linkedPR(linkedRepoA, 2, "heuristic"))
				wantLinkedRows(t, rows, "unassigned||1")
			})
			t.Run(name+"/a lower tier counts when it is the issue's best", func(t *testing.T) {
				for _, tier := range []string{"explicit_text", "heuristic"} {
					rows, evidence := resolve(t, GithubWorkItemDerivationFacts{Repos: []GithubWorkItemDerivationRepoFact{ownerA}}, linkedPR(linkedRepoA, 4, tier))
					wantLinkedRows(t, rows, "repo_ownership|team-a|1")
					if !strings.HasSuffix(evidence["team-a"], ";tier="+tier) {
						t.Errorf("evidence %q, want tier %s", evidence["team-a"], tier)
					}
				}
			})
			t.Run(name+"/an unknown tier never counts", func(t *testing.T) {
				rows, _ := resolve(t, GithubWorkItemDerivationFacts{Repos: []GithubWorkItemDerivationRepoFact{ownerA}}, linkedPR(linkedRepoA, 4, "guess"))
				wantLinkedRows(t, rows, "unassigned||1")
			})
			t.Run(name+"/a repository with a main owner and a co-owner fact", func(t *testing.T) {
				coOwner := linkedRepoFact(repoProvider, linkedRepoA, "team-c", AttributionCoOwner, 100)
				rows, _ := resolve(t, GithubWorkItemDerivationFacts{Repos: []GithubWorkItemDerivationRepoFact{ownerA, coOwner}}, linkedPR(linkedRepoA, 1, "native"))
				wantLinkedRows(t, rows, "repo_ownership|team-a|1", "repo_ownership|team-c|2")
			})
			t.Run(name+"/a repository with only a co-owner fact gives no primary", func(t *testing.T) {
				coOwner := linkedRepoFact(repoProvider, linkedRepoA, "team-c", AttributionCoOwner, 100)
				rows, _ := resolve(t, GithubWorkItemDerivationFacts{Repos: []GithubWorkItemDerivationRepoFact{coOwner}}, linkedPR(linkedRepoA, 1, "native"))
				wantLinkedRows(t, rows, "repo_ownership|team-c|0", "unassigned||1")
			})
			t.Run(name+"/an inactive owner gives nothing", func(t *testing.T) {
				rows, _ := resolve(t, GithubWorkItemDerivationFacts{
					Repos: []GithubWorkItemDerivationRepoFact{ownerA},
					Teams: []GithubWorkItemDerivationTeamFact{{Provider: repoProvider, TeamID: "team-a", TeamName: "Team team-a", Inactive: true}},
				}, linkedPR(linkedRepoA, 1, "native"))
				wantLinkedRows(t, rows, "unassigned||1")
			})
			t.Run(name+"/the project owner wins; the repository owner is a provenance row", func(t *testing.T) {
				projectID := "PRJ"
				owned := subject
				owned.ProjectID = &projectID
				derived := NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{
					Repos:    []GithubWorkItemDerivationRepoFact{ownerA},
					Projects: []GithubWorkItemDerivationProjectFact{{Provider: kind.provider, TeamID: "team-p", TeamName: "P", ProjectID: projectID, IsPrimary: 1, Specificity: 100}},
				})
				derived.LinkedPullRequests = map[string][]GithubWorkItemDerivationLinkedPullRequest{owned.WorkItemID: {linkedPR(linkedRepoA, 1, "native")}}
				rows, _ := linkedRows(t, derived, owned)
				wantLinkedRows(t, rows, "project_ownership|team-p|1", "repo_ownership|team-a|0")
			})
			t.Run(name+"/the repository owner wins over a project fallback rule", func(t *testing.T) {
				projectKey := "PRJ"
				ruled := subject
				ruled.ProjectKey = &projectKey
				derived := NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{
					Repos:           []GithubWorkItemDerivationRepoFact{ownerA},
					ManualFallbacks: []GithubWorkItemDerivationManualFallback{{Provider: kind.provider, ScopeType: "project", ScopeID: projectKey, TeamID: "team-rule", TeamName: "Rule", Priority: 1}},
				})
				derived.LinkedPullRequests = map[string][]GithubWorkItemDerivationLinkedPullRequest{ruled.WorkItemID: {linkedPR(linkedRepoA, 1, "native")}}
				rows, _ := linkedRows(t, derived, ruled)
				wantLinkedRows(t, rows, "manual_fallback|team-rule|0", "repo_ownership|team-a|1")
			})
		}
	}
}

// A pull or merge request takes repo_ownership from its own repository only; links of work_graph_issue_pr are an
// issue's.
func TestAPullRequestIgnoresLinkedPullRequestRepositories(t *testing.T) {
	for _, kind := range []struct{ provider, itemType string }{{"github", "pr"}, {"gitlab", "merge_request"}} {
		t.Run(kind.provider, func(t *testing.T) {
			own := linkedRepoN
			derived := NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{Repos: []GithubWorkItemDerivationRepoFact{
				linkedRepoFact(kind.provider, linkedRepoN, "team-own", 1, 100), linkedRepoFact(kind.provider, linkedRepoA, "team-a", 1, 100),
			}})
			subject := GithubWorkItemDerivationSubject{WorkItemID: kind.provider + ":pr#1", Provider: kind.provider, Type: kind.itemType, RepoID: &own, OrgID: "org"}
			derived.LinkedPullRequests = map[string][]GithubWorkItemDerivationLinkedPullRequest{subject.WorkItemID: {linkedPR(linkedRepoA, 1, "native")}}
			rows, evidence := linkedRows(t, derived, subject)
			wantLinkedRows(t, rows, "repo_ownership|team-own|1")
			if want := "repo_ownership=" + linkedRepoN; evidence["team-own"] != want {
				t.Errorf("evidence %q, want %q", evidence["team-own"], want)
			}
		})
	}
}

func TestBestTierLinkedPullRequests(t *testing.T) {
	got := BestTierLinkedPullRequests([]GithubWorkItemDerivationLinkedPullRequest{
		linkedPR(linkedRepoB, 2, "explicit_text"), linkedPR(linkedRepoA, 9, "heuristic"),
		linkedPR(linkedRepoA, 3, "explicit_text"), linkedPR(" "+linkedRepoA, 3, "explicit_text"), linkedPR("", 1, "native"),
	})
	want := []GithubWorkItemDerivationLinkedPullRequest{linkedPR(linkedRepoA, 3, "explicit_text"), linkedPR(linkedRepoB, 2, "explicit_text")}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("best tier %v, want %v", got, want)
	}
	if got := BestTierLinkedPullRequests([]GithubWorkItemDerivationLinkedPullRequest{linkedPR(linkedRepoA, 1, "guess")}); got != nil {
		t.Errorf("unknown tier kept %v", got)
	}
}
