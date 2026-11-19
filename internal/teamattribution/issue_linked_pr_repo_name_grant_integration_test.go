//go:build integration

package teamattribution

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// A name-keyed repository grant (repo_id NULL, as the GitHub team catalog writes every grant) reaches an issue
// through its linked pull request's repository: LoadRepos resolves the grant's name to the repository id through
// the repos rows of the same organization. A name that names no repository of the organization, or only a
// repository of another organization, gives the issue nothing.
func TestAnIssueTakesANameKeyedGrantOfItsLinkedPullRequestsRepository(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	conn := openTeamAttributionSchema(ctx, t)
	const org, otherOrg = "org-name-grant", "org-name-grant-other"
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	repoOwned := "33333333-3333-4333-8333-333333333333"
	repoOtherOrg := "44444444-4444-4444-8444-444444444444"
	repoUnknown := "55555555-5555-4555-8555-555555555555"
	repoLonger := "66666666-6666-4666-8666-666666666666"
	for _, row := range []struct{ org, id, name string }{
		{org, repoOwned, "acme/api"},
		{otherOrg, repoOtherOrg, "acme/elsewhere"},
		{org, repoLonger, "acme/api-gateway"},
	} {
		if err := conn.Exec(ctx, `INSERT INTO repos (id, repo, org_id, provider, last_synced) VALUES (toUUID(?), ?, ?, 'github', ?)`,
			row.id, row.name, row.org, at); err != nil {
			t.Fatal(err)
		}
	}
	for _, grant := range []struct{ team, repo string }{
		{"gh:api", "acme/api"},
		{"gh:elsewhere", "acme/elsewhere"},
		{"gh:ghost", "acme/ghost"},
	} {
		if err := conn.Exec(ctx, `INSERT INTO team_repo_ownership
			(org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, priority, valid_from, valid_to, updated_at)
			VALUES (?, 'github', ?, NULL, ?, 'exact', 'provider_access', 0, 100, 10, ?, NULL, ?)`,
			org, grant.team, grant.repo, at, at); err != nil {
			t.Fatalf("insert %s: %v", grant.team, err)
		}
	}
	facts, err := ClickHouseFactSource{Conn: conn}.LoadRepos(ctx, org, at.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	derived := NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{Repos: facts})
	for _, cell := range []struct{ name, linkedRepo, want string }{
		{"a grant name of a repository of the organization", repoOwned, "gh:api repo_ownership 1"},
		{"a grant name that is only another organization's repository", repoOtherOrg, ""},
		{"a linked repository no grant names", repoUnknown, ""},
		{"a repository whose name only starts with a grant name", repoLonger, ""},
	} {
		t.Run(cell.name, func(t *testing.T) {
			issueID := "gh:acme/tracker#1"
			derived.LinkedPullRequests = map[string][]GithubWorkItemDerivationLinkedPullRequest{
				issueID: {{RepoID: cell.linkedRepo, PRNumber: 7, Tier: "native"}},
			}
			_, _, candidates := derived.Resolve(GithubWorkItemDerivationSubject{WorkItemID: issueID, Provider: "github", Type: "issue", OrgID: org})
			got := ""
			for _, candidate := range candidates {
				if candidate.Source == "repo_ownership" {
					got = fmt.Sprintf("%s %s %d", GithubWorkItemDerivationStringValue(candidate.TeamID), candidate.Source, candidate.IsPrimary)
				}
			}
			if got != cell.want {
				t.Fatalf("repo_ownership candidate = %q, want %q (candidates %+v)", got, cell.want, candidates)
			}
		})
	}
}
