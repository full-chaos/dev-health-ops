//go:build integration

package teamattribution

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// LoadRepos carries a co-owner row (is_primary = 2) of a repository (CHAOS-9409):
// an item of the repository gets the main owner as its primary row and the
// co-owner as a co-owner row; a repository with co-owners only gives no primary
// from repository ownership, because a co-owner is never the primary.
func TestLoadReposCarriesCoOwnerRows(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	conn := openTeamAttributionSchema(ctx, t)
	const org = "org-repo-coowner"
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	repoShared, repoTied := "11111111-1111-4111-8111-111111111111", "22222222-2222-4222-8222-222222222222"
	for _, row := range []struct {
		team, repo, repoID string
		primary            uint8
	}{
		{"team-main", "acme/shared", repoShared, 1},
		{"team-co", "acme/shared", repoShared, 2},
		{"team-tied-a", "acme/tied", repoTied, 2},
		{"team-tied-b", "acme/tied", repoTied, 2},
	} {
		if err := conn.Exec(ctx, `INSERT INTO team_repo_ownership
			(org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, priority, valid_from, valid_to, updated_at)
			VALUES (?, 'github', ?, toUUID(?), ?, 'exact', 'inferred', ?, 150, 0, ?, NULL, ?)`,
			org, row.team, row.repoID, row.repo, row.primary, at, at); err != nil {
			t.Fatalf("insert %s: %v", row.team, err)
		}
	}
	facts, err := ClickHouseFactSource{Conn: conn}.LoadRepos(ctx, org, at.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	roles := map[string]int{}
	for _, fact := range facts {
		roles[fact.TeamID] = fact.IsPrimary
	}
	if len(facts) != 4 || roles["team-main"] != 1 || roles["team-co"] != 2 || roles["team-tied-a"] != 2 || roles["team-tied-b"] != 2 {
		t.Fatalf("facts = %+v, want the main owner and the three co-owner rows with their roles", facts)
	}
	derived := NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{Repos: facts})
	for repo, want := range map[string]map[string]string{
		repoShared: {"team-main": "map[repo_ownership=1:1]", "team-co": "map[repo_ownership=2:1]"},
		repoTied:   {"team-tied-a": "map[]", "team-tied-b": "map[]"},
	} {
		repo := repo
		_, _, candidates := derived.Resolve(GithubWorkItemDerivationSubject{WorkItemID: "ghpr:" + repo + "#1", Provider: "github", RepoID: &repo, OrgID: org})
		byTeam := rowsByTeam(candidates)
		for team, wantMarks := range want {
			if got := fmt.Sprint(marks(byTeam[team])); got != wantMarks {
				t.Errorf("repo %s: %s marks %s, want %s", repo, team, got, wantMarks)
			}
		}
	}
}
