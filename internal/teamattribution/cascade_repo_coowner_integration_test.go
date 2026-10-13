//go:build integration

package teamattribution

import (
	"context"
	"testing"
	"time"
)

// LoadRepos leaves out a co-owner row (is_primary = 2): repository ownership
// gives an item one team, so a co-owner never becomes a repo_ownership
// candidate, and a repository with co-owners only gives none.
func TestLoadReposLeavesOutCoOwnerRows(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	conn := openTeamAttributionSchema(ctx, t)
	const org = "org-repo-coowner"
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, row := range []struct {
		team, repo string
		primary    uint8
	}{
		{"team-main", "acme/shared", 1},
		{"team-co", "acme/shared", 2},
		{"team-tied-a", "acme/tied", 2},
		{"team-tied-b", "acme/tied", 2},
	} {
		if err := conn.Exec(ctx, `INSERT INTO team_repo_ownership
			(org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, priority, valid_from, valid_to, updated_at)
			VALUES (?, 'github', ?, NULL, ?, 'exact', 'inferred', ?, 150, 0, ?, NULL, ?)`,
			org, row.team, row.repo, row.primary, at, at); err != nil {
			t.Fatalf("insert %s: %v", row.team, err)
		}
	}
	facts, err := ClickHouseFactSource{Conn: conn}.LoadRepos(ctx, org, at.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if len(facts) != 1 || facts[0].TeamID != "team-main" || facts[0].RepoFullName != "acme/shared" {
		t.Fatalf("facts = %+v, want only team-main on acme/shared", facts)
	}
}
