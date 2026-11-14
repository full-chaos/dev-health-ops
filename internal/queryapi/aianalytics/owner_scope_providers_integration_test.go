//go:build integration

package aianalytics

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// Provider coverage on a real ClickHouse: per provider, a repository owned by
// repo_id, one owned through the name join (repo_id NULL, provider + full name),
// and one the team's pattern selects with no owner row. The owned ones are in
// the team scope and carry the team; the pattern-only one is out and has no team.
func TestRealClickHouse_OwnerRuleCoversEveryProvider(t *testing.T) {
	ctx, conn, client := startStore(t)
	const org = "00000000-0000-0000-0000-0000000000a3"
	const team = "team-owner"
	providers := []string{"github", "gitlab", "jira", "linear"}
	const (
		byID        = 1
		byName      = 2
		patternOnly = 3
	)
	repoID := func(pi, kind int) string { return fmt.Sprintf("00000000-0000-4000-80%02d-%012d", pi, kind) }
	repoName := func(provider string, kind int) string { return fmt.Sprintf("acme/%s-%d", provider, kind) }
	var patterns, all []string
	for pi, p := range providers {
		for _, kind := range []int{byID, byName, patternOnly} {
			exec(t, ctx, conn, `INSERT INTO repos (id, repo, provider, org_id, created_at, last_synced)
            SELECT '%s', '%s', '%s', '%s', now64(3), now64(3)`, repoID(pi, kind), repoName(p, kind), p, org)
			all = append(all, repoID(pi, kind))
		}
		patterns = append(patterns, "'"+repoName(p, patternOnly)+"'")
		exec(t, ctx, conn, `INSERT INTO team_repo_ownership (org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, priority, valid_from, valid_to, updated_at)
            SELECT '%s', '%s', '%s', '%s', '%s', 'exact', 'native', 1, 0, 0, toDateTime64('2026-01-01 00:00:00', 3, 'UTC'), NULL, now64(3)`,
			org, p, team, repoID(pi, byID), repoName(p, byID))
		exec(t, ctx, conn, `INSERT INTO team_repo_ownership (org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, priority, valid_from, valid_to, updated_at)
            SELECT '%s', '%s', '%s', NULL, '%s', 'exact', 'provider_access', 1, 0, 0, toDateTime64('2026-01-01 00:00:00', 3, 'UTC'), NULL, now64(3)`,
			org, p, team, repoName(p, byName))
	}
	exec(t, ctx, conn, `INSERT INTO teams (id, team_uuid, name, repo_patterns, org_id, updated_at)
        SELECT '%s', generateUUIDv4(), 'Owner Team', [%s], '%s', now64(6)`, team, strings.Join(patterns, ", "), org)

	ids, ok := teamRepoIDs(ctx, client, org, team, "test")
	if !ok {
		t.Fatal("the team scope must resolve")
	}
	inScope := map[string]bool{}
	for _, id := range ids {
		inScope[id] = true
	}
	owners, err := ownerTeams(ctx, client, org, all, "", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	for pi, p := range providers {
		t.Run(p, func(t *testing.T) {
			for _, kind := range []int{byID, byName} {
				id := repoID(pi, kind)
				if !inScope[id] {
					t.Errorf("owned %s repository %s (kind %d) is not in the team scope %v", p, id, kind, ids)
				}
				if nameOrNil(owners[id]) != team {
					t.Errorf("owned %s repository %s (kind %d) has team %s, want %s", p, id, kind, nameOrNil(owners[id]), team)
				}
			}
			id := repoID(pi, patternOnly)
			if inScope[id] {
				t.Errorf("pattern-only %s repository %s is in the team scope", p, id)
			}
			if owners[id] != nil {
				t.Errorf("pattern-only %s repository %s has team %s, want none", p, id, *owners[id])
			}
		})
	}
}
