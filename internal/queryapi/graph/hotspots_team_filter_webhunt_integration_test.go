//go:build integration

package graph

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/crossorg"
)

// The web code page sends teamIds in HotspotsInput; the resolver must narrow
// the served hotspots to the repositories the team owns.
func TestWebhuntHotspotsTeamFilterNarrowsServedRepos(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	admin, client := crossorg.Start(ctx, t)

	const org = "webhunt-hotspots-org"
	const repoA = "00000000-0000-4000-8000-0000000000a1"
	const repoB = "00000000-0000-4000-8000-0000000000b2"
	synced := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	from := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	for id, tc := range map[string][2]string{repoA: {"acme/alpha", "team-a"}, repoB: {"acme/beta", "team-b"}} {
		crossorg.Exec(ctx, t, admin, `INSERT INTO repos (id, repo, provider, org_id, created_at, last_synced) VALUES (?, ?, ?, ?, ?, ?)`,
			id, tc[0], "github", org, synced, synced)
		crossorg.Exec(ctx, t, admin, `INSERT INTO team_repo_ownership (org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, priority, valid_from, valid_to, updated_at, share) VALUES (?, 'github', ?, ?, ?, 'exact', 'inferred', 1, 0, 0, ?, NULL, ?, 1.0)`,
			org, tc[1], id, tc[0], from, synced)
		crossorg.Exec(ctx, t, admin, fmt.Sprintf(
			"INSERT INTO file_hotspot_daily (repo_id, day, file_path, churn_loc_30d, churn_commits_30d, cyclomatic_total, cyclomatic_avg, blame_concentration, risk_score, computed_at, org_id) VALUES (toUUID('%s'), toDate('2026-09-07'), '%s/main.go', 1, 1, 1, 1, 0.5, 5, toDateTime('2026-09-07 04:00:00'), '%s')",
			id, tc[1], org))
	}

	resolver := &queryResolver{&Resolver{ClickHouse: client}}
	rctx := authctx.WithClaims(ctx, authctx.Claims{OrgID: org})
	since := time.Date(2026, 8, 9, 0, 0, 0, 0, time.UTC)
	until := time.Date(2026, 9, 7, 0, 0, 0, 0, time.UTC)

	all, err := resolver.Hotspots(rctx, model.HotspotsInput{SinceUtc: since, UntilUtc: until})
	if err != nil {
		t.Fatalf("Hotspots without a team: %v", err)
	}
	if len(all.Rows) != 2 {
		t.Fatalf("control: no team rows = %d, want 2", len(all.Rows))
	}
	byRepo, err := resolver.Hotspots(rctx, model.HotspotsInput{SinceUtc: since, UntilUtc: until, RepoIds: []string{repoA}})
	if err != nil {
		t.Fatalf("Hotspots by repo: %v", err)
	}
	if len(byRepo.Rows) != 1 {
		t.Fatalf("control: repoIds=[A] rows = %d, want 1", len(byRepo.Rows))
	}
	team, err := resolver.Hotspots(rctx, model.HotspotsInput{SinceUtc: since, UntilUtc: until, TeamIds: []string{"team-a"}})
	if err != nil {
		t.Fatalf("Hotspots by team: %v", err)
	}
	if len(team.Rows) != 1 || len(team.Repos) != 1 {
		t.Fatalf("teamIds=[team-a] rows = %d, repos = %d, want 1 and 1 (team-b repo must be dropped)", len(team.Rows), len(team.Repos))
	}
}
