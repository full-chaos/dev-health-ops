//go:build integration

package quadrant

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/crossorg"
)

// TestTheTeamReviewQuadrantGroupsByTheRepositoryOwner reads the team review
// load / review latency quadrant. A team is the ownership of repositories: the
// point of a team holds the reviews in the repositories it owns now, whatever
// team id the person's rows store (that id is the person's membership team).
//
//   - repo-own (owned by team-own): p1 stores team-member (a team that owns no
//     repository), p2 stores team-old (inactive now). Both are team-own's.
//   - repo-none (owned by nobody): p3 stores team-own. No team's point.
//   - repo-retired (owned by team-retired, inactive now): p4. No point.
//   - repo-shared (owned by team-own AND team-co, no linked share measured, so
//     0.5 each): p5. A point of each owner, at half weight (CHAOS-9562).
//
// team-own: review load (6+2+0.5*3)/(1+1+0.5), review latency (4+8+0.5*5)/(1+1+0.5).
// team-co: review load 0.5*3/0.5, review latency 5.
func TestTheTeamReviewQuadrantGroupsByTheRepositoryOwner(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	admin, client := crossorg.Start(ctx, t)

	const org = "owner-org"
	const (
		repoOwn     = "00000000-0000-4000-8000-0000000000c1"
		repoNone    = "00000000-0000-4000-8000-0000000000c2"
		repoRetired = "00000000-0000-4000-8000-0000000000c3"
		repoShared  = "00000000-0000-4000-8000-0000000000c4"
	)
	day := time.Date(2026, 9, 15, 0, 0, 0, 0, time.UTC)
	synced := time.Date(2026, 9, 16, 0, 0, 0, 0, time.UTC)
	for id, name := range map[string]string{repoOwn: "acme/own", repoNone: "acme/none", repoRetired: "acme/retired", repoShared: "acme/shared"} {
		crossorg.Exec(ctx, t, admin, `INSERT INTO repos (id, repo, provider, org_id, created_at, last_synced) VALUES (?, ?, 'github', ?, ?, ?)`, id, name, org, synced, synced)
	}
	for team, active := range map[string]uint8{"team-own": 1, "team-co": 1, "team-member": 1, "team-old": 0, "team-retired": 0} {
		crossorg.Exec(ctx, t, admin, `INSERT INTO teams (id, team_uuid, name, manual_members, project_keys, repo_patterns, is_active, updated_at, last_synced, org_id, provider) VALUES (?, generateUUIDv4(), ?, [], [], [], ?, ?, ?, ?, 'github')`,
			team, "Name of "+team, active, synced, synced, org)
	}
	for _, owner := range []struct{ repo, name, team string }{
		{repoOwn, "acme/own", "team-own"}, {repoRetired, "acme/retired", "team-retired"},
		{repoShared, "acme/shared", "team-own"}, {repoShared, "acme/shared", "team-co"},
	} {
		crossorg.Exec(ctx, t, admin, `INSERT INTO team_repo_ownership (org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, priority, valid_from, valid_to, updated_at) VALUES (?, 'github', ?, ?, ?, 'exact', 'inferred', 1, 0, 0, ?, NULL, ?)`,
			org, owner.team, owner.repo, owner.name, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), synced)
	}
	for _, row := range []struct {
		repo, person, team string
		reviews            uint32
		latency            float64
	}{
		{repoOwn, "p1", "team-member", 6, 4},
		{repoOwn, "p2", "team-old", 2, 8},
		{repoNone, "p3", "team-own", 10, 1},
		{repoRetired, "p4", "team-retired", 5, 2},
		{repoShared, "p5", "team-member", 3, 5},
	} {
		crossorg.Exec(ctx, t, admin, `INSERT INTO user_metrics_daily (org_id, repo_id, day, author_email, identity_id, team_id, reviews_given, pr_first_review_p50_hours, computed_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, now())`,
			org, row.repo, day, row.person, row.person, row.team, row.reviews, row.latency)
	}

	end := time.Date(2026, 9, 21, 0, 0, 0, 0, time.UTC)
	run := func(teams []string) string {
		t.Helper()
		resp, err := BuildResponse(ctx, client, org, Params{
			Type: "review_load_latency", ScopeType: "team", Bucket: "week", RangeDays: 30, EndDate: &end, TeamIDs: teams,
		})
		if err != nil {
			t.Fatalf("BuildResponse(teams %v): %v", teams, err)
		}
		var points []string
		for _, point := range resp.Points {
			if point.NoData {
				points = append(points, fmt.Sprintf("%s(%s) no data", point.EntityID, point.EntityLabel))
				continue
			}
			points = append(points, fmt.Sprintf("%s(%s) load=%.4f latency=%.4f", point.EntityID, point.EntityLabel, *point.X, *point.Y))
		}
		sort.Strings(points)
		return fmt.Sprint(points)
	}

	for _, tc := range []struct {
		name  string
		teams []string
		want  string
	}{
		{"every team", nil, "[team-co(Name of team-co) load=3.0000 latency=5.0000 team-member(Name of team-member) no data team-own(Name of team-own) load=3.8000 latency=5.8000]"},
		{"the owner selected", []string{"team-own"}, "[team-own(Name of team-own) load=3.8000 latency=5.8000]"},
		{"a co-owner selected", []string{"team-co"}, "[team-co(Name of team-co) load=3.0000 latency=5.0000]"},
		{"a membership-only team selected", []string{"team-member"}, "[team-member(Name of team-member) no data]"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := run(tc.teams); got != tc.want {
				t.Fatalf("points = %s, want %s", got, tc.want)
			}
		})
	}
}
