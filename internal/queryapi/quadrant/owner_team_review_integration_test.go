//go:build integration

package quadrant

import (
	"context"
	"fmt"
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
//
// team-own: review load (6+2)/2 = 4, review latency (4+8)/2 = 6.
func TestTheTeamReviewQuadrantGroupsByTheRepositoryOwner(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	admin, client := crossorg.Start(ctx, t)

	const org = "owner-org"
	const (
		repoOwn     = "00000000-0000-4000-8000-0000000000c1"
		repoNone    = "00000000-0000-4000-8000-0000000000c2"
		repoRetired = "00000000-0000-4000-8000-0000000000c3"
	)
	day := time.Date(2026, 9, 15, 0, 0, 0, 0, time.UTC)
	synced := time.Date(2026, 9, 16, 0, 0, 0, 0, time.UTC)
	for id, name := range map[string]string{repoOwn: "acme/own", repoNone: "acme/none", repoRetired: "acme/retired"} {
		crossorg.Exec(ctx, t, admin, `INSERT INTO repos (id, repo, provider, org_id, created_at, last_synced) VALUES (?, ?, 'github', ?, ?, ?)`, id, name, org, synced, synced)
	}
	for team, active := range map[string]uint8{"team-own": 1, "team-member": 1, "team-old": 0, "team-retired": 0} {
		crossorg.Exec(ctx, t, admin, `INSERT INTO teams (id, team_uuid, name, manual_members, project_keys, repo_patterns, is_active, updated_at, last_synced, org_id, provider) VALUES (?, generateUUIDv4(), ?, [], [], [], ?, ?, ?, ?, 'github')`,
			team, "Name of "+team, active, synced, synced, org)
	}
	for repo, team := range map[string]string{repoOwn: "team-own", repoRetired: "team-retired"} {
		crossorg.Exec(ctx, t, admin, `INSERT INTO team_repo_ownership (org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, priority, valid_from, valid_to, updated_at) VALUES (?, 'github', ?, ?, '', 'exact', 'inferred', 1, 0, 0, ?, NULL, ?)`,
			org, team, repo, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), synced)
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
			points = append(points, fmt.Sprintf("%s(%s) load=%v latency=%v", point.EntityID, point.EntityLabel, point.X, point.Y))
		}
		return fmt.Sprint(points)
	}

	for _, tc := range []struct {
		name  string
		teams []string
		want  string
	}{
		{"every team", nil, "[team-own(Name of team-own) load=4 latency=6]"},
		{"the owner selected", []string{"team-own"}, "[team-own(Name of team-own) load=4 latency=6]"},
		{"a membership-only team selected", []string{"team-member"}, "[]"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := run(tc.teams); got != tc.want {
				t.Fatalf("points = %s, want %s", got, tc.want)
			}
		})
	}
}
