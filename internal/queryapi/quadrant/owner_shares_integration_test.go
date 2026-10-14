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

// CHAOS-9693 on a real store: a churn_throughput repository point names its ACTIVE owning teams with the share each
// holds (team_repo_ownership.share renormalised over every owner, D5988). A team scope removes rows and never
// re-weighs the ones that stay; a repository with no active owner names none.
//
//   - acme/shared: team-a 0.7, team-b 0.3 (stored shares), churn 100.
//   - acme/solo: team-a only, churn 40.
//   - acme/retired: owned by team-retired (inactive now): names no owner.
//   - acme/orphan: owned by nobody: names no owner.
func TestChurnPointsNameTheirActiveOwnersWithShares(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	admin, client := crossorg.Start(ctx, t)

	const org = "owner-shares-org"
	repos := map[string]string{
		"00000000-0000-4000-8000-0000000000d1": "acme/shared",
		"00000000-0000-4000-8000-0000000000d2": "acme/solo",
		"00000000-0000-4000-8000-0000000000d3": "acme/retired",
		"00000000-0000-4000-8000-0000000000d4": "acme/orphan",
	}
	day := time.Date(2026, 9, 15, 0, 0, 0, 0, time.UTC)
	synced := time.Date(2026, 9, 16, 0, 0, 0, 0, time.UTC)
	churn := map[string]int{"acme/shared": 100, "acme/solo": 40, "acme/retired": 9, "acme/orphan": 7}
	for id, name := range repos {
		crossorg.Exec(ctx, t, admin, `INSERT INTO repos (id, repo, provider, org_id, created_at, last_synced) VALUES (?, ?, 'github', ?, ?, ?)`, id, name, org, synced, synced)
		crossorg.Exec(ctx, t, admin, `INSERT INTO repo_metrics_daily (org_id, repo_id, day, total_loc_touched, prs_merged, computed_at) VALUES (?, ?, ?, ?, 1, now())`, org, id, day, churn[name])
	}
	for team, active := range map[string]uint8{"team-a": 1, "team-b": 1, "team-retired": 0} {
		crossorg.Exec(ctx, t, admin, `INSERT INTO teams (id, team_uuid, name, manual_members, project_keys, repo_patterns, is_active, updated_at, last_synced, org_id, provider) VALUES (?, generateUUIDv4(), ?, [], [], [], ?, ?, ?, ?, 'github')`,
			team, "Name of "+team, active, synced, synced, org)
	}
	for _, owner := range []struct {
		repo, team string
		share      float64
	}{
		{"00000000-0000-4000-8000-0000000000d1", "team-a", 0.7}, {"00000000-0000-4000-8000-0000000000d1", "team-b", 0.3},
		{"00000000-0000-4000-8000-0000000000d2", "team-a", 1.0}, {"00000000-0000-4000-8000-0000000000d3", "team-retired", 1.0},
	} {
		crossorg.Exec(ctx, t, admin, `INSERT INTO team_repo_ownership (org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, priority, valid_from, valid_to, updated_at, share) VALUES (?, 'github', ?, ?, ?, 'exact', 'inferred', 1, 0, 0, ?, NULL, ?, ?)`,
			org, owner.team, owner.repo, repos[owner.repo], time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), synced, owner.share)
	}

	end := time.Date(2026, 9, 21, 0, 0, 0, 0, time.UTC)
	run := func(scopeType string, teams []string) string {
		t.Helper()
		resp, err := BuildResponse(ctx, client, org, Params{
			Type: "churn_throughput", ScopeType: scopeType, Bucket: "week", RangeDays: 30, EndDate: &end, TeamIDs: teams,
		})
		if err != nil {
			t.Fatalf("BuildResponse(%s, teams %v): %v", scopeType, teams, err)
		}
		var points []string
		for _, point := range resp.Points {
			owners := ""
			for _, owner := range point.OwnerShares {
				owners += fmt.Sprintf(" %s(%s)=%.2f", owner.TeamID, owner.TeamLabel, owner.Share)
			}
			points = append(points, fmt.Sprintf("%s x=%.0f owners:%s", point.EntityID, point.X, owners))
		}
		sort.Strings(points)
		return fmt.Sprint(points)
	}

	for _, tc := range []struct {
		name      string
		scopeType string
		teams     []string
		want      string
	}{
		{"org scope", "org", nil, "[acme/orphan x=7 owners: acme/retired x=9 owners: acme/shared x=100 owners: team-a(Name of team-a)=0.70 team-b(Name of team-b)=0.30 acme/solo x=40 owners: team-a(Name of team-a)=1.00]"},
		{"team scope keeps the co-owner's own share", "team", []string{"team-b"}, "[acme/shared x=100 owners: team-b(Name of team-b)=0.30]"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := run(tc.scopeType, tc.teams); got != tc.want {
				t.Fatalf("points = %s, want %s", got, tc.want)
			}
		})
	}
}
