//go:build integration

package quadrant

import (
	"context"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/crossorg"
)

// TestEveryActiveTeamInScopeIsAPointAndAnIdleOneIsNoData reads the team grain on
// a real ClickHouse (CHAOS-9815). The org holds the active teams team-a, team-b,
// team-d, an INACTIVE team-x, and team-o of ANOTHER org. Only team-a has work in
// the window. Every quadrant type that plots teams must send team-b and team-d
// as no-data points (null x and y, no_data true, never a zero), team-a as a
// plotted point, and neither team-x nor team-o.
func TestEveryActiveTeamInScopeIsAPointAndAnIdleOneIsNoData(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	admin, client := crossorg.Start(ctx, t)
	_, repo2, repo3 := seedOwnerShareRepos(ctx, t, admin)

	crossorg.Exec(ctx, t, admin, `INSERT INTO teams (id, team_uuid, name, manual_members, project_keys, repo_patterns, is_active, updated_at, last_synced, org_id, provider) VALUES ('team-x', generateUUIDv4(), 'Retired', [], [], [], 0, ?, ?, ?, 'github')`,
		ownerShareSynced, ownerShareSynced, ownerShareOrg)
	crossorg.Exec(ctx, t, admin, `INSERT INTO teams (id, team_uuid, name, manual_members, project_keys, repo_patterns, is_active, updated_at, last_synced, org_id, provider) VALUES ('team-o', generateUUIDv4(), 'Elsewhere', [], [], [], 1, ?, ?, 'another-org', 'github')`,
		ownerShareSynced, ownerShareSynced)

	// team-a (sole owner of repo-2) has work in the window on every reader.
	seedWindowCycleItems(ctx, t, admin, repo2, repo3)
	crossorg.Exec(ctx, t, admin, `DELETE FROM work_item_team_attributions WHERE org_id = ? AND team_id = 'team-b'`, ownerShareOrg)
	for _, day := range windowDays {
		crossorg.Exec(ctx, t, admin, `INSERT INTO work_item_metrics_daily (org_id, day, provider, work_scope_id, team_id, items_completed, wip_count_end_of_day, computed_at) VALUES (?, ?, 'github', 's', 'team-a', 2, 3, now())`,
			ownerShareOrg, day)
		crossorg.Exec(ctx, t, admin, `INSERT INTO user_metrics_daily (org_id, repo_id, day, author_email, identity_id, team_id, reviews_given, loc_touched, pr_first_review_p50_hours, computed_at) VALUES (?, ?, ?, 'p1', 'p1', 'team-member', 4, 10, 6, now())`,
			ownerShareOrg, repo2, day)
	}

	for _, quadrantType := range []string{"wip_throughput", "cycle_throughput", "review_load_latency"} {
		t.Run(quadrantType, func(t *testing.T) {
			resp := runWindow(ctx, t, client, quadrantType, "team", "week")
			byID := map[string]Point{}
			var ids []string
			for _, p := range resp.Points {
				byID[p.EntityID] = p
				ids = append(ids, p.EntityID)
			}
			sort.Strings(ids)
			if got := strings.Join(ids, " "); got != "team-a team-b team-d" {
				t.Fatalf("points = %s, want the active teams of the org: team-a team-b team-d", got)
			}
			if a := byID["team-a"]; a.NoData || a.X == nil || a.Y == nil {
				t.Errorf("team-a = %+v, want a plotted point", a)
			}
			for _, id := range []string{"team-b", "team-d"} {
				p := byID[id]
				if !p.NoData || p.X != nil || p.Y != nil || p.Trajectory != nil {
					t.Errorf("%s = %+v, want a no-data point with null x and y", id, p)
				}
				if p.EntityLabel != "Name of "+id || p.WindowStart != "2026-08-23" || p.WindowEnd != "2026-09-22" {
					t.Errorf("%s label/window = %q %s..%s, want its name and the requested window", id, p.EntityLabel, p.WindowStart, p.WindowEnd)
				}
			}
		})
	}

	// A retired team id that only survives in a metrics table is no team: no point.
	crossorg.Exec(ctx, t, admin, `INSERT INTO work_item_metrics_daily (org_id, day, provider, work_scope_id, team_id, items_completed, wip_count_end_of_day, computed_at) VALUES (?, ?, 'github', 's', 'team-x', 5, 5, now())`,
		ownerShareOrg, windowDays[0])
	for _, p := range runWindow(ctx, t, client, "wip_throughput", "team", "week").Points {
		if p.EntityID == "team-x" {
			t.Errorf("inactive team-x with metric rows was returned: %+v", p)
		}
	}

	// A selection narrows the no-data points to the selected active teams.
	resp, err := BuildResponse(ctx, client, ownerShareOrg, Params{
		Type: "wip_throughput", ScopeType: "team", Bucket: "week", RangeDays: 30, EndDate: &ownerShareEnd, TeamIDs: []string{"team-b", "team-x"},
	})
	if err != nil {
		t.Fatalf("BuildResponse(selected): %v", err)
	}
	if len(resp.Points) != 1 || resp.Points[0].EntityID != "team-b" || !resp.Points[0].NoData {
		t.Errorf("points for the selection team-b, team-x = %+v, want only team-b as a no-data point", resp.Points)
	}

	// The repository grain plots repositories; no team is listed there.
	repoResp := runWindow(ctx, t, client, "wip_throughput", "repo", "week")
	for _, p := range repoResp.Points {
		if p.NoData {
			t.Errorf("repository grain sent a no-data point %+v", p)
		}
	}
}
