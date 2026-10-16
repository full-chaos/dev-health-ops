//go:build integration

package quadrant

import (
	"context"
	"fmt"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/crossorg"
)

// windowDays are three days in three different weeks (the bucket is a week) inside the window that ends at
// ownerShareEnd and is 30 days long.
var windowDays = []time.Time{
	time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC),
	time.Date(2026, 9, 8, 0, 0, 0, 0, time.UTC),
	time.Date(2026, 9, 15, 0, 0, 0, 0, time.UTC),
}

func runWindow(ctx context.Context, t *testing.T, client QueryClient, quadrantType, scope string, bucket string) *Response {
	t.Helper()
	resp, err := BuildResponse(ctx, client, ownerShareOrg, Params{
		Type: quadrantType, ScopeType: scope, Bucket: bucket, RangeDays: 30, EndDate: &ownerShareEnd,
	})
	if err != nil {
		t.Fatalf("BuildResponse(%s): %v", quadrantType, err)
	}
	return resp
}

func seedWindowCycleItems(ctx context.Context, t *testing.T, admin stdclickhouse.Conn, repo2, repo3 string) {
	t.Helper()
	type item struct {
		id, repo, team string
		day            time.Time
		cycle          float64
	}
	items := []item{
		{"a1", repo2, "team-a", windowDays[0], 24},
		{"a2", repo2, "team-a", windowDays[1], 48},
		{"a3", repo2, "team-a", windowDays[1], 72},
		{"a4", repo2, "team-a", windowDays[2], 120},
		{"b1", repo3, "team-b", windowDays[0], 12},
		{"b2", repo3, "team-b", windowDays[2], 36},
	}
	for _, it := range items {
		computed := it.day.Add(3 * time.Hour)
		crossorg.Exec(ctx, t, admin, `
INSERT INTO work_item_cycle_times
    (work_item_id, provider, day, work_scope_id, type, status, created_at, started_at, completed_at, cycle_time_hours, lead_time_hours, computed_at, org_id)
VALUES (?, 'github', ?, 'KEY', 'story', 'done', ?, ?, ?, ?, 6, ?, ?)`,
			it.id, it.day, it.day, it.day, it.day.Add(time.Hour), it.cycle, computed, ownerShareOrg)
		crossorg.Exec(ctx, t, admin, `
INSERT INTO work_item_team_attributions
    (org_id, repo_id, work_item_id, provider, team_id, team_name, source, is_primary, confidence, evidence, computed_at)
VALUES (?, ?, ?, 'github', ?, ?, 'issue_project', 1, 'high', 'KEY', ?)`,
			ownerShareOrg, it.repo, it.id, it.team, "Name of "+it.team, computed)
	}
}

// TestTheTeamPointAggregatesTheWholeWindowNotTheLastBucket reads cycle time x throughput at the team grain with a
// weekly bucket over three weeks (CHAOS-9816). team-a completes 1, 2 and 1 items (cycle 24 | 48, 72 | 120 h) and
// team-b completes 1 and 1 (12 | 36 h). The point is the whole window: throughput is the sum over the window, the
// cycle time is the mean over the window's items (a mean of the weekly means, 68 h for team-a, is wrong), and the
// trajectory keeps the weekly values.
func TestTheTeamPointAggregatesTheWholeWindowNotTheLastBucket(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	admin, client := crossorg.Start(ctx, t)
	_, repo2, repo3 := seedOwnerShareRepos(ctx, t, admin)
	seedWindowCycleItems(ctx, t, admin, repo2, repo3)

	resp := runWindow(ctx, t, client, "cycle_throughput", "team", "week")
	checkPoints(t, pointsByTeam(t, resp), map[string][2]float64{
		"team-a": {66.0 / 24, 4},
		"team-b": {24.0 / 24, 2},
	})
	for _, point := range resp.Points {
		if len(point.Trajectory) < 2 {
			t.Errorf("%s trajectory = %v, want one entry per weekly bucket", point.EntityID, point.Trajectory)
		}
		if point.Trajectory[len(point.Trajectory)-1].Y == point.Y {
			t.Errorf("%s: the point equals its last weekly bucket (%v): the window was not aggregated", point.EntityID, point.Y)
		}
	}
	a := resp.Points[0]
	if a.EntityID != "team-a" {
		a = resp.Points[1]
	}
	wantTrajectory := []PointTrajectory{
		{X: 1, Y: 1, Window: "2026-08-30"}, {X: 60.0 / 24, Y: 2, Window: "2026-09-06"}, {X: 5, Y: 1, Window: "2026-09-13"},
	}
	if fmt.Sprint(a.Trajectory) != fmt.Sprint(wantTrajectory) {
		t.Errorf("team-a trajectory = %v, want %v", a.Trajectory, wantTrajectory)
	}
	if a.WindowStart != "2026-08-23" || a.WindowEnd != "2026-09-22" {
		t.Errorf("team-a window = %s..%s, want the requested window 2026-08-23..2026-09-22", a.WindowStart, a.WindowEnd)
	}
	// A month bucket over a window of two calendar months reads the same window point.
	month := runWindow(ctx, t, client, "cycle_throughput", "team", "month")
	checkPoints(t, pointsByTeam(t, month), map[string][2]float64{
		"team-a": {66.0 / 24, 4},
		"team-b": {24.0 / 24, 2},
	})
}

// TestTheReviewAndWipTeamPointsAggregateTheWholeWindow covers the owner-team reader (review load x latency) and the
// plain team_id reader (wip x throughput): both read the same window point as the weekly buckets' union.
//
// repo-2 is owned by team-a alone. Reviews (person, day, reviews, first-review hours): p1 d0 10 4h, p1 d1 20 8h,
// p2 d1 6 2h. Window: load (10+20+6) over 2 reviewers = 18, latency (4+8+2)/3. Last bucket: load 26/2, latency 5.
// work_item_metrics_daily of team-a: wip 2, 4, 9 and items 1, 2, 3 on the three days: wip 5, throughput 6.
func TestTheReviewAndWipTeamPointsAggregateTheWholeWindow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	admin, client := crossorg.Start(ctx, t)
	_, repo2, _ := seedOwnerShareRepos(ctx, t, admin)

	for _, row := range []struct {
		person  string
		day     time.Time
		reviews uint32
		latency float64
	}{
		{"p1", windowDays[0], 10, 4}, {"p1", windowDays[1], 20, 8}, {"p2", windowDays[1], 6, 2},
	} {
		crossorg.Exec(ctx, t, admin, `INSERT INTO user_metrics_daily (org_id, repo_id, day, author_email, identity_id, team_id, reviews_given, pr_first_review_p50_hours, computed_at) VALUES (?, ?, ?, ?, ?, 'team-member', ?, ?, now())`,
			ownerShareOrg, repo2, row.day, row.person, row.person, row.reviews, row.latency)
	}
	for i, day := range windowDays {
		crossorg.Exec(ctx, t, admin, `INSERT INTO work_item_metrics_daily (org_id, day, provider, work_scope_id, team_id, items_completed, wip_count_end_of_day, computed_at) VALUES (?, ?, 'github', 's', 'team-a', ?, ?, now())`,
			ownerShareOrg, day, []uint32{1, 2, 3}[i], []float64{2, 4, 9}[i])
	}

	review := runWindow(ctx, t, client, "review_load_latency", "team", "week")
	checkPoints(t, pointsByTeam(t, review), map[string][2]float64{"team-a": {18, 14.0 / 3}})

	wip := runWindow(ctx, t, client, "wip_throughput", "team", "week")
	checkPoints(t, pointsByTeam(t, wip), map[string][2]float64{"team-a": {5, 6}})
}
