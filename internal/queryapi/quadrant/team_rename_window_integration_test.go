//go:build integration

package quadrant

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/crossorg"
)

// TestATeamRenamedInTheWindowIsOnePoint reads wip x throughput at the team grain
// while team-a carries two names in work_item_metrics_daily (renamed between the
// two weeks). The daily rows of one team are one point whatever name they carry:
// wip is the mean over both days, throughput the sum.
func TestATeamRenamedInTheWindowIsOnePoint(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	admin, client := crossorg.Start(ctx, t)
	seedOwnerShareRepos(ctx, t, admin)

	for _, row := range []struct {
		day  time.Time
		name string
		wip  float64
	}{{windowDays[0], "Old name", 10}, {windowDays[1], "New name", 20}} {
		crossorg.Exec(ctx, t, admin, `INSERT INTO work_item_metrics_daily (org_id, day, provider, work_scope_id, team_id, team_name, items_completed, wip_count_end_of_day, computed_at) VALUES (?, ?, 'github', 's', 'team-a', ?, 2, ?, now())`,
			ownerShareOrg, row.day, row.name, row.wip)
	}

	for _, bucket := range []string{"week", "month"} {
		resp := runWindow(ctx, t, client, "wip_throughput", "team", bucket)
		checkPoints(t, pointsByTeam(t, resp), map[string][2]float64{"team-a": {15, 4}})
	}
}

// TestTheRepositoryPointAggregatesTheWholeWindow reads churn x throughput at the
// repository grain (repo_metrics_daily, summed): three weeks of 10, 20 and 30
// loc and 1, 2 and 3 merged pull requests are a point of 60 loc and 6 merges.
func TestTheRepositoryPointAggregatesTheWholeWindow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	admin, client := crossorg.Start(ctx, t)
	repo1, _, _ := seedOwnerShareRepos(ctx, t, admin)

	for i, day := range windowDays {
		crossorg.Exec(ctx, t, admin, `INSERT INTO repo_metrics_daily (repo_id, day, total_loc_touched, prs_merged, computed_at, org_id) VALUES (?, ?, ?, ?, now(), ?)`,
			repo1, day, uint32(10*(i+1)), uint32(i+1), ownerShareOrg)
	}
	resp := runWindow(ctx, t, client, "churn_throughput", "repo", "week")
	if len(resp.Points) != 1 || resp.Points[0].EntityID != "acme/one" || resp.Points[0].X != 60 || resp.Points[0].Y != 6 {
		t.Fatalf("points = %+v, want acme/one at (60, 6)", resp.Points)
	}
	if got := len(resp.Points[0].Trajectory); got != 3 {
		t.Errorf("trajectory has %d entries, want one per week", got)
	}
}
