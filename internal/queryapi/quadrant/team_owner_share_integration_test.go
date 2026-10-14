//go:build integration

package quadrant

import (
	"context"
	"fmt"
	"math"
	"sort"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/crossorg"
)

const ownerShareOrg = "owner-share-org"

var (
	ownerShareDay    = time.Date(2026, 9, 15, 0, 0, 0, 0, time.UTC)
	ownerShareSynced = time.Date(2026, 9, 16, 0, 0, 0, 0, time.UTC)
	ownerShareEnd    = time.Date(2026, 9, 21, 0, 0, 0, 0, time.UTC)
)

// seedOwnerShareRepos writes three repositories and the teams of the org:
//
//	repo-1  owned by team-a (main, linked share 0.7) and team-b (co-owner, 0.3)
//	repo-2  owned by team-a alone
//	repo-3  owned by team-b alone
//
// team-d owns none of them (a project-only co-owner of an item holds no share of the item's repository).
func seedOwnerShareRepos(ctx context.Context, t *testing.T, admin stdclickhouse.Conn) (repo1, repo2, repo3 string) {
	t.Helper()
	repo1 = "00000000-0000-4000-8000-0000000000d1"
	repo2 = "00000000-0000-4000-8000-0000000000d2"
	repo3 = "00000000-0000-4000-8000-0000000000d3"
	for id, name := range map[string]string{repo1: "acme/one", repo2: "acme/two", repo3: "acme/three"} {
		crossorg.Exec(ctx, t, admin, `INSERT INTO repos (id, repo, provider, org_id, created_at, last_synced) VALUES (?, ?, 'github', ?, ?, ?)`,
			id, name, ownerShareOrg, ownerShareSynced, ownerShareSynced)
	}
	for _, team := range []string{"team-a", "team-b", "team-d"} {
		crossorg.Exec(ctx, t, admin, `INSERT INTO teams (id, team_uuid, name, manual_members, project_keys, repo_patterns, is_active, updated_at, last_synced, org_id, provider) VALUES (?, generateUUIDv4(), ?, [], [], [], 1, ?, ?, ?, 'github')`,
			team, "Name of "+team, ownerShareSynced, ownerShareSynced, ownerShareOrg)
	}
	for _, owner := range []struct {
		repo, name, team string
		primary          int
		share            float64
	}{
		{repo1, "acme/one", "team-a", 1, 0.7}, {repo1, "acme/one", "team-b", 2, 0.3},
		{repo2, "acme/two", "team-a", 1, 1.0}, {repo3, "acme/three", "team-b", 1, 1.0},
	} {
		crossorg.Exec(ctx, t, admin, `INSERT INTO team_repo_ownership (org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, priority, valid_from, valid_to, updated_at, share) VALUES (?, 'github', ?, ?, ?, 'exact', 'inferred', ?, 0, 0, ?, NULL, ?, ?)`,
			ownerShareOrg, owner.team, owner.repo, owner.name, owner.primary, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), ownerShareSynced, owner.share)
	}
	return repo1, repo2, repo3
}

func pointsByTeam(t *testing.T, resp *Response) map[string][2]float64 {
	t.Helper()
	got := map[string][2]float64{}
	for _, point := range resp.Points {
		got[point.EntityID] = [2]float64{point.X, point.Y}
	}
	return got
}

func checkPoints(t *testing.T, got map[string][2]float64, want map[string][2]float64) {
	t.Helper()
	var gotTeams, wantTeams []string
	for team := range got {
		gotTeams = append(gotTeams, team)
	}
	for team := range want {
		wantTeams = append(wantTeams, team)
	}
	sort.Strings(gotTeams)
	sort.Strings(wantTeams)
	if fmt.Sprint(gotTeams) != fmt.Sprint(wantTeams) {
		t.Fatalf("points by team = %v, want teams %v", got, wantTeams)
	}
	for team, w := range want {
		g := got[team]
		if math.Abs(g[0]-w[0]) > 1e-6 || math.Abs(g[1]-w[1]) > 1e-6 {
			t.Errorf("%s point (x, y) = (%.6f, %.6f), want (%.6f, %.6f)", team, g[0], g[1], w[0], w[1])
		}
	}
}

// TestTheCycleThroughputTeamPointsSplitAnItemByOwnerShareAndConserveTheOrgTotal reads cycle time x throughput at
// the team grain (CHAOS-9562, D5983, D5998). Five completed items, the attribution rows as the producer writes them
// (is_primary 1 the primary team, 2 a co-owner):
//
//	i1  repo-1  team-a, team-b   cycle 10 h   weights 0.7 / 0.3 (the repository's owner shares)
//	i2  repo-1  team-a, team-b   cycle 20 h   weights 0.7 / 0.3
//	i3  repo-2  team-a           cycle  6 h   weight 1 (a sole team holds the whole item)
//	i4  repo-1  team-a, team-d   cycle  8 h   team-d owns no part of repo-1: no weight is invented, 0.5 / 0.5
//	i5  repo-1  team-a, team-b   no cycle time, weights 0.7 / 0.3 (counts in throughput, not in the mean)
//
// team-a: throughput 0.7+0.7+1+0.5+0.7 = 3.6, cycle (0.7*10+0.7*20+6+0.5*8)/2.9 h.
// team-b: throughput 0.3+0.3+0.3 = 0.9, cycle (0.3*10+0.3*20)/0.6 = 15 h.
// team-d: throughput 0.5, cycle 8 h.
// The org holds 5 items; the team points hold 3.6 + 0.9 + 0.5 = 5 (a whole point for each owner holds 5 + 3 + 1 = 9).
func TestTheCycleThroughputTeamPointsSplitAnItemByOwnerShareAndConserveTheOrgTotal(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	admin, client := crossorg.Start(ctx, t)
	repo1, repo2, _ := seedOwnerShareRepos(ctx, t, admin)

	type item struct {
		id, repo string
		cycle    any
		teams    []string
	}
	computed := ownerShareDay.Add(3 * time.Hour)
	items := []item{
		{"i1", repo1, 10.0, []string{"team-a", "team-b"}},
		{"i2", repo1, 20.0, []string{"team-a", "team-b"}},
		{"i3", repo2, 6.0, []string{"team-a"}},
		{"i4", repo1, 8.0, []string{"team-a", "team-d"}},
		{"i5", repo1, nil, []string{"team-a", "team-b"}},
	}
	for _, it := range items {
		crossorg.Exec(ctx, t, admin, `
INSERT INTO work_item_cycle_times
    (work_item_id, provider, day, work_scope_id, type, status, created_at, started_at, completed_at, cycle_time_hours, lead_time_hours, computed_at, org_id)
VALUES (?, 'github', ?, 'KEY', 'story', 'done', ?, ?, ?, ?, 6, ?, ?)`,
			it.id, ownerShareDay, ownerShareDay, ownerShareDay, ownerShareDay.Add(time.Hour), it.cycle, computed, ownerShareOrg)
		for i, team := range it.teams {
			primary := 1
			source := "issue_project"
			if i > 0 {
				primary, source = 2, "project_ownership"
			}
			crossorg.Exec(ctx, t, admin, `
INSERT INTO work_item_team_attributions
    (org_id, repo_id, work_item_id, provider, team_id, team_name, source, is_primary, confidence, evidence, computed_at)
VALUES (?, ?, ?, 'github', ?, ?, ?, ?, 'high', 'KEY', ?)`,
				ownerShareOrg, it.repo, it.id, team, "Name of "+team, source, primary, computed)
		}
	}

	run := func(teams []string) map[string][2]float64 {
		t.Helper()
		resp, err := BuildResponse(ctx, client, ownerShareOrg, Params{
			Type: "cycle_throughput", ScopeType: "team", Bucket: "week", RangeDays: 30, EndDate: &ownerShareEnd, TeamIDs: teams,
		})
		if err != nil {
			t.Fatalf("BuildResponse(teams %v): %v", teams, err)
		}
		return pointsByTeam(t, resp)
	}

	all := run(nil)
	checkPoints(t, all, map[string][2]float64{
		"team-a": {(0.7*10 + 0.7*20 + 6 + 0.5*8) / 2.9 / 24, 3.6},
		"team-b": {15.0 / 24, 0.9},
		"team-d": {8.0 / 24, 0.5},
	})

	if sum, ok := conservesTotal(all, 1, float64(len(items))); !ok {
		t.Errorf("sum of throughput over the team points = %v, want the org's %d items", sum, len(items))
	}

	// A team in view keeps its own share of each item: the selection narrows the points, never re-weighs them.
	checkPoints(t, run([]string{"team-b"}), map[string][2]float64{"team-b": {15.0 / 24, 0.9}})
	checkPoints(t, run([]string{"team-d"}), map[string][2]float64{"team-d": {8.0 / 24, 0.5}})
}

// TestTheReviewQuadrantTeamPointsWeighEachRowByOwnerShareAndConserveTheOrgReviews reads review load x review
// latency at the team grain (CHAOS-9562, D5983, D5998). repo-1 is owned by team-a (0.7) and team-b (0.3), repo-2 by
// team-a, repo-3 by team-b. Rows (person, repository, reviews, first-review hours):
//
//	p1 repo-1 10 reviews 4 h     p2 repo-1 20 reviews 8 h
//	p1 repo-2  6 reviews 2 h     p3 repo-3  5 reviews 3 h
//
// team-a: reviews 0.7*10 + 0.7*20 + 6 = 27 over the reviewers' shares (p1 1, p2 0.7) = 1.7; latency
// (0.7*4 + 0.7*8 + 2) / 2.4. team-b: reviews 0.3*10 + 0.3*20 + 5 = 14 over (p1 0.3, p2 0.3, p3 1) = 1.6; latency
// (0.3*4 + 0.3*8 + 3) / 1.6. The org holds 41 reviews, the team points 27 + 14 = 41 (a whole point for each owner holds 36 + 35 = 71).
func TestTheReviewQuadrantTeamPointsWeighEachRowByOwnerShareAndConserveTheOrgReviews(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	admin, client := crossorg.Start(ctx, t)
	repo1, repo2, repo3 := seedOwnerShareRepos(ctx, t, admin)

	rows := []struct {
		repo, person string
		reviews      uint32
		latency      float64
	}{
		{repo1, "p1", 10, 4}, {repo1, "p2", 20, 8}, {repo2, "p1", 6, 2}, {repo3, "p3", 5, 3},
	}
	var orgReviews float64
	for _, row := range rows {
		orgReviews += float64(row.reviews)
		crossorg.Exec(ctx, t, admin, `INSERT INTO user_metrics_daily (org_id, repo_id, day, author_email, identity_id, team_id, reviews_given, pr_first_review_p50_hours, computed_at) VALUES (?, ?, ?, ?, ?, 'team-member', ?, ?, now())`,
			ownerShareOrg, row.repo, ownerShareDay, row.person, row.person, row.reviews, row.latency)
	}

	run := func(teams []string) map[string][2]float64 {
		t.Helper()
		resp, err := BuildResponse(ctx, client, ownerShareOrg, Params{
			Type: "review_load_latency", ScopeType: "team", Bucket: "week", RangeDays: 30, EndDate: &ownerShareEnd, TeamIDs: teams,
		})
		if err != nil {
			t.Fatalf("BuildResponse(teams %v): %v", teams, err)
		}
		return pointsByTeam(t, resp)
	}

	all := run(nil)
	checkPoints(t, all, map[string][2]float64{
		"team-a": {27.0 / 1.7, (0.7*4 + 0.7*8 + 2) / 2.4},
		"team-b": {14.0 / 1.6, (0.3*4 + 0.3*8 + 3) / 1.6},
	})

	// The numerator of the load (reviews x share) of the team points is the org's reviews: load x reviewer weight.
	teamReviews := all["team-a"][0]*1.7 + all["team-b"][0]*1.6
	if math.Abs(teamReviews-orgReviews) > 1e-6 {
		t.Errorf("sum of reviews over the team points = %v, want the org's %v", teamReviews, orgReviews)
	}

	checkPoints(t, run([]string{"team-b"}), map[string][2]float64{"team-b": {14.0 / 1.6, (0.3*4 + 0.3*8 + 3) / 1.6}})
}
