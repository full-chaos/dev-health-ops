//go:build integration

package daily

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/teamkeytables"
)

// cyclePoint is one live point of ic_landscape_rolling_30d of the last day.
type cyclePoint struct {
	XNorm, Churn, Delivery, WIP float64
	Cycle                       *float64
}

type cycleWorld struct {
	provider, org, alpha, beta string
	day                        time.Time
	// points by "identity in team on map".
	points map[string]cyclePoint
	// the newest row of carol's earlier cycle point of the last day: live or not.
	carolOldCycleLive bool
	// cycle_p50_hours of the newest user_metrics_daily rows, by "identity day".
	storedCycle map[string]float64
}

const (
	cycleAlice = "alice@cycle.example"
	cycleBob   = "bob@cycle.example"
	cycleCarol = "carol@cycle.example"
	cycleDave  = "dave@cycle.example"
	cycleErin  = "erin@cycle.example"
	cycleFrank = "frank@cycle.example"
)

// TestTheLandscapeCycleReadsOnlyTheDaysWithAMergedPullRequest holds the 30-day
// cycle of a landscape point to the days of the window that hold a cycle: a day
// with a merged pull request. A day with no merged pull request has no cycle, it
// is not a cycle of 0 hours. A person with no such day has no 30-day cycle, no
// point on the cycle map, and is no sample of a team's mean.
//
// The world of each provider (day D, ic_finalize runs for D-2, D-1 and D; team
// alpha owns the git repository, team beta takes dave's work item):
//
//   - alice: D-2 one merged pull request of 10 h; D-1 and D commits only;
//   - bob: D-1 one merged pull request of 0 h; D commits only;
//   - carol: D-2, D-1 and D commits only; an earlier compute stored her a point
//     of D on the cycle map;
//   - erin: D-2 two merged pull requests, median 4 h; D-1 one, 8 h;
//   - frank: a row of D-5 that ic_finalize never rewrote (one merged pull request
//     of 5 h as repouser wrote it; cycle_p50_hours and delivery_units hold their
//     column defaults);
//   - dave: one work item of the provider done on D, no git row.
func TestTheLandscapeCycleReadsOnlyTheDaysWithAMergedPullRequest(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	conn := workItemAttributionLinkedIssueMigratedClickHouse(t, ctx)
	day := time.Date(2026, 9, 21, 0, 0, 0, 0, time.UTC)
	for index, provider := range []string{"jira", "gitlab", "github", "linear"} {
		world := buildCycleWorld(ctx, t, conn, provider, index, day)
		t.Run(provider, func(t *testing.T) {
			checkCycleWorld(t, world)
		})
	}
}

func checkCycleWorld(t *testing.T, world cycleWorld) {
	point := func(identity, team, mapName string) (cyclePoint, bool) {
		value, ok := world.points[identity+" in "+team+" on "+mapName]
		return value, ok
	}
	cycleOf := func(t *testing.T, identity, team string) *float64 {
		t.Helper()
		value, ok := point(identity, team, "churn_throughput")
		if !ok {
			t.Fatalf("%s has no churn point in %s: %v", identity, team, world.points)
		}
		return value.Cycle
	}
	show := func(value *float64) string {
		if value == nil {
			return "NULL"
		}
		return fmt.Sprint(*value)
	}
	wantCycle := func(t *testing.T, identity, team string, want float64) {
		t.Helper()
		if got := cycleOf(t, identity, team); got == nil || *got != want {
			t.Errorf("%s in %s: 30-day cycle %s, want %v", identity, team, show(got), want)
		}
	}
	wantNoCycle := func(t *testing.T, identity, team string) {
		t.Helper()
		for _, mapName := range []string{"churn_throughput", "wip_throughput"} {
			value, ok := point(identity, team, mapName)
			if !ok {
				t.Errorf("%s in %s has no %s point", identity, team, mapName)
				continue
			}
			if value.Cycle != nil {
				t.Errorf("%s in %s on %s: 30-day cycle %v, want NULL (no day with a cycle)", identity, team, mapName, *value.Cycle)
			}
		}
	}
	cycleMapMembers := func(team string) []string {
		var members []string
		for key := range world.points {
			if strings.HasSuffix(key, " in "+team+" on cycle_throughput") {
				members = append(members, strings.TrimSuffix(key, " in "+team+" on cycle_throughput"))
			}
		}
		sort.Strings(members)
		return members
	}
	t.Logf("points: %v", world.points)

	t.Run("K1 a day with no merged pull request takes no part in the 30-day cycle", func(t *testing.T) {
		wantCycle(t, cycleAlice, world.alpha, 10)
	})
	t.Run("K2 a merged pull request of 0 hours is a cycle of 0", func(t *testing.T) {
		wantCycle(t, cycleBob, world.alpha, 0)
		if _, ok := point(cycleBob, world.alpha, "cycle_throughput"); !ok {
			t.Errorf("bob has a cycle of 0 hours and no point on the cycle map")
		}
	})
	t.Run("K3 a person with no merged pull request in the window has no 30-day cycle", func(t *testing.T) {
		wantNoCycle(t, cycleCarol, world.alpha)
	})
	t.Run("K4 a person with work items only has no 30-day cycle", func(t *testing.T) {
		wantNoCycle(t, cycleDave, world.beta)
	})
	t.Run("K5 the cycle map holds the people with a cycle only", func(t *testing.T) {
		if got := cycleMapMembers(world.alpha); strings.Join(got, ",") != strings.Join(sortedStrings(cycleAlice, cycleBob, cycleErin), ",") {
			t.Errorf("cycle map of alpha = %v, want alice, bob and erin", got)
		}
		if got := cycleMapMembers(world.beta); len(got) != 0 {
			t.Errorf("cycle map of beta = %v, want no point (dave has no cycle)", got)
		}
		// The ranks of the cycle map are over its three people: bob (0 h) < erin
		// (6 h) < alice (10 h).
		for identity, want := range map[string]float64{cycleBob: 0.5 / 3, cycleErin: 1.5 / 3, cycleAlice: 2.5 / 3} {
			if value, ok := point(identity, world.alpha, "cycle_throughput"); !ok || math.Abs(value.XNorm-want) > 1e-9 {
				t.Errorf("%s on the cycle map: %+v (present %v), want x_norm %v", identity, value, ok, want)
			}
		}
		// The churn map still ranks every person of the team.
		var churnMembers int
		for key := range world.points {
			if strings.HasSuffix(key, " in "+world.alpha+" on churn_throughput") {
				churnMembers++
			}
		}
		if churnMembers != 5 {
			t.Errorf("churn map of alpha holds %d people, want 5 (alice, bob, carol, erin, frank)", churnMembers)
		}
	})
	t.Run("K6 the team mean of the cycle leaves out a person with no cycle", func(t *testing.T) {
		var total float64
		var samples int
		for key, value := range world.points {
			if strings.HasSuffix(key, " in "+world.alpha+" on churn_throughput") && value.Cycle != nil {
				total += *value.Cycle
				samples++
			}
		}
		if want := (10.0 + 0 + 6) / 3; samples != 3 || math.Abs(total/float64(samples)-want) > 1e-9 {
			t.Errorf("alpha's mean cycle = %v over %d people, want %v over 3 (alice, bob, erin)", total/float64(samples), samples, want)
		}
	})
	t.Run("K7 churn, delivery and wip do not change", func(t *testing.T) {
		for identity, want := range map[string][3]float64{
			cycleAlice: {60, 1, 0}, cycleBob: {10, 1, 0}, cycleCarol: {21, 0, 0}, cycleErin: {10, 3, 0}, cycleFrank: {0, 0, 0},
		} {
			value, ok := point(identity, world.alpha, "churn_throughput")
			if !ok || value.Churn != want[0] || value.Delivery != want[1] || value.WIP != want[2] {
				t.Errorf("%s in alpha = %+v (present %v), want churn %v, delivery %v, wip %v", identity, value, ok, want[0], want[1], want[2])
			}
		}
		if value, ok := point(cycleDave, world.beta, "churn_throughput"); !ok || value.Delivery != 1 {
			t.Errorf("dave in beta = %+v (present %v), want delivery 1", value, ok)
		}
	})
	t.Run("K8 the stored user rows do not change", func(t *testing.T) {
		for key, want := range map[string]float64{
			cycleAlice + " -2": 10, cycleAlice + " -1": 0, cycleCarol + " 0": 0, cycleBob + " -1": 0,
		} {
			if got, ok := world.storedCycle[key]; !ok || got != want {
				t.Errorf("user_metrics_daily %s: cycle_p50_hours %v (present %v), want %v", key, got, ok, want)
			}
		}
	})
	t.Run("K9 a recompute retracts a stored cycle point of a person with no cycle", func(t *testing.T) {
		if world.carolOldCycleLive {
			t.Errorf("carol's cycle point of D is still live after the recompute, want a row of zeros")
		}
	})
	t.Run("K10 a window with no day of zero keeps its median", func(t *testing.T) {
		wantCycle(t, cycleErin, world.alpha, 6)
	})
	t.Run("K11 a row ic_finalize has not rewritten gives no cycle", func(t *testing.T) {
		wantNoCycle(t, cycleFrank, world.alpha)
	})
}

func buildCycleWorld(
	ctx context.Context, t *testing.T, conn driver.Conn, provider string, index int, day time.Time,
) cycleWorld {
	t.Helper()
	world := cycleWorld{
		provider: provider, day: day,
		org:   fmt.Sprintf("00000000-0000-4000-8000-0000125620%02d", index),
		alpha: provider + ":alpha", beta: provider + ":beta",
	}
	org := world.org
	exec := func(what, query string, args ...any) {
		t.Helper()
		if err := conn.Exec(ctx, query, args...); err != nil {
			t.Fatalf("%s %s: %v", provider, what, err)
		}
	}
	longBefore := day.AddDate(0, 0, -60)
	gitRepo := uuid.NewSHA1(uuid.NameSpaceURL, []byte(org+"/git"))
	itemRepo := uuid.NewSHA1(uuid.NameSpaceURL, []byte(org+"/items"))
	at := func(days int, hour int) time.Time {
		return day.AddDate(0, 0, days).Add(time.Duration(hour) * time.Hour)
	}

	for _, team := range []string{world.alpha, world.beta} {
		exec("insert team "+team, `INSERT INTO teams
    (id, team_uuid, name, repo_patterns, updated_at, last_synced, org_id, provider, is_active)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1)`,
			team, uuid.NewSHA1(uuid.NameSpaceURL, []byte(org+"/team/"+team)), "Team "+team, []string{}, longBefore, longBefore, org, provider)
	}
	exec("insert the git repository", `INSERT INTO repos (id, repo, org_id, provider, last_synced) VALUES (?, ?, ?, 'github', ?)`,
		gitRepo, "acme/"+provider+"-cycle", org, longBefore)
	exec("insert the ownership of the git repository", `INSERT INTO team_repo_ownership
    (org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, valid_from, valid_to, updated_at)
    VALUES (?, 'github', ?, ?, ?, 'exact', 'native', 1, 10, ?, NULL, ?)`,
		org, world.alpha, gitRepo, "acme/"+provider+"-cycle", longBefore, longBefore)

	// dave's work item, done on D, primary beta.
	workItemID := provider + ":S1-cycle"
	exec("insert dave's item", `INSERT INTO work_items (
    repo_id, work_item_id, provider, status, project_key, project_id, native_team_key, project_name,
    type, assignees, created_at, started_at, completed_at, closed_at, story_points, org_id, last_synced)
    VALUES (?, ?, ?, 'done', 'S1', 'S1', '', '', 'task', ?, ?, ?, ?, NULL, NULL, ?, ?)`,
		itemRepo, workItemID, provider, []string{cycleDave}, day.AddDate(0, 0, -10), at(0, 8), at(0, 12), org, day.AddDate(0, 0, 2))
	exec("insert the attribution of dave's item", `INSERT INTO work_item_team_attributions
    (org_id, repo_id, work_item_id, provider, team_id, team_name, source, is_primary, confidence, evidence, computed_at)
    VALUES (?, ?, ?, ?, ?, ?, 'issue_project', 1, 'high', '{}', ?)`,
		org, itemRepo, workItemID, provider, world.beta, world.beta, day.AddDate(0, 0, -20))

	// What the repository/user family writes: lines, merged pull requests and
	// their median cycle. ic_finalize rewrites the rows of D-2, D-1 and D.
	gitRow := func(identity string, offset int, loc uint32, merged uint32, median float64) {
		t.Helper()
		exec("insert the git row of "+identity, `INSERT INTO user_metrics_daily
    (repo_id, day, author_email, identity_id, team_id, team_name, commits_count, loc_added, loc_deleted,
     prs_authored, prs_merged, median_pr_cycle_hours, pr_cycle_p90_hours, computed_at, org_id)
    VALUES (?, ?, ?, ?, 'unassigned', 'Unassigned', 1, ?, 0, ?, ?, ?, ?, ?, ?)`,
			gitRepo, day.AddDate(0, 0, offset), identity, identity, loc, merged, merged, median, median, at(offset, 20), org)
	}
	gitRow(cycleAlice, -2, 10, 1, 10)
	gitRow(cycleAlice, -1, 20, 0, 0)
	gitRow(cycleAlice, 0, 30, 0, 0)
	gitRow(cycleBob, -1, 5, 1, 0)
	gitRow(cycleBob, 0, 5, 0, 0)
	gitRow(cycleCarol, -2, 7, 0, 0)
	gitRow(cycleCarol, -1, 7, 0, 0)
	gitRow(cycleCarol, 0, 7, 0, 0)
	gitRow(cycleErin, -2, 8, 2, 4)
	gitRow(cycleErin, -1, 2, 1, 8)
	gitRow(cycleFrank, -5, 3, 1, 5)

	// An earlier compute of D stored carol a point on the cycle map.
	exec("insert carol's earlier cycle point", `INSERT INTO ic_landscape_rolling_30d
    (repo_id, as_of_day, identity_id, team_id, map_name, x_raw, y_raw, x_norm, y_norm,
     churn_loc_30d, delivery_units_30d, cycle_p50_30d_hours, wip_max_30d, computed_at, org_id)
    VALUES (?, ?, ?, ?, 'cycle_throughput', 0, 0, 0.5, 0.25, 21, 0, 0, 0, ?, ?)`,
		uuid.UUID{}, day, cycleCarol, world.alpha, at(0, 21), org)

	workItems, err := NewWorkItemExecutor(conn)
	if err != nil {
		t.Fatal(err)
	}
	finalize := NewICFinalizeExecutor(conn)
	partition := Partition{ID: "partition-cycle-" + provider, RepoIDs: []RepositoryID{RepositoryID(itemRepo.String())}}
	for offset := -2; offset <= 0; offset++ {
		target := day.AddDate(0, 0, offset)
		run := Run{ID: uuid.NewString(), OrganizationID: org, TargetDay: target}
		if _, err := workItems.ComputeFamily(ctx, run, partition); err != nil {
			t.Fatalf("%s work_item of %s: %v", provider, target.Format("2006-01-02"), err)
		}
		if _, err := finalize.ComputeFinalizeFamily(ctx, run); err != nil {
			t.Fatalf("%s ic_finalize of %s: %v", provider, target.Format("2006-01-02"), err)
		}
	}

	live := teamkeytables.ICLandscapeRolling30d.LiveRow("")
	world.points = map[string]cyclePoint{}
	rows, err := conn.Query(ctx, `SELECT identity_id, toString(team_id), toString(map_name), x_norm,
    toFloat64(churn_loc_30d), toFloat64(delivery_units_30d), toFloat64(wip_max_30d), cycle_p50_30d_hours
FROM ic_landscape_rolling_30d FINAL
WHERE org_id = ? AND as_of_day = ? AND `+live, org, day)
	if err != nil {
		t.Fatalf("%s read the points: %v", provider, err)
	}
	defer rows.Close()
	for rows.Next() {
		var identity, teamID, mapName string
		var value cyclePoint
		if err := rows.Scan(&identity, &teamID, &mapName, &value.XNorm, &value.Churn, &value.Delivery, &value.WIP, &value.Cycle); err != nil {
			t.Fatal(err)
		}
		world.points[identity+" in "+teamID+" on "+mapName] = value
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}

	var carolLive uint8
	if err := conn.QueryRow(ctx, `SELECT toUInt8(`+live+`)
FROM ic_landscape_rolling_30d FINAL
WHERE org_id = ? AND as_of_day = ? AND identity_id = ? AND team_id = ? AND map_name = 'cycle_throughput'`,
		org, day, cycleCarol, world.alpha).Scan(&carolLive); err != nil {
		t.Fatalf("%s read carol's cycle point: %v", provider, err)
	}
	world.carolOldCycleLive = carolLive == 1

	world.storedCycle = map[string]float64{}
	stored, err := conn.Query(ctx, `SELECT author_email, toInt32(dateDiff('day', ?, day)), cycle_p50_hours FROM (
    SELECT * FROM user_metrics_daily ORDER BY computed_at DESC LIMIT 1 BY org_id, repo_id, author_email, day
) WHERE org_id = ? AND day >= ? AND day <= ?`, day, org, day.AddDate(0, 0, -2), day)
	if err != nil {
		t.Fatalf("%s read the stored user rows: %v", provider, err)
	}
	defer stored.Close()
	for stored.Next() {
		var identity string
		var offset int32
		var cycle float64
		if err := stored.Scan(&identity, &offset, &cycle); err != nil {
			t.Fatal(err)
		}
		world.storedCycle[fmt.Sprintf("%s %d", identity, offset)] = cycle
	}
	if err := stored.Err(); err != nil {
		t.Fatal(err)
	}
	return world
}
