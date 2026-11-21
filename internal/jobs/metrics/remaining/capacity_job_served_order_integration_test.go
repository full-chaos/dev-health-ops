//go:build integration

package remaining

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/rowview"
)

// The capacity job stores the throughput history and the backlog it reads from
// work_item_metrics_daily. On a computed_at tie it must read the version the
// served pages read (rowview.WorkItemMetricsNewestOrder), in both insert orders,
// for the org form and the one-team form (CHAOS-14413). The cells own their
// ClickHouse with merges stopped (startTieEnv), so both tied versions stay.

const capacityServedOrderTeam = "team-14413"

type capacityVersion struct {
	completed, wip int
	at             time.Time
}

func capacityVersionInsert(c tieClock, v capacityVersion) string {
	return fmt.Sprintf(`INSERT INTO work_item_metrics_daily
		(day, provider, work_scope_id, team_id, team_name, items_completed, wip_count_end_of_day,
		 computed_at, org_id, row_view)
		VALUES ('%s', 'jira', 'S1', '%s', '%s', %d, %d, '%s', '%s', '')`,
		tieDay(c.day1), capacityServedOrderTeam, capacityServedOrderTeam, v.completed, v.wip, tieStamp(v.at), tieOrg)
}

// capacityJobReads is what the job reads for the target: the throughput days and the backlog.
func capacityJobReads(t *testing.T, env *tieEnv, oneTeam bool) ([]int, int) {
	t.Helper()
	executor := &CapacityExecutor{conn: env.conn}
	target := capacityTarget{}
	if oneTeam {
		team := capacityServedOrderTeam
		target.TeamID = &team
	}
	history, err := executor.loadThroughput(env.ctx, tieOrg, target, 90, env.clock.end)
	if err != nil {
		t.Fatalf("job throughput: %v", err)
	}
	backlog, err := executor.loadBacklog(env.ctx, tieOrg, target)
	if err != nil {
		t.Fatalf("job backlog: %v", err)
	}
	return history.DailyThroughputs, backlog
}

// capacityPageReads is what the served pages read: the same two columns of the page's own pick.
func capacityPageReads(t *testing.T, env *tieEnv, oneTeam bool) ([]int, int) {
	t.Helper()
	source := fmt.Sprintf(`(SELECT * FROM work_item_metrics_daily WHERE work_item_metrics_daily.org_id = {org_id:String}
		ORDER BY %s DESC LIMIT 1 BY work_item_metrics_daily.org_id, work_item_metrics_daily.day, work_item_metrics_daily.provider,
		work_item_metrics_daily.work_scope_id, work_item_metrics_daily.team_id, work_item_metrics_daily.row_view) AS work_item_metrics_daily`,
		rowview.WorkItemMetricsNewestOrder)
	if oneTeam {
		source = rowview.ServedTeamSource("work_item_metrics_daily", "team_id = {team_id:String}", rowview.WorkItemMetricsNewestOrder) +
			" AS work_item_metrics_daily"
	}
	rows, err := env.conn.Query(env.ctx, "SELECT items_completed, wip_count_end_of_day FROM "+source,
		clickhouse.Named("org_id", tieOrg), clickhouse.Named("team_id", capacityServedOrderTeam))
	if err != nil {
		t.Fatalf("page read: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var throughput []int
	backlog := 0
	for rows.Next() {
		var completed, wip uint32
		if err := rows.Scan(&completed, &wip); err != nil {
			t.Fatal(err)
		}
		throughput = append(throughput, int(completed))
		backlog = int(wip)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return throughput, backlog
}

// capacityDisagreeingPair finds two measure sets whose order under cityHash64(tuple(*)) is the
// opposite of their order under the 19-measure hash, so the old and the served pick differ.
func capacityDisagreeingPair(t *testing.T, env *tieEnv) (low, high capacityVersion) {
	t.Helper()
	for k := 1; k <= 200; k++ {
		env.reset(t)
		a := capacityVersion{completed: k, wip: k + 1, at: env.clock.at}
		b := capacityVersion{completed: k + 100, wip: k + 101, at: env.clock.at}
		env.exec(t, capacityVersionInsert(env.clock, a))
		env.exec(t, capacityVersionInsert(env.clock, b))
		rows, err := env.conn.Query(env.ctx, fmt.Sprintf(`SELECT items_completed,
			cityHash64(tuple(*)), cityHash64(tuple(%s)) FROM work_item_metrics_daily ORDER BY items_completed`, capacityMeasureList))
		if err != nil {
			t.Fatalf("hash read: %v", err)
		}
		var whole, measures [2]uint64
		n := 0
		for rows.Next() {
			var completed uint32
			if err := rows.Scan(&completed, &whole[n], &measures[n]); err != nil {
				t.Fatal(err)
			}
			n++
		}
		_ = rows.Close()
		if n == 2 && (whole[0] > whole[1]) != (measures[0] > measures[1]) {
			if measures[0] > measures[1] {
				return b, a
			}
			return a, b
		}
	}
	t.Fatal("no pair of versions whose whole-row order differs from the 19-measure order")
	return
}

const capacityMeasureList = `items_started, items_completed, items_started_unassigned, items_completed_unassigned,
	wip_count_end_of_day, wip_unassigned_end_of_day, bug_completed_ratio, story_points_completed,
	new_bugs_count, new_items_count, defect_intro_rate, wip_congestion_ratio, predictability_score,
	cycle_time_p50_hours, cycle_time_p90_hours, lead_time_p50_hours, lead_time_p90_hours,
	wip_age_p50_hours, wip_age_p90_hours`

// low sorts below high by the served order: the page serves high on a tie. The old pick
// (whole-row hash) serves low.
func TestCapacityJobPicksTheServedVersionOnATie(t *testing.T) {
	env := startTieEnv(t)
	low, high := capacityDisagreeingPair(t, env)
	for _, oneTeam := range []bool{false, true} {
		for _, order := range []string{"low then high", "high then low"} {
			t.Run(fmt.Sprintf("oneTeam=%v/%s", oneTeam, order), func(t *testing.T) {
				env.reset(t)
				first, second := low, high
				if order == "high then low" {
					first, second = high, low
				}
				env.exec(t, capacityVersionInsert(env.clock, first))
				env.exec(t, capacityVersionInsert(env.clock, second))
				throughput, backlog := capacityJobReads(t, env, oneTeam)
				if !reflect.DeepEqual(throughput, []int{high.completed}) || backlog != high.wip {
					t.Errorf("job stored throughput=%v backlog=%d, want the served version [%d] %d",
						throughput, backlog, high.completed, high.wip)
				}
			})
		}
	}
}

// A version stamped one second later wins over any hash, in both insert orders.
func TestCapacityJobPicksTheNewerVersionOverTheServedHash(t *testing.T) {
	env := startTieEnv(t)
	low, high := capacityDisagreeingPair(t, env)
	newer := low
	newer.at = env.clock.at.Add(time.Second)
	for _, oneTeam := range []bool{false, true} {
		for _, order := range []string{"newer first", "newer last"} {
			t.Run(fmt.Sprintf("oneTeam=%v/%s", oneTeam, order), func(t *testing.T) {
				env.reset(t)
				first, second := high, newer
				if order == "newer first" {
					first, second = newer, high
				}
				env.exec(t, capacityVersionInsert(env.clock, first))
				env.exec(t, capacityVersionInsert(env.clock, second))
				throughput, backlog := capacityJobReads(t, env, oneTeam)
				if !reflect.DeepEqual(throughput, []int{newer.completed}) || backlog != newer.wip {
					t.Errorf("job stored throughput=%v backlog=%d, want the newer version [%d] %d",
						throughput, backlog, newer.completed, newer.wip)
				}
			})
		}
	}
}

// One data set, both pickers: the job and the page read one version, for the org form and one team.
func TestCapacityJobAndPageReadOneVersionOfOneDataSet(t *testing.T) {
	env := startTieEnv(t)
	low, high := capacityDisagreeingPair(t, env)
	env.reset(t)
	env.exec(t, capacityVersionInsert(env.clock, low))
	env.exec(t, capacityVersionInsert(env.clock, high))
	for _, oneTeam := range []bool{false, true} {
		jobThroughput, jobBacklog := capacityJobReads(t, env, oneTeam)
		pageThroughput, pageBacklog := capacityPageReads(t, env, oneTeam)
		if !reflect.DeepEqual(jobThroughput, pageThroughput) || jobBacklog != pageBacklog {
			t.Errorf("oneTeam=%v: job reads throughput=%v backlog=%d, page reads throughput=%v backlog=%d",
				oneTeam, jobThroughput, jobBacklog, pageThroughput, pageBacklog)
		}
	}
}
