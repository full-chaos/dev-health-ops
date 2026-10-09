//go:build integration

package daily

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
)

// The acceptance case of the stale-key rule for the work-item families, through
// the real executors on the schema of the migration chain.
//
// A stored day is computed, then the teams change, then the same day is
// computed again:
//
//   - team ENG is carried to the keyed id linear:ENG (the bare id goes
//     inactive, the keyed id takes its native key);
//   - team OPS goes inactive with no successor (a retired team, no carry).
//
// After the second compute a read with no team filter must count the day once,
// a read of the keyed team must hold what the bare team held, and no measure
// may stay under an inactive team id. A third compute must change nothing a
// reader sees.
//
// The reads are the statements of the query-api readers that take no team
// filter: sankey fetchExpenseCounts and fetchStateStatusCounts
// (internal/queryapi/sankey/queries.go), Home fetchBlockedHours
// (internal/queryapi/home/queries_metrics.go) and the capacity throughput read
// (internal/queryapi/capacityforecast/clickhouse.go). The file uses no symbol
// of the rule, so the same file runs on a tree without the rule.

const staleKeyAcceptanceOrg = "00000000-0000-4000-8000-00000000e0a0"

var staleKeyAcceptanceDay = time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC)

type staleKeyAcceptanceTeam struct {
	id, nativeKey string
	active        bool
}

func seedStaleKeyAcceptanceTeams(
	t *testing.T, ctx context.Context, conn driver.Conn, at time.Time, teams ...staleKeyAcceptanceTeam,
) {
	t.Helper()
	batch, err := conn.PrepareBatch(ctx, `INSERT INTO teams
    (id, team_uuid, name, members, updated_at, last_synced, org_id, provider, native_team_key, is_active)`)
	if err != nil {
		t.Fatalf("prepare teams: %v", err)
	}
	for _, team := range teams {
		active := uint8(0)
		if team.active {
			active = 1
		}
		nativeKey := team.nativeKey
		if err := batch.Append(
			team.id, uuid.NewSHA1(uuid.NameSpaceURL, []byte("team:"+team.id)), "Team "+team.id, []string{},
			at, at, staleKeyAcceptanceOrg, "linear", &nativeKey, active,
		); err != nil {
			t.Fatalf("append team %s: %v", team.id, err)
		}
	}
	if err := batch.Send(); err != nil {
		t.Fatalf("send teams: %v", err)
	}
}

// seedStaleKeyAcceptanceItems stores three linear work items with no
// repository: two of team key ENG in work scope proj-eng (one completed on the
// day, one blocked for the whole day) and one of team key OPS in work scope
// proj-ops (completed on the day).
func seedStaleKeyAcceptanceItems(t *testing.T, ctx context.Context, conn driver.Conn) {
	t.Helper()
	day := staleKeyAcceptanceDay
	items, err := conn.PrepareBatch(ctx, `INSERT INTO work_items (
    repo_id, work_item_id, provider, type, status, project_id, native_team_key,
    created_at, completed_at, story_points, org_id, last_synced)`)
	if err != nil {
		t.Fatalf("prepare work_items: %v", err)
	}
	done, points := day.Add(10*time.Hour), 3.0
	for _, item := range []struct {
		id, status, project, teamKey string
		completedAt                  *time.Time
	}{
		{"ENG-1", "done", "proj-eng", "ENG", &done},
		{"ENG-2", "blocked", "proj-eng", "ENG", nil},
		{"OPS-1", "done", "proj-ops", "OPS", &done},
	} {
		if err := items.Append(
			uuid.Nil, item.id, "linear", "story", item.status, item.project, item.teamKey,
			day.Add(-48*time.Hour), item.completedAt, &points, staleKeyAcceptanceOrg, day.Add(12*time.Hour),
		); err != nil {
			t.Fatalf("append work item %s: %v", item.id, err)
		}
	}
	if err := items.Send(); err != nil {
		t.Fatalf("send work_items: %v", err)
	}
	transitions, err := conn.PrepareBatch(ctx, `INSERT INTO work_item_transitions
    (repo_id, work_item_id, occurred_at, provider, from_status, to_status, from_status_raw, to_status_raw, actor, org_id, last_synced)`)
	if err != nil {
		t.Fatalf("prepare work_item_transitions: %v", err)
	}
	for _, transition := range []struct {
		id, from, to string
		at           time.Time
	}{
		{"ENG-1", "todo", "in_progress", day.Add(-24 * time.Hour)},
		{"ENG-1", "in_progress", "done", done},
		{"ENG-2", "todo", "blocked", day.Add(-24 * time.Hour)},
		{"OPS-1", "todo", "in_progress", day.Add(-24 * time.Hour)},
		{"OPS-1", "in_progress", "done", done},
	} {
		if err := transitions.Append(
			uuid.Nil, transition.id, transition.at, "linear", transition.from, transition.to,
			transition.from, transition.to, "", staleKeyAcceptanceOrg, day.Add(12*time.Hour),
		); err != nil {
			t.Fatalf("append transition of %s: %v", transition.id, err)
		}
	}
	if err := transitions.Send(); err != nil {
		t.Fatalf("send work_item_transitions: %v", err)
	}
}

// runStaleKeyAcceptanceFamilies computes the day once with the four work-item
// families in their run order, at one clock.
func runStaleKeyAcceptanceFamilies(t *testing.T, ctx context.Context, conn driver.Conn, clock time.Time) {
	t.Helper()
	run := Run{ID: uuid.NewString(), OrganizationID: staleKeyAcceptanceOrg, TargetDay: staleKeyAcceptanceDay}
	partition := Partition{ID: uuid.NewString(), RunID: run.ID, RepoIDs: []RepositoryID{RepositoryID(uuid.Nil.String())}}
	now := func() time.Time { return clock }

	attribution, err := NewWorkItemAttributionExecutor(conn)
	if err != nil {
		t.Fatal(err)
	}
	attribution.nowUTC = now
	metrics, err := NewWorkItemExecutor(conn)
	if err != nil {
		t.Fatal(err)
	}
	metrics.nowUTC = now
	estimate, err := NewWorkItemEstimateExecutor(conn)
	if err != nil {
		t.Fatal(err)
	}
	estimate.nowUTC = now
	state, err := NewWorkItemStateExecutor(conn)
	if err != nil {
		t.Fatal(err)
	}
	state.nowUTC = now
	for _, family := range []struct {
		name     string
		executor NativeFamilyExecutor
	}{
		{"work_item_attribution", attribution}, {"work_item", metrics},
		{"work_item_estimate", estimate}, {"work_item_state", state},
	} {
		if _, err := family.executor.ComputeFamily(ctx, run, partition); err != nil {
			t.Fatalf("%s at %s: %v", family.name, clock.Format(time.RFC3339), err)
		}
	}
}

// staleKeyAcceptanceReads is what the readers return for the day.
type staleKeyAcceptanceReads struct {
	// The reads with no team filter.
	expenseNewItems, expenseCompleted float64 // sankey fetchExpenseCounts
	stateItemsTouched                 float64 // sankey fetchStateStatusCounts, all statuses
	blockedHours                      float64 // Home fetchBlockedHours
	throughputCompleted               uint64  // capacity loadThroughput
	estimateBacklog                   uint64  // estimate coverage, FINAL
	// The reads of one team.
	teamCompleted map[string]uint64
	teamBlocked   map[string]float64
}

func (reads staleKeyAcceptanceReads) orgScope() string {
	return fmt.Sprintf("new_items=%v completed=%v items_touched=%v blocked_hours=%v throughput=%d backlog=%d",
		reads.expenseNewItems, reads.expenseCompleted, reads.stateItemsTouched, reads.blockedHours,
		reads.throughputCompleted, reads.estimateBacklog)
}

func readStaleKeyAcceptance(t *testing.T, ctx context.Context, conn driver.Conn, teams ...string) staleKeyAcceptanceReads {
	t.Helper()
	org, day, next := staleKeyAcceptanceOrg, staleKeyAcceptanceDay, staleKeyAcceptanceDay.AddDate(0, 0, 1)
	reads := staleKeyAcceptanceReads{teamCompleted: map[string]uint64{}, teamBlocked: map[string]float64{}}
	scan := func(what, query string, args []any, targets ...any) {
		t.Helper()
		if err := conn.QueryRow(ctx, query, args...).Scan(targets...); err != nil {
			t.Fatalf("%s: %v", what, err)
		}
	}
	scan("sankey expense counts", `
SELECT CAST(sum(new_items_count) AS Float64), CAST(sum(items_completed) AS Float64)
FROM work_item_metrics_daily FINAL
WHERE day >= ? AND day < ? AND org_id = ?`, []any{day, next, org}, &reads.expenseNewItems, &reads.expenseCompleted)
	scan("sankey state status counts", `
SELECT CAST(sum(items_touched) AS Float64)
FROM work_item_state_durations_daily FINAL
WHERE day >= ? AND day < ? AND org_id = ?`, []any{day, next, org}, &reads.stateItemsTouched)
	const blocked = `
SELECT sum(duration_hours) FROM (
    SELECT day, provider, work_scope_id, team_id, status, argMax(duration_hours, computed_at) AS duration_hours
    FROM work_item_state_durations_daily
    WHERE day >= ? AND day < ? AND status = 'blocked' %s AND org_id = ?
    GROUP BY day, provider, work_scope_id, team_id, status)`
	scan("home blocked hours", fmt.Sprintf(blocked, ""), []any{day, next, org}, &reads.blockedHours)
	scan("capacity throughput", `
SELECT toUInt64(sum(items_completed)) FROM work_item_metrics_daily FINAL
WHERE day >= ? AND org_id = ?`, []any{day, org}, &reads.throughputCompleted)
	scan("estimate coverage backlog", `
SELECT toUInt64(sum(backlog_size)) FROM estimate_coverage_metrics_daily FINAL
WHERE day = ? AND org_id = ?`, []any{day, org}, &reads.estimateBacklog)
	for _, team := range teams {
		var completed uint64
		var hours float64
		scan("team throughput of "+team, `
SELECT toUInt64(sum(items_completed)) FROM work_item_metrics_daily FINAL
WHERE day >= ? AND org_id = ? AND team_id IN ?`, []any{day, org, []string{team}}, &completed)
		scan("team blocked hours of "+team, fmt.Sprintf(blocked, "AND team_id IN ?"), []any{day, next, []string{team}, org}, &hours)
		reads.teamCompleted[team], reads.teamBlocked[team] = completed, hours
	}
	return reads
}

// staleKeyAcceptanceHeld is what the newest row of each key of a team holds in
// the three tables, summed: 0 means no measure is stored under the team.
func staleKeyAcceptanceHeld(t *testing.T, ctx context.Context, conn driver.Conn, team string) float64 {
	t.Helper()
	var metrics, state, estimate float64
	for _, read := range []struct {
		target *float64
		query  string
	}{
		{&metrics, `SELECT toFloat64(sum(items_started + items_completed + wip_count_end_of_day + new_items_count + story_points_completed))
FROM work_item_metrics_daily FINAL WHERE org_id = ? AND day = ? AND team_id = ?`},
		{&state, `SELECT toFloat64(sum(duration_hours + items_touched + avg_wip))
FROM work_item_state_durations_daily FINAL WHERE org_id = ? AND day = ? AND team_id = ?`},
		{&estimate, `SELECT toFloat64(sum(estimated_count + unestimated_count + backlog_size))
FROM estimate_coverage_metrics_daily FINAL WHERE org_id = ? AND day = ? AND ifNull(team_id, '') = ?`},
	} {
		if err := conn.QueryRow(ctx, read.query, staleKeyAcceptanceOrg, staleKeyAcceptanceDay, team).Scan(read.target); err != nil {
			t.Fatalf("read what team %s holds: %v", team, err)
		}
	}
	return metrics + state + estimate
}

// staleKeyAcceptanceRowsSince is the number of stored row versions of a team
// in the three tables that were computed at or after a time.
func staleKeyAcceptanceRowsSince(t *testing.T, ctx context.Context, conn driver.Conn, team string, since time.Time) uint64 {
	t.Helper()
	var total uint64
	for _, table := range []string{
		"work_item_metrics_daily", "work_item_state_durations_daily", "estimate_coverage_metrics_daily",
	} {
		var count uint64
		if err := conn.QueryRow(ctx,
			`SELECT count() FROM `+table+` WHERE org_id = ? AND day = ? AND ifNull(team_id, '') = ? AND computed_at >= ?`,
			staleKeyAcceptanceOrg, staleKeyAcceptanceDay, team, since,
		).Scan(&count); err != nil {
			t.Fatalf("count the rows of team %s in %s: %v", team, table, err)
		}
		total += count
	}
	return total
}

func TestARecomputedDayIsCountedOnceAfterATeamIDChange(t *testing.T) {
	ctx := context.Background()
	conn := workItemAttributionLinkedIssueMigratedClickHouse(t, ctx)
	day := staleKeyAcceptanceDay

	// The day is computed while the two teams have their first ids.
	seedStaleKeyAcceptanceTeams(t, ctx, conn, day.Add(-72*time.Hour),
		staleKeyAcceptanceTeam{id: "ENG", nativeKey: "ENG", active: true},
		staleKeyAcceptanceTeam{id: "OPS", nativeKey: "OPS", active: true},
	)
	seedStaleKeyAcceptanceItems(t, ctx, conn)
	first := day.Add(30 * time.Hour)
	runStaleKeyAcceptanceFamilies(t, ctx, conn, first)
	before := readStaleKeyAcceptance(t, ctx, conn, "ENG", "OPS", "linear:ENG")
	if before.expenseCompleted != 2 || before.blockedHours != 24 || before.teamCompleted["ENG"] != 1 ||
		before.teamCompleted["OPS"] != 1 || before.teamBlocked["ENG"] != 24 || before.teamCompleted["linear:ENG"] != 0 {
		t.Fatalf("the first compute is not the case: %s teams=%v blocked=%v",
			before.orgScope(), before.teamCompleted, before.teamBlocked)
	}

	// ENG is carried to linear:ENG; OPS goes inactive with no successor.
	seedStaleKeyAcceptanceTeams(t, ctx, conn, first.Add(time.Hour),
		staleKeyAcceptanceTeam{id: "ENG", nativeKey: "ENG", active: false},
		staleKeyAcceptanceTeam{id: "linear:ENG", nativeKey: "ENG", active: true},
		staleKeyAcceptanceTeam{id: "OPS", nativeKey: "OPS", active: false},
	)

	// The same day is computed again.
	second := first.Add(10 * time.Hour)
	runStaleKeyAcceptanceFamilies(t, ctx, conn, second)
	after := readStaleKeyAcceptance(t, ctx, conn, "ENG", "OPS", "linear:ENG", "unassigned")

	if after.orgScope() != before.orgScope() {
		t.Errorf("a read with no team filter must count the recomputed day once:\n before %s\n after  %s",
			before.orgScope(), after.orgScope())
	}
	if after.teamCompleted["linear:ENG"] != before.teamCompleted["ENG"] || after.teamBlocked["linear:ENG"] != before.teamBlocked["ENG"] {
		t.Errorf("the keyed team must hold what the bare team held: completed %d (was %d), blocked hours %v (was %v)",
			after.teamCompleted["linear:ENG"], before.teamCompleted["ENG"], after.teamBlocked["linear:ENG"], before.teamBlocked["ENG"])
	}
	if after.teamCompleted["unassigned"] != before.teamCompleted["OPS"] {
		t.Errorf("the work of the retired team must be counted once, with no team: completed %d, want %d",
			after.teamCompleted["unassigned"], before.teamCompleted["OPS"])
	}
	for _, team := range []string{"ENG", "OPS"} {
		if held := staleKeyAcceptanceHeld(t, ctx, conn, team); held != 0 {
			t.Errorf("inactive team %s still holds measures of the recomputed day: %v", team, held)
		}
		if after.teamCompleted[team] != 0 || after.teamBlocked[team] != 0 {
			t.Errorf("a read of inactive team %s returns completed=%d blocked=%v, want 0",
				team, after.teamCompleted[team], after.teamBlocked[team])
		}
	}

	// A third compute changes nothing a reader sees, and writes no further
	// row under an inactive team id.
	third := second.Add(10 * time.Hour)
	runStaleKeyAcceptanceFamilies(t, ctx, conn, third)
	again := readStaleKeyAcceptance(t, ctx, conn, "ENG", "OPS", "linear:ENG", "unassigned")
	if again.orgScope() != after.orgScope() || fmt.Sprint(again.teamCompleted) != fmt.Sprint(after.teamCompleted) ||
		fmt.Sprint(again.teamBlocked) != fmt.Sprint(after.teamBlocked) {
		t.Errorf("a further compute changed what a reader sees:\n second %s %v %v\n third  %s %v %v",
			after.orgScope(), after.teamCompleted, after.teamBlocked, again.orgScope(), again.teamCompleted, again.teamBlocked)
	}
	for _, team := range []string{"ENG", "OPS"} {
		if rows := staleKeyAcceptanceRowsSince(t, ctx, conn, team, third); rows != 0 {
			t.Errorf("the third compute wrote %d row(s) under inactive team %s, want 0", rows, team)
		}
	}
}
