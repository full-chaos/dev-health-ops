//go:build integration

package daily

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
)

// Two partitions of one run that share a work scope, through the real
// executors on the schema of the migration chain.
//
// Partition A holds repository api and partition B repository web. Both hold
// items of the work scope board-1, so both compute the rows of that scope,
// each from the attributions that are stored when it reads. The day is first
// computed under the bare team ids. Then both teams are replaced by keyed ids
// and the day is computed again, with partition B run WHOLE inside one family
// of partition A, at a named point: A's read is then older than B's write of
// its attributions, and A computes B's items under the old id.
//
// Whatever the order of the two partitions, and whatever their clocks and the
// clock of the end of the run are, the day must end with each team's work
// under its keyed id, once, in the three tables that the partitions share, and
// with no measure under an old id. A partition that decided the stale keys
// from its own read wrote a row of zeros over the row the other partition had
// just written; the rule is applied once for the run instead (endStaleKeyRun).
//
// The file uses one symbol of the rule, endStaleKeyRun (see its file for how
// the same test runs on a tree without the run-level retraction).

var (
	sharedScopeDay     = time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC)
	sharedScopeRepoAPI = uuid.MustParse("00000000-0000-4000-8000-0000000005a1")
	sharedScopeRepoWeb = uuid.MustParse("00000000-0000-4000-8000-0000000005a2")
)

// sharedScopeHookConn runs a hook once, before the first batch whose INSERT
// names a table.
type sharedScopeHookConn struct {
	driver.Conn
	beforeInsertInto string
	hook             func()
	fired            bool
}

func (conn *sharedScopeHookConn) PrepareBatch(
	ctx context.Context, query string, opts ...driver.PrepareBatchOption,
) (driver.Batch, error) {
	if !conn.fired && conn.beforeInsertInto != "" && strings.Contains(query, "INSERT INTO "+conn.beforeInsertInto+" ") {
		conn.fired = true
		conn.hook()
	}
	return conn.Conn.PrepareBatch(ctx, query, opts...)
}

func seedSharedScopeTeams(
	t *testing.T, ctx context.Context, conn driver.Conn, org string, at time.Time, teams ...staleKeyAcceptanceTeam,
) {
	t.Helper()
	for _, team := range teams {
		active := uint8(0)
		if team.active {
			active = 1
		}
		nativeKey := team.nativeKey
		if err := conn.Exec(ctx, `INSERT INTO teams
    (id, team_uuid, name, members, updated_at, last_synced, org_id, provider, native_team_key, is_active)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			team.id, uuid.NewSHA1(uuid.NameSpaceURL, []byte("team:"+team.id)), "Team "+team.id, []string{},
			at, at, org, "linear", &nativeKey, active); err != nil {
			t.Fatalf("insert team %s: %v", team.id, err)
		}
	}
}

// seedSharedScopeItems stores, in each of the two repositories, one item that
// was completed on the day and one that is in progress with an estimate. All
// four are in the work scope board-1; the items of api are of team key ENG and
// the items of web of team key OPS.
func seedSharedScopeItems(t *testing.T, ctx context.Context, conn driver.Conn, org string) {
	t.Helper()
	day := sharedScopeDay
	done, started, points := day.Add(10*time.Hour), day.Add(-24*time.Hour), 3.0
	for _, item := range []struct {
		repo        uuid.UUID
		id, teamKey string
		status      string
		completedAt *time.Time
	}{
		{sharedScopeRepoAPI, "ENG-1", "ENG", "done", &done},
		{sharedScopeRepoAPI, "ENG-2", "ENG", "in_progress", nil},
		{sharedScopeRepoWeb, "OPS-1", "OPS", "done", &done},
		{sharedScopeRepoWeb, "OPS-2", "OPS", "in_progress", nil},
	} {
		if err := conn.Exec(ctx, `INSERT INTO work_items (
    repo_id, work_item_id, provider, type, status, project_id, native_team_key,
    created_at, started_at, completed_at, story_points, org_id, last_synced)
    VALUES (?, ?, 'linear', 'story', ?, 'board-1', ?, ?, ?, ?, ?, ?, ?)`,
			item.repo, item.id, item.status, item.teamKey, day.Add(-48*time.Hour), started, item.completedAt,
			points, org, day.Add(12*time.Hour)); err != nil {
			t.Fatalf("insert work item %s: %v", item.id, err)
		}
		transitions := [][3]any{{"todo", "in_progress", started}}
		if item.completedAt != nil {
			transitions = append(transitions, [3]any{"in_progress", "done", done})
		}
		for _, transition := range transitions {
			if err := conn.Exec(ctx, `INSERT INTO work_item_transitions
    (repo_id, work_item_id, occurred_at, provider, from_status, to_status, from_status_raw, to_status_raw, actor, org_id, last_synced)
    VALUES (?, ?, ?, 'linear', ?, ?, ?, ?, '', ?, ?)`,
				item.repo, item.id, transition[2], transition[0], transition[1], transition[0], transition[1],
				org, day.Add(12*time.Hour)); err != nil {
				t.Fatalf("insert transition of %s: %v", item.id, err)
			}
		}
	}
}

// runSharedScopeFamily runs one family of one partition at a clock.
func runSharedScopeFamily(
	t *testing.T, ctx context.Context, conn driver.Conn, family string, run Run, repo uuid.UUID, clock time.Time,
) {
	t.Helper()
	now := func() time.Time { return clock }
	var executor NativeFamilyExecutor
	switch family {
	case "work_item_attribution":
		attribution, err := NewWorkItemAttributionExecutor(conn)
		if err != nil {
			t.Fatal(err)
		}
		attribution.nowUTC = now
		executor = attribution
	case "work_item":
		metrics, err := NewWorkItemExecutor(conn)
		if err != nil {
			t.Fatal(err)
		}
		metrics.nowUTC = now
		executor = metrics
	case "work_item_estimate":
		estimate, err := NewWorkItemEstimateExecutor(conn)
		if err != nil {
			t.Fatal(err)
		}
		estimate.nowUTC = now
		executor = estimate
	case "work_item_state":
		state, err := NewWorkItemStateExecutor(conn)
		if err != nil {
			t.Fatal(err)
		}
		state.nowUTC = now
		executor = state
	default:
		t.Fatalf("no family %q", family)
	}
	partition := Partition{ID: uuid.NewString(), RunID: run.ID, RepoIDs: []RepositoryID{RepositoryID(repo.String())}}
	if _, err := executor.ComputeFamily(ctx, run, partition); err != nil {
		t.Fatalf("%s of %s at %s: %v", family, repo, clock.Format(time.RFC3339Nano), err)
	}
}

var sharedScopeFamilies = []string{"work_item_attribution", "work_item", "work_item_estimate", "work_item_state"}

// readSharedScope is what a reader of the newest rows sees for the day in the
// three shared tables, for each team id that holds a measure.
func readSharedScope(t *testing.T, ctx context.Context, conn driver.Conn, org string) map[string]string {
	t.Helper()
	held := map[string][]string{}
	for _, read := range []struct{ name, query string }{
		{"work_item_metrics_daily", `SELECT toString(team_id), toFloat64(sum(items_completed)), toFloat64(sum(wip_count_end_of_day))
FROM work_item_metrics_daily FINAL WHERE org_id = ? AND day = ? GROUP BY team_id`},
		{"estimate_coverage_metrics_daily", `SELECT ifNull(team_id, ''), toFloat64(sum(backlog_size)), toFloat64(sum(estimated_count))
FROM estimate_coverage_metrics_daily FINAL WHERE org_id = ? AND day = ? GROUP BY ifNull(team_id, '')`},
		{"work_item_state_durations_daily", `SELECT toString(team_id), toFloat64(sum(items_touched)), toFloat64(sum(duration_hours))
FROM work_item_state_durations_daily FINAL WHERE org_id = ? AND day = ? GROUP BY team_id`},
	} {
		rows, err := conn.Query(ctx, read.query, org, sharedScopeDay)
		if err != nil {
			t.Fatalf("read %s: %v", read.name, err)
		}
		for rows.Next() {
			var team string
			var first, second float64
			if err := rows.Scan(&team, &first, &second); err != nil {
				t.Fatalf("scan %s: %v", read.name, err)
			}
			if first != 0 || second != 0 {
				held[team] = append(held[team], fmt.Sprintf("%s=%v/%v", read.name, first, second))
			}
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("iterate %s: %v", read.name, err)
		}
		_ = rows.Close()
	}
	out := map[string]string{}
	for team, parts := range held {
		sort.Strings(parts)
		out[team] = strings.Join(parts, " ")
	}
	return out
}

// countSharedScopeRowsOfTeamsSince is the number of stored rows of the team
// ids in the three shared tables whose computed_at is a time or later.
func countSharedScopeRowsOfTeamsSince(
	t *testing.T, ctx context.Context, conn driver.Conn, org string, since time.Time, teams ...string,
) uint64 {
	t.Helper()
	var total uint64
	for _, table := range []string{"work_item_metrics_daily", "estimate_coverage_metrics_daily", "work_item_state_durations_daily"} {
		var count uint64
		if err := conn.QueryRow(ctx, "SELECT count() FROM "+table+
			" WHERE org_id = ? AND day = ? AND ifNull(team_id, '') IN ? AND computed_at >= ?",
			org, sharedScopeDay, teams, since).Scan(&count); err != nil {
			t.Fatalf("count the rows of %v in %s: %v", teams, table, err)
		}
		total += count
	}
	return total
}

func TestTwoPartitionsOfOneWorkScopeEndTheDayUnderTheNewTeamIDs(t *testing.T) {
	ctx := context.Background()
	conn := workItemAttributionLinkedIssueMigratedClickHouse(t, ctx)
	day := sharedScopeDay
	for index, testCase := range []struct {
		name string
		// family is the family of partition A that partition B runs inside,
		// before A's first batch into the table. An empty family runs the two
		// partitions one after the other.
		family, beforeInsertInto string
		// skew is B's clock against A's; endOfRun is the clock of the end of
		// the run against A's.
		skew, endOfRun time.Duration
	}{
		{"one partition after the other", "", "", 0, time.Minute},
		{"B inside A's work_item before its write, one second", "work_item", "work_item_metrics_daily", 0, time.Minute},
		{"B inside A's work_item before its write, the run ends in the same second", "work_item", "work_item_metrics_daily", 0, 0},
		{"B inside A's work_item after its write, one second", "work_item", "work_item_user_metrics_daily", 0, time.Minute},
		{"B inside A's work_item after its write, B's clock 200 ms behind", "work_item", "work_item_user_metrics_daily", -200 * time.Millisecond, time.Minute},
		{"B inside A's work_item before its write, B one second later, the run ends on a clock 2 s behind", "work_item", "work_item_metrics_daily", time.Second, -2 * time.Second},
		{"B inside A's work_item_estimate before its write, one second", "work_item_estimate", "estimate_coverage_metrics_daily", 0, time.Minute},
		{"B inside A's work_item_estimate before its write, B's clock 200 ms behind, the run ends in the same second", "work_item_estimate", "estimate_coverage_metrics_daily", -200 * time.Millisecond, 0},
		{"B inside A's work_item_state before its write, one second", "work_item_state", "work_item_state_durations_daily", 0, time.Minute},
		{"B inside A's work_item_state before its write, B's clock 200 ms behind, the run ends in the same second", "work_item_state", "work_item_state_durations_daily", -200 * time.Millisecond, 0},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			org := fmt.Sprintf("00000000-0000-4000-8000-0000005c%04d", index+1)
			repos := []RepositoryID{RepositoryID(sharedScopeRepoAPI.String()), RepositoryID(sharedScopeRepoWeb.String())}
			seedSharedScopeTeams(t, ctx, conn, org, day.Add(-72*time.Hour),
				staleKeyAcceptanceTeam{id: "ENG", nativeKey: "ENG", active: true},
				staleKeyAcceptanceTeam{id: "OPS", nativeKey: "OPS", active: true},
			)
			seedSharedScopeItems(t, ctx, conn, org)

			// The day under the bare ids, one partition after the other.
			history := day.Add(30 * time.Hour)
			first := Run{ID: uuid.NewString(), OrganizationID: org, TargetDay: day, FullOrg: true, DiscoveredRepoIDs: repos}
			for _, repo := range []uuid.UUID{sharedScopeRepoAPI, sharedScopeRepoWeb} {
				for _, family := range sharedScopeFamilies {
					runSharedScopeFamily(t, ctx, conn, family, first, repo, history)
				}
			}
			endStaleKeyRun(t, ctx, conn, first, history.Add(time.Minute))
			before := readSharedScope(t, ctx, conn, org)
			if len(before) != 2 || before["ENG"] == "" || before["OPS"] == "" ||
				strings.Count(before["ENG"], "=") != 3 || strings.Count(before["OPS"], "=") != 3 {
				t.Fatalf("the first compute is not the case: %v", before)
			}

			// Both teams are replaced by keyed ids.
			seedSharedScopeTeams(t, ctx, conn, org, history.Add(time.Hour),
				staleKeyAcceptanceTeam{id: "ENG", nativeKey: "ENG", active: false},
				staleKeyAcceptanceTeam{id: "linear:ENG", nativeKey: "ENG", active: true},
				staleKeyAcceptanceTeam{id: "OPS", nativeKey: "OPS", active: false},
				staleKeyAcceptanceTeam{id: "linear:OPS", nativeKey: "OPS", active: true},
			)

			// The recompute. Partition A attributes its items; partition B
			// runs whole at the named point of A; A goes on.
			clockA := history.Add(10 * time.Hour)
			clockB := clockA.Add(testCase.skew)
			second := Run{ID: uuid.NewString(), OrganizationID: org, TargetDay: day, FullOrg: true, DiscoveredRepoIDs: repos}
			partitionB := func() {
				for _, family := range sharedScopeFamilies {
					runSharedScopeFamily(t, ctx, conn, family, second, sharedScopeRepoWeb, clockB)
				}
			}
			hooked := &sharedScopeHookConn{Conn: conn, beforeInsertInto: testCase.beforeInsertInto, hook: partitionB}
			for _, family := range sharedScopeFamilies {
				on := driver.Conn(conn)
				if family == testCase.family {
					on = hooked
				}
				runSharedScopeFamily(t, ctx, on, family, second, sharedScopeRepoAPI, clockA)
			}
			switch {
			case testCase.family == "":
				partitionB()
			case !hooked.fired:
				t.Fatalf("partition B did not run inside %s: the case did not measure", testCase.family)
			}
			endStaleKeyRun(t, ctx, conn, second, clockA.Add(testCase.endOfRun))

			want := map[string]string{"linear:ENG": before["ENG"], "linear:OPS": before["OPS"]}
			after := readSharedScope(t, ctx, conn, org)
			if !reflect.DeepEqual(after, want) {
				t.Errorf("the day after the recompute, by team:\n got  %v\n want %v", after, want)
			}

			// The end of a run can be tried again: it then changes nothing a
			// reader sees and writes no row under an old id.
			again := clockA.Add(2 * time.Hour)
			endStaleKeyRun(t, ctx, conn, second, again)
			if after := readSharedScope(t, ctx, conn, org); !reflect.DeepEqual(after, want) {
				t.Errorf("a second end of the run changed the day:\n got  %v\n want %v", after, want)
			}
			if rows := countSharedScopeRowsOfTeamsSince(t, ctx, conn, org, again, "ENG", "OPS"); rows != 0 {
				t.Errorf("a second end of the run wrote %d row(s) under the old ids, want 0", rows)
			}
		})
	}
}

// The rows of a shared work scope, not only its keys: an item moves from one
// active team to another, and the partition that holds the item writes the new
// attribution while the other partition has already read the old one. Both
// keys are right, so no key is superseded; but the partition that read first
// and wrote last stores the item under its old team, and the day counts the
// item twice. The end of the run computes the rows of the scope once, after
// every partition wrote its attributions, and stores them as the newest rows,
// in each of the three work-item tables.
func TestTheEndOfARunSettlesTheRowsOfASharedWorkScope(t *testing.T) {
	ctx := context.Background()
	conn := workItemAttributionLinkedIssueMigratedClickHouse(t, ctx)
	day := sharedScopeDay
	for index, testCase := range []struct {
		// family is the family of partition A that partition B runs inside,
		// before A's first batch into table; measure is read from table.
		family, table, measure string
	}{
		{"work_item", "work_item_metrics_daily", "wip_count_end_of_day"},
		{"work_item_estimate", "estimate_coverage_metrics_daily", "backlog_size"},
		{"work_item_state", "work_item_state_durations_daily", "items_touched"},
	} {
		t.Run(testCase.family, func(t *testing.T) {
			org := fmt.Sprintf("00000000-0000-4000-8000-0000005d%04d", index+1)
			repos := []RepositoryID{RepositoryID(sharedScopeRepoAPI.String()), RepositoryID(sharedScopeRepoWeb.String())}
			seedSharedScopeTeams(t, ctx, conn, org, day.Add(-72*time.Hour),
				staleKeyAcceptanceTeam{id: "ENG", nativeKey: "ENG", active: true},
				staleKeyAcceptanceTeam{id: "OPS", nativeKey: "OPS", active: true},
			)
			started := day.Add(-24 * time.Hour)
			item := func(repo uuid.UUID, id, teamKey string, synced time.Time) {
				t.Helper()
				if err := conn.Exec(ctx, `INSERT INTO work_items (
    repo_id, work_item_id, provider, type, status, project_id, native_team_key,
    created_at, started_at, story_points, org_id, last_synced)
    VALUES (?, ?, 'linear', 'story', 'in_progress', 'board-1', ?, ?, ?, ?, ?, ?)`,
					repo, id, teamKey, day.Add(-48*time.Hour), started, 3.0, org, synced); err != nil {
					t.Fatalf("insert work item %s: %v", id, err)
				}
			}
			for _, seeded := range []struct {
				repo uuid.UUID
				id   string
			}{{sharedScopeRepoAPI, "A-1"}, {sharedScopeRepoWeb, "W-1"}} {
				item(seeded.repo, seeded.id, "ENG", day.Add(12*time.Hour))
				if err := conn.Exec(ctx, `INSERT INTO work_item_transitions
    (repo_id, work_item_id, occurred_at, provider, from_status, to_status, from_status_raw, to_status_raw, actor, org_id, last_synced)
    VALUES (?, ?, ?, 'linear', 'todo', 'in_progress', 'todo', 'in_progress', '', ?, ?)`,
					seeded.repo, seeded.id, started, org, day.Add(12*time.Hour)); err != nil {
					t.Fatalf("insert transition of %s: %v", seeded.id, err)
				}
			}
			byTeam := func() map[string]float64 {
				t.Helper()
				rows, err := conn.Query(ctx, "SELECT ifNull(team_id, ''), toFloat64(sum("+testCase.measure+")) FROM "+testCase.table+
					" FINAL WHERE org_id = ? AND day = ? GROUP BY ifNull(team_id, '') HAVING sum("+testCase.measure+") > 0", org, day)
				if err != nil {
					t.Fatal(err)
				}
				defer rows.Close()
				out := map[string]float64{}
				for rows.Next() {
					var team string
					var value float64
					if err := rows.Scan(&team, &value); err != nil {
						t.Fatal(err)
					}
					out[team] = value
				}
				return out
			}

			history := day.Add(30 * time.Hour)
			first := Run{ID: uuid.NewString(), OrganizationID: org, TargetDay: day, FullOrg: true, DiscoveredRepoIDs: repos}
			for _, repo := range []uuid.UUID{sharedScopeRepoAPI, sharedScopeRepoWeb} {
				for _, family := range sharedScopeFamilies {
					runSharedScopeFamily(t, ctx, conn, family, first, repo, history)
				}
			}
			endStaleKeyRun(t, ctx, conn, first, history.Add(time.Minute))
			if before := byTeam(); !reflect.DeepEqual(before, map[string]float64{"ENG": 2}) {
				t.Fatalf("the first compute is not the case: %s of %s by team = %v", testCase.measure, testCase.table, before)
			}

			// The item of repository web moves to team OPS.
			item(sharedScopeRepoWeb, "W-1", "OPS", history.Add(time.Hour))

			// The recompute, both partitions in one second: A reads the scope,
			// then B runs whole (it writes the new attribution of its item and
			// its rows), then A writes the rows of its older read.
			clock := history.Add(10 * time.Hour)
			second := Run{ID: uuid.NewString(), OrganizationID: org, TargetDay: day, FullOrg: true, DiscoveredRepoIDs: repos}
			hooked := &sharedScopeHookConn{Conn: conn, beforeInsertInto: testCase.table, hook: func() {
				for _, family := range sharedScopeFamilies {
					runSharedScopeFamily(t, ctx, conn, family, second, sharedScopeRepoWeb, clock)
				}
			}}
			for _, family := range sharedScopeFamilies {
				on := driver.Conn(conn)
				if family == testCase.family {
					on = hooked
				}
				runSharedScopeFamily(t, ctx, on, family, second, sharedScopeRepoAPI, clock)
			}
			if !hooked.fired {
				t.Fatal("partition B did not run inside partition A: the case did not measure")
			}
			endStaleKeyRun(t, ctx, conn, second, clock)

			if after, want := byTeam(), map[string]float64{"ENG": 1, "OPS": 1}; !reflect.DeepEqual(after, want) {
				t.Errorf("%s of %s by team = %v, want %v: each item once, under its team of today",
					testCase.measure, testCase.table, after, want)
			}
		})
	}
}

// Two runs of one day (two generations: the nightly run and a tagged re-run)
// that end at the same time: the end of the second run runs whole inside the
// end of the first, before its first write, on the same clock. Both read the
// same stored inputs, so both compute the same rows and the same stale keys,
// and the day ends right whichever write is stored last.
func TestTwoRunsOfOneDayThatEndTogetherLeaveTheDayRight(t *testing.T) {
	ctx := context.Background()
	conn := workItemAttributionLinkedIssueMigratedClickHouse(t, ctx)
	day := sharedScopeDay
	const org = "00000000-0000-4000-8000-0000005e0001"
	repos := []RepositoryID{RepositoryID(sharedScopeRepoAPI.String()), RepositoryID(sharedScopeRepoWeb.String())}
	seedSharedScopeTeams(t, ctx, conn, org, day.Add(-72*time.Hour),
		staleKeyAcceptanceTeam{id: "ENG", nativeKey: "ENG", active: true},
		staleKeyAcceptanceTeam{id: "OPS", nativeKey: "OPS", active: true},
	)
	seedSharedScopeItems(t, ctx, conn, org)
	partitions := func(run Run, clock time.Time) {
		for _, repo := range []uuid.UUID{sharedScopeRepoAPI, sharedScopeRepoWeb} {
			for _, family := range sharedScopeFamilies {
				runSharedScopeFamily(t, ctx, conn, family, run, repo, clock)
			}
		}
	}
	history := day.Add(30 * time.Hour)
	first := Run{ID: uuid.NewString(), OrganizationID: org, TargetDay: day, FullOrg: true, DiscoveredRepoIDs: repos}
	partitions(first, history)
	endStaleKeyRun(t, ctx, conn, first, history.Add(time.Minute))
	before := readSharedScope(t, ctx, conn, org)
	seedSharedScopeTeams(t, ctx, conn, org, history.Add(time.Hour),
		staleKeyAcceptanceTeam{id: "ENG", nativeKey: "ENG", active: false},
		staleKeyAcceptanceTeam{id: "linear:ENG", nativeKey: "ENG", active: true},
		staleKeyAcceptanceTeam{id: "OPS", nativeKey: "OPS", active: false},
		staleKeyAcceptanceTeam{id: "linear:OPS", nativeKey: "OPS", active: true},
	)

	// Two runs compute the day; then both end on one clock, the second inside
	// the first.
	clock := history.Add(10 * time.Hour)
	nightly := Run{ID: uuid.NewString(), OrganizationID: org, TargetDay: day, FullOrg: true, DiscoveredRepoIDs: repos}
	tagged := Run{ID: uuid.NewString(), OrganizationID: org, TargetDay: day, FullOrg: true, DiscoveredRepoIDs: repos}
	partitions(nightly, clock)
	partitions(tagged, clock)
	hooked := &sharedScopeHookConn{Conn: conn, beforeInsertInto: "work_item_metrics_daily", hook: func() {
		endStaleKeyRun(t, ctx, conn, tagged, clock)
	}}
	endStaleKeyRun(t, ctx, hooked, nightly, clock)
	if !hooked.fired {
		t.Fatal("the end of the second run did not run inside the end of the first: the case did not measure")
	}
	want := map[string]string{"linear:ENG": before["ENG"], "linear:OPS": before["OPS"]}
	if after := readSharedScope(t, ctx, conn, org); !reflect.DeepEqual(after, want) || len(want) != 2 {
		t.Errorf("the day after two runs ended together, by team:\n got  %v\n want %v", after, want)
	}
}

// A key comes back. The rows of zeros of a run are raised above the stored
// rows, so they can be newer than the clock; a REAL row that a later run
// writes for the same key must then be strictly newer than that row of zeros,
// or real work reads as zero. The partitions write at their clock and can
// lose; the end of the run stores the rows of the day strictly newer than
// every stored row, so after the run ended every form of "the newest row"
// (FINAL, argMax, LIMIT 1 BY) returns the real row.
//
// The teams are replaced by keyed ids and the day is computed again: the old
// keys get rows of zeros. Then the old ids are active again and the day is
// computed a third time, in the SAME second as the second run, and on a clock
// two seconds BEHIND it.
func TestARealRowOfAKeyThatComesBackIsNewerThanItsRowOfZeros(t *testing.T) {
	ctx := context.Background()
	conn := workItemAttributionLinkedIssueMigratedClickHouse(t, ctx)
	day := sharedScopeDay
	for index, testCase := range []struct {
		name  string
		third time.Duration // the clock of the third run against the second
	}{
		{"the third run in the same second as the second", 0},
		{"the third run on a clock two seconds behind the second", -2 * time.Second},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			org := fmt.Sprintf("00000000-0000-4000-8000-0000005f%04d", index+1)
			repos := []RepositoryID{RepositoryID(sharedScopeRepoAPI.String()), RepositoryID(sharedScopeRepoWeb.String())}
			// The attribution family has its own clock: the attributions of a
			// run are an input here, and the case is about the rows of the
			// three tables, not about the version of an attribution.
			compute := func(attributed, clock time.Time) {
				t.Helper()
				run := Run{ID: uuid.NewString(), OrganizationID: org, TargetDay: day, FullOrg: true, DiscoveredRepoIDs: repos}
				for _, repo := range []uuid.UUID{sharedScopeRepoAPI, sharedScopeRepoWeb} {
					for _, family := range sharedScopeFamilies {
						at := clock
						if family == "work_item_attribution" {
							at = attributed
						}
						runSharedScopeFamily(t, ctx, conn, family, run, repo, at)
					}
				}
				endStaleKeyRun(t, ctx, conn, run, clock)
			}
			teams := func(at time.Time, bareActive bool) {
				t.Helper()
				seedSharedScopeTeams(t, ctx, conn, org, at,
					staleKeyAcceptanceTeam{id: "ENG", nativeKey: "ENG", active: bareActive},
					staleKeyAcceptanceTeam{id: "linear:ENG", nativeKey: "ENG", active: !bareActive},
					staleKeyAcceptanceTeam{id: "OPS", nativeKey: "OPS", active: bareActive},
					staleKeyAcceptanceTeam{id: "linear:OPS", nativeKey: "OPS", active: !bareActive},
				)
			}
			seedSharedScopeTeams(t, ctx, conn, org, day.Add(-72*time.Hour),
				staleKeyAcceptanceTeam{id: "ENG", nativeKey: "ENG", active: true},
				staleKeyAcceptanceTeam{id: "OPS", nativeKey: "OPS", active: true},
			)
			seedSharedScopeItems(t, ctx, conn, org)
			history := day.Add(30 * time.Hour)
			compute(history, history)
			before := readSharedScope(t, ctx, conn, org)

			teams(history.Add(time.Hour), false)
			second := history.Add(10 * time.Hour)
			compute(history.Add(90*time.Minute), second)
			if keyed := readSharedScope(t, ctx, conn, org); keyed["linear:ENG"] != before["ENG"] || len(keyed) != 2 {
				t.Fatalf("the second compute is not the case: %v", keyed)
			}
			// The newest row of the key of team ENG is now its row of zeros.
			// A tie with it is not enough: FINAL takes the later insert of a
			// tie, the other two forms can take either row.
			keyTables := []struct{ table, measure, extraKey string }{
				{"work_item_metrics_daily", "items_completed", ""},
				{"estimate_coverage_metrics_daily", "backlog_size", ""},
				{"work_item_state_durations_daily", "items_touched", " AND status = 'in_progress'"},
			}
			keyOf := func(extraKey string) string {
				return " WHERE org_id = ? AND day = ? AND provider = 'linear' AND work_scope_id = 'board-1' AND ifNull(team_id, '') = 'ENG'" + extraKey
			}
			newestOf := func(table, extraKey string) time.Time {
				t.Helper()
				var newest time.Time
				if err := conn.QueryRow(ctx, "SELECT max(computed_at) FROM "+table+keyOf(extraKey), org, day).Scan(&newest); err != nil {
					t.Fatalf("read the newest row of %s: %v", table, err)
				}
				return newest
			}
			zeros := map[string]time.Time{}
			for _, read := range keyTables {
				zeros[read.table] = newestOf(read.table, read.extraKey)
			}

			// The old ids are active again; the third run is not later than
			// the rows of zeros of the second.
			teams(history.Add(2*time.Hour), true)
			compute(history.Add(150*time.Minute), second.Add(testCase.third))

			if after := readSharedScope(t, ctx, conn, org); !reflect.DeepEqual(after, before) {
				t.Errorf("FINAL: the day by team after the keys came back:\n got  %v\n want %v", after, before)
			}
			// The other two forms of "the newest row of a key", for the key of
			// team ENG in each table.
			for _, read := range keyTables {
				where := keyOf(read.extraKey)
				if real := newestOf(read.table, read.extraKey); !real.After(zeros[read.table]) {
					t.Errorf("%s: the newest row of the key that came back is at %s, its row of zeros at %s; want strictly newer",
						read.table, real.Format(time.RFC3339Nano), zeros[read.table].Format(time.RFC3339Nano))
				}
				var byArgMax, byLimit float64
				if err := conn.QueryRow(ctx, "SELECT toFloat64(argMax("+read.measure+", computed_at)) FROM "+read.table+where,
					org, day).Scan(&byArgMax); err != nil {
					t.Fatalf("argMax read of %s: %v", read.table, err)
				}
				if err := conn.QueryRow(ctx, "SELECT toFloat64("+read.measure+") FROM "+read.table+where+
					" ORDER BY computed_at DESC LIMIT 1 BY org_id", org, day).Scan(&byLimit); err != nil {
					t.Fatalf("LIMIT 1 BY read of %s: %v", read.table, err)
				}
				if byArgMax == 0 || byLimit == 0 {
					t.Errorf("%s of %s for the key that came back: argMax %v, LIMIT 1 BY %v; want the real row in both",
						read.measure, read.table, byArgMax, byLimit)
				}
			}
		})
	}
}
