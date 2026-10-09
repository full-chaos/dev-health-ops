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

			// The end of a run can be tried again: it then writes nothing.
			if written := endStaleKeyRun(t, ctx, conn, second, clockA.Add(2*time.Hour)); written != 0 {
				t.Errorf("a second end of the run wrote %d row(s) of zeros, want 0", written)
			}
			if again := readSharedScope(t, ctx, conn, org); !reflect.DeepEqual(again, want) {
				t.Errorf("a second end of the run changed the day:\n got  %v\n want %v", again, want)
			}
		})
	}
}
