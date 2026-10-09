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

// A rerun starts a second run (generation) for a day that already has one.
// Both generations write the same keys to the daily tables, and the readers
// must count the day once: a later generation replaces an earlier one through
// the table engine (ReplacingMergeTree on computed_at) or through the
// reader's own newest-row rule (argMax on computed_at), never by adding up.
//
// The case computes one stored day with the four work-item families three
// times, as three runs with three clocks and unchanged teams, and reads it with
// the statements of the readers that take no team filter (sankey
// fetchExpenseCounts and fetchStateStatusCounts, Home fetchBlockedHours,
// capacity throughput, estimate coverage). It also reads the rows as stored,
// before any merge: the rerun must really have written a second generation,
// or the test proves nothing.

const rerunReaderOrg = "00000000-0000-4000-8000-0000000009d0"

var rerunReaderDay = time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC)

func seedRerunReaderStore(t *testing.T, ctx context.Context, conn driver.Conn) {
	t.Helper()
	day := rerunReaderDay
	teams, err := conn.PrepareBatch(ctx, `INSERT INTO teams
    (id, team_uuid, name, members, updated_at, last_synced, org_id, provider, native_team_key, is_active)`)
	if err != nil {
		t.Fatalf("prepare teams: %v", err)
	}
	for _, key := range []string{"ENG", "OPS"} {
		nativeKey := key
		if err := teams.Append(
			"linear:"+key, uuid.NewSHA1(uuid.NameSpaceURL, []byte("rerun-team:"+key)), "Team "+key, []string{},
			day.Add(-72*time.Hour), day.Add(-72*time.Hour), rerunReaderOrg, "linear", &nativeKey, uint8(1),
		); err != nil {
			t.Fatalf("append team %s: %v", key, err)
		}
	}
	if err := teams.Send(); err != nil {
		t.Fatalf("send teams: %v", err)
	}
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
			day.Add(-48*time.Hour), item.completedAt, &points, rerunReaderOrg, day.Add(12*time.Hour),
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
			transition.from, transition.to, "", rerunReaderOrg, day.Add(12*time.Hour),
		); err != nil {
			t.Fatalf("append transition of %s: %v", transition.id, err)
		}
	}
	if err := transitions.Send(); err != nil {
		t.Fatalf("send work_item_transitions: %v", err)
	}
}

// computeRerunReaderRun is one run of the day: a new run id, a new partition id
// and its own clock, as a rerun under a new tag is.
func computeRerunReaderRun(t *testing.T, ctx context.Context, conn driver.Conn, clock time.Time) {
	t.Helper()
	run := Run{ID: uuid.NewString(), OrganizationID: rerunReaderOrg, TargetDay: rerunReaderDay}
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

type rerunReaderReads struct {
	newItems, completed, itemsTouched, blockedHours float64
	throughput, backlog                             uint64
	teamCompleted                                   map[string]uint64
	teamBlocked                                     map[string]float64
}

func (reads rerunReaderReads) String() string {
	return fmt.Sprintf("new_items=%v completed=%v items_touched=%v blocked_hours=%v throughput=%d backlog=%d teams=%v blocked=%v",
		reads.newItems, reads.completed, reads.itemsTouched, reads.blockedHours, reads.throughput, reads.backlog,
		reads.teamCompleted, reads.teamBlocked)
}

func readRerunReaders(t *testing.T, ctx context.Context, conn driver.Conn) rerunReaderReads {
	t.Helper()
	org, day, next := rerunReaderOrg, rerunReaderDay, rerunReaderDay.AddDate(0, 0, 1)
	reads := rerunReaderReads{teamCompleted: map[string]uint64{}, teamBlocked: map[string]float64{}}
	scan := func(what, query string, args []any, targets ...any) {
		t.Helper()
		if err := conn.QueryRow(ctx, query, args...).Scan(targets...); err != nil {
			t.Fatalf("%s: %v", what, err)
		}
	}
	scan("sankey expense counts", `
SELECT CAST(sum(new_items_count) AS Float64), CAST(sum(items_completed) AS Float64)
FROM work_item_metrics_daily FINAL
WHERE day >= ? AND day < ? AND org_id = ?`, []any{day, next, org}, &reads.newItems, &reads.completed)
	scan("sankey state status counts", `
SELECT CAST(sum(items_touched) AS Float64)
FROM work_item_state_durations_daily FINAL
WHERE day >= ? AND day < ? AND org_id = ?`, []any{day, next, org}, &reads.itemsTouched)
	const blocked = `
SELECT sum(duration_hours) FROM (
    SELECT day, provider, work_scope_id, team_id, status, argMax(duration_hours, computed_at) AS duration_hours
    FROM work_item_state_durations_daily
    WHERE day >= ? AND day < ? AND status = 'blocked' %s AND org_id = ?
    GROUP BY day, provider, work_scope_id, team_id, status)`
	scan("home blocked hours", fmt.Sprintf(blocked, ""), []any{day, next, org}, &reads.blockedHours)
	scan("capacity throughput", `
SELECT toUInt64(sum(items_completed)) FROM work_item_metrics_daily FINAL
WHERE day >= ? AND org_id = ?`, []any{day, org}, &reads.throughput)
	scan("estimate coverage backlog", `
SELECT toUInt64(sum(backlog_size)) FROM estimate_coverage_metrics_daily FINAL
WHERE day = ? AND org_id = ?`, []any{day, org}, &reads.backlog)
	for _, team := range []string{"linear:ENG", "linear:OPS"} {
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

// storedRerunRows counts the rows of the day as stored in the three tables,
// with no FINAL and no dedup.
func storedRerunRows(t *testing.T, ctx context.Context, conn driver.Conn) uint64 {
	t.Helper()
	var total uint64
	for _, table := range []string{"work_item_metrics_daily", "work_item_state_durations_daily", "estimate_coverage_metrics_daily"} {
		var count uint64
		if err := conn.QueryRow(ctx, `SELECT count() FROM `+table+` WHERE org_id = ? AND day = ?`,
			rerunReaderOrg, rerunReaderDay).Scan(&count); err != nil {
			t.Fatalf("count %s: %v", table, err)
		}
		total += count
	}
	return total
}

func TestARerunGenerationOfADayIsCountedOnceByTheReaders(t *testing.T) {
	ctx := context.Background()
	conn := workItemAttributionLinkedIssueMigratedClickHouse(t, ctx)
	seedRerunReaderStore(t, ctx, conn)

	first := rerunReaderDay.Add(30 * time.Hour)
	computeRerunReaderRun(t, ctx, conn, first)
	before := readRerunReaders(t, ctx, conn)
	if before.completed != 2 || before.blockedHours != 24 || before.teamCompleted["linear:ENG"] != 1 ||
		before.teamCompleted["linear:OPS"] != 1 || before.teamBlocked["linear:ENG"] != 24 {
		t.Fatalf("the first run is not the case: %s", before)
	}
	storedOnce := storedRerunRows(t, ctx, conn)

	second := first.Add(10 * time.Hour)
	computeRerunReaderRun(t, ctx, conn, second)
	third := second.Add(10 * time.Hour)
	computeRerunReaderRun(t, ctx, conn, third)

	if stored := storedRerunRows(t, ctx, conn); stored <= storedOnce {
		t.Fatalf("a rerun must store a further generation (rows as stored: %d after the first run, %d after three); "+
			"a test of readers over one stored generation proves nothing", storedOnce, stored)
	}
	after := readRerunReaders(t, ctx, conn)
	if after.String() != before.String() {
		t.Errorf("a day computed by three runs must read as one:\n after one run    %s\n after three runs %s", before, after)
	}
}
