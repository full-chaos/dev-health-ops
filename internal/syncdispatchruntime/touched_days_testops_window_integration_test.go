//go:build integration

package syncdispatchruntime

import (
	"context"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
)

// insertRetractionSuite writes one retraction row (retracted = 1, every measure 0) of a run, as the GitHub tests route does for
// a dropped report.
func insertRetractionSuite(t *testing.T, ctx context.Context, conn driver.Conn, org string, suite touchedTestSuite) {
	t.Helper()
	if err := conn.Exec(ctx, `INSERT INTO test_suite_results (repo_id, run_id, suite_id, suite_name, total_count,
		passed_count, failed_count, skipped_count, started_at, finished_at, org_id, last_synced, report_slot, run_attempt, retracted)
		VALUES (?, ?, ?, 'pkg/a', 0, 0, 0, 0, ?, ?, ?, ?, ?, ?, 1)`,
		suite.repo, suite.run, suite.suite, suite.started, suite.finished, org, suite.synced, suite.slot, suite.attempt); err != nil {
		t.Fatal(err)
	}
}

// windowEvents counts the rows of each (day, repository) of the testops window lane.
func windowEvents(t *testing.T, ctx context.Context, conn driver.Conn, org, kind string) map[string]uint64 {
	t.Helper()
	rows, err := conn.Query(ctx, `SELECT toString(day), toString(repo_id), count() FROM daily_metrics_touched_days
		WHERE org_id = ? AND kind = ? GROUP BY day, repo_id`, org, kind)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	out := map[string]uint64{}
	for rows.Next() {
		var day, repo string
		var count uint64
		if err := rows.Scan(&day, &repo, &count); err != nil {
			t.Fatal(err)
		}
		out[day+"|"+repo] = count
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// windowKeys are the keys base+1 .. base+29 of repo for every base day.
func windowKeys(repo uuid.UUID, bases ...time.Time) map[string]uint64 {
	keys := map[string]uint64{}
	for _, base := range bases {
		for offset := 1; offset <= 29; offset++ {
			keys[base.AddDate(0, 0, offset).Format("2006-01-02")+"|"+repo.String()] = 1
		}
	}
	return keys
}

func sortedKeys(keys map[string]uint64) []string {
	out := make([]string, 0, len(keys))
	for key := range keys {
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}

// C9 (R8): one record writes exactly one row for each distinct (day, repository) key of the window of the base days of the runs
// a retraction row was written for, and reports that count. A run that spans two days has two base days; overlapping windows
// give one key. A run written in the sync run without a retraction row, a retraction row written before the run and another
// organization give none.
func TestTestopsWindowRecordWritesEachKeyOnceForTheRunsARetractionNamed(t *testing.T) {
	ctx, conn := newReadbackIntegrationConn(t)
	store, err := NewClickHouseTouchedDaysStore(conn)
	if err != nil {
		t.Fatal(err)
	}
	inRun := touchedTestSince.Add(time.Hour)
	june1, june2, june10 := touchedTestDay(time.June, 1), touchedTestDay(time.June, 2), touchedTestDay(time.June, 10)
	// run-1 of repo A spans June 1 and June 2: a live suite on June 1, the retraction row on June 2.
	insertTouchedTestSuites(t, ctx, conn, touchedTestOrg,
		touchedTestSuite{repo: touchedTestRepoA, run: "run-1", suite: "live", slot: "leg", attempt: 1, started: at(june1, 1), synced: inRun},
		touchedTestSuite{repo: touchedTestRepoB, run: "run-live", suite: "live", slot: "leg", attempt: 1,
			started: at(touchedTestDay(time.June, 20), 1), synced: inRun},
	)
	insertRetractionSuite(t, ctx, conn, touchedTestOrg,
		touchedTestSuite{repo: touchedTestRepoA, run: "run-1", suite: "total", slot: "total", attempt: 1, started: at(june2, 1), synced: inRun})
	insertRetractionSuite(t, ctx, conn, touchedTestOrg,
		touchedTestSuite{repo: touchedTestRepoA, run: "run-2", suite: "total", slot: "total", attempt: 1, started: at(june10, 1), synced: inRun})
	insertRetractionSuite(t, ctx, conn, touchedTestOrg,
		touchedTestSuite{repo: touchedTestRepoB, run: "run-3", suite: "total", slot: "total", attempt: 1, finished: at(june1, 3), synced: inRun})
	// Written before the sync run: not this record's.
	insertRetractionSuite(t, ctx, conn, touchedTestOrg,
		touchedTestSuite{repo: touchedTestRepoB, run: "run-old", suite: "total", slot: "total", attempt: 1,
			started: at(touchedTestDay(time.April, 1), 1), synced: touchedTestSince.Add(-time.Hour)})
	insertRetractionSuite(t, ctx, conn, touchedTestOtherOrg,
		touchedTestSuite{repo: touchedTestRepoA, run: "run-1", suite: "total", slot: "total", attempt: 1, started: at(june2, 1), synced: inRun})

	var report TestopsWindowReport
	if _, err := store.RecordTouchedTests(WithTestopsWindowSink(ctx, &report), touchedTestOrg, touchedTestSince); err != nil {
		t.Fatal(err)
	}
	want := windowKeys(touchedTestRepoA, june1, june2, june10)
	for key, count := range windowKeys(touchedTestRepoB, june1) {
		want[key] = count
	}
	got := windowEvents(t, ctx, conn, touchedTestOrg, "touched_testops_window")
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("window rows (key -> rows)\n got %v\nwant %v", sortedKeys(got), sortedKeys(want))
	}
	for key, rows := range got {
		if rows != 1 {
			t.Errorf("key %s has %d rows, want 1", key, rows)
		}
	}
	if report.Keys != uint64(len(want)) || report.OverBound() {
		t.Errorf("report %+v, want %d keys under the bound", report, len(want))
	}
	if other := windowEvents(t, ctx, conn, touchedTestOtherOrg, "touched_testops_window"); len(other) != 0 {
		t.Errorf("the other organization was not recorded; it has window keys %v", sortedKeys(other))
	}
	// The touched-day lane holds the run days only (run-1, run-2, run-3, run-live): no window day.
	if days := pendingTouchedTestKeys(t, ctx, conn, touchedTestOrg); len(days) != 5 {
		t.Errorf("touched-day lane keys %v, want the 5 run days", days)
	}
}

// C2 (store): a record with no retraction row written in the sync run writes no window key and reports 0.
func TestTestopsWindowRecordWithoutARetractionWritesNoKey(t *testing.T) {
	ctx, conn := newReadbackIntegrationConn(t)
	store, err := NewClickHouseTouchedDaysStore(conn)
	if err != nil {
		t.Fatal(err)
	}
	insertTouchedTestSuites(t, ctx, conn, touchedTestOrg,
		touchedTestSuite{repo: touchedTestRepoA, run: "run-1", suite: "live", slot: "leg", attempt: 2,
			started: at(touchedTestDay(time.June, 1), 1), synced: touchedTestSince.Add(time.Hour)})
	report := TestopsWindowReport{Keys: 99}
	if _, err := store.RecordTouchedTests(WithTestopsWindowSink(ctx, &report), touchedTestOrg, touchedTestSince); err != nil {
		t.Fatal(err)
	}
	if got := windowEvents(t, ctx, conn, touchedTestOrg, "touched_testops_window"); len(got) != 0 || report.Keys != 0 {
		t.Fatalf("window keys %v, report %+v; want none", sortedKeys(got), report)
	}
}

// C8: the window stops at the day of the record (the ClickHouse clock): a later day has no stored row to correct.
func TestTestopsWindowRecordStopsAtTheDayOfTheRecord(t *testing.T) {
	ctx, conn := newReadbackIntegrationConn(t)
	store, err := NewClickHouseTouchedDaysStore(conn)
	if err != nil {
		t.Fatal(err)
	}
	var nowMillis int64
	if err := conn.QueryRow(ctx, `SELECT toUnixTimestamp64Milli(now64(3))`).Scan(&nowMillis); err != nil {
		t.Fatal(err)
	}
	now := time.UnixMilli(nowMillis).UTC()
	today := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	base := today.AddDate(0, 0, -3)
	insertRetractionSuite(t, ctx, conn, touchedTestOrg,
		touchedTestSuite{repo: touchedTestRepoA, run: "run-recent", suite: "total", slot: "total", attempt: 1,
			started: at(base, 1), synced: now})
	if _, err := store.RecordTouchedTests(ctx, touchedTestOrg, now.Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	want := map[string]uint64{}
	for day := base.AddDate(0, 0, 1); !day.After(today); day = day.AddDate(0, 0, 1) {
		want[day.Format("2006-01-02")+"|"+touchedTestRepoA.String()] = 1
	}
	if got := windowEvents(t, ctx, conn, touchedTestOrg, "touched_testops_window"); !reflect.DeepEqual(got, want) {
		t.Fatalf("window keys %v, want the days after the base day up to today %v", sortedKeys(got), sortedKeys(want))
	}
}

// C7 + C7b (R11): the two lanes read their own kinds only. A window key is no key of the touched-day lane (pending, backlog or
// return), and the window lane returns to pending no key that has no window touch, whatever its other rows hold.
func TestTestopsWindowLaneAndTouchedDayLaneKeepTheirKeysApart(t *testing.T) {
	ctx, conn := newReadbackIntegrationConn(t)
	days, err := NewClickHouseTouchedDaysStore(conn)
	if err != nil {
		t.Fatal(err)
	}
	window := days.TestopsWindowLane()
	june1 := touchedTestDay(time.June, 1)
	insertRetractionSuite(t, ctx, conn, touchedTestOrg,
		touchedTestSuite{repo: touchedTestRepoA, run: "run-1", suite: "total", slot: "total", attempt: 1,
			started: at(june1, 1), synced: touchedTestSince.Add(time.Hour)})
	if _, err := days.RecordTouchedTests(ctx, touchedTestOrg, touchedTestSince); err != nil {
		t.Fatal(err)
	}
	windowDay := june1.AddDate(0, 0, 5)

	normal, err := days.PendingDays(ctx, touchedTestOrg, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(normal.Days) != 1 || !normal.Days[0].Equal(time.Date(2026, time.June, 1, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("touched-day lane pending days %v, want June 1 only", normal.Days)
	}
	backlog, err := days.Backlog(ctx, touchedTestOrg, 100)
	if err != nil || len(backlog.Days) != 1 {
		t.Fatalf("touched-day lane backlog %v (err %v), want June 1 only", backlog.Days, err)
	}
	pending, err := window.PendingDays(ctx, touchedTestOrg, 100)
	if err != nil || len(pending.Days) != 29 {
		t.Fatalf("window lane pending days %d (err %v), want 29", len(pending.Days), err)
	}

	// The window key of June 6 is dispatched: a run of every repository of that day in the touched-day lane returns nothing.
	if err := window.MarkDispatched(ctx, touchedTestOrg, pending.TakenAt, []time.Time{windowDay}, nil); err != nil {
		t.Fatal(err)
	}
	returned, err := days.ReturnToPending(ctx, touchedTestOrg, []TouchedRunKeys{{RunID: "r", Day: windowDay, FullOrganization: true}})
	if err != nil || returned != 0 {
		t.Fatalf("touched-day lane returned %d days (err %v) for a window-only key, want 0", returned, err)
	}
	// It is the window lane's to return.
	returned, err = window.ReturnToPending(ctx, touchedTestOrg, []TouchedRunKeys{{RunID: "r", Day: windowDay, FullOrganization: true}})
	if err != nil || returned != 1 {
		t.Fatalf("window lane returned %d days (err %v), want 1", returned, err)
	}

	// R11: a key with a 'dispatched_testops_window' row and no window touch is not the window lane's to return.
	lonely := touchedTestDay(time.May, 2)
	if err := conn.Exec(ctx, `INSERT INTO daily_metrics_touched_days (org_id, day, repo_id, kind, at)
		VALUES (?, ?, ?, 'dispatched_testops_window', now64(3, 'UTC'))`, touchedTestOrg, lonely, touchedTestRepoB); err != nil {
		t.Fatal(err)
	}
	returned, err = window.ReturnToPending(ctx, touchedTestOrg, []TouchedRunKeys{{RunID: "r", Day: lonely, RepositoryIDs: []string{touchedTestRepoB.String()}}})
	if err != nil || returned != 0 {
		t.Fatalf("window lane returned %d days (err %v) for a key with no window touch, want 0", returned, err)
	}
	if touches := windowEvents(t, ctx, conn, touchedTestOrg, "touched_testops_window")[lonely.Format("2006-01-02")+"|"+touchedTestRepoB.String()]; touches != 0 {
		t.Fatalf("the window lane appended %d touches for a key it never touched", touches)
	}
}
