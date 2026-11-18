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

type touchedTestSuite struct {
	repo     uuid.UUID
	run      string
	suite    string
	slot     string
	attempt  uint32
	started  *time.Time
	finished *time.Time
	synced   time.Time
}

func insertTouchedTestSuites(t *testing.T, ctx context.Context, conn driver.Conn, org string, suites ...touchedTestSuite) {
	t.Helper()
	for _, suite := range suites {
		if err := conn.Exec(ctx, `INSERT INTO test_suite_results (repo_id, run_id, suite_id, suite_name, total_count,
			passed_count, failed_count, skipped_count, started_at, finished_at, org_id, last_synced, report_slot, run_attempt)
			VALUES (?, ?, ?, 'pkg/a', 3, 2, 1, 0, ?, ?, ?, ?, ?, ?)`,
			suite.repo, suite.run, suite.suite, suite.started, suite.finished, org, suite.synced, suite.slot, suite.attempt); err != nil {
			t.Fatal(err)
		}
	}
}

// touchedEventsAt counts the 'touched' rows of (day, repository) that one record call wrote (one `at`).
func touchedEventsAt(t *testing.T, ctx context.Context, conn driver.Conn, org string) map[string]uint64 {
	t.Helper()
	rows, err := conn.Query(ctx, `SELECT toString(day), toString(repo_id), count() FROM daily_metrics_touched_days
		WHERE org_id = ? AND kind = 'touched' GROUP BY day, repo_id`, org)
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

func at(day time.Time, hours int) *time.Time {
	value := day.Add(time.Duration(hours) * time.Hour)
	return &value
}

// CHAOS-14006: attempt 1 of run-1 (slot test-results) carried D = 06-01 and was written before the sync run; attempt 2,
// written in the run, moved the counted suite OFF D to 06-05. The record names D (carried by the attempt-1 row
// suite-attempt-1, run_attempt 1) and 06-05. Also: a legacy row (slot ”) of the same run on 06-02 names 06-02; a run
// written only before the run and a row of another organization name nothing.
func TestTouchedTestsRecordTheDayAnOlderAttemptCarriedWhenANewerAttemptMovesOff(t *testing.T) {
	ctx, conn := newReadbackIntegrationConn(t)
	store, err := NewClickHouseTouchedDaysStore(conn)
	if err != nil {
		t.Fatal(err)
	}
	beforeRun := touchedTestSince.Add(-24 * time.Hour)
	inRun := touchedTestSince.Add(time.Hour)
	dayD := touchedTestDay(time.June, 1)
	insertTouchedTestSuites(t, ctx, conn, touchedTestOrg,
		// The row that carries D: the older attempt, written before the sync run.
		touchedTestSuite{repo: touchedTestRepoA, run: "run-1", suite: "suite-attempt-1", slot: "test-results", attempt: 1,
			started: at(dayD, 1), finished: at(dayD, 2), synced: beforeRun},
		touchedTestSuite{repo: touchedTestRepoA, run: "run-1", suite: "suite-attempt-2", slot: "test-results", attempt: 2,
			started: at(touchedTestDay(time.June, 5), 1), finished: at(touchedTestDay(time.June, 5), 2), synced: inRun},
		// A legacy row of the same run (no slot, no attempt), with no start: its day is the finish day.
		touchedTestSuite{repo: touchedTestRepoA, run: "run-1", suite: "suite-legacy", finished: at(touchedTestDay(time.June, 2), 3), synced: beforeRun},
		// Written only before the run: no key.
		touchedTestSuite{repo: touchedTestRepoB, run: "run-old", suite: "s", slot: "test-results", attempt: 1,
			started: at(touchedTestDay(time.April, 9), 1), synced: beforeRun},
	)
	insertTouchedTestSuites(t, ctx, conn, touchedTestOtherOrg,
		touchedTestSuite{repo: touchedTestRepoA, run: "run-1", suite: "other", slot: "test-results", attempt: 2,
			started: at(touchedTestDay(time.April, 10), 1), synced: inRun},
	)

	recorded, err := store.RecordTouchedTests(ctx, touchedTestOrg, touchedTestSince)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		touchedTestKey(time.June, 1, touchedTestRepoA), // carried by suite-attempt-1 (run_attempt 1), moved off by attempt 2
		touchedTestKey(time.June, 2, touchedTestRepoA), // carried by suite-legacy
		touchedTestKey(time.June, 5, touchedTestRepoA), // carried by suite-attempt-2
	}
	sort.Strings(want)
	got := pendingTouchedTestKeys(t, ctx, conn, touchedTestOrg)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("pending keys (recorded %d)\n got %v\nwant %v", recorded, got, want)
	}
	if recorded != 3 {
		t.Fatalf("recorded %d keys, want 3", recorded)
	}
	if got := pendingTouchedTestKeys(t, ctx, conn, touchedTestOtherOrg); len(got) != 0 {
		t.Fatalf("the other organization was not recorded, so it has no key; got %v", got)
	}
}

// Control: a re-run whose newer attempt keeps every suite on D. D is recorded once: one key, one 'touched'
// row for (D, repo), and recording the same sync run again adds no second pending key.
func TestTouchedTestsRecordADayOnceWhenTheReRunStaysOnIt(t *testing.T) {
	ctx, conn := newReadbackIntegrationConn(t)
	store, err := NewClickHouseTouchedDaysStore(conn)
	if err != nil {
		t.Fatal(err)
	}
	beforeRun := touchedTestSince.Add(-24 * time.Hour)
	inRun := touchedTestSince.Add(time.Hour)
	dayD := touchedTestDay(time.June, 1)
	insertTouchedTestSuites(t, ctx, conn, touchedTestOrg,
		touchedTestSuite{repo: touchedTestRepoA, run: "run-1", suite: "a1-x", slot: "test-results", attempt: 1, started: at(dayD, 1), synced: beforeRun},
		touchedTestSuite{repo: touchedTestRepoA, run: "run-1", suite: "a1-y", slot: "test-results", attempt: 1, started: at(dayD, 2), synced: beforeRun},
		touchedTestSuite{repo: touchedTestRepoA, run: "run-1", suite: "a2-x", slot: "test-results", attempt: 2, started: at(dayD, 5), synced: inRun},
		touchedTestSuite{repo: touchedTestRepoA, run: "run-1", suite: "a2-y", slot: "test-results", attempt: 2, started: at(dayD, 6), synced: inRun},
	)

	recorded, err := store.RecordTouchedTests(ctx, touchedTestOrg, touchedTestSince)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{touchedTestKey(time.June, 1, touchedTestRepoA)}
	if got := pendingTouchedTestKeys(t, ctx, conn, touchedTestOrg); !reflect.DeepEqual(got, want) || recorded != 1 {
		t.Fatalf("pending keys %v (recorded %d), want %v once", got, recorded, want)
	}
	if events := touchedEventsAt(t, ctx, conn, touchedTestOrg); events[want[0]] != 1 || len(events) != 1 {
		t.Fatalf("touched rows %v, want one row for %s", events, want[0])
	}
	// A redelivery of the same fan-out records again: still one pending key for D, no second day.
	if _, err := store.RecordTouchedTests(ctx, touchedTestOrg, touchedTestSince); err != nil {
		t.Fatal(err)
	}
	if got := pendingTouchedTestKeys(t, ctx, conn, touchedTestOrg); !reflect.DeepEqual(got, want) {
		t.Fatalf("after a second record the pending keys are %v, want %v", got, want)
	}
}
