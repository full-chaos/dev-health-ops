//go:build integration

package daily

// CHAOS-14006 end-to-end: a cross-day re-run moves the counted suite of run f3Run from D to D+4. The post-sync window of
// the re-run's sync computes D+4 only; D keeps its stale live row. The touched-test-day record
// (ClickHouseTouchedDaysStore.RecordTouchedTests) names D; the touched-day run of D (Run{TargetDay: D}, Partition{RepoIDs:
// the pending repos of D}, the shape StartTouchedDayTx starts) retires the D row through the retracted marker. The cell
// calls the store and the executors directly: River, the Postgres run rows and the fan-out take are not run here.
// CELL14006_PLANT_NO_TEST_RECORD=1 skips RecordTouchedTests (all else equal) to show the record is load-bearing: the
// test then fails at STEP3 (no pending repository for D). Unset in CI.

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/syncdispatchruntime"
)

func (e *f3Env) computeRepos(org string, day time.Time, repos []string) {
	e.t.Helper()
	time.Sleep(1100 * time.Millisecond)
	run := Run{ID: fmt.Sprintf("run-%s-%s-%d", org, day.Format(time.DateOnly), time.Now().UnixNano()), OrganizationID: org, TargetDay: day}
	ids := make([]RepositoryID, 0, len(repos))
	for _, repo := range repos {
		ids = append(ids, RepositoryID(repo))
	}
	part := Partition{ID: "part-" + run.ID, RunID: run.ID, RepoIDs: ids}
	pipeline, err := NewTestopsPipelineExecutor(e.stack.conn)
	if err != nil {
		e.t.Fatal(err)
	}
	tests, err := NewTestopsTestExecutor(e.stack.conn)
	if err != nil {
		e.t.Fatal(err)
	}
	if _, err := pipeline.ComputeFamily(e.ctx, run, part); err != nil {
		e.t.Fatal(err)
	}
	if _, err := tests.ComputeFamily(e.ctx, run, part); err != nil {
		e.t.Fatal(err)
	}
	time.Sleep(5 * time.Millisecond)
}

func TestTestopsTouchedTestDayRetiresTheStaleRowEndToEnd(t *testing.T) {
	dayD := time.Date(2026, 7, 22, 0, 0, 0, 0, time.UTC)
	dayD4 := dayD.Add(4 * 24 * time.Hour)
	e := f3Start(t)
	e.exec("SYSTEM STOP MERGES testops_test_metrics_daily")
	at := func(day time.Time, h int) time.Time { return day.Add(time.Duration(h) * time.Hour) }
	a1 := attemptsArtifact{9301, "go-test-x", "2026-07-22T10:20:00Z", false,
		f3Junit(t, "pkg/x", "2026-07-22T10:02:00Z", f3Case("TestXX", "failed")+f3Case("TestXY", "passed"))}
	a2 := attemptsArtifact{9402, "go-test-x", "2026-07-26T00:30:00Z", false,
		f3Junit(t, "pkg/x", "2026-07-26T00:12:00Z", f3Case("TestXX", "passed")+f3Case("TestXY", "passed"))}
	attempt1 := attemptsRunState{attempt: 1, started: "2026-07-22T10:01:00Z", updated: "2026-07-22T10:30:00Z", artifacts: []attemptsArtifact{a1}}
	attempt2 := attemptsRunState{created: "2026-07-22T10:01:00Z", attempt: 2, started: "2026-07-26T00:10:00Z", updated: "2026-07-26T00:40:00Z",
		artifacts: []attemptsArtifact{a1, a2}}

	org := "c14006-" + uuid.NewString()[:8]
	store, err := syncdispatchruntime.NewClickHouseTouchedDaysStore(e.stack.conn)
	if err != nil {
		t.Fatal(err)
	}

	// Sync 1: attempt 1 on D; its daily run writes the live D row.
	e.stack.pass(t, org, f3Run, at(dayD, 10), attempt1)
	repo := e.stack.repoID(t, org)
	e.computeRepos(org, dayD, []string{repo.String()})
	if got := e.stored(org, repo, dayD); got.rows != 1 || got.retracted != 0 || got.totalCases != 2 {
		t.Fatalf("STEP1: D stores %+v, want one live row of 2 cases", got)
	}

	// Sync 2 (the re-run): its fan-out bound is taken before it writes; attempt 2 moves the counted suite to D+4.
	// The harness stamps last_synced with its normalizedAt = unit since + 65 min (production: the chunk checkpoint time,
	// the wall clock of the unit), so sync 1 rows carry D 11:05 and sync 2 rows D+4 01:05. RunStartedAt - margin of sync 2
	// on that clock: D+4 00:00.
	bound := at(dayD4, 0)
	e.stack.pass(t, org, f3Run, at(dayD4, 0), attempt2)
	synced, err := e.stack.conn.Query(e.ctx, `SELECT suite_id, run_attempt, toString(coalesce(started_at, finished_at)), toString(last_synced)
FROM test_suite_results WHERE org_id = ? ORDER BY last_synced`, org)
	if err != nil {
		t.Fatal(err)
	}
	for synced.Next() {
		var suite, start, last string
		var attempt uint32
		if err := synced.Scan(&suite, &attempt, &start, &last); err != nil {
			t.Fatal(err)
		}
		t.Logf("CELL SUITE suite=%s attempt=%d day_from=%s last_synced=%s bound=%s", suite[:12], attempt, start, last, bound.Format(time.RFC3339))
	}
	synced.Close()

	// The post-sync window of sync 2 (D+4 only; D is outside it) leaves D live: the stale state without the touched day.
	e.computeRepos(org, dayD4, []string{repo.String()})
	if got := e.stored(org, repo, dayD); got.rows != 1 || got.retracted != 0 {
		t.Fatalf("STEP2: D stores %+v after the window compute, want the stale live row still there", got)
	}
	e.wantRow("STEP2", org, dayD, map[string]float64{"total_cases": 2})
	t.Logf("CELL STEP2 PASS: window compute of D+4 leaves D live (stale)")

	// The touched-test-day record of sync 2's fan-out.
	// The sync write itself (CHAOS-14016 ci_pipeline_runs finish-day move) already touched D. Record it, then mark every
	// pending key dispatched (the fan-out's own MarkDispatched) so only RecordTouchedTests can make D pending again.
	before, err := store.PendingDays(e.ctx, org, 50)
	if err != nil {
		t.Fatal(err)
	}
	var beforeDays []string
	for _, day := range before.Days {
		beforeDays = append(beforeDays, day.Format(time.DateOnly))
	}
	t.Logf("CELL STEP3a pending after the sync write, before the test record: %v", beforeDays)
	if err := store.MarkDispatched(e.ctx, org, before.TakenAt, before.Days, nil); err != nil {
		t.Fatal(err)
	}
	if cleared, err := store.PendingDays(e.ctx, org, 50); err != nil || len(cleared.Days) != 0 {
		t.Fatalf("STEP3b: pending after MarkDispatched %+v (err %v), want none", cleared, err)
	}
	t.Logf("CELL STEP3b pending after MarkDispatched: none")
	var recorded uint64
	if os.Getenv("CELL14006_PLANT_NO_TEST_RECORD") == "1" {
		t.Logf("CELL PLANT: RecordTouchedTests skipped")
	} else if recorded, err = store.RecordTouchedTests(e.ctx, org, bound); err != nil {
		t.Fatal(err)
	}
	pending, err := store.PendingDays(e.ctx, org, 50)
	if err != nil {
		t.Fatal(err)
	}
	var days []string
	for _, day := range pending.Days {
		days = append(days, day.Format(time.DateOnly))
	}
	repos, err := store.PendingRepositories(e.ctx, org, pending.Days, 1000)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("CELL STEP3 recorded=%d pending_days=%v repos=%v", recorded, days, repos)
	if got := repos[dayD.Format(time.DateOnly)]; len(got) != 1 || got[0] != repo.String() {
		t.Fatalf("STEP3: pending repos of D = %v, want [%s]", got, repo)
	}

	// The touched-day run of D: TargetDay D, the pending repositories of D.
	e.computeRepos(org, dayD, repos[dayD.Format(time.DateOnly)])
	got := e.stored(org, repo, dayD)
	if got.rows != 2 || got.retracted != 1 || got.totalCases != 0 || !got.newest.After(got.previous) {
		t.Errorf("STEP4: D stores %+v, want 2 rows, the newest the retracted marker, strictly newer", got)
	}
	e.wantNoRow("STEP4", org, dayD)
	e.wantRow("STEP4", org, dayD4, map[string]float64{"total_cases": 2, "passed_count": 2, "pass_rate": 1})
	e.noBenchmarkPointOn("STEP4", org, dayD, dayD4)
	if !t.Failed() {
		t.Logf("CELL STEP4 PASS: the touched-day run of D retires the stale D row")
	}
}
