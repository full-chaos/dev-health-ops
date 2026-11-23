//go:build integration

package daily

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"
)

// CHAOS-14259 round r1-6906 findings 1 (rollback) and 2 (legacy): cells only, no fix. Each asserts the value the run synced
// once serves (CONTROL) and logs what the reader serves. A red cell here is the measured size of an open finding.

// retiredBaseCaseMeasures is the base binary's case read over the stored rows (the base kept-suite rule and the base own-suite
// sets, no retired-slot term): today's flake and distinct names, today's failed names, and those of them that failed in history.
func retiredBaseCaseMeasures(t *testing.T, stack attemptsStack, org string, day time.Time) (flake, distinct, failedToday, recurrent uint64) {
	t.Helper()
	repoID := stack.repoID(t, org)
	keptToday := testopsSuiteResultsSource + "\n  AND " + retiredBaseRule(t, "window")
	args := append(testopsCaseArgs(org, repoID, "", day.AddDate(0, 0, -29), day, day.Add(24*time.Hour)),
		clickhouse.Named("failure_statuses", testopsFailureStatuses))
	if err := stack.conn.QueryRow(stack.ctx, `
SELECT countIf(has(statuses, 'passed') AND has(statuses, 'failed')), count(), countIf(has(statuses, 'failed')),
       countIf(has(statuses, 'failed') AND failed_before)
FROM (
  SELECT case_name, groupUniqArrayIf(lower(trim(status)), in_today) AS statuses, max(in_today) AS seen_today, max(in_history) AS failed_before
  FROM (
    SELECT case_name, status,
      run_id IN (SELECT run_id`+keptToday+`) AS run_in_today,
      run_in_today AND (run_id, suite_id) IN (
        SELECT run_id, suite_id FROM test_suite_results FINAL
        WHERE org_id = {org_id:String} AND repo_id IN {repo_ids:Array(UUID)} AND coalesce(started_at, finished_at) < {end:DateTime64(3, 'UTC')}
      ) AS in_today,
      NOT run_in_today AND lower(trim(status)) IN {failure_statuses:Array(String)} AND (run_id, suite_id) IN (
        SELECT run_id, suite_id FROM test_suite_results FINAL
        WHERE org_id = {org_id:String} AND repo_id IN {repo_ids:Array(UUID)}
          AND coalesce(started_at, finished_at) >= {history_start:DateTime64(3, 'UTC')} AND coalesce(started_at, finished_at) < {start:DateTime64(3, 'UTC')}
      ) AS in_history
    FROM test_case_results FINAL
    WHERE org_id = {org_id:String} AND repo_id IN {repo_ids:Array(UUID)} AND case_name != ''
  )
  WHERE in_today OR in_history
  GROUP BY case_name
)
WHERE seen_today`, args...).Scan(&flake, &distinct, &failedToday, &recurrent); err != nil {
		t.Fatal(err)
	}
	return flake, distinct, failedToday, recurrent
}

func TestTestopsRetiredSlotOpenFindingsRollbackAndLegacy(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	env := startRetiredStack(t, ctx)
	stack := env.stack
	newOrg := func(name string) string { return "org-chaos14259-" + name + "-" + uuid.NewString()[:8] }

	// Finding 1, K4 under the base reader: CONTROL is 1 flake (c) of 3 names.
	t.Run("F1 rollback K4 flake", func(t *testing.T) {
		org := newOrg("f1-k4")
		retiredTwoAttemptRun(t, stack, org, "43600014261", retiredDay(), true)
		flake, distinct, _, _ := retiredBaseCaseMeasures(t, stack, org, retiredDay())
		t.Logf("base reader: flake %d of %d names (CONTROL 1 of 3)", flake, distinct)
		if flake != 1 || distinct != 3 {
			t.Errorf("base reader serves flake %d of %d names, want CONTROL 1 of 3", flake, distinct)
		}
	})

	// Finding 1, K5 under the base reader: CONTROL is case a failed today and not recurrent (0 of 1).
	t.Run("F1 rollback K5 recurrence", func(t *testing.T) {
		org := newOrg("f1-k5")
		retiredTwoAttemptRun(t, stack, org, "43600014262", retiredDay().AddDate(0, 0, -1), false)
		stack.pass(t, org, "43600014263", retiredDay().Add(10*time.Hour), retiredState(false, []attemptsArtifact{
			{21, "today", "2026-07-22T10:20:00Z", false, retiredArchive(t, "2026-07-22T10:02:00", matrixTotalsSuite{"t", "1.0", []matrixTotalsCase{{"a", "failed", "0.5"}}})},
		}))
		_, _, failedToday, recurrent := retiredBaseCaseMeasures(t, stack, org, retiredDay())
		t.Logf("base reader: recurrent %d of %d failed names (CONTROL 0 of 1)", recurrent, failedToday)
		if failedToday != 1 || recurrent != 0 {
			t.Errorf("base reader serves recurrent %d of %d failed names, want CONTROL 0 of 1", recurrent, failedToday)
		}
	})

	// Finding 1, a drifted suite key in the same attempt: the stale live row (a failure) and the retraction row hold two keys of
	// one slot. CONTROL is the kept slot alone: 1 case, 1 passed, 0 failed, 1 timed suite.
	t.Run("F1 rollback drifted key", func(t *testing.T) {
		org := newOrg("f1-drift")
		repoID := uuid.New()
		started := retiredDay().Add(10 * time.Hour)
		insert := func(suite, slot string, total, passed, failed uint32, duration *float64, synced time.Time, retracted uint8) {
			t.Helper()
			if err := stack.conn.Exec(ctx, `INSERT INTO test_suite_results
(org_id, repo_id, run_id, suite_id, suite_name, total_count, passed_count, failed_count, skipped_count, error_count, quarantined_count,
 duration_seconds, started_at, last_synced, report_slot, run_attempt, retracted)
VALUES (?, ?, 'drift-run', ?, 'drift', ?, ?, ?, 0, 0, 0, ?, ?, ?, ?, 1, ?)`, org, repoID, suite, total, passed, failed, duration, started, synced, slot, retracted); err != nil {
				t.Fatal(err)
			}
		}
		three := 3.0
		insert("kept-1", "kept", 1, 1, 0, &three, started.Add(time.Hour), 0)
		insert("drift-old", "total-b", 1, 0, 1, &three, started.Add(time.Hour), 0)
		insert("drift-new", "total-b", 0, 0, 0, nil, started.Add(2*time.Hour), 1)
		read := readRetiredBase(t, stack, org)
		t.Logf("base reader: %+v (CONTROL total 1, passed 1, failed 0, timed 1)", read)
		if read.total != 1 || read.passed != 1 || read.failed != 0 || read.timed != 1 {
			t.Errorf("base reader serves total %d, passed %d, failed %d, timed %d; want CONTROL 1, 1, 0, 1", read.total, read.passed, read.failed, read.timed)
		}
	})

	// Finding 2(a), the new readers: a pre-slot copy (report_slot = '') of total-b's attempt-1 suite that failed case a, under
	// the id the retraction rows list. CONTROL is K4's day: 1 flake (c) and 1 failed name (c).
	t.Run("F2 legacy copy of a failed case", func(t *testing.T) {
		org := newOrg("f2-copy")
		retiredTwoAttemptRun(t, stack, org, "43600014261", retiredDay(), true)
		repoID := stack.repoID(t, org)
		listed := retiredAttemptLegacyID(t, stack, org, "43600014261")
		started := retiredDay().Add(10*time.Hour + time.Minute)
		if err := stack.conn.Exec(ctx, `INSERT INTO test_suite_results
(org_id, repo_id, run_id, suite_id, suite_name, total_count, passed_count, failed_count, skipped_count, error_count, quarantined_count,
 duration_seconds, started_at, last_synced)
VALUES (?, ?, '43600014261', ?, 'left', 1, 0, 1, 0, 0, 0, 3.0, ?, ?)`, org, repoID, listed, started, retiredDay().Add(9*time.Hour)); err != nil {
			t.Fatal(err)
		}
		if err := stack.conn.Exec(ctx, `INSERT INTO test_case_results
(org_id, repo_id, run_id, suite_id, case_id, case_name, status, retry_attempt, last_synced)
VALUES (?, ?, '43600014261', ?, 'legacy-a', 'a', 'failed', 0, ?)`, org, repoID, listed, retiredDay().Add(9*time.Hour)); err != nil {
			t.Fatal(err)
		}
		got := retiredMetricOf(t, stack, org, retiredDay())
		want := retiredMetric{total: 3, passed: 3, suites: 3, flake: 1, distinct: 3, failedNames: 1}
		t.Logf("new readers: %+v (CONTROL %+v)", got, want)
		if got != want {
			t.Errorf("new readers serve %+v, want CONTROL %+v", got, want)
		}
	})

	// Finding 2(b), the new readers: total-b's attempt-1 artifact is no longer listed when attempt 2 drops total-b, so the
	// retraction rows do not list its pre-slot id; a pre-slot copy under that id. CONTROL is leg B's two parts: 2 cases, 2 suites.
	t.Run("F2 legacy id of an older attempt only", func(t *testing.T) {
		org := newOrg("f2-older")
		runID := "43600014264"
		date := retiredDay().Format(time.DateOnly)
		stamp := func(clock string) string { return date + "T" + clock + "Z" }
		ts1, ts2 := date+"T10:02:00", date+"T11:12:00"
		attempt1Total := []matrixTotalsSuite{{"left", "3.0", []matrixTotalsCase{{"a", "failed", "0.25"}}}, {"right", "3.0", []matrixTotalsCase{{"b", "", "0.25"}}}}
		attempt2Leg := []matrixTotalsSuite{{"left", "3.0", []matrixTotalsCase{{"a", "", "0.25"}}}, {"right", "3.0", []matrixTotalsCase{{"b", "", "0.25"}}}}
		attempt1 := []attemptsArtifact{{6, "total-b", stamp("10:20:00"), false, retiredArchive(t, ts1, attempt1Total...)}}
		attempt2 := []attemptsArtifact{
			{13, "leg-b-left", stamp("11:30:00"), false, retiredArchive(t, ts2, attempt2Leg[0])},
			{14, "leg-b-right", stamp("11:30:00"), false, retiredArchive(t, ts2, attempt2Leg[1])},
			{16, "total-b", stamp("11:30:00"), false, retiredArchive(t, ts2, attempt2Leg...)},
		}
		stack.pass(t, org, runID, retiredDay().Add(10*time.Hour), attemptsRunState{attempt: 1, started: stamp("10:01:00"), updated: stamp("10:30:00"), artifacts: attempt1})
		listed := retiredAttemptLegacyID(t, stack, org, runID)
		stack.pass(t, org, runID, retiredDay().Add(11*time.Hour), attemptsRunState{created: stamp("10:01:00"), attempt: 2, started: stamp("11:10:00"),
			updated: stamp("11:40:00"), artifacts: attempt2})
		repoID := stack.repoID(t, org)
		started := retiredDay().Add(10*time.Hour + time.Minute)
		if err := stack.conn.Exec(ctx, `INSERT INTO test_suite_results
(org_id, repo_id, run_id, suite_id, suite_name, total_count, passed_count, failed_count, skipped_count, error_count, quarantined_count,
 duration_seconds, started_at, last_synced)
VALUES (?, ?, ?, ?, 'left', 1, 0, 1, 0, 0, 0, 3.0, ?, ?)`, org, repoID, runID, listed, started, retiredDay().Add(9*time.Hour)); err != nil {
			t.Fatal(err)
		}
		got := retiredMetricOf(t, stack, org, retiredDay())
		t.Logf("new readers: %+v (CONTROL total 2, suites 2, failed 0)", got)
		if got.total != 2 || got.suites != 2 || got.failed != 0 {
			t.Errorf("new readers serve total %d, suites %d, failed %d; want CONTROL 2, 2, 0", got.total, got.suites, got.failed)
		}
		kept := retiredKeptSuites(t, stack, org)
		if kept[listed] {
			t.Errorf("the pooled rule keeps the pre-slot copy %s of the retired slot", listed)
		}
	})
}

// retiredAttemptLegacyID is the pre-slot id total-b's attempt-1 left suite lists (artifact 6, no superseded copy).
func retiredAttemptLegacyID(t *testing.T, stack attemptsStack, org, runID string) string {
	t.Helper()
	var ids []string
	if err := stack.conn.QueryRow(stack.ctx, `SELECT legacy_suite_ids FROM test_suite_results FINAL
WHERE org_id = ? AND run_id = ? AND report_slot = 'total-b' AND run_attempt = 1 AND suite_name = 'left' AND retracted = 0`, org, runID).Scan(&ids); err != nil {
		t.Fatal(err)
	}
	if len(ids) != 1 || slices.Contains(ids, "") {
		t.Fatalf("total-b attempt 1 lists legacy ids %v, want one", ids)
	}
	return ids[0]
}
