//go:build integration

package daily

// CHAOS-14591: the raw arm of testopspooled.SuiteRunsSource computes the newest-attempt rule with window functions over ONE
// read of its repositories. It must serve exactly the rows of the text before the change, whose every rule select read the
// organization (ops ef4d8699, verbatim below), and the same P95 and median (the AI report's expressions over the same source,
// internal/jobs/report/pooled.go). The cell builds runs through the production route and sink (a retired slot over two
// attempts with a flaky kept slot, a slot retired at the same attempt, a revived slot, a pre-slot copy of a retired slot, a
// plain run, runs on two days) with merges stopped (every older row version stays in its own part, plus an explicit older
// version of a kept key) and compares: every day stored, a mixed window (one day stored by the writer, one day without
// samples), every day without samples. A second organization holds the SAME repository and run ids with a day without
// samples: the first organization's rows and read are unchanged by it.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/testopspooled"
)

// suiteRunsSourceBefore is testopspooled.SuiteRunsSource at ops ef4d8699 (every inner select organization-wide), rendered by a
// named test at that commit.
const suiteRunsSourceBefore = `(
    SELECT org_id, repo_id, service_id, day, duration_sample
    FROM (
        SELECT
            org_id,
            repo_id,
            stored_service_id AS service_id,
            day,
            stored_duration AS duration_sample
        FROM (
            SELECT org_id, repo_id, day, pooled_samples_stored, pooled_service_ids, pooled_durations
            FROM testops_test_metrics_daily
            WHERE org_id = {org_id:String} AND day >= {start_date:Date} AND day < {end_date:Date}
            ORDER BY computed_at DESC, cityHash64(tuple(*)) DESC
            LIMIT 1 BY org_id, repo_id, day
        )
        ARRAY JOIN pooled_service_ids AS stored_service_id, pooled_durations AS stored_duration
        WHERE pooled_samples_stored = 1
        UNION ALL
        SELECT
            org_id,
            repo_id,
            service_id,
            toDate(coalesce(started_at, finished_at)) AS day,
            if(duration_seconds >= 0, duration_seconds, NULL) AS duration_sample
        FROM test_suite_results FINAL
        WHERE org_id = {org_id:String}
          AND toDate(coalesce(started_at, finished_at)) >= {start_date:Date} AND toDate(coalesce(started_at, finished_at)) < {end_date:Date}
          AND ((report_slot != '' AND (repo_id, run_id, report_slot, run_attempt) IN (SELECT repo_id, run_id, report_slot, max(run_attempt) FROM test_suite_results FINAL WHERE org_id = {org_id:String} AND report_slot != '' GROUP BY repo_id, run_id, report_slot) AND retracted = 0 AND (repo_id, run_id, report_slot) NOT IN (SELECT repo_id, run_id, report_slot FROM test_suite_results FINAL WHERE org_id = {org_id:String} AND report_slot != '' GROUP BY repo_id, run_id, report_slot HAVING argMax(retracted, (run_attempt, last_synced, retracted)) = 1)) OR (report_slot = '' AND (repo_id, run_id, suite_id) NOT IN (SELECT repo_id, run_id, arrayJoin(legacy_suite_ids) FROM test_suite_results FINAL WHERE org_id = {org_id:String} AND report_slot != '' AND (repo_id, run_id, report_slot, run_attempt) IN (SELECT repo_id, run_id, report_slot, max(run_attempt) FROM test_suite_results FINAL WHERE org_id = {org_id:String} AND report_slot != '' GROUP BY repo_id, run_id, report_slot)) AND (repo_id, run_id, suite_id) NOT IN (SELECT repo_id, run_id, arrayJoin(legacy_suite_ids) FROM test_suite_results FINAL WHERE org_id = {org_id:String} AND report_slot != '' AND (repo_id, run_id, report_slot) IN (SELECT repo_id, run_id, report_slot FROM test_suite_results FINAL WHERE org_id = {org_id:String} AND report_slot != '' GROUP BY repo_id, run_id, report_slot HAVING argMax(retracted, (run_attempt, last_synced, retracted)) = 1))))
          AND (repo_id, toDate(coalesce(started_at, finished_at))) IN (SELECT repo_id, day FROM (
            SELECT org_id, repo_id, day, pooled_samples_stored
            FROM testops_test_metrics_daily
            WHERE org_id = {org_id:String} AND day >= {start_date:Date} AND day < {end_date:Date}
            ORDER BY computed_at DESC, cityHash64(tuple(*)) DESC
            LIMIT 1 BY org_id, repo_id, day
        ) WHERE pooled_samples_stored != 1)
    )
    WHERE (repo_id, day) IN (
        SELECT repo_id, day FROM testops_test_metrics_daily
        WHERE org_id = {org_id:String} AND day >= {start_date:Date} AND day < {end_date:Date}
        GROUP BY repo_id, day
        HAVING (argMax(testops_test_metrics_daily.retracted, testops_test_metrics_daily.computed_at) = 0))
) AS testops_suite_runs`

const suiteRunsSourceBeforeSHA256 = "60c79d1481d4ec34c0e92b5d25fc8fd03e2804eed52088ea9c87e115ab0a07df"

func TestTestopsPooledSuiteSourceServesTheRowsOfTheOrganizationWideRead(t *testing.T) {
	if sum := sha256.Sum256([]byte(suiteRunsSourceBefore)); hex.EncodeToString(sum[:]) != suiteRunsSourceBeforeSHA256 {
		t.Fatalf("the reference source text changed: sha256 %x", sum)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	stack := startRetiredStack(t, ctx).stack
	if err := stack.conn.Exec(ctx, "SYSTEM STOP MERGES test_suite_results"); err != nil {
		t.Fatal(err)
	}
	org, other := "org-chaos14591-pooled-"+uuid.NewString()[:8], "org-chaos14591-other-"+uuid.NewString()[:8]
	both, withoutLegB, _ := retiredArtifacts(t)
	dayD, dayBefore := retiredDay(), retiredDay().AddDate(0, 0, -1)

	retiredTwoAttemptRun(t, stack, org, "43600014601", dayD, true)
	retiredTwoAttemptRun(t, stack, org, "43600014602", dayBefore, false)
	stack.pass(t, org, "43600014603", dayD.Add(10*time.Hour), retiredState(false, withoutLegB))
	stack.pass(t, org, "43600014603", dayD.Add(11*time.Hour), retiredState(true, both))
	stack.pass(t, org, "43600014604", dayD.Add(10*time.Hour), retiredState(false, withoutLegB))
	stack.pass(t, org, "43600014604", dayD.Add(11*time.Hour), retiredState(true, both))
	stack.pass(t, org, "43600014604", dayD.Add(12*time.Hour), attemptsRunState{attempt: 1, started: "2026-07-22T10:01:00Z",
		updated: "2026-07-22T12:40:00Z", artifacts: withoutLegB})
	retiredInsertPreSlotCopy(t, stack, org, "43600014601", retiredAttemptLegacyID(t, stack, org, "43600014601"), dayD.Add(10*time.Hour+time.Minute), "a")
	stack.pass(t, org, "43600014605", dayD.Add(10*time.Hour), retiredState(false, []attemptsArtifact{
		{21, "plain", "2026-07-22T10:20:00Z", false, retiredArchive(t, "2026-07-22T10:02:00", matrixTotalsSuite{"p", "2.0", []matrixTotalsCase{{"z", "", "0.5"}}})},
	}))
	repo := stack.repoID(t, org)
	// An OLDER version of a kept key with another duration, in a part of its own: FINAL drops it, a non-FINAL read would see it.
	if err := stack.conn.Exec(ctx, `INSERT INTO test_suite_results SELECT * REPLACE (toFloat64(999) AS duration_seconds, last_synced - INTERVAL 1 DAY AS last_synced)
FROM test_suite_results FINAL WHERE org_id = ? AND run_id = '43600014605' LIMIT 1`, org); err != nil {
		t.Fatal(err)
	}
	// The other organization: the same repository and run ids (a copy of every row).
	if err := stack.conn.Exec(ctx, `INSERT INTO test_suite_results SELECT * REPLACE (? AS org_id) FROM test_suite_results WHERE org_id = ?`, other, org); err != nil {
		t.Fatal(err)
	}

	type served struct {
		rows     []string
		p95, p50 string
		readRows uint64
	}
	serve := func(label, of, source string) served {
		t.Helper()
		args := []any{clickhouse.Named("org_id", of), clickhouse.Named("start_date", dayBefore.Format(time.DateOnly)),
			clickhouse.Named("end_date", dayD.AddDate(0, 0, 1).Format(time.DateOnly))}
		id := "c14591-" + uuid.NewString()
		result, err := stack.conn.Query(clickhouse.Context(ctx, clickhouse.WithQueryID(id)),
			"SELECT toString(tuple(org_id, toString(repo_id), service_id, day, duration_sample)) FROM "+source, args...)
		if err != nil {
			t.Fatalf("%s: %v", label, err)
		}
		var out served
		for result.Next() {
			var row string
			if err := result.Scan(&row); err != nil {
				t.Fatal(err)
			}
			out.rows = append(out.rows, row)
		}
		if err := result.Err(); err != nil {
			t.Fatal(err)
		}
		result.Close()
		slices.Sort(out.rows)
		if err := stack.conn.QueryRow(ctx, "SELECT toString("+testopspooled.P95+"), toString("+testopspooled.Median+") FROM "+source, args...).
			Scan(&out.p95, &out.p50); err != nil {
			t.Fatalf("%s expressions: %v", label, err)
		}
		if err := stack.conn.Exec(ctx, "SYSTEM FLUSH LOGS"); err != nil {
			t.Fatal(err)
		}
		if err := stack.conn.QueryRow(ctx, `SELECT read_rows FROM system.query_log WHERE query_id = ? AND type = 'QueryFinish'`, id).Scan(&out.readRows); err != nil {
			t.Fatalf("%s query_log: %v", label, err)
		}
		return out
	}
	compare := func(phase string) served {
		t.Helper()
		before, after := serve(phase+" before", org, suiteRunsSourceBefore), serve(phase+" after", org, testopspooled.SuiteRunsSource)
		t.Logf("CELL14591 %s: %d rows before, %d after; p95 %s / %s, median %s / %s; read_rows %d after", phase, len(before.rows), len(after.rows),
			before.p95, after.p95, before.p50, after.p50, after.readRows)
		if !reflect.DeepEqual(after.rows, before.rows) || after.p95 != before.p95 || after.p50 != before.p50 {
			t.Errorf("%s: the source serves\n%s\n(p95 %s, median %s)\nwant the organization-wide read's\n%s\n(p95 %s, median %s)", phase,
				strings.Join(after.rows, "\n"), after.p95, after.p50, strings.Join(before.rows, "\n"), before.p95, before.p50)
		}
		return after
	}
	noSamples := func(of string, day time.Time) {
		t.Helper()
		if err := stack.conn.Exec(ctx, `INSERT INTO testops_test_metrics_daily (repo_id, day, org_id, computed_at) VALUES (?, ?, ?, now() + INTERVAL 1 HOUR)`,
			repo, day, of); err != nil {
			t.Fatal(err)
		}
	}

	compare("every day stored")
	// Mixed: the writer stores the day before with its samples; day D has a newest row without samples.
	tests, err := NewTestopsTestExecutor(stack.conn)
	if err != nil {
		t.Fatal(err)
	}
	run := Run{ID: "run-" + org, OrganizationID: org, TargetDay: dayBefore}
	if _, err := tests.ComputeFamily(ctx, run, Partition{ID: "part-" + org, RunID: run.ID, RepoIDs: []RepositoryID{RepositoryID(repo.String())}}); err != nil {
		t.Fatal(err)
	}
	noSamples(org, dayD)
	mixed := compare("mixed window")
	if len(mixed.rows) == 0 {
		t.Fatalf("control: the mixed window serves no row, so the cell compares nothing")
	}
	noSamples(org, dayBefore)
	whole := compare("every day without samples")
	if len(whole.rows) == 0 {
		t.Fatalf("control: every day without samples serves no row")
	}
	// The other organization's day without samples changes neither the first organization's rows nor its read.
	noSamples(other, dayD)
	if again := compare("after the other organization's day"); !reflect.DeepEqual(again.rows, whole.rows) || again.readRows != whole.readRows {
		t.Errorf("the other organization's day changed the first's answer or read: %d rows / read %d, want %d rows / read %d",
			len(again.rows), again.readRows, len(whole.rows), whole.readRows)
	}
	if otherRows := serve("other organization", other, testopspooled.SuiteRunsSource); len(otherRows.rows) == 0 {
		t.Errorf("control: the other organization serves no raw suite on its day without samples")
	}
}
