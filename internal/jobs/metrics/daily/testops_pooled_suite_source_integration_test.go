//go:build integration

package daily

// CHAOS-14259: the raw arm of testopspooled.SuiteRunsSource reads only the runs of the repository-days without stored samples.
// The rule groups by run, so the source must serve exactly the rows it served when every select read the organization: the
// cell builds runs through the production route and sink (a retired slot over two attempts with a flaky kept slot, a slot
// retired at the same attempt, a revived slot, a pre-slot copy of a retired slot, a plain run, runs on two days), stores both
// days without samples, and compares every row of the source with the text before the change (ops ef4d8699, verbatim below).

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
	org := "org-chaos14259-pooled-" + uuid.NewString()[:8]
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

	rows := func(label, source string) []string {
		t.Helper()
		result, err := stack.conn.Query(ctx, "SELECT toString(tuple(org_id, toString(repo_id), service_id, day, duration_sample)) FROM "+source,
			clickhouse.Named("org_id", org), clickhouse.Named("start_date", dayBefore.Format(time.DateOnly)),
			clickhouse.Named("end_date", dayD.AddDate(0, 0, 1).Format(time.DateOnly)))
		if err != nil {
			t.Fatalf("%s: %v", label, err)
		}
		defer result.Close()
		var out []string
		for result.Next() {
			var row string
			if err := result.Scan(&row); err != nil {
				t.Fatal(err)
			}
			out = append(out, row)
		}
		if err := result.Err(); err != nil {
			t.Fatal(err)
		}
		slices.Sort(out)
		return out
	}

	for _, phase := range []string{"stored days", "days without samples"} {
		if phase == "days without samples" {
			for _, day := range []time.Time{dayBefore, dayD} {
				if err := stack.conn.Exec(ctx, `INSERT INTO testops_test_metrics_daily (repo_id, day, org_id, computed_at) VALUES (?, ?, ?, now())`,
					repo, day, org); err != nil {
					t.Fatal(err)
				}
			}
		}
		before, after := rows(phase+" before", suiteRunsSourceBefore), rows(phase+" after", testopspooled.SuiteRunsSource)
		t.Logf("CELL14259 %s: %d rows before, %d after", phase, len(before), len(after))
		if phase == "days without samples" && len(before) == 0 {
			t.Fatalf("control: the organization-wide read serves no raw suite, so the cell compares nothing")
		}
		if !reflect.DeepEqual(after, before) {
			t.Errorf("%s: the source serves\n%s\nwant the organization-wide read's\n%s", phase, strings.Join(after, "\n"), strings.Join(before, "\n"))
		}
	}
}
