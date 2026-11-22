//go:build integration

package report

import (
	"fmt"
	"testing"
)

// A re-run (retry_count above 0) gives no queue sample in any reader of the queue time: the cicd and TestOps daily writers
// (the stored value and the stored samples) and both arms of each pooled source (the stored samples, and the raw runs of a
// repository-day whose newest row holds no samples). A day whose only run is a re-run has no queue time: NULL in the
// writer and no point in a chart, never 0. Its duration point stays, so the day is read. The first-attempt run (retry_count
// 0, the shape of every GitLab pipeline row) keeps its sample.

const (
	rerunMixedDay = "2026-01-04"
	rerunOnlyDay  = "2026-01-05"
)

func rerunQueueRuns() []cicdRun {
	return []cicdRun{
		// Created 08:59, latest attempt started 11:00: the 121 min (7260 s) between them is no wait of one attempt.
		{id: "rq-rerun", status: "success", queued: "2026-01-04 08:59:00.000", started: "2026-01-04 11:00:00.000",
			finished: "2026-01-04 11:30:00.000", retry: 1, queueS: 7260.0},
		{id: "rq-first", status: "success", queued: "2026-01-04 11:59:00.000", started: "2026-01-04 12:00:00.000",
			finished: "2026-01-04 12:20:00.000", queueS: 60.0},
		{id: "rq-only", status: "success", queued: "2026-01-05 08:00:00.000", started: "2026-01-05 09:00:00.000",
			finished: "2026-01-05 09:10:00.000", retry: 2, queueS: 3600.0},
	}
}

// requirePooledQueueSamples checks the newest row of the day: samples stored, one element per counted run, and the number
// of elements that hold a queue sample.
func (e *pooledEnv) requirePooledQueueSamples(table, column, org, repo, day string, runs, samples uint64) {
	e.t.Helper()
	var marker uint8
	var length, nonNull uint64
	if err := e.conn.QueryRow(e.ctx, fmt.Sprintf(`SELECT toUInt8(pooled_samples_stored), toUInt64(length(%s)), toUInt64(arrayCount(x -> isNotNull(x), %s))
FROM %s WHERE org_id = ? AND repo_id = ? AND day = ? ORDER BY computed_at DESC LIMIT 1`, column, column, table), org, repo, day).Scan(&marker, &length, &nonNull); err != nil {
		e.t.Fatal(err)
	}
	if marker != 1 || length != runs || nonNull != samples {
		e.t.Errorf("%s.%s on %s: stored %d, %d elements, %d samples; want 1, %d, %d", table, column, day, marker, length, nonNull, runs, samples)
	}
}

// rowWithoutSamples writes a copy of the newest row of the day one second newer with pooled_samples_stored 0 (the shape of
// a row written before migration 161): the pooled source then reads the day's raw runs.
func (e *pooledEnv) rowWithoutSamples(table, org, repo, day string) {
	e.t.Helper()
	e.exec(fmt.Sprintf(`INSERT INTO %s SELECT * REPLACE (computed_at + 1 AS computed_at, toUInt8(0) AS pooled_samples_stored)
FROM %s WHERE org_id = ? AND repo_id = ? AND day = ? ORDER BY computed_at DESC LIMIT 1`, table, table), org, repo, day)
	if retracted, marker := e.newestRetracted(table, org, repo, day); retracted != 0 || marker != 0 {
		e.t.Fatalf("the newest row of %s %s holds retracted %d, pooled_samples_stored %d; want a live row without samples (0, 0)", table, day, retracted, marker)
	}
}

func requireNoStoredValue(t *testing.T, what string, stored map[string]float64, day string) {
	t.Helper()
	if value, ok := stored[day]; ok {
		t.Errorf("%s on %s = %v, want NULL (a day of re-runs only, never 0)", what, day, value)
	}
}

func TestRerunGivesNoQueueSampleInTheCicdReaders(t *testing.T) {
	e := startPooledEnv(t)
	org := e.newOrg()
	const repo = "13902000-0000-4000-8000-0000000000a1"
	const table = "cicd_metrics_daily"
	e.insertCICDRuns(org, repo, rerunQueueRuns()...)
	e.computeCICD(org, []string{repo}, rerunMixedDay, rerunOnlyDay)

	queue := e.storedDelivery(org, repo, table, "avg_queue_minutes")
	mustEqual(t, "writer avg_queue_minutes", queue, rerunMixedDay, 1)
	requireNoStoredValue(t, "writer avg_queue_minutes", queue, rerunOnlyDay)
	mustEqual(t, "writer avg_duration_minutes", e.storedDelivery(org, repo, table, "avg_duration_minutes"), rerunOnlyDay, 10)
	e.requirePooledQueueSamples(table, "pooled_queue_minutes", org, repo, rerunMixedDay, 2, 1)
	e.requirePooledQueueSamples(table, "pooled_queue_minutes", org, repo, rerunOnlyDay, 1, 0)

	charts := func(arm string) {
		t.Helper()
		day := func(metric, d string) map[string]float64 {
			return pointsOf(e.cicdChart(org, metric, cicdSpec{chartType: "scorecard", start: d, end: d}))
		}
		mustEqual(t, arm+" avg_queue_minutes", day("avg_queue_minutes", rerunMixedDay), "total", 1)
		mustHaveNoPoint(t, arm+" avg_queue_minutes of the re-run day", day("avg_queue_minutes", rerunOnlyDay))
		mustEqual(t, arm+" avg_duration_minutes of the re-run day", day("avg_duration_minutes", rerunOnlyDay), "total", 10)
		mustEqual(t, arm+" avg_queue_minutes window", pointsOf(e.cicdChart(org, "avg_queue_minutes", cicdSpec{start: rerunMixedDay, end: rerunOnlyDay})), "total", 1)
	}
	charts("stored")

	writerTick()
	e.rowWithoutSamples(table, org, repo, rerunMixedDay)
	e.rowWithoutSamples(table, org, repo, rerunOnlyDay)
	charts("raw")
}

func TestRerunGivesNoQueueSampleInTheTestopsReaders(t *testing.T) {
	e := startPooledEnv(t)
	org := e.newOrg()
	const repo = "13902000-0000-4000-8000-0000000000b1"
	const table = "testops_pipeline_metrics_daily"
	e.insertRepo(org, repo)
	e.insertCICDRuns(org, repo, rerunQueueRuns()...)
	e.computeDaily(org, []string{repo}, rerunMixedDay, rerunOnlyDay)

	for _, metric := range []string{"avg_queue_seconds", "p95_queue_seconds"} {
		stored := e.storedDelivery(org, repo, table, metric)
		mustEqual(t, "writer "+metric, stored, rerunMixedDay, 60)
		requireNoStoredValue(t, "writer "+metric, stored, rerunOnlyDay)
	}
	mustEqual(t, "writer p95_duration_seconds", e.storedDelivery(org, repo, table, "p95_duration_seconds"), rerunOnlyDay, 600)
	e.requirePooledQueueSamples(table, "pooled_queues", org, repo, rerunMixedDay, 2, 1)
	e.requirePooledQueueSamples(table, "pooled_queues", org, repo, rerunOnlyDay, 1, 0)

	charts := func(arm string) {
		t.Helper()
		day := func(metric, d string) map[string]float64 {
			return pointsOf(e.chart(org, metric, "", "scorecard", d, d))
		}
		for _, metric := range []string{"avg_queue_seconds", "p95_queue_seconds"} {
			mustEqual(t, arm+" "+metric, day(metric, rerunMixedDay), "total", 60)
			mustHaveNoPoint(t, arm+" "+metric+" of the re-run day", day(metric, rerunOnlyDay))
			mustEqual(t, arm+" "+metric+" window", e.value(org, metric, "", rerunMixedDay, rerunOnlyDay), "total", 60)
		}
		mustEqual(t, arm+" p95_duration_seconds of the re-run day", day("p95_duration_seconds", rerunOnlyDay), "total", 600)
	}
	charts("stored")

	writerTick()
	e.rowWithoutSamples(table, org, repo, rerunMixedDay)
	e.rowWithoutSamples(table, org, repo, rerunOnlyDay)
	charts("raw")
}
