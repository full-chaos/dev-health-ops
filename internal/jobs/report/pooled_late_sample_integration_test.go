//go:build integration

package report

import (
	"fmt"
	"testing"
	"time"
)

// A pooled chart and the stored daily row of one repository-day come from one sample set: a sample that syncs after the
// daily writer ran, a newer version of a sampled row, or an incident mapping that expires after the writer ran moves
// neither until the writer runs again, and then moves both.

type lateSampleKey struct{ metric, table string }

var lateSampleTestopsKeys = []lateSampleKey{
	{"p95_duration_seconds", "testops_pipeline_metrics_daily"},
	{"median_duration_seconds", "testops_pipeline_metrics_daily"},
	{"avg_queue_seconds", "testops_pipeline_metrics_daily"},
	{"p95_queue_seconds", "testops_pipeline_metrics_daily"},
	{"suite_duration_p95_seconds", "testops_test_metrics_daily"},
	{"suite_duration_p50_seconds", "testops_test_metrics_daily"},
}

var lateSampleCICDKeys = []lateSampleKey{
	{"avg_duration_minutes", "cicd_metrics_daily"},
	{"p90_duration_minutes", "cicd_metrics_daily"},
	{"avg_queue_minutes", "cicd_metrics_daily"},
}

// writerTick passes one second, so a re-run's computed_at (a DateTime of seconds) is newer than the first run's.
func writerTick() { time.Sleep(1100 * time.Millisecond) }

// sameSampleSet compares the stored value of the repository-day with the chart of a window that holds only that day.
func (e *pooledEnv) sameSampleSet(phase, org, repo, day string, keys []lateSampleKey) (moved map[string]bool, values map[string]string) {
	e.t.Helper()
	values = map[string]string{}
	moved = map[string]bool{}
	show := func(value float64, ok bool) string {
		if !ok {
			return "none"
		}
		return fmt.Sprint(value)
	}
	for _, key := range keys {
		stored, storedOK := e.storedDelivery(org, repo, key.table, key.metric)[day]
		chart, chartOK := e.deliveryChart(org, key.metric, "", "scorecard", day, day)["total"]
		equal := storedOK == chartOK && (!storedOK || near(stored, chart))
		e.t.Logf("LATE14025 %-7s %-24s stored=%s chart=%s equal=%v", phase, key.metric, show(stored, storedOK), show(chart, chartOK), equal)
		values[key.metric] = show(stored, storedOK) + "|" + show(chart, chartOK)
		if !equal {
			moved[key.metric] = true
		}
	}
	return moved, values
}

func (e *pooledEnv) requireSameSampleSet(phase, org, repo, day string, keys []lateSampleKey) map[string]string {
	e.t.Helper()
	differ, values := e.sameSampleSet(phase, org, repo, day, keys)
	for _, key := range keys {
		if differ[key.metric] {
			e.t.Errorf("%s %s: the chart and the stored row differ (stored|chart %s)", phase, key.metric, values[key.metric])
		}
	}
	return values
}

func (e *pooledEnv) requireMovedFrom(before, after map[string]string, keys []lateSampleKey) {
	e.t.Helper()
	for _, key := range keys {
		if before[key.metric] == after[key.metric] {
			e.t.Errorf("rerun %s: the writer re-run did not move the value (%s)", key.metric, after[key.metric])
		}
	}
}

func TestPooledLateSampleTestops(t *testing.T) {
	e := startPooledEnv(t)
	org := e.newOrg()
	const day = "2026-01-02"
	repo := e.seedViaRoute(org, "acme/late", 4500, []seedRun{
		{id: 9001, day: day, startMin: 0, durationS: 10, queueS: 10, suiteTimes: []string{"10"}},
		{id: 9002, day: day, startMin: 1, durationS: 30, queueS: 30, suiteTimes: []string{"30"}},
	})
	e.own(org, "team-late", repo)
	e.computeDaily(org, []string{repo}, day)
	before := e.requireSameSampleSet("before", org, repo, day, lateSampleTestopsKeys)

	if late := e.seedViaRoute(org, "acme/late", 4500, []seedRun{
		{id: 9003, day: day, startMin: 2, durationS: 500, queueS: 500, suiteTimes: []string{"500"}},
	}); late != repo {
		t.Fatalf("the late run derived repository %s, want %s", late, repo)
	}
	e.requireSameSampleSet("late", org, repo, day, lateSampleTestopsKeys)

	writerTick()
	e.computeDaily(org, []string{repo}, day)
	e.requireMovedFrom(before, e.requireSameSampleSet("rerun", org, repo, day, lateSampleTestopsKeys), lateSampleTestopsKeys)
}

func TestPooledLateSampleCICD(t *testing.T) {
	e := startPooledEnv(t)
	org := e.newOrg()
	const repo = "14025000-0000-4000-8000-0000000000c1"
	const day = "2026-01-02"
	e.insertCICDRuns(org, repo,
		cicdRun{id: "c-1", status: "success", queued: "2026-01-02 00:29:00.000", started: "2026-01-02 00:30:00.000", finished: "2026-01-02 00:40:00.000"},
		cicdRun{id: "c-2", status: "success", queued: "2026-01-02 00:57:00.000", started: "2026-01-02 01:00:00.000", finished: "2026-01-02 01:20:00.000"},
	)
	e.computeCICD(org, []string{repo}, day)
	before := e.requireSameSampleSet("before", org, repo, day, lateSampleCICDKeys)

	e.insertCICDRuns(org, repo,
		cicdRun{id: "c-3", status: "success", queued: "2026-01-02 02:50:00.000", started: "2026-01-02 03:00:00.000", finished: "2026-01-02 04:00:00.000"})
	e.requireSameSampleSet("late", org, repo, day, lateSampleCICDKeys)

	writerTick()
	e.computeCICD(org, []string{repo}, day)
	e.requireMovedFrom(before, e.requireSameSampleSet("rerun", org, repo, day, lateSampleCICDKeys), lateSampleCICDKeys)
}

// A newer version of a sampled run (a re-sync with new timestamps), not a new run: the raw row the chart reads is
// replaced, so no "rows synced before the writer ran" filter on the raw table can give back the old sample.
func TestPooledLateVersionCICD(t *testing.T) {
	e := startPooledEnv(t)
	org := e.newOrg()
	const repo = "14025000-0000-4000-8000-0000000000c2"
	const day = "2026-01-02"
	e.insertCICDRuns(org, repo,
		cicdRun{id: "v-1", status: "success", queued: "2026-01-02 00:29:00.000", started: "2026-01-02 00:30:00.000", finished: "2026-01-02 00:40:00.000"},
		cicdRun{id: "v-2", status: "success", queued: "2026-01-02 00:57:00.000", started: "2026-01-02 01:00:00.000", finished: "2026-01-02 01:20:00.000"},
	)
	e.computeCICD(org, []string{repo}, day)
	before := e.requireSameSampleSet("before", org, repo, day, lateSampleCICDKeys)

	e.insertCICDRuns(org, repo,
		cicdRun{id: "v-1", status: "success", synced: "2026-02-02 00:00:00.000", queued: "2026-01-02 00:00:00.000", started: "2026-01-02 00:30:00.000", finished: "2026-01-02 02:30:00.000"})
	e.requireSameSampleSet("late", org, repo, day, lateSampleCICDKeys)

	writerTick()
	e.computeCICD(org, []string{repo}, day)
	e.requireMovedFrom(before, e.requireSameSampleSet("rerun", org, repo, day, lateSampleCICDKeys), lateSampleCICDKeys)
}

func TestPooledLateSampleDelivery(t *testing.T) {
	e := startPooledEnv(t)
	org := e.newOrg()
	const repo = "14025000-0000-4000-8000-0000000000d1"
	day := deliveryDay1.Format(time.DateOnly)
	keys := make([]lateSampleKey, 0, len(deliveryPooledMetrics))
	for _, metric := range deliveryPooledMetrics {
		keys = append(keys, lateSampleKey{metric.name, metric.table})
	}
	e.insertRepo(org, repo)
	e.mapIncidentService(org, "svc-late", repo)
	at := func(hour int) time.Time { return deliveryDay1.Add(time.Duration(hour) * time.Hour) }
	e.insertDeployment(org, repo, "d-1", "success", at(1), 2)
	e.insertPullRequest(org, repo, 1, at(1), 10, 10)
	e.insertBug(org, repo, "b-1", "bug", at(1), at(3))
	e.insertIncident(org, "svc-late", "i-1", at(1), at(3), 0)
	e.computeDelivery(org, []string{repo}, day)
	before := e.requireSameSampleSet("before", org, repo, day, keys)

	e.insertDeployment(org, repo, "d-late", "success", at(5), 12)
	e.insertPullRequest(org, repo, 2, at(5), 100, 100)
	e.insertBug(org, repo, "b-late", "bug", at(5), at(17))
	e.insertIncident(org, "svc-late", "i-late", at(5), at(17), 0)
	e.requireSameSampleSet("late", org, repo, day, keys)

	writerTick()
	e.computeDelivery(org, []string{repo}, day)
	e.requireMovedFrom(before, e.requireSameSampleSet("rerun", org, repo, day, keys), keys)
}

// The incident mapping expires after the writer ran (a newer revision with valid_to before the chart's instant). The
// chart and the stored row read the writer's mappings, so neither moves at "late". At "rerun" the writer finds no
// incident for the repository-day and retires it with a marker row: the stored value and the chart are both absent
// (none|none), never the first run's value and never 0.
func TestPooledLateMappingExpiryIncident(t *testing.T) {
	e := startPooledEnv(t)
	org := e.newOrg()
	const repo = "14025000-0000-4000-8000-0000000000e1"
	const service = "svc-expire"
	day := deliveryDay1.Format(time.DateOnly)
	keys := []lateSampleKey{{"mttr_p50_hours", "incident_metrics_daily"}, {"mttr_p90_hours", "incident_metrics_daily"}}
	e.insertRepo(org, repo)
	e.mapIncidentService(org, service, repo)
	at := func(hour int) time.Time { return deliveryDay1.Add(time.Duration(hour) * time.Hour) }
	e.insertIncident(org, service, "i-1", at(1), at(3), 0)
	e.insertIncident(org, service, "i-2", at(2), at(8), 0)
	e.computeDelivery(org, []string{repo}, day)
	before := e.requireSameSampleSet("before", org, repo, day, keys)

	e.exec(`INSERT INTO operational_service_repository_mappings
    (org_id, provider, provider_instance_id, source_entity_type, external_id,
     source_version_at, source_revision, source_conflict_key, ingest_revision, ordering_contract,
     id, observed_at, last_synced, service_id, repo_id, mapping_kind, valid_from, valid_to, is_active)
VALUES (?, 'pagerduty', 'pd', 'repository_mapping', ?, now64(6), 2, '', 2, 2, ?, now64(6), now64(6), ?, toUUID(?), 'repository_derived', NULL, '2026-02-15 00:00:00.000000', 1)`,
		org, "map-"+service+"-"+repo, "map-"+service+"-"+repo, service, repo)
	e.requireSameSampleSet("late", org, repo, day, keys)

	writerTick()
	e.computeDelivery(org, []string{repo}, day)
	rerun := e.requireSameSampleSet("rerun", org, repo, day, keys)
	for _, key := range keys {
		t.Logf("RETIRED14148 %s: after the re-run the emptied repository-day serves %s (stored|chart), the first run served %s", key.metric, rerun[key.metric], before[key.metric])
		if rerun[key.metric] != "none|none" {
			t.Errorf("%s: the emptied repository-day serves %s (stored|chart), want none|none (retired, never the stale %s)", key.metric, rerun[key.metric], before[key.metric])
		}
	}
	if rows, retracted := e.keyRows("incident_metrics_daily", org, repo, day); retracted != 1 {
		t.Errorf("the newest row of the emptied repository-day holds retracted %d (%d rows), want the marker row", retracted, rows)
	}
}

// C2: a newer stored row's samples are what the chart reads, not the raw rows of its repository-day.
func TestPooledChartReadsTheStoredSamples(t *testing.T) {
	e := startPooledEnv(t)
	org := e.newOrg()
	const repo = "14025000-0000-4000-8000-0000000000f1"
	day := deliveryDay1.Format(time.DateOnly)
	e.insertRepo(org, repo)
	at := func(hour int) time.Time { return deliveryDay1.Add(time.Duration(hour) * time.Hour) }
	e.insertDeployment(org, repo, "d-1", "success", at(1), 1)
	e.insertDeployment(org, repo, "d-2", "success", at(2), 3)
	e.computeDelivery(org, []string{repo}, day)
	e.exec(`INSERT INTO deploy_metrics_daily (repo_id, day, deployments_count, failed_deployments_count, deploy_time_p50_hours,
    lead_time_p50_hours, computed_at, org_id, pooled_deploy_hours, pooled_samples_stored)
VALUES (?, ?, 2, 0, 50, NULL, now() + INTERVAL 1 HOUR, ?, [40, 60], 1)`, repo, day, org)
	chart, ok := e.deliveryChart(org, "deploy_time_p50_hours", "", "scorecard", day, day)["total"]
	t.Logf("C2 deploy_time_p50_hours chart=%v ok=%v (stored samples [40 60], raw rows [1 3])", chart, ok)
	if !ok || !near(chart, 50) {
		t.Errorf("C2: the chart is %v (ok %v), want 50, the median of the stored samples", chart, ok)
	}
}

// C4: a repository-day whose newest row holds no samples (a row written before migration 161, with that build's column
// list) reads its raw rows, as before; a window with one such day and one written day pools both.
func TestPooledLegacyRowReadsTheRawRows(t *testing.T) {
	e := startPooledEnv(t)
	org := e.newOrg()
	const repo = "14025000-0000-4000-8000-0000000000f2"
	day1, day2 := deliveryDay1.Format(time.DateOnly), deliveryDay2.Format(time.DateOnly)
	e.insertRepo(org, repo)
	at := func(day time.Time, hour int) time.Time { return day.Add(time.Duration(hour) * time.Hour) }
	e.insertDeployment(org, repo, "d1-1", "success", at(deliveryDay1, 1), 1)
	e.insertDeployment(org, repo, "d1-2", "success", at(deliveryDay1, 2), 3)
	e.computeDelivery(org, []string{repo}, day1)
	e.insertDeployment(org, repo, "d2-1", "success", at(deliveryDay2, 1), 10)
	e.insertDeployment(org, repo, "d2-2", "success", at(deliveryDay2, 2), 20)
	e.exec(`INSERT INTO deploy_metrics_daily (repo_id, day, deployments_count, failed_deployments_count, deploy_time_p50_hours,
    lead_time_p50_hours, computed_at, org_id)
VALUES (?, ?, 2, 0, 15, NULL, now(), ?)`, repo, day2, org)
	legacy, legacyOK := e.deliveryChart(org, "deploy_time_p50_hours", "", "scorecard", day2, day2)["total"]
	both, bothOK := e.deliveryChart(org, "deploy_time_p50_hours", "", "scorecard", day1, day2)["total"]
	t.Logf("C4 legacy day chart=%v ok=%v (raw [10 20]); window chart=%v ok=%v (pool [1 3 10 20])", legacy, legacyOK, both, bothOK)
	if !legacyOK || !near(legacy, 15) {
		t.Errorf("C4: the legacy day chart is %v (ok %v), want 15 from its raw rows", legacy, legacyOK)
	}
	if !bothOK || !near(both, 6.5) {
		t.Errorf("C4: the window chart is %v (ok %v), want 6.5, the median of the stored and the raw samples", both, bothOK)
	}
}

// C5: a TestOps repository-day of two services stores one row with no service (the groups disagree); a service group
// still pools each service's own runs, from the stored per-sample service ids.
func TestPooledTestopsServiceGroupFromStoredSamples(t *testing.T) {
	e := startPooledEnv(t)
	org := e.newOrg()
	const repo = "14025000-0000-4000-8000-0000000000f3"
	const day = "2026-01-02"
	e.insertRepo(org, repo)
	runs := []struct {
		id, service string
		minutes     int
	}{{"s-a1", "svc-a", 1}, {"s-a2", "svc-a", 3}, {"s-b1", "svc-b", 10}, {"s-b2", "svc-b", 30}}
	for i, run := range runs {
		started := time.Date(2026, 1, 2, 1, i, 0, 0, time.UTC)
		e.exec(`INSERT INTO ci_pipeline_runs (repo_id, run_id, status, queued_at, started_at, finished_at, last_synced, org_id, retry_count, service_id)
VALUES (?, ?, 'success', ?, ?, ?, '2026-02-01 00:00:00.000', ?, 0, ?)`,
			repo, run.id, started.Add(-time.Minute).Format(deliveryTimeFormat), started.Format(deliveryTimeFormat),
			started.Add(time.Duration(run.minutes)*time.Minute).Format(deliveryTimeFormat), org, run.service)
	}
	e.computeDaily(org, []string{repo}, day)
	got := pointsOf(e.chart(org, "p95_duration_seconds", "service", "bar", day, day))
	t.Logf("C5 p95_duration_seconds by service: %v", got)
	mustEqual(t, "C5 svc-a", got, "svc-a", inclusive(floats(60, 180), 0.95))
	mustEqual(t, "C5 svc-b", got, "svc-b", inclusive(floats(600, 1800), 0.95))
}

// The parallel sample arrays of every written row have one length (the readers ARRAY JOIN them).
func TestPooledSampleArraysHaveEqualLengths(t *testing.T) {
	e := startPooledEnv(t)
	org := e.newOrg()
	const day = "2026-01-02"
	repo := e.seedViaRoute(org, "acme/len", 4600, []seedRun{
		{id: 9101, day: day, startMin: 0, durationS: 10, queueS: 10, suiteTimes: []string{"10", ""}},
		{id: 9102, day: day, startMin: 1, durationS: 30, queueS: -1, suiteTimes: []string{"30"}},
		{id: 9103, day: day, startMin: 2, durationS: -1, queueS: 20},
	})
	e.computeDaily(org, []string{repo}, day)
	const cicdRepo = "14025000-0000-4000-8000-0000000000f4"
	e.insertCICDRuns(org, cicdRepo,
		cicdRun{id: "l-1", status: "success", queued: "2026-01-02 00:29:00.000", started: "2026-01-02 00:30:00.000", finished: "2026-01-02 00:40:00.000"},
		cicdRun{id: "l-2", status: "success", started: "2026-01-02 01:00:00.000", finished: "2026-01-02 01:20:00.000"},
	)
	e.computeCICD(org, []string{cicdRepo}, day)
	for _, check := range []struct{ table, lengths string }{
		{"testops_pipeline_metrics_daily", "length(pooled_service_ids) = length(pooled_durations) AND length(pooled_durations) = length(pooled_queues)"},
		{"testops_test_metrics_daily", "length(pooled_service_ids) = length(pooled_durations)"},
		{"cicd_metrics_daily", "length(pooled_run_minutes) = length(pooled_queue_minutes)"},
	} {
		var rows, unequal, stored uint64
		if err := e.conn.QueryRow(e.ctx, fmt.Sprintf(`SELECT count(), countIf(NOT (%s)), countIf(pooled_samples_stored = 1) FROM %s WHERE org_id = ?`,
			check.lengths, check.table), org).Scan(&rows, &unequal, &stored); err != nil {
			t.Fatalf("%s: %v", check.table, err)
		}
		t.Logf("LEN14025 %s rows=%d unequal=%d stored=%d", check.table, rows, unequal, stored)
		if rows == 0 || unequal != 0 || stored != rows {
			t.Errorf("%s: %d rows, %d with unequal sample arrays, %d marked stored", check.table, rows, unequal, stored)
		}
	}
}

// C8 (reader): a newest row over the writers' cap (pooled_samples_stored 2, empty arrays, values over all samples) reads
// its raw rows: the chart is exact, never a cut set.
func TestPooledOverCapRowReadsTheRawRows(t *testing.T) {
	e := startPooledEnv(t)
	org := e.newOrg()
	const repo = "14025000-0000-4000-8000-0000000000f5"
	day := deliveryDay1.Format(time.DateOnly)
	e.insertRepo(org, repo)
	at := func(hour int) time.Time { return deliveryDay1.Add(time.Duration(hour) * time.Hour) }
	// Three deployments of 1, 2 and 3 hours, all deployed (finished) on the day.
	for i, hours := range []float64{1, 2, 3} {
		e.insertDeployment(org, repo, fmt.Sprintf("d-%d", i), "success", at(i+1), hours)
	}
	e.exec(`INSERT INTO deploy_metrics_daily (repo_id, day, deployments_count, failed_deployments_count, deploy_time_p50_hours,
    lead_time_p50_hours, computed_at, org_id, pooled_deploy_hours, pooled_samples_stored)
VALUES (?, ?, 3, 0, 2, NULL, now(), ?, [], 2)`, repo, day, org)
	chart, ok := e.deliveryChart(org, "deploy_time_p50_hours", "", "scorecard", day, day)["total"]
	stored := e.storedDelivery(org, repo, "deploy_metrics_daily", "deploy_time_p50_hours")[day]
	t.Logf("C8 over-cap row: chart=%v ok=%v stored=%v (raw [1 2 3])", chart, ok, stored)
	if !ok || !near(chart, 2) || !near(stored, chart) {
		t.Errorf("C8: chart %v (ok %v), stored %v; want both 2, the median of the raw rows", chart, ok, stored)
	}
}

// A computed repository-day with counted rows and no sample: the stored row exists, its value is NULL, and the chart has
// no point (never 0), for every pooled key.
func TestPooledCountedRowsWithNoSampleAreNoPoint(t *testing.T) {
	e := startPooledEnv(t)
	rowExists := func(org, table string) {
		t.Helper()
		var rows uint64
		if err := e.conn.QueryRow(e.ctx, fmt.Sprintf(`SELECT count() FROM %s WHERE org_id = ? AND pooled_samples_stored = 1`, table), org).Scan(&rows); err != nil {
			t.Fatal(err)
		}
		if rows == 0 {
			t.Fatalf("%s: the writer wrote no row for the counted day", table)
		}
	}
	noPoint := func(phase, org, repo, day string, keys []lateSampleKey) {
		t.Helper()
		values := e.requireSameSampleSet(phase, org, repo, day, keys)
		for _, key := range keys {
			rowExists(org, key.table)
			if values[key.metric] != "none|none" {
				t.Errorf("%s %s: stored|chart %s, want none|none (no point, never 0)", phase, key.metric, values[key.metric])
			}
		}
	}

	// TestOps: two runs and a suite with no duration and no queue.
	testopsOrg := e.newOrg()
	const testopsDay = "2026-01-02"
	testopsRepo := e.seedViaRoute(testopsOrg, "acme/nosample", 4700, []seedRun{
		{id: 9201, day: testopsDay, startMin: 0, durationS: -1, queueS: -1, suiteTimes: []string{""}},
		{id: 9202, day: testopsDay, startMin: 1, durationS: -1, queueS: -1},
	})
	e.computeDaily(testopsOrg, []string{testopsRepo}, testopsDay)
	noPoint("testops", testopsOrg, testopsRepo, testopsDay, lateSampleTestopsKeys)

	// CI/CD: a run finished before it started (a negative duration) with no queued time.
	cicdOrg := e.newOrg()
	const cicdRepo = "14025000-0000-4000-8000-0000000000f6"
	e.insertCICDRuns(cicdOrg, cicdRepo,
		cicdRun{id: "ns-1", status: "success", started: "2026-01-02 10:10:00.000", finished: "2026-01-02 10:00:00.000"})
	e.computeCICD(cicdOrg, []string{cicdRepo}, testopsDay)
	noPoint("cicd", cicdOrg, cicdRepo, testopsDay, lateSampleCICDKeys)

	// Delivery: a deployment of negative duration, a merged pull request of 0 lines, an incident resolved before it
	// started, and no bug.
	deliveryOrg := e.newOrg()
	const deliveryRepo = "14025000-0000-4000-8000-0000000000f7"
	day := deliveryDay1.Format(time.DateOnly)
	e.insertRepo(deliveryOrg, deliveryRepo)
	e.mapIncidentService(deliveryOrg, "svc-nosample", deliveryRepo)
	at := func(hour int) time.Time { return deliveryDay1.Add(time.Duration(hour) * time.Hour) }
	e.insertDeployment(deliveryOrg, deliveryRepo, "d-negative", "success", at(5), -1)
	e.insertPullRequest(deliveryOrg, deliveryRepo, 1, at(5), 0, 0)
	e.insertIncident(deliveryOrg, "svc-nosample", "i-negative", at(5), at(4), 0)
	e.computeDelivery(deliveryOrg, []string{deliveryRepo}, day)
	keys := make([]lateSampleKey, 0, len(deliveryPooledMetrics))
	for _, metric := range deliveryPooledMetrics {
		keys = append(keys, lateSampleKey{metric.name, metric.table})
	}
	noPoint("delivery", deliveryOrg, deliveryRepo, day, keys)
}

// A retired CI/CD repository-day (its newest row holds retracted = 1, a row written without the pooled columns, so it
// holds no samples) is absent in both arms of the pooled source: the stored samples of the older live row are not
// served, and a run synced after the retraction does not enter through the raw arm. A live row without samples still
// reads its raw runs.
func TestPooledCICDRetiredDayIsAbsentInBothArms(t *testing.T) {
	e := startPooledEnv(t)
	org := e.newOrg()
	const repo = "14025000-0000-4000-8000-0000000000f8"
	const day, live = "2026-01-02", "2026-01-03"
	e.insertCICDRuns(org, repo,
		cicdRun{id: "rt-1", status: "success", queued: "2026-01-02 00:29:00.000", started: "2026-01-02 00:30:00.000", finished: "2026-01-02 00:40:00.000"})
	e.computeCICD(org, []string{repo}, day)
	e.requireSameSampleSet("stored", org, repo, day, lateSampleCICDKeys)

	newest := func(d string) (retracted, marker uint8) {
		t.Helper()
		if err := e.conn.QueryRow(e.ctx, `SELECT argMax(retracted, computed_at), argMax(pooled_samples_stored, computed_at) FROM cicd_metrics_daily
WHERE org_id = ? AND repo_id = ? AND day = ?`, org, repo, d).Scan(&retracted, &marker); err != nil {
			t.Fatal(err)
		}
		return retracted, marker
	}
	absent := func(phase string) {
		t.Helper()
		retracted, marker := newest(day)
		if retracted != 1 || marker != 0 {
			t.Fatalf("%s: the newest row of %s holds retracted %d, pooled_samples_stored %d; want the retraction row (1, 0)", phase, day, retracted, marker)
		}
		for _, metric := range cicdPooledMetrics {
			for _, chartType := range []string{"scorecard", "bar"} {
				got := pointsOf(e.cicdChart(org, metric, cicdSpec{chartType: chartType, start: day, end: day}))
				t.Logf("RETIRED14025 %-6s %-22s %-9s points=%v", phase, metric, chartType, got)
				if len(got) != 0 {
					t.Errorf("%s %s %s: the retired day has points %v, want none (absent, never 0)", phase, metric, chartType, got)
				}
			}
		}
	}

	// Stored arm: the older live row holds samples (marker 1); the newer retraction row retires the day.
	writerTick()
	e.retireCICDDay(org, repo, day)
	absent("stored")

	// Raw arm: a run synced after the retraction does not enter.
	e.insertCICDRuns(org, repo,
		cicdRun{id: "rt-late", status: "success", queued: "2026-01-02 05:00:00.000", started: "2026-01-02 05:10:00.000", finished: "2026-01-02 06:10:00.000"})
	absent("raw")

	// A live row written without samples (marker 0, retracted 0) still reads its raw runs.
	e.insertCICDRuns(org, repo,
		cicdRun{id: "lv-1", status: "success", queued: "2026-01-03 00:25:00.000", started: "2026-01-03 00:30:00.000", finished: "2026-01-03 00:50:00.000"})
	e.computeCICD(org, []string{repo}, live)
	writerTick()
	e.liveCICDDay(org, repo, live, 1)
	if retracted, marker := newest(live); retracted != 0 || marker != 0 {
		t.Fatalf("the newest row of %s holds retracted %d, pooled_samples_stored %d; want a live row without samples (0, 0)", live, retracted, marker)
	}
	got := pointsOf(e.cicdChart(org, "avg_duration_minutes", cicdSpec{chartType: "scorecard", start: live, end: live}))
	t.Logf("RETIRED14025 live-raw avg_duration_minutes points=%v", got)
	mustEqual(t, "a live row without samples reads its raw runs", got, "total", 20)
}
