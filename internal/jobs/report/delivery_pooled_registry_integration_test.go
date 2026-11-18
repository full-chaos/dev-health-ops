//go:build integration

package report

import (
	"fmt"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily"
)

// The AI report charts the deploy time median, the pull request size percentiles and the MTTR metrics over the window's
// raw rows, with the daily writers' own sample rules, never as the mean of the stored per-repository-day values. Every
// cell inserts the raw rows, runs the REAL daily writers (deploy, repo_user_commit, incident) and charts through the real
// adapter.

const deliveryTimeFormat = "2006-01-02 15:04:05.000"

func (e *pooledEnv) insertRepo(org, repo string) {
	e.t.Helper()
	e.exec(`INSERT INTO repos (id, repo, provider, org_id, created_at, last_synced) VALUES (?, ?, 'github', ?, now(), now())`, repo, "acme/"+repo[len(repo)-6:], org)
}

func (e *pooledEnv) insertDeployment(org, repo, id, status string, started time.Time, hours float64) {
	e.t.Helper()
	finished := started.Add(time.Duration(hours * float64(time.Hour)))
	e.exec(`INSERT INTO deployments (repo_id, deployment_id, status, started_at, finished_at, deployed_at, org_id, last_synced) VALUES (?, ?, ?, ?, ?, ?, ?, '2026-02-01 00:00:00.000')`,
		repo, id, status, started.Format(deliveryTimeFormat), finished.Format(deliveryTimeFormat), finished.Format(deliveryTimeFormat), org)
}

// insertPullRequest stores a pull request merged at merged (the zero time leaves it open).
func (e *pooledEnv) insertPullRequest(org, repo string, number int, merged time.Time, additions, deletions int) {
	e.t.Helper()
	var mergedAt any
	if !merged.IsZero() {
		mergedAt = merged.Format(deliveryTimeFormat)
	}
	created := time.Date(2026, 1, 20, 0, 0, 0, 0, time.UTC)
	e.exec(`INSERT INTO git_pull_requests (repo_id, number, author_name, author_email, created_at, merged_at, additions, deletions, last_synced, org_id)
VALUES (?, ?, 'dev', 'dev@example.invalid', ?, ?, ?, ?, '2026-02-01 00:00:00.000', ?)`,
		repo, uint32(number), created.Format(deliveryTimeFormat), mergedAt, uint32(additions), uint32(deletions), org)
}

func (e *pooledEnv) insertBug(org, repo, id, kind string, started, completed time.Time) {
	e.t.Helper()
	e.exec(`INSERT INTO work_items (repo_id, work_item_id, provider, type, created_at, updated_at, started_at, completed_at, last_synced, org_id)
VALUES (?, ?, 'jira', ?, ?, ?, ?, ?, '2026-02-01 00:00:00.000', ?)`,
		repo, id, kind, started.Format(deliveryTimeFormat), completed.Format(deliveryTimeFormat), started.Format(deliveryTimeFormat), completed.Format(deliveryTimeFormat), org)
}

// insertIncident stores an incident of service, mapped to repo once per org (mapIncidentService).
func (e *pooledEnv) insertIncident(org, service, id string, started, resolved time.Time, deleted uint8) {
	e.t.Helper()
	e.exec(`INSERT INTO operational_incidents
    (org_id, provider, provider_instance_id, source_entity_type, external_id,
     source_version_at, source_revision, source_conflict_key, ingest_revision, ordering_contract,
     id, observed_at, last_synced, normalized_status, service_id, title, started_at, resolved_at, is_deleted)
VALUES (?, 'pagerduty', 'pd', 'incident', ?, now64(6), 1, '', 1, 2, ?, now64(6), now64(6), 'resolved', ?, 'incident', ?, ?, ?)`,
		org, id, id, service, started.Format(deliveryTimeFormat), resolved.Format(deliveryTimeFormat), deleted)
}

func (e *pooledEnv) mapIncidentService(org, service, repo string) {
	e.t.Helper()
	e.exec(`INSERT INTO operational_service_repository_mappings
    (org_id, provider, provider_instance_id, source_entity_type, external_id,
     source_version_at, source_revision, source_conflict_key, ingest_revision, ordering_contract,
     id, observed_at, last_synced, service_id, repo_id, mapping_kind, valid_from, valid_to, is_active)
VALUES (?, 'pagerduty', 'pd', 'repository_mapping', ?, now64(6), 1, '', 1, 2, ?, now64(6), now64(6), ?, toUUID(?), 'repository_derived', NULL, NULL, 1)`,
		org, "map-"+service+"-"+repo, "map-"+service+"-"+repo, service, repo)
}

// computeDelivery runs the real deploy, repo_user_commit and incident daily writers for the days.
func (e *pooledEnv) computeDelivery(org string, repos []string, days ...string) {
	e.t.Helper()
	deploy, err := daily.NewDeployExecutor(e.conn)
	if err != nil {
		e.t.Fatal(err)
	}
	repoUser, err := daily.NewRepoUserCommitExecutor(e.conn)
	if err != nil {
		e.t.Fatal(err)
	}
	incident, err := daily.NewIncidentExecutor(e.conn)
	if err != nil {
		e.t.Fatal(err)
	}
	ids := make([]daily.RepositoryID, 0, len(repos))
	for _, repo := range repos {
		ids = append(ids, daily.RepositoryID(repo))
	}
	for i, day := range days {
		target, _ := time.Parse(time.DateOnly, day)
		run := daily.Run{ID: fmt.Sprintf("delivery-%s-%d", org, i), OrganizationID: org, TargetDay: target}
		part := daily.Partition{ID: fmt.Sprintf("delivery-part-%s-%d", org, i), RunID: run.ID, RepoIDs: ids}
		for _, executor := range []daily.NativeFamilyExecutor{deploy, repoUser, incident} {
			if _, err := executor.ComputeFamily(e.ctx, run, part); err != nil {
				e.t.Fatal(err)
			}
		}
	}
}

// storedDelivery reads the writers' newest value of metric for each day of repo.
func (e *pooledEnv) storedDelivery(org, repo, table, metric string) map[string]float64 {
	e.t.Helper()
	rows, err := e.conn.Query(e.ctx, fmt.Sprintf(`SELECT day, %s FROM %s WHERE org_id = ? AND repo_id = ? ORDER BY computed_at DESC LIMIT 1 BY day`, metric, table), org, repo)
	if err != nil {
		e.t.Fatal(err)
	}
	defer rows.Close()
	out := map[string]float64{}
	for rows.Next() {
		var day time.Time
		var value *float64
		if err := rows.Scan(&day, &value); err != nil {
			e.t.Fatal(err)
		}
		if value != nil {
			out[day.Format(time.DateOnly)] = *value
		}
	}
	return out
}

type deliveryMetric struct{ name, table string }

var deliveryPooledMetrics = []deliveryMetric{
	{"deploy_time_p50_hours", "deploy_metrics_daily"},
	{"pr_size_p50_loc", "repo_metrics_daily"},
	{"pr_size_p90_loc", "repo_metrics_daily"},
	{"mttr_hours", "repo_metrics_daily"},
	{"mttr_p50_hours", "incident_metrics_daily"},
	{"mttr_p90_hours", "incident_metrics_daily"},
}

func (e *pooledEnv) deliveryChart(org, metric, groupBy, chartType, start, end string, teams ...string) map[string]float64 {
	e.t.Helper()
	result := e.chart(org, metric, groupBy, chartType, start, end, teams...)
	return pointsOf(result)
}

var (
	deliveryDay1 = time.Date(2026, 2, 2, 0, 0, 0, 0, time.UTC)
	deliveryDay2 = time.Date(2026, 2, 3, 0, 0, 0, 0, time.UTC)
)

// seedDeliveryCell seeds the executed numbers of the ticket on every family: day 1 holds two samples (1 and 3 hours, or 10
// and 30 lines), day 2 eight samples of 10 hours (or 100 lines). Each day also has the rows no sample comes from.
func seedDeliveryCell(e *pooledEnv, org, repo, service string) {
	e.t.Helper()
	e.insertRepo(org, repo)
	e.mapIncidentService(org, service, repo)
	at := func(day time.Time, i int) time.Time { return day.Add(time.Duration(i+1) * 30 * time.Minute) }
	for i, hours := range []float64{1, 3} {
		e.insertDeployment(org, repo, fmt.Sprintf("d1-%d", i), "success", at(deliveryDay1, i), hours)
		e.insertBug(org, repo, fmt.Sprintf("b1-%d-%s", i, service), "bug", at(deliveryDay1, i), at(deliveryDay1, i).Add(time.Duration(hours)*time.Hour))
		e.insertIncident(org, service, fmt.Sprintf("i1-%d-%s", i, service), at(deliveryDay1, i), at(deliveryDay1, i).Add(time.Duration(hours)*time.Hour), 0)
	}
	e.insertPullRequest(org, repo, 1, at(deliveryDay1, 0), 6, 4)
	e.insertPullRequest(org, repo, 2, at(deliveryDay1, 1), 20, 10)
	for i := 0; i < 8; i++ {
		e.insertDeployment(org, repo, fmt.Sprintf("d2-%d", i), "success", at(deliveryDay2, i), 10)
		e.insertBug(org, repo, fmt.Sprintf("b2-%d-%s", i, service), "bug", at(deliveryDay2, i), at(deliveryDay2, i).Add(10*time.Hour))
		e.insertIncident(org, service, fmt.Sprintf("i2-%d-%s", i, service), at(deliveryDay2, i), at(deliveryDay2, i).Add(10*time.Hour), 0)
		e.insertPullRequest(org, repo, 10+i, at(deliveryDay2, i), 60, 40)
	}
	// No sample: a cancelled deployment, a merged pull request of no lines, an open pull request, a story, a deleted
	// incident and an incident resolved the next day.
	e.insertDeployment(org, repo, "d2-cancelled", "cancelled", at(deliveryDay2, 9), 100)
	e.insertPullRequest(org, repo, 30, at(deliveryDay2, 9), 0, 0)
	e.insertPullRequest(org, repo, 31, time.Time{}, 500, 500)
	e.insertBug(org, repo, "s2-"+service, "story", at(deliveryDay2, 9), at(deliveryDay2, 9).Add(100*time.Hour))
	e.insertIncident(org, service, "i2-deleted-"+service, at(deliveryDay2, 9), at(deliveryDay2, 9).Add(100*time.Hour), 1)
	e.insertIncident(org, service, "i2-next-day-"+service, at(deliveryDay2, 9), deliveryDay2.Add(30*time.Hour), 0)
}

// The pooled value of each metric over the cell's two days, and the mean of the stored day values the old chart gave.
var deliveryWant = map[string]struct{ pooled, storedMean float64 }{
	"deploy_time_p50_hours": {10, 6},
	"pr_size_p50_loc":       {100, 60},
	"pr_size_p90_loc":       {100, 64},
	"mttr_hours":            {8.4, 6},
	"mttr_p50_hours":        {10, 6},
	"mttr_p90_hours":        {10, 6.4},
}

func TestRegistryDeliveryPercentilesPoolTheWindowRows(t *testing.T) {
	e := startPooledEnv(t)
	org := e.newOrg()
	const repo = "13887000-0000-4000-8000-0000000000a1"
	seedDeliveryCell(e, org, repo, "svc-a")
	e.computeDelivery(org, []string{repo}, deliveryDay1.Format(time.DateOnly), deliveryDay2.Format(time.DateOnly))
	for _, metric := range deliveryPooledMetrics {
		stored := e.storedDelivery(org, repo, metric.table, metric.name)
		if len(stored) != 2 {
			t.Fatalf("%s: the writer stored %v, want both days", metric.name, stored)
		}
		storedMean := (stored["2026-02-02"] + stored["2026-02-03"]) / 2
		want := deliveryWant[metric.name]
		if !near(storedMean, want.storedMean) {
			t.Fatalf("%s: the stored days %v have mean %v, want %v", metric.name, stored, storedMean, want.storedMean)
		}
		for _, chartType := range []string{"bar", "scorecard"} {
			got := e.deliveryChart(org, metric.name, "", chartType, "2026-02-01", "2026-02-05")
			t.Logf("CELL %s %s: chart=%v pooled=%v stored-mean=%v stored=%v", metric.name, chartType, got["total"], want.pooled, storedMean, stored)
			mustEqual(t, metric.name+" "+chartType, got, "total", want.pooled)
		}
		// A one-repository-day bucket is the stored value of that day.
		days := e.deliveryChart(org, metric.name, "day", "bar", "2026-02-01", "2026-02-05")
		for day, value := range stored {
			mustEqual(t, metric.name+" day", days, day, value)
		}
		if len(days) != 2 {
			t.Errorf("%s by day: %v, want the two stored days", metric.name, days)
		}
	}
}

// A row the writer takes no sample from is no sample of the chart either, and a repository-day the writer did not compute
// is not read. The metrics have no team dimension: a team group is one 'unscoped' bucket, as before.
func TestRegistryDeliveryRowsWithNoSample(t *testing.T) {
	e := startPooledEnv(t)
	org := e.newOrg()
	const repo = "13887000-0000-4000-8000-0000000000b1"
	e.insertRepo(org, repo)
	e.own(org, "team-1", "13887000-0000-4000-8000-0000000000b9")
	e.mapIncidentService(org, "svc-b", repo)
	day3 := deliveryDay1.AddDate(0, 0, 2)
	at := func(day time.Time, hour int) time.Time { return day.Add(time.Duration(hour) * time.Hour) }
	// Day 1, one sample per family: 2 hours, 10 lines.
	e.insertDeployment(org, repo, "d-ok", "success", at(deliveryDay1, 1), 2)
	e.insertDeployment(org, repo, "d-cancelled", "canceled", at(deliveryDay1, 2), 100)
	e.insertPullRequest(org, repo, 1, at(deliveryDay1, 1), 7, 3)
	e.insertPullRequest(org, repo, 2, at(deliveryDay1, 2), 0, 0)
	e.insertPullRequest(org, repo, 3, time.Time{}, 900, 100)
	e.insertBug(org, repo, "b-ok", "bug", at(deliveryDay1, 1), at(deliveryDay1, 3))
	e.insertBug(org, repo, "b-story", "story", at(deliveryDay1, 1), at(deliveryDay1, 20))
	// The writer's mean includes a negative repair time: (2 - 1) / 2.
	e.insertBug(org, repo, "b-negative", "bug", at(deliveryDay1, 5), at(deliveryDay1, 4))
	e.insertIncident(org, "svc-b", "i-ok", at(deliveryDay1, 1), at(deliveryDay1, 3), 0)
	e.insertIncident(org, "svc-b", "i-deleted", at(deliveryDay1, 1), at(deliveryDay1, 20), 1)
	e.insertIncident(org, "svc-b", "i-next-day", at(deliveryDay1, 1), at(deliveryDay1, 30), 0)
	e.insertIncident(org, "svc-b", "i-negative", at(deliveryDay1, 5), at(deliveryDay1, 4), 0)
	e.insertIncident(org, "svc-unmapped", "i-unmapped", at(deliveryDay1, 1), at(deliveryDay1, 20), 0)
	// Day 3 holds a sample of every family, but its writers never ran.
	e.insertDeployment(org, repo, "d-day3", "success", at(day3, 1), 100)
	e.insertPullRequest(org, repo, 4, at(day3, 1), 900, 100)
	e.insertBug(org, repo, "b-day3", "bug", at(day3, 1), at(day3, 20))
	e.insertIncident(org, "svc-b", "i-day3", at(day3, 1), at(day3, 20), 0)
	e.computeDelivery(org, []string{repo}, deliveryDay1.Format(time.DateOnly))

	want := map[string]float64{"deploy_time_p50_hours": 2, "pr_size_p50_loc": 10, "pr_size_p90_loc": 10, "mttr_hours": 0.5, "mttr_p50_hours": 2, "mttr_p90_hours": 2}
	for _, metric := range deliveryPooledMetrics {
		stored := e.storedDelivery(org, repo, metric.table, metric.name)
		if len(stored) != 1 || !near(stored["2026-02-02"], want[metric.name]) {
			t.Fatalf("%s: the writer stored %v, want %v on 2026-02-02 only", metric.name, stored, want[metric.name])
		}
		got := e.deliveryChart(org, metric.name, "", "scorecard", "2026-02-01", "2026-02-05")
		t.Logf("CELL %s no-sample rows: chart=%v want=%v", metric.name, got["total"], want[metric.name])
		mustEqual(t, metric.name+" scorecard", got, "total", want[metric.name])
		for _, teams := range [][]string{nil, {"team-1"}} {
			grouped := e.deliveryChart(org, metric.name, "team", "bar", "2026-02-01", "2026-02-05", teams...)
			if len(grouped) != 1 {
				t.Errorf("%s by team (teams %v): %v, want one unscoped bucket", metric.name, teams, grouped)
			}
			mustEqual(t, metric.name+" by team", grouped, "unscoped", want[metric.name])
		}
		if empty := e.deliveryChart(org, metric.name, "day", "bar", "2026-02-03", "2026-02-05"); len(empty) != 0 {
			t.Errorf("%s: days with no computed row have points %v, want none (never 0)", metric.name, empty)
		}
	}
}

// A one-repository-day bucket is the stored value even when a deployment or a pull request has two stored versions in
// unmerged parts (a resync): the chart reads the rows the writer reads. An incident repair time keeps the microseconds the
// writer subtracts.
func TestRegistryDeliveryDayBucketIsTheStoredValue(t *testing.T) {
	e := startPooledEnv(t)
	org := e.newOrg()
	const repo = "13887000-0000-4000-8000-0000000000c1"
	for _, table := range []string{"deployments", "git_pull_requests"} {
		e.exec("SYSTEM STOP MERGES " + table)
	}
	t.Cleanup(func() {
		for _, table := range []string{"deployments", "git_pull_requests"} {
			_ = e.conn.Exec(e.ctx, "SYSTEM START MERGES "+table)
		}
	})
	e.insertRepo(org, repo)
	e.mapIncidentService(org, "svc-c", repo)
	at := func(hour int) time.Time { return deliveryDay1.Add(time.Duration(hour) * time.Hour) }
	// One INSERT per version: the two versions of d-1 and of pull request 1 stay two parts.
	e.insertDeployment(org, repo, "d-1", "success", at(1), 2)
	e.insertDeployment(org, repo, "d-1", "success", at(1), 12)
	e.insertDeployment(org, repo, "d-2", "success", at(2), 4)
	e.insertPullRequest(org, repo, 1, at(1), 10, 10)
	e.insertPullRequest(org, repo, 1, at(1), 100, 20)
	e.insertPullRequest(org, repo, 2, at(2), 30, 10)
	e.exec(`INSERT INTO operational_incidents
    (org_id, provider, provider_instance_id, source_entity_type, external_id,
     source_version_at, source_revision, source_conflict_key, ingest_revision, ordering_contract,
     id, observed_at, last_synced, normalized_status, service_id, title, started_at, resolved_at, is_deleted)
VALUES (?, 'pagerduty', 'pd', 'incident', 'i-us', now64(6), 1, '', 1, 2, 'i-us', now64(6), now64(6), 'resolved', 'svc-c', 'incident',
        toDateTime64('2026-02-02 01:00:00.000000', 6, 'UTC'), toDateTime64('2026-02-02 02:00:00.000500', 6, 'UTC'), 0)`, org)
	e.computeDelivery(org, []string{repo}, deliveryDay1.Format(time.DateOnly))
	for _, metric := range deliveryPooledMetrics {
		if metric.name == "mttr_hours" {
			continue
		}
		stored := e.storedDelivery(org, repo, metric.table, metric.name)
		if len(stored) != 1 {
			t.Fatalf("%s: the writer stored %v, want one day", metric.name, stored)
		}
		got := e.deliveryChart(org, metric.name, "", "scorecard", "2026-02-01", "2026-02-05")
		t.Logf("CELL %s day bucket: chart=%.17g stored=%.17g", metric.name, got["total"], stored["2026-02-02"])
		mustEqual(t, metric.name+" day bucket", got, "total", stored["2026-02-02"])
	}
}
