//go:build integration

package report

// The incident, deploy and repo_user_commit daily writers retire a repository-day whose samples are gone: a recompute that
// produces no record for a repository of its partition writes a marker row over the stored live row (the newest row reads
// retracted = 1), so the stored value and every chart of the day read absent, never 0. A repository with no stored row
// gets nothing; a second empty recompute writes nothing; the day is live again when its data returns; an older empty
// compute never retires a newer measured row; a repository outside the run's partition is never retired.

import (
	"fmt"
	"testing"
	"time"
)

type pooledWriterCase struct {
	name, table string
	metrics     []string
	// seed writes the data of repo on deliveryDay1, empty makes the day have no sample, restore brings the data back.
	seed, empty, restore func(e *pooledEnv, org, repo string)
}

func (e *pooledEnv) keyRows(table, org, repo, day string) (rows uint64, retracted uint8) {
	e.t.Helper()
	if err := e.conn.QueryRow(e.ctx, fmt.Sprintf(`SELECT count(), argMax(retracted, computed_at) FROM %s WHERE org_id = ? AND repo_id = ? AND day = ?`, table),
		org, repo, day).Scan(&rows, &retracted); err != nil {
		e.t.Fatal(err)
	}
	return rows, retracted
}

const writerLater = "now64(3) + INTERVAL 1 DAY"

var pooledWriterCases = []pooledWriterCase{
	{
		name: "incident", table: "incident_metrics_daily", metrics: []string{"mttr_p50_hours", "mttr_p90_hours"},
		seed: func(e *pooledEnv, org, repo string) {
			e.mapIncidentService(org, "svc-"+repo[len(repo)-4:], repo)
			e.insertIncident(org, "svc-"+repo[len(repo)-4:], "i-"+repo, deliveryDay1.Add(time.Hour), deliveryDay1.Add(3*time.Hour), 0)
		},
		// The service mapping expires (a newer revision with valid_to before the re-run).
		empty: func(e *pooledEnv, org, repo string) { e.mappingRevision(org, repo, 2, "'2026-02-15 00:00:00.000000'") },
		// The mapping is open again (a newer revision with no valid_to).
		restore: func(e *pooledEnv, org, repo string) { e.mappingRevision(org, repo, 3, "NULL") },
	},
	{
		name: "deploy", table: "deploy_metrics_daily", metrics: []string{"deploy_time_p50_hours"},
		seed: func(e *pooledEnv, org, repo string) {
			e.insertDeployment(org, repo, "d-"+repo, "success", deliveryDay1.Add(time.Hour), 2)
		},
		// The deployment is re-synced as cancelled (no deployment); the writer reads without FINAL, so the table is merged.
		empty:   func(e *pooledEnv, org, repo string) { e.deploymentStatus(org, repo, "cancelled") },
		restore: func(e *pooledEnv, org, repo string) { e.deploymentStatus(org, repo, "success") },
	},
	{
		name: "repo_user_commit", table: "repo_metrics_daily", metrics: []string{"pr_size_p50_loc", "mttr_hours"},
		seed: func(e *pooledEnv, org, repo string) {
			e.insertPullRequest(org, repo, 1, deliveryDay1.Add(time.Hour), 10, 10)
			e.insertBug(org, repo, "b-"+repo, "bug", deliveryDay1.Add(time.Hour), deliveryDay1.Add(3*time.Hour))
		},
		// The merge and the bug's completion move to the next day.
		empty:   func(e *pooledEnv, org, repo string) { e.moveRepoDay(org, repo, 1) },
		restore: func(e *pooledEnv, org, repo string) { e.moveRepoDay(org, repo, -1) },
	},
}

func (e *pooledEnv) mappingRevision(org, repo string, revision int, validTo string) {
	e.t.Helper()
	service := "svc-" + repo[len(repo)-4:]
	e.exec(fmt.Sprintf(`INSERT INTO operational_service_repository_mappings
    (org_id, provider, provider_instance_id, source_entity_type, external_id,
     source_version_at, source_revision, source_conflict_key, ingest_revision, ordering_contract,
     id, observed_at, last_synced, service_id, repo_id, mapping_kind, valid_from, valid_to, is_active)
VALUES (?, 'pagerduty', 'pd', 'repository_mapping', ?, now64(6), %d, '', %d, 2, ?, now64(6), now64(6), ?, toUUID(?), 'repository_derived', NULL, %s, 1)`,
		revision, revision, validTo), org, "map-"+service+"-"+repo, "map-"+service+"-"+repo, service, repo)
}

func (e *pooledEnv) deploymentStatus(org, repo, status string) {
	e.t.Helper()
	e.exec(`INSERT INTO deployments SELECT * REPLACE (? AS status, `+writerLater+` AS last_synced)
FROM deployments FINAL WHERE org_id = ? AND repo_id = ?`, status, org, repo)
	e.exec(`OPTIMIZE TABLE deployments FINAL`)
}

func (e *pooledEnv) moveRepoDay(org, repo string, days int) {
	e.t.Helper()
	e.exec(fmt.Sprintf(`INSERT INTO git_pull_requests SELECT * REPLACE (merged_at + INTERVAL %d DAY AS merged_at, `+writerLater+` AS last_synced)
FROM git_pull_requests FINAL WHERE org_id = ? AND repo_id = ?`, days), org, repo)
	e.exec(fmt.Sprintf(`INSERT INTO work_items SELECT * REPLACE (completed_at + INTERVAL %d DAY AS completed_at, `+writerLater+` AS last_synced)
FROM work_items FINAL WHERE org_id = ? AND repo_id = ?`, days), org, repo)
	e.exec(`OPTIMIZE TABLE git_pull_requests FINAL`)
}

func TestPooledWritersRetireAnEmptiedDay(t *testing.T) {
	for index, c := range pooledWriterCases {
		t.Run(c.name, func(t *testing.T) {
			e := startPooledEnv(t)
			org := e.newOrg()
			repo := fmt.Sprintf("14148000-0000-4000-8000-0000000002%d1", index)
			empty := fmt.Sprintf("14148000-0000-4000-8000-0000000002%d2", index)
			outside := fmt.Sprintf("14148000-0000-4000-8000-0000000002%d3", index)
			day := deliveryDay1.Format(time.DateOnly)
			for _, r := range []string{repo, empty, outside} {
				e.insertRepo(org, r)
			}
			c.seed(e, org, repo)
			e.computeDelivery(org, []string{repo, empty}, day)
			if rows, retracted := e.keyRows(c.table, org, repo, day); rows != 1 || retracted != 0 {
				t.Fatalf("after the first compute the key has %d rows, retracted %d; want one live row", rows, retracted)
			}

			c.empty(e, org, repo)
			writerTick()
			e.computeDelivery(org, []string{repo, empty}, day)
			rows, retracted := e.keyRows(c.table, org, repo, day)
			t.Logf("WRITER14148 %-16s emptied: rows=%d newest retracted=%d", c.name, rows, retracted)
			if rows != 2 || retracted != 1 {
				t.Errorf("an emptied day: %d rows, newest retracted %d; want the measured row and its marker row (2, 1)", rows, retracted)
			}
			if rows, _ := e.keyRows(c.table, org, empty, day); rows != 0 {
				t.Errorf("a repository with no stored row has %d rows, want none (missing stays missing)", rows)
			}
			for _, metric := range c.metrics {
				got := pointsOf(e.chart(org, metric, "", "scorecard", day, day))
				t.Logf("WRITER14148 %-16s emptied: %s points=%v", c.name, metric, got)
				if len(got) != 0 {
					t.Errorf("%s: the emptied day has points %v, want none (absent, never 0)", metric, got)
				}
			}

			writerTick()
			e.computeDelivery(org, []string{repo, empty}, day)
			if rows, _ := e.keyRows(c.table, org, repo, day); rows != 2 {
				t.Errorf("a recompute of an already retired day: %d rows, want 2 (no second marker)", rows)
			}

			c.restore(e, org, repo)
			writerTick()
			e.computeDelivery(org, []string{repo, empty}, day)
			if _, retracted := e.keyRows(c.table, org, repo, day); retracted != 0 {
				t.Errorf("after the data came back the newest row reads retracted %d, want a live row", retracted)
			}

			// An older empty compute never retires a newer measured row: the marker carries the run's own instant.
			c.empty(e, org, repo)
			e.exec(fmt.Sprintf(`INSERT INTO %s (org_id, repo_id, day, computed_at) VALUES (?, ?, ?, ?)`, c.table),
				org, repo, day, time.Now().UTC().Add(time.Hour).Truncate(time.Second))
			writerTick()
			e.computeDelivery(org, []string{repo, empty}, day)
			if _, retracted := e.keyRows(c.table, org, repo, day); retracted != 0 {
				t.Errorf("an older empty compute retired a newer measured row")
			}

			// A repository outside the run's partition is never retired, also when its day is emptied.
			c.seed(e, org, outside)
			writerTick()
			e.computeDelivery(org, []string{outside}, day)
			c.empty(e, org, outside)
			writerTick()
			e.computeDelivery(org, []string{repo, empty}, day)
			if rows, retracted := e.keyRows(c.table, org, outside, day); rows != 1 || retracted != 0 {
				t.Errorf("a run of another partition touched a repository outside its set: %d rows, retracted %d", rows, retracted)
			}
		})
	}
}
