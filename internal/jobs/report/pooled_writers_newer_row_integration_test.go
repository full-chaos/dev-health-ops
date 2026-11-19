//go:build integration

package report

// A daily writer that recomputes a repository-day with no sample writes NO marker row over a key whose stored row is
// newer than the run's own snapshot (a newer run wrote it): the marker of an older read is not written at all, not
// only not served. One cell per writer of the four marked pooled tables.

import (
	"fmt"
	"testing"
	"time"
)

type newerRowCase struct {
	name, table string
	seed, empty func(e *pooledEnv, org, repo string)
	compute     func(e *pooledEnv, org, repo, day string)
}

func (e *pooledEnv) computePipelineDay(org, repo, day string) {
	e.computeDaily(org, []string{repo}, day)
}

func (e *pooledEnv) computeDeliveryDay(org, repo, day string) {
	e.computeDelivery(org, []string{repo}, day)
}

func newerRowCases() []newerRowCase {
	delivery := map[string]pooledWriterCase{}
	for _, c := range pooledWriterCases {
		delivery[c.name] = c
	}
	out := []newerRowCase{}
	for _, name := range []string{"incident", "deploy", "repo_user_commit"} {
		c := delivery[name]
		out = append(out, newerRowCase{name: c.name, table: c.table, seed: c.seed, empty: c.empty, compute: (*pooledEnv).computeDeliveryDay})
	}
	out = append(out, newerRowCase{
		name: "testops_pipeline", table: "testops_pipeline_metrics_daily",
		seed: func(e *pooledEnv, org, repo string) {
			started := deliveryDay1.Add(time.Hour)
			e.insertCICDRuns(org, repo, cicdRun{id: "n-" + repo, status: "success",
				queued: started.Add(-time.Minute).Format(deliveryTimeFormat), started: started.Format(deliveryTimeFormat),
				finished: started.Add(10 * time.Minute).Format(deliveryTimeFormat)})
		},
		empty: func(e *pooledEnv, org, repo string) {
			e.exec(`INSERT INTO ci_pipeline_runs SELECT * REPLACE ('deleted' AS status, `+writerLater+` AS last_synced)
FROM ci_pipeline_runs FINAL WHERE org_id = ? AND repo_id = ?`, org, repo)
		},
		compute: (*pooledEnv).computePipelineDay,
	})
	return out
}

func TestPooledWritersWriteNoMarkerUnderANewerStoredRow(t *testing.T) {
	for index, c := range newerRowCases() {
		t.Run(c.name, func(t *testing.T) {
			e := startPooledEnv(t)
			org := e.newOrg()
			repo := fmt.Sprintf("14148000-0000-4000-8000-0000000003%d1", index)
			day := deliveryDay1.Format(time.DateOnly)
			e.insertRepo(org, repo)
			c.seed(e, org, repo)
			c.compute(e, org, repo, day)
			if rows, retracted := e.keyRows(c.table, org, repo, day); rows != 1 || retracted != 0 {
				t.Fatalf("after the first compute the key has %d rows, retracted %d; want one live row", rows, retracted)
			}
			// A newer run wrote a measured row an hour after this run's snapshot; then this run reads no sample.
			c.empty(e, org, repo)
			e.exec(fmt.Sprintf(`INSERT INTO %s (org_id, repo_id, day, computed_at) VALUES (?, ?, ?, ?)`, c.table),
				org, repo, day, time.Now().UTC().Add(time.Hour).Truncate(time.Second))
			writerTick()
			c.compute(e, org, repo, day)
			rows, retracted := e.keyRows(c.table, org, repo, day)
			var markers uint64
			if err := e.conn.QueryRow(e.ctx, fmt.Sprintf(`SELECT countIf(retracted = 1) FROM %s WHERE org_id = ? AND repo_id = ? AND day = ?`, c.table),
				org, repo, day).Scan(&markers); err != nil {
				t.Fatal(err)
			}
			t.Logf("NEWER14148 %-16s rows=%d markers=%d newest retracted=%d", c.name, rows, markers, retracted)
			if markers != 0 {
				t.Errorf("a key whose stored row is newer than the run's snapshot got %d marker row(s), want none", markers)
			}
			if retracted != 0 {
				t.Errorf("an older empty compute retired a newer measured row")
			}
		})
	}
}
