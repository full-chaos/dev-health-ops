//go:build integration

package providersync

import (
	"testing"
	"time"
)

// TestRunInProcessRunsTheChunkedTestsRouteWithoutAWorker (CHAOS-6711): the github
// `cicd` route (the canonical route of the `tests` alias too) is chunked, so the worker gives it a durable chunk store; in
// process the executor gets InProcessChunkLedger. The route's own rows land in a
// real ClickHouse (the report artifact is parsed into runs, jobs, acceptance
// checks, suites, cases and coverage), the executor's own count of committed rows
// is what landed, and a second run converges on the same logical rows.
func TestRunInProcessRunsTheChunkedTestsRouteWithoutAWorker(t *testing.T) {
	ctx, sink := newGitHubTestsIntegrationSink(t)
	archive := githubTestsZip(t, map[string]string{
		"reports/good.xml":  githubTestsJUnitFixture,
		"reports/good.info": githubTestsLCOVFixture,
	})
	since := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	run := InProcessRun{
		OrgID: "org-inprocess", Provider: "github", Dataset: "cicd",
		SourceExternalID: "acme/api", SourceName: "acme/api", SinceAt: &since,
		BeforeAt: time.Date(2026, 7, 31, 23, 59, 59, 0, time.UTC), Credential: map[string]string{"token": "cli-token"},
		Config: map[string]string{"base_url": "https://api.github.com"}, Conn: sink.Conn,
		Doer: &githubTestsRouteDoer{t: t, archive: archive},
	}
	counts := func() map[string]uint64 {
		t.Helper()
		out := map[string]uint64{}
		for _, table := range []string{"ci_pipeline_runs", "ci_job_runs", "ci_acceptance_checks", "test_suite_results", "test_case_results", "coverage_snapshots"} {
			var count uint64
			if err := sink.Conn.QueryRow(ctx, "SELECT count() FROM "+table+" FINAL WHERE org_id = ?", run.OrgID).Scan(&count); err != nil {
				t.Fatal(err)
			}
			out[table] = count
		}
		return out
	}
	result, err := RunInProcess(ctx, run)
	if err != nil {
		t.Fatalf("RunInProcess: %v", err)
	}
	first := counts()
	total := uint64(0)
	for table, count := range first {
		if count == 0 && table != "ci_acceptance_checks" {
			t.Errorf("%s: the route wrote no row", table)
		}
		total += count
	}
	if result.Effects.Written == 0 || uint64(result.CommittedRows) != total {
		t.Fatalf("result = %+v, rows in the tables = %d: the executor's own count of committed rows must match what landed", result, total)
	}
	// A second run is a new claim with a new (empty) in-process ledger: it writes its
	// rows again under its own generation and ReplacingMergeTree converges them, so
	// the logical rows (FINAL) are the same ones.
	if _, err := RunInProcess(ctx, run); err != nil {
		t.Fatalf("second run: %v", err)
	}
	for table, count := range counts() {
		if count != first[table] {
			t.Errorf("%s: %d logical rows after the second run, %d after the first", table, count, first[table])
		}
	}
}
