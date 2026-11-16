//go:build integration

package providersync

import "testing"

func TestGitLabTestsNonFiniteSuiteTimeStoresOtherSuitesAndRuns(t *testing.T) {
	ctx, sink := newGitHubTestsIntegrationSink(t)
	claim := nativeTestClaim("gitlab", "tests")
	walk := nonFiniteCollect(t, "collect", nonFiniteReportBody("NaN", "0.5"))
	for _, effect := range walk.batch.Effects {
		if err := sink.WriteEffect(ctx, claim, effect); err != nil {
			t.Fatalf("%s: %v", effect.Destination, err)
		}
	}
	var suites, nullTimes, runs uint64
	if err := sink.Conn.QueryRow(ctx, `SELECT count(),countIf(isNull(duration_seconds)) FROM test_suite_results FINAL WHERE org_id=? AND run_id='9001'`, claim.OrgID).Scan(&suites, &nullTimes); err != nil {
		t.Fatal(err)
	}
	if suites != 4 || nullTimes != 1 {
		t.Fatalf("suites=%d NULL times=%d, want 4 and 1", suites, nullTimes)
	}
	if err := sink.Conn.QueryRow(ctx, `SELECT count() FROM ci_pipeline_runs FINAL WHERE org_id=? AND run_id='9001'`, claim.OrgID).Scan(&runs); err != nil {
		t.Fatal(err)
	}
	if runs != 1 {
		t.Fatalf("pipeline runs=%d, want 1", runs)
	}
}
