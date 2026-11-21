//go:build integration

package aianalytics

import "testing"

// Two stored versions of one testops_test_metrics_daily day written in the same computed_at second serve one value
// whatever the insert order across separate statements (same key, same rows, table truncated between scenarios; the table collapses same-key rows of ONE insert block at insert time, so only separate statements keep two versions); a version
// from a newer second still wins over the tiebreak. CHAOS-14286.
func TestFlakyScoreSameSecondVersionsServeOneValue(t *testing.T) {
	ctx, conn, client := startStore(t)
	if err := conn.Exec(ctx, `SYSTEM STOP MERGES testops_test_metrics_daily`); err != nil {
		t.Fatal(err)
	}
	const repo = "14286000-0000-4000-8000-0000000000a1"
	const same = "2026-11-19 12:00:00"
	row := func(flake int, at string) string {
		rate, n := "0", "0"
		if flake > 0 {
			rate, n = "0.2", "20"
		}
		return `SELECT toUUID('` + repo + `'), '` + org1 + `', today() - 2, toUInt32(100), toFloat64(` + rate + `), toUInt32(` + n + `), toUInt32(100), toDateTime('` + at + `', 'UTC')`
	}
	// scenario truncates, runs the inserts (one statement each) and reports whether the repository is served.
	scenario := func(statements ...string) bool {
		t.Helper()
		if err := conn.Exec(ctx, `TRUNCATE TABLE testops_test_metrics_daily`); err != nil {
			t.Fatal(err)
		}
		for _, v := range statements {
			if err := conn.Exec(ctx, `INSERT INTO testops_test_metrics_daily (repo_id, org_id, day, total_cases, flake_rate, flake_cases, distinct_cases, computed_at) `+v); err != nil {
				t.Fatal(err)
			}
		}
		got, err := loadFlaky(ctx, client, org1, "")
		if err != nil {
			t.Fatal(err)
		}
		return len(got) == 1 && got[0].repoID == repo
	}
	flakyFirst := scenario(row(20, same), row(0, same))
	calmFirst := scenario(row(0, same), row(20, same))
	if calmFirst != flakyFirst {
		t.Errorf("same rows, same second, served: flaky inserted first=%v, calm inserted first=%v; want one value", flakyFirst, calmFirst)
	}
	if scenario(row(20, same), row(0, "2026-11-19 12:00:01")) {
		t.Errorf("older flaky + newer calm served: the newer second must win over the tiebreak")
	}
	if !scenario(row(0, same), row(20, "2026-11-19 12:00:01")) {
		t.Errorf("older calm + newer flaky not served: the newer second must win over the tiebreak")
	}
}
