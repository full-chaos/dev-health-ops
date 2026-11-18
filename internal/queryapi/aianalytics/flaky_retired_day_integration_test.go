//go:build integration

package aianalytics

import (
	"strings"
	"testing"
)

// A day whose newest testops_test_metrics_daily row retires it (retracted = 1, as the daily writer stores it over a day that
// counts no kept suite any more) is not a day of the flaky window: its older row adds no case, and its marker row (NULL flake
// counts) does not make the repository's rate unavailable. The score is the live day's alone.
func TestFlakyScoreReadsARetiredDayAsAbsent(t *testing.T) {
	ctx, conn, client := startStore(t)
	const repo = "13928000-0000-4000-8000-0000000000c1"
	seed := func(statement string) {
		t.Helper()
		if err := conn.Exec(ctx, statement, repo, org1); err != nil {
			t.Fatalf("seed: %v\n%s", err, statement)
		}
	}
	seed(`INSERT INTO testops_test_metrics_daily (repo_id, org_id, day, total_cases, flake_rate, flake_cases, distinct_cases, computed_at)
  SELECT ?, ?, today() - 3, 100, 0, 0, 100, now() - INTERVAL 2 HOUR`)
	seed(`INSERT INTO testops_test_metrics_daily (repo_id, org_id, day, computed_at, retracted)
  SELECT ?, ?, today() - 3, now() - INTERVAL 1 HOUR, 1`)
	seed(`INSERT INTO testops_test_metrics_daily (repo_id, org_id, day, total_cases, flake_rate, flake_cases, distinct_cases, computed_at)
  SELECT ?, ?, today() - 2, 100, 0.2, 20, 100, now() - INTERVAL 2 HOUR`)

	got, err := loadFlaky(ctx, client, org1, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].repoID != repo {
		t.Fatalf("flaky opportunities = %+v, want one for %s (the retired day must not blank its rate)", got, repo)
	}
	if want := "flake at 20.0% of distinct cases across 100 executions"; !strings.Contains(got[0].why, want) {
		t.Errorf("rationale = %q, want %q: the retired day adds nothing to either side", got[0].why, want)
	}
}
