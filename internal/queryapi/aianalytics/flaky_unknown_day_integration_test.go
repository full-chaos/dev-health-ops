//go:build integration

package aianalytics

import (
	"strings"
	"testing"
)

// A repo-day with no measured case (its flake measures are unknown) stores NULL
// in flake_rate. The flaky score weighs the known days only: the unknown day's
// cases join neither side of the weighted rate, and a repository whose every day
// is unknown has no score at all (no opportunity), never a score of 0.
func TestFlakyScoreSkipsAnUnknownDay(t *testing.T) {
	ctx, conn, client := startStore(t)
	seed := func(repo string, daysAgo int, flake string) {
		t.Helper()
		exec(t, ctx, conn, `INSERT INTO testops_test_metrics_daily (repo_id, day, total_cases, flake_rate, org_id, computed_at)
            SELECT '%s', today() - %d, 100, %s, '%s', now()`, repo, daysAgo, flake, org1)
	}
	seed(rB, 1, "0.2")
	seed(rB, 2, "NULL")
	seed(rA, 1, "NULL")
	seed(rA, 2, "NULL")

	got, err := loadFlaky(ctx, client, org1, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].repoID != rB {
		t.Fatalf("flaky opportunities = %+v, want one for %s (the all-unknown %s has no score)", got, rB, rA)
	}
	if want := "flaked at 20.0% across 100 executions"; !strings.Contains(got[0].why, want) {
		t.Errorf("rationale = %q, want %q: the unknown day must add nothing to either side", got[0].why, want)
	}
}
