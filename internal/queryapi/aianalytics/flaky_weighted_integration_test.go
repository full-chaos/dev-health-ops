//go:build integration

package aianalytics

import (
	"strings"
	"testing"
)

const (
	rC = "33333333-3333-3333-3333-333333333333"
	rD = "44444444-4444-4444-4444-444444444444"
)

// The flaky card says the share of DISTINCT test cases that flaked, the same rate as the TestOps Tests tab
// (SUM(flake_cases) / SUM(distinct_cases)), never the stored daily rate weighted by executions.
//
//	day A  1,000 executions, 10 distinct cases, 5 flaked   (stored rate 0.5)
//	day B     10 executions, 10 distinct cases, 0 flaked   (stored rate 0.0)
//	day C  a day that measured no case (rate NULL, counts 0/0)
//
// The weighted-by-executions rate is (0.5 x 1,000 + 0 x 10) / 1,010 = 49.5%; the rule is 5 / 20 = 25.0%.
func TestFlakyCardIsTheShareOfDistinctCasesThatFlaked(t *testing.T) {
	ctx, conn, client := startStore(t)
	seed := func(repo string, daysAgo, total int, rate, flake, distinct string) {
		t.Helper()
		exec(t, ctx, conn, `INSERT INTO testops_test_metrics_daily (repo_id, day, total_cases, flake_rate, flake_cases, distinct_cases, org_id, computed_at)
            SELECT '%s', today() - %d, %d, %s, %s, %s, '%s', now()`, repo, daysAgo, total, rate, flake, distinct, org1)
	}
	seed(rA, 3, 1000, "0.5", "5", "10")
	seed(rA, 2, 10, "0.0", "0", "10")
	seed(rA, 1, 0, "NULL", "0", "0")

	got, err := loadFlaky(ctx, client, org1, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].repoID != rA {
		t.Fatalf("flaky opportunities = %+v, want one for %s", got, rA)
	}
	if want := "25.0% of distinct cases across 1010 executions"; !strings.Contains(got[0].why, want) {
		t.Errorf("rationale = %q, want %q (the stored rate weighted by executions would say 49.5%%)", got[0].why, want)
	}
	if !strings.Contains(got[0].why, "appear") {
		t.Errorf("rationale = %q, want the hedged wording (appears to flake)", got[0].why)
	}
	if len(got[0].refs) != 1 || got[0].refs[0] != "testops_test_metrics_daily:flake_rate:"+rA {
		t.Errorf("evidence refs = %v, want the unchanged key for %s", got[0].refs, rA)
	}
}

// Rows written before the case counts existed have NULL counts: the repository has no rate (all or nothing, as the
// series), never a rate over the rows that happen to have counts. A repository whose distinct cases sum to 0 has no
// rate either. The threshold applies to the new rate: 4.9% is no card, 5.0% is.
func TestFlakyCardNeedsCountsAndAppliesTheThresholdToTheNewRate(t *testing.T) {
	ctx, conn, client := startStore(t)
	seed := func(repo string, daysAgo, total int, flake, distinct string) {
		t.Helper()
		exec(t, ctx, conn, `INSERT INTO testops_test_metrics_daily (repo_id, day, total_cases, flake_rate, flake_cases, distinct_cases, org_id, computed_at)
            SELECT '%s', today() - %d, %d, 0.9, %s, %s, '%s', now()`, repo, daysAgo, total, flake, distinct, org1)
	}
	// rA: one row has counts, one row has none: no card although the stored rate (0.9) and the counts of the
	// first row would qualify.
	seed(rA, 1, 1000, "500", "1000")
	seed(rA, 2, 1000, "NULL", "NULL")
	// rB: 49 of 1,000 distinct cases flaked: 4.9%, below the 5% threshold although the stored rate is 0.9.
	seed(rB, 1, 1000, "49", "1000")
	// rC: 50 of 1,000: exactly 5.0%.
	seed(rC, 1, 1000, "50", "1000")
	// rD: enough executions, no distinct case at all: nothing to divide by, no card.
	seed(rD, 1, 1000, "0", "0")

	got, err := loadFlaky(ctx, client, org1, "")
	if err != nil {
		t.Fatal(err)
	}
	var repos []string
	for _, o := range got {
		repos = append(repos, o.repoID)
	}
	if len(got) != 1 || got[0].repoID != rC {
		t.Fatalf("flaky opportunities for %v, want only %s (50 of 1,000 = 5.0%%)", repos, rC)
	}
}

// Fewer than the minimum executions of the days that measured cases is no card, whatever the share; the repository
// filter and the organization filter still apply.
func TestFlakyCardKeepsItsMinimumExecutionsAndScope(t *testing.T) {
	ctx, conn, client := startStore(t)
	seed := func(org, repo string, total int, flake, distinct string) {
		t.Helper()
		exec(t, ctx, conn, `INSERT INTO testops_test_metrics_daily (repo_id, day, total_cases, flake_rate, flake_cases, distinct_cases, org_id, computed_at)
            SELECT '%s', today() - 1, %d, 0.5, %s, %s, '%s', now()`, repo, total, flake, distinct, org)
	}
	seed(org1, rA, 20, "10", "20")
	seed(org1, rB, 200, "100", "200")
	seed("other-org", rC, 200, "100", "200")

	all, err := loadFlaky(ctx, client, org1, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 1 || all[0].repoID != rB {
		t.Fatalf("org read = %+v, want only %s (%s has 20 executions, another organization's repository is out)", all, rB, rA)
	}
	one, err := loadFlaky(ctx, client, org1, rB)
	if err != nil || len(one) != 1 || one[0].repoID != rB {
		t.Fatalf("repo read = %+v, %v", one, err)
	}
	none, err := loadFlaky(ctx, client, org1, rA)
	if err != nil || len(none) != 0 {
		t.Fatalf("repo read of the small repository = %+v, %v; want none", none, err)
	}
}
