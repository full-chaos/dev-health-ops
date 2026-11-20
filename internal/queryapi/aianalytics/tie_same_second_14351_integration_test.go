//go:build integration

package aianalytics

import (
	"fmt"
	"testing"
)

const tieRepo = "14351000-0000-4000-8000-0000000000a1"

// served runs loadFlowRows and returns the served repo row of tieRepo.
func tieServed(t *testing.T, reload func() ([]flowRow, error)) flowRow {
	t.Helper()
	rows, err := reload()
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range rows {
		if r.EntityID == tieRepo {
			return r
		}
	}
	t.Fatal("repository not served")
	return flowRow{}
}

// Two stored versions of each repo_metrics_daily day written in the same computed_at second serve one first-review
// median whatever the insert order across separate statements (same-key rows of ONE insert block collapse at insert);
// a version from a newer second still wins over the tiebreak. CHAOS-14351 site 3.
func TestFlowRepoMetricsSameSecondVersionsServeOneValue(t *testing.T) {
	ctx, conn, client := startStore(t)
	exec(t, ctx, conn, `SYSTEM STOP MERGES repo_metrics_daily`)
	const same = "2026-11-19 12:00:00"
	version := func(p50 float64, at string) []string {
		var out []string
		for d := 1; d <= 5; d++ {
			out = append(out, fmt.Sprintf(`INSERT INTO repo_metrics_daily (repo_id, org_id, day, pr_first_review_p50_hours, computed_at) SELECT toUUID('%s'), '%s', today() - %d, %v, toDateTime('%s', 'UTC')`, tieRepo, org1, d, p50, at))
		}
		return out
	}
	scenario := func(groups ...[]string) float64 {
		t.Helper()
		exec(t, ctx, conn, `TRUNCATE TABLE repo_metrics_daily`)
		for _, g := range groups {
			for _, s := range g {
				exec(t, ctx, conn, "%s", s)
			}
		}
		r := tieServed(t, func() ([]flowRow, error) {
			repos, _, err := loadFlowRows(ctx, client, org1, "", "", 30)
			return repos, err
		})
		if r.ReviewP50 == nil {
			t.Fatal("no first-review median served")
		}
		return *r.ReviewP50
	}
	slowFirst := scenario(version(40, same), version(2, same))
	fastFirst := scenario(version(2, same), version(40, same))
	if slowFirst != fastFirst {
		t.Errorf("same rows, same second, served: slow inserted first=%v, fast inserted first=%v; want one value", slowFirst, fastFirst)
	}
	if got := scenario(version(40, same), version(2, "2026-11-19 12:00:01")); got != 2 {
		t.Errorf("older 40 + newer 2 served %v: the newer second must win over the tiebreak", got)
	}
	if got := scenario(version(2, same), version(40, "2026-11-19 12:00:01")); got != 40 {
		t.Errorf("older 2 + newer 40 served %v: the newer second must win over the tiebreak", got)
	}
}

// Same for repo_change_failure_daily (computed_at DateTime64(3): the same millisecond). CHAOS-14351 site 4.
func TestFlowChangeFailureSameInstantVersionsServeOneValue(t *testing.T) {
	ctx, conn, client := startStore(t)
	exec(t, ctx, conn, `SYSTEM STOP MERGES repo_change_failure_daily`)
	const same = "2026-11-19 12:00:00.000"
	// the window needs five repository days of repo_metrics_daily
	for d := 1; d <= 5; d++ {
		exec(t, ctx, conn, `INSERT INTO repo_metrics_daily (repo_id, org_id, day, pr_first_review_p50_hours, computed_at) SELECT toUUID('%s'), '%s', today() - %d, 3, now()`, tieRepo, org1, d)
	}
	version := func(failed int, at string) []string {
		var out []string
		for d := 1; d <= 5; d++ {
			out = append(out, fmt.Sprintf(`INSERT INTO repo_change_failure_daily (org_id, repo_id, day, deployments_count, failed_deployments_native, incidents_direct, computed_at) SELECT '%s', toUUID('%s'), today() - %d, 10, %d, 1, toDateTime64('%s', 3, 'UTC')`, org1, tieRepo, d, failed, at))
		}
		return out
	}
	scenario := func(groups ...[]string) float64 {
		t.Helper()
		exec(t, ctx, conn, `TRUNCATE TABLE repo_change_failure_daily`)
		for _, g := range groups {
			for _, s := range g {
				exec(t, ctx, conn, "%s", s)
			}
		}
		r := tieServed(t, func() ([]flowRow, error) {
			repos, _, err := loadFlowRows(ctx, client, org1, "", "", 30)
			return repos, err
		})
		if r.ChangeFailure == nil {
			t.Fatal("no change failure rate served")
		}
		return *r.ChangeFailure
	}
	failFirst := scenario(version(5, same), version(0, same))
	cleanFirst := scenario(version(0, same), version(5, same))
	if failFirst != cleanFirst {
		t.Errorf("same rows, same instant, served: failing inserted first=%v, clean inserted first=%v; want one value", failFirst, cleanFirst)
	}
	if got := scenario(version(5, same), version(0, "2026-11-19 12:00:00.001")); got != 0 {
		t.Errorf("older failing + newer clean served %v: the newer computed_at must win over the tiebreak", got)
	}
	if got := scenario(version(0, same), version(5, "2026-11-19 12:00:00.001")); got != 0.5 {
		t.Errorf("older clean + newer failing served %v: the newer computed_at must win over the tiebreak", got)
	}
}
