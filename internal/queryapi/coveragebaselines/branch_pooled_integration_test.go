//go:build integration

package coveragebaselines

import (
	"context"
	"math"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"
	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graphqldate"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// CHAOS-14219: the branch baseline is the branches covered over the branches total of the stored counts of the window,
// never the mean of the stored per-day percents. A day with no counts adds nothing.
func TestBranchBaselineIsPooledOverStoredCounts_RealClickHouse(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	ch, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse: %v", err)
	}
	t.Cleanup(func() { _ = ch.Close(context.Background()) })
	chschema.Apply(ctx, t, ch)
	options, err := stdclickhouse.ParseDSN(ch.URI)
	if err != nil {
		t.Fatal(err)
	}
	admin, err := stdclickhouse.Open(options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = admin.Close() })
	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: ch.URI})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })

	const (
		org   = "org-14219"
		repoA = "14219000-0000-4000-8000-00000000000a"
		repoB = "14219000-0000-4000-8000-00000000000b"
		repoC = "14219000-0000-4000-8000-00000000000c"
	)
	end := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	computed := time.Date(2026, 9, 2, 1, 0, 0, 0, time.UTC)
	for _, r := range []struct{ id, name string }{{repoA, "acme/a"}, {repoB, "acme/b"}, {repoC, "acme/c"}} {
		if err := admin.Exec(ctx, `INSERT INTO repos (id, repo, provider, org_id, created_at, last_synced) VALUES (?, ?, ?, ?, ?, ?)`, r.id, r.name, "github", org, computed, computed); err != nil {
			t.Fatal(err)
		}
	}
	cov := func(repo string, back int, pct any, total, covered any) {
		t.Helper()
		if err := admin.Exec(ctx, `INSERT INTO testops_coverage_metrics_daily
			(repo_id, day, branch_coverage_pct, branches_total, branches_covered, uncovered_files_count, coverage_regression_count, org_id, computed_at)
			VALUES (?, ?, ?, ?, ?, 0, 0, ?, ?)`, repo, end.AddDate(0, 0, -back), pct, total, covered, org, computed); err != nil {
			t.Fatal(err)
		}
	}
	// A: day 1 is 900 of 1,000 branches (90), days 2-7 are 10 of 100 (10). Mean of the percents 21.43; pooled 960 / 1,600.
	// An 8th day carries a percent and no counts: it adds nothing to either sum.
	cov(repoA, 1, 90.0, uint32(1000), uint32(900))
	for back := 2; back <= 7; back++ {
		cov(repoA, back, 10.0, uint32(100), uint32(10))
	}
	cov(repoA, 8, 99.0, nil, nil)
	// B: seven days with a percent and no counts (a row written before the counts): no value.
	for back := 1; back <= 7; back++ {
		cov(repoB, back, 70.0, nil, nil)
	}
	// C: seven days of 100 of 100 (100).
	for back := 1; back <= 7; back++ {
		cov(repoC, back, 100.0, uint32(100), uint32(100))
	}

	near := func(name string, got *float64, want float64) {
		t.Helper()
		if got == nil || math.Abs(*got-want) > 1e-9 {
			t.Errorf("%s = %v, want %v", name, show(got), want)
		}
	}
	all, err := Resolve(ctx, client, org, graphqldate.New(end), Scope{})
	if err != nil {
		t.Fatal(err)
	}
	byRepo := map[string]*float64{}
	days := map[string]int{}
	for _, r := range all {
		byRepo[r.RepoID] = r.BranchBaselinePct
		days[r.RepoID] = r.BranchDays
	}
	if days[repoA] != 7 || days[repoB] != 0 || days[repoC] != 7 {
		t.Errorf("branch days A=%d B=%d C=%d, want 7, 0, 7 (a day with no counts is not a day with a value)", days[repoA], days[repoB], days[repoC])
	}
	t.Logf("BRANCHCELL baselines A=%v B=%v C=%v (pooled want A=%v B=nil C=100)", show(byRepo[repoA]), show(byRepo[repoB]), show(byRepo[repoC]), 960.0/1600*100)
	near("A branch baseline", byRepo[repoA], 960.0/1600*100)
	if byRepo[repoB] != nil {
		t.Errorf("B branch baseline = %v, want none (no row carries counts)", *byRepo[repoB])
	}

	// Scope of A and C: the pooled value of the window is (960 + 700) / (1,600 + 700).
	scope, err := ResolveScope(ctx, client, org, graphqldate.New(end), Scope{RepoIDs: []string{repoA, repoC}})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("BRANCHCELL scope=%v (pooled want %v)", show(scope.BranchBaselinePct), 1660.0/2300*100)
	near("scope branch baseline", scope.BranchBaselinePct, 1660.0/2300*100)
	if scope.BranchDays != 7 {
		t.Errorf("scope branch days = %d, want 7", scope.BranchDays)
	}
	// B alone: every row has a percent and no counts, so no value over no day.
	onlyB, err := ResolveScope(ctx, client, org, graphqldate.New(end), Scope{RepoIDs: []string{repoB}})
	if err != nil {
		t.Fatal(err)
	}
	if onlyB.BranchBaselinePct != nil || onlyB.BranchDays != 0 {
		t.Errorf("scope of B = %v over %d days, want no value over 0 days", show(onlyB.BranchBaselinePct), onlyB.BranchDays)
	}
}
