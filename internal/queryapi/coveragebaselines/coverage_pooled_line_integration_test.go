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

// Line coverage baselines pool the stored line counts: covered lines over total lines across the counted repo-days of
// the window. A repo-day without both counts, or with no lines, is in neither sum and is not a counted day; a window of
// only such repo-days has no value, never 0.
func TestBaselinesPoolTheLineCounts(t *testing.T) {
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
		org   = "org-14237"
		repoA = "14237000-0000-4000-8000-00000000000a"
		repoB = "14237000-0000-4000-8000-00000000000b"
		repoC = "14237000-0000-4000-8000-00000000000c"
	)
	end := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	older := time.Date(2026, 9, 2, 1, 0, 0, 0, time.UTC)
	newer := time.Date(2026, 9, 2, 2, 0, 0, 0, time.UTC)
	exec := func(statement string, args ...any) {
		t.Helper()
		if err := admin.Exec(ctx, statement, args...); err != nil {
			t.Fatalf("seed: %v\n%s", err, statement)
		}
	}
	// The table is a ReplacingMergeTree: a background merge could drop the older versions before the read, and the
	// version cells (older counted row under a newer row without counts) would then pin nothing.
	exec(`SYSTEM STOP MERGES testops_coverage_metrics_daily`)
	for repo, name := range map[string]string{repoA: "acme/alpha", repoB: "acme/beta", repoC: "acme/gamma"} {
		exec(`INSERT INTO repos (id, repo, provider, org_id, created_at, last_synced) VALUES (?, ?, 'github', ?, ?, ?)`, repo, name, org, older, older)
	}
	coverage := func(repo string, back int, pct, covered, total any, computed time.Time) {
		t.Helper()
		exec(`INSERT INTO testops_coverage_metrics_daily
			(repo_id, day, line_coverage_pct, lines_covered, lines_total, uncovered_files_count, coverage_regression_count, org_id, computed_at)
			VALUES (?, ?, ?, ?, ?, 0, 0, ?, ?)`, repo, end.AddDate(0, 0, -back), pct, covered, total, org, computed)
	}

	// A: five days of 900/1000 and five of 10/100. The mean of the day percents is 50; pooled it is 4550/5500.
	for back := 1; back <= 5; back++ {
		coverage(repoA, back, 90.0, uint32(900), uint32(1000), newer)
		coverage(repoA, back+5, 10.0, uint32(10), uint32(100), newer)
	}
	coverage(repoA, 1, 0.0, uint32(0), uint32(1000), older)       // an older version of a day: the newer counts win
	coverage(repoA, 11, 50.0, nil, nil, newer)                    // a percent without counts: in neither sum, not a day
	coverage(repoA, 12, 0.0, uint32(0), uint32(0), newer)         // no lines: in neither sum, not a day
	coverage(repoA, 13, 100.0, uint32(1000), uint32(1000), older) // counted only in an older version...
	coverage(repoA, 13, 100.0, nil, nil, newer)                   // ...whose newer version has no counts
	coverage(repoA, 14, nil, nil, uint32(1000), newer)            // a total without covered lines: not 0 covered
	coverage(repoA, 15, nil, uint32(5), nil, newer)               // covered lines without a total
	coverage(repoA, 31, 0.0, uint32(0), uint32(1000), newer)      // one day before the window
	coverage(repoA, 0, 0.0, uint32(0), uint32(1000), newer)       // the end day itself: not in the window
	// B: four days of 30/100 and four of 270/300. The mean is 60; pooled it is 1200/1600 = 75.
	for back := 1; back <= 4; back++ {
		coverage(repoB, back, 30.0, uint32(30), uint32(100), newer)
		coverage(repoB, back+4, 90.0, uint32(270), uint32(300), newer)
	}
	// C: eight days of a percent without counts. No value, over 0 counted days.
	for back := 1; back <= 8; back++ {
		coverage(repoC, back, 70.0, nil, nil, newer)
	}

	near := func(got *float64, want float64) bool { return got != nil && math.Abs(*got-want) < 1e-9 }
	rows, err := Resolve(ctx, client, org, graphqldate.New(end), Scope{})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("POOLCELL per-repository: %+v", rows)
	if len(rows) != 3 || rows[0].RepoID != repoA || rows[1].RepoID != repoB || rows[2].RepoID != repoC {
		t.Fatalf("%d rows, want A, B and C in order: %+v", len(rows), rows)
	}
	if a := rows[0]; !near(a.LineBaselinePct, 100*4550.0/5500.0) || a.LineDays != 10 {
		t.Errorf("A line = %v over %d days, want 100*4550/5500 over 10 counted days", show(a.LineBaselinePct), a.LineDays)
	}
	if b := rows[1]; !near(b.LineBaselinePct, 75) || b.LineDays != 8 {
		t.Errorf("B line = %v over %d days, want 75 over 8 counted days", show(b.LineBaselinePct), b.LineDays)
	}
	if c := rows[2]; c.LineBaselinePct != nil || c.LineDays != 0 {
		t.Errorf("C line = %v over %d days, want no value over 0 counted days", show(c.LineBaselinePct), c.LineDays)
	}

	// The scope is one ratio over every counted repo-day, not a mean of day values; its days are the days on which at
	// least one repository is counted.
	whole, err := ResolveScope(ctx, client, org, graphqldate.New(end), Scope{})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("POOLCELL scope: %+v", whole)
	if !near(whole.LineBaselinePct, 100*(4550.0+1200.0)/(5500.0+1600.0)) || whole.LineDays != 10 {
		t.Errorf("scope line = %v over %d days, want 100*5750/7100 over 10 days", show(whole.LineBaselinePct), whole.LineDays)
	}
	gamma, err := ResolveScope(ctx, client, org, graphqldate.New(end), Scope{RepoIDs: []string{"acme/gamma"}})
	if err != nil {
		t.Fatal(err)
	}
	if gamma.LineBaselinePct != nil || gamma.LineDays != 0 {
		t.Errorf("scope of C only: line = %v over %d days, want no value over 0 days", show(gamma.LineBaselinePct), gamma.LineDays)
	}
}
