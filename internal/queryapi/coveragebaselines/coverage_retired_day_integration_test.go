//go:build integration

package coveragebaselines

import (
	"context"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"
	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graphqldate"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// CHAOS-14008 C3 (coverage baselines): a day whose newest row retires it is not a day of the window, and a repository whose
// every day is retired has no baseline row (it was a row with 0 days before).
func TestBaselinesLeaveOutARetiredDay(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
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
		org   = "org-14008"
		repoA = "14008000-0000-4000-8000-00000000000a"
		repoB = "14008000-0000-4000-8000-00000000000b"
	)
	end := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	t1 := time.Date(2026, 9, 2, 1, 0, 0, 0, time.UTC)
	t2 := t1.Add(time.Hour)
	for _, repo := range []string{repoA, repoB} {
		if err := admin.Exec(ctx, `INSERT INTO repos (id, repo, provider, org_id, created_at, last_synced) VALUES (?, ?, 'github', ?, ?, ?)`,
			repo, "acme/"+repo[len(repo)-1:], org, t1, t1); err != nil {
			t.Fatal(err)
		}
	}
	measured := func(repo string, back int) {
		t.Helper()
		if err := admin.Exec(ctx, `INSERT INTO testops_coverage_metrics_daily (repo_id, day, line_coverage_pct, lines_covered, lines_total, branch_coverage_pct,
 uncovered_files_count, coverage_regression_count, org_id, computed_at) VALUES (?, ?, 80, 80, 100, 70, 0, 0, ?, ?)`,
			repo, end.AddDate(0, 0, -back), org, t1); err != nil {
			t.Fatal(err)
		}
	}
	retire := func(repo string, back int) {
		t.Helper()
		if err := admin.Exec(ctx, `INSERT INTO testops_coverage_metrics_daily (repo_id, day, uncovered_files_count,
 coverage_regression_count, org_id, computed_at, retracted) VALUES (?, ?, 0, 0, ?, ?, 1)`,
			repo, end.AddDate(0, 0, -back), org, t2); err != nil {
			t.Fatal(err)
		}
	}
	for back := 1; back <= 10; back++ {
		measured(repoA, back)
	}
	retire(repoA, 2)
	retire(repoA, 3)
	measured(repoB, 1)
	retire(repoB, 1)

	rows, err := Resolve(ctx, client, org, graphqldate.New(end), Scope{})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("C3CELL baselines: %d row(s): %+v", len(rows), rows)
	if len(rows) != 1 || rows[0].RepoID != repoA {
		t.Fatalf("%d baseline row(s), want repository A only (repository B has only a retired day)", len(rows))
	}
	if rows[0].LineDays != 8 || rows[0].LineBaselinePct == nil || *rows[0].LineBaselinePct != 80 {
		t.Fatalf("A line = %v over %d days, want 80 over 8 (10 days less the 2 retired)", rows[0].LineBaselinePct, rows[0].LineDays)
	}
}
