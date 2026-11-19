//go:build integration

package report

import (
	"context"
	"fmt"
	"testing"
	"time"

	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// CHAOS-14008 C3 (report): the chart source of testops_coverage_metrics_daily leaves out a day whose newest row retires it.
func TestCoverageChartSourceLeavesOutARetiredDay(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	ch, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse: %v", err)
	}
	defer func() { _ = ch.Close(context.Background()) }()
	chschema.Apply(ctx, t, ch)
	admin, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(ch.URI))
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()

	const org, repo = "org-14008", "14008000-0000-4000-8000-0000000000a1"
	day1 := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	t1 := time.Date(2026, 8, 3, 1, 0, 0, 0, time.UTC)
	for _, day := range []time.Time{day1, day1.AddDate(0, 0, 1)} {
		if err := admin.Exec(ctx, `INSERT INTO testops_coverage_metrics_daily (repo_id, day, line_coverage_pct, uncovered_files_count,
 coverage_regression_count, org_id, computed_at) VALUES (toUUID(?), ?, 80, 3, 0, ?, ?)`, repo, day, org, t1); err != nil {
			t.Fatal(err)
		}
	}
	if err := admin.Exec(ctx, `INSERT INTO testops_coverage_metrics_daily (repo_id, day, uncovered_files_count, coverage_regression_count,
 org_id, computed_at, retracted) VALUES (toUUID(?), ?, 0, 0, ?, ?, 1)`, repo, day1, org, t1.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}

	var days uint64
	query := fmt.Sprintf("SELECT count() FROM %s WHERE org_id = ?", dedupFromSource("testops_coverage_metrics_daily"))
	if err := admin.QueryRow(ctx, query, org).Scan(&days); err != nil {
		t.Fatalf("%v\n%s", err, query)
	}
	t.Logf("C3CELL report source: %d day(s)", days)
	if days != 1 {
		t.Fatalf("the chart source holds %d day(s), want the one live day", days)
	}
}
