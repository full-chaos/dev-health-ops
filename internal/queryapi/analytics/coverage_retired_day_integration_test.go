//go:build integration

package analytics

import (
	"context"
	"fmt"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// CHAOS-14008 C3 (analytics time series): the measure source of testops_coverage_metrics_daily leaves out a day whose newest row
// retires it. uncovered_files_count is not Nullable, so the marker row's 0 would be a sample of 0.
func TestCoverageMeasureSourceLeavesOutARetiredDay(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	ch, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse: %v", err)
	}
	defer func() { _ = ch.Close(context.Background()) }()
	chschema.Apply(ctx, t, ch)
	options, err := stdclickhouse.ParseDSN(ch.URI)
	if err != nil {
		t.Fatal(err)
	}
	admin, err := stdclickhouse.Open(options)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = admin.Close() }()

	const org, repo = "org-14008", "14008000-0000-4000-8000-0000000000a1"
	day1 := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	t1 := time.Date(2026, 8, 3, 1, 0, 0, 0, time.UTC)
	for _, row := range []struct {
		day       time.Time
		uncovered uint32
	}{{day1, 9}, {day1.AddDate(0, 0, 1), 4}} {
		if err := admin.Exec(ctx, `INSERT INTO testops_coverage_metrics_daily (repo_id, day, line_coverage_pct, uncovered_files_count,
 coverage_regression_count, org_id, computed_at) VALUES (toUUID(?), ?, 80, ?, 0, ?, ?)`, repo, row.day, row.uncovered, org, t1); err != nil {
			t.Fatal(err)
		}
	}
	if err := admin.Exec(ctx, `INSERT INTO testops_coverage_metrics_daily (repo_id, day, uncovered_files_count, coverage_regression_count,
 org_id, computed_at, retracted) VALUES (toUUID(?), ?, 0, 0, ?, ?, 1)`, repo, day1, org, t1.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}

	source, alias := dedupFromMeasureSource("testops_coverage_metrics_daily")
	var days uint64
	var mean float64
	query := fmt.Sprintf("SELECT count(), avg(%s.uncovered_files_count) FROM %s WHERE %s.org_id = ?", alias, source, alias)
	if err := admin.QueryRow(ctx, query, org).Scan(&days, &mean); err != nil {
		t.Fatalf("%v\n%s", err, query)
	}
	t.Logf("C3CELL analytics source: %d day(s), mean uncovered_files_count %v", days, mean)
	if days != 1 || mean != 4 {
		t.Fatalf("the source holds %d day(s) with mean %v, want the one live day (4) and no sample of 0", days, mean)
	}
}
