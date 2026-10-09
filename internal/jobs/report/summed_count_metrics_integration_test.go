//go:build integration

package report

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"testing"
	"time"

	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// isSummedMetric is the rule buildChartQuery applies: a name that ends in
// _count, or a registry unit of count, is charted with sum().
func isSummedMetric(name string, definition metricDefinition) bool {
	return strings.HasSuffix(name, "_count") || definition.Unit == "count"
}

// TestClickHouseQueryAdapterChartsEverySummedMetric charts EVERY summed
// (count) metric of the embedded registry through the real report reader on
// the schema of the migration chain. sum() of an unsigned integer column is
// UInt64 in ClickHouse; a chart read that scans it into a float refuses the
// row and the whole report fails. One metric per source table is not enough:
// the column types differ by table, so each metric is charted on its own.
//
// Each metric gets one row in its own table holding a distinct value, and both
// chart shapes the reader has (scorecard total, and a line by day) must return
// that value.
func TestClickHouseQueryAdapterChartsEverySummedMetric(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := instance.Close(context.Background()); err != nil {
			t.Errorf("close ClickHouse: %v", err)
		}
	}()
	chschema.Apply(ctx, t, instance)
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	const org = "org-summed-counts"
	var names []string
	for name, definition := range supportedMetrics {
		if isSummedMetric(name, definition) {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	if len(names) < 20 {
		t.Fatalf("only %d summed metrics found in the registry; the rule changed", len(names))
	}
	// buildChartWhere filters every source table on a `day` column. The
	// snapshot table file_complexity_snapshots keys on as_of_day instead, so
	// EVERY chart of its metrics fails on a missing column, summed or not: a
	// separate defect (reported apart from the scan type), listed here so the
	// gap is loud. The list fails when the table gains a `day` column.
	hasDay := func(table string) bool {
		var count uint64
		row := conn.QueryRow(ctx, "SELECT count() FROM system.columns WHERE database = currentDatabase() AND table = ? AND name = 'day'", table)
		if err := row.Scan(&count); err != nil {
			t.Fatalf("read columns of %s: %v", table, err)
		}
		return count == 1
	}
	knownWithoutDay := map[string]bool{"file_complexity_snapshots": true}
	var charted []string
	for _, name := range names {
		table := supportedMetrics[name].SourceTable
		switch {
		case hasDay(table) && knownWithoutDay[table]:
			t.Fatalf("%s now has a day column: remove it from knownWithoutDay", table)
		case !hasDay(table) && !knownWithoutDay[table]:
			t.Fatalf("%s has no day column and is not a known gap", table)
		case !hasDay(table):
			t.Logf("KNOWN GAP (separate defect): %s charts on table %s which has no day column", name, table)
		default:
			charted = append(charted, name)
		}
	}
	names = charted
	// One row per table holds every summed metric of that table: the dedup
	// source keeps the newest row per natural key, so two rows of one key
	// would hide each other's values.
	want := map[string]float64{}
	columns := map[string][]string{}
	values := map[string][]string{}
	for index, name := range names {
		table := supportedMetrics[name].SourceTable
		value := 3 + index
		want[name] = float64(value)
		columns[table] = append(columns[table], name)
		values[table] = append(values[table], fmt.Sprint(value))
	}
	for table := range columns {
		statement := fmt.Sprintf(
			"INSERT INTO %s (org_id, day, %s) VALUES ('%s', '2026-01-05', %s)",
			table, strings.Join(columns[table], ", "), org, strings.Join(values[table], ", "))
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatalf("seed %s: %v", table, err)
		}
	}

	for _, shape := range []struct{ chartType, groupBy string }{{"scorecard", ""}, {"line", "day"}} {
		for _, name := range names {
			loader := reportLoaderFunc(func(context.Context, QueryInput) (ReportDefinition, error) {
				return ReportDefinition{
					Plan: Plan{PlanID: "plan-1", ReportType: "weekly_health", OrganizationID: org},
					Charts: []ChartSpec{{
						ChartID: "chart-1", PlanID: "plan-1", ChartType: shape.chartType, GroupBy: shape.groupBy,
						Metric: name, TimeRangeStart: "2026-01-01", TimeRangeEnd: "2026-01-31",
						OrganizationID: org,
					}},
				}, nil
			})
			adapter, err := NewClickHouseQueryAdapter(loader, conn)
			if err != nil {
				t.Fatal(err)
			}
			result, err := adapter.Query(ctx, QueryInput{ReportID: "report-1", RunID: "run-1"})
			if err != nil {
				t.Errorf("%s as %s chart: %v", name, shape.chartType, err)
				continue
			}
			if len(result.Charts) != 1 || len(result.Charts[0].DataPoints) != 1 {
				t.Errorf("%s as %s chart: result = %#v, want one point", name, shape.chartType, result.Charts)
				continue
			}
			if got := result.Charts[0].DataPoints[0].Y; got != want[name] {
				t.Errorf("%s as %s chart: y = %v, want %v", name, shape.chartType, got, want[name])
			}
		}
	}
}

// TestClickHouseQueryAdapterFailedChartIsLoudAndNotZero holds the other half of
// the defect: a chart read that fails must fail the report (never a measured
// 0 or an empty chart) with the bounded error, and the log must carry the
// metric name and the real cause. functions_count charts on a table the
// reader cannot filter by day, so the server refuses the query.
func TestClickHouseQueryAdapterFailedChartIsLoudAndNotZero(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = instance.Close(context.Background()) }()
	chschema.Apply(ctx, t, instance)
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	defer slog.SetDefault(previous)

	loader := reportLoaderFunc(func(context.Context, QueryInput) (ReportDefinition, error) {
		return ReportDefinition{
			Plan: Plan{PlanID: "plan-1", ReportType: "weekly_health", OrganizationID: "org-1"},
			Charts: []ChartSpec{{
				ChartID: "chart-1", PlanID: "plan-1", ChartType: "scorecard",
				Metric: "functions_count", TimeRangeStart: "2026-01-01", TimeRangeEnd: "2026-01-31",
				OrganizationID: "org-1",
			}},
		}, nil
	})
	adapter, err := NewClickHouseQueryAdapter(loader, conn)
	if err != nil {
		t.Fatal(err)
	}
	result, err := adapter.Query(ctx, QueryInput{ReportID: "report-1", RunID: "run-1"})
	if !errors.Is(err, ErrDependencyUnavailable) {
		t.Fatalf("err = %v, want ErrDependencyUnavailable (result %#v)", err, result)
	}
	if len(result.Charts) != 0 {
		t.Fatalf("a failed chart rendered: %#v", result.Charts)
	}
	for _, want := range []string{"report.chart_read_failed", "metric=functions_count", "stage=query", "day"} {
		if !strings.Contains(logs.String(), want) {
			t.Fatalf("log %q lacks %q", logs.String(), want)
		}
	}
}
