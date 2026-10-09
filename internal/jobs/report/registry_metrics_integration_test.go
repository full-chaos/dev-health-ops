//go:build integration

package report

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// TestClickHouseQueryAdapterChartsEveryRegistryMetric charts EVERY metric of
// the embedded registry through the real report reader on the schema of the
// migration chain, with no exclusion list. Two defects hid behind a chart that
// was never run against the real schema: sum() of an unsigned integer column
// is UInt64 in ClickHouse and the chart read scanned it into a float (every
// summed count metric failed), and the snapshot table file_complexity_snapshots
// keys on as_of_day while the reader filtered every table on `day` (every one
// of its metrics failed). The column types and the date column differ by
// table, so each metric is charted on its own.
//
// Each table gets one row holding a distinct value per metric; both chart
// shapes the reader has (scorecard total, and a line by day) must return the
// value the metric's aggregation gives for that row.
func TestClickHouseQueryAdapterChartsEveryRegistryMetric(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
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

	const org = "org-registry-metrics"
	names := make([]string, 0, len(supportedMetrics))
	for name := range supportedMetrics {
		names = append(names, name)
	}
	sort.Strings(names)
	if len(names) < 100 {
		t.Fatalf("only %d metrics found in the registry", len(names))
	}

	// The class of each metric comes from the migrated column type. The
	// declaration in the reader (ValueKind) must agree with it, and every
	// registry table and column must exist.
	columnTypes := map[string]string{}
	typeRows, err := conn.Query(ctx, "SELECT table, name, type FROM system.columns WHERE database = currentDatabase()")
	if err != nil {
		t.Fatal(err)
	}
	for typeRows.Next() {
		var table, column, columnType string
		if err := typeRows.Scan(&table, &column, &columnType); err != nil {
			t.Fatal(err)
		}
		columnTypes[table+"."+column] = columnType
	}
	if err := typeRows.Err(); err != nil {
		t.Fatal(err)
	}
	typeRows.Close()
	numericType := regexp.MustCompile(`^(?:Nullable\()?(?:U?Int(?:8|16|32|64|128|256)|Float(?:32|64)|Decimal.*)\)?$`)
	var numeric, nonNumeric []string
	for _, name := range names {
		definition := supportedMetrics[name]
		columnType, ok := columnTypes[definition.SourceTable+"."+name]
		if !ok {
			t.Errorf("registry metric %s: column %s.%s does not exist in the migrated schema", name, definition.SourceTable, name)
			continue
		}
		if _, ok := columnTypes[definition.SourceTable+"."+definition.dayColumn()]; !ok {
			t.Errorf("registry metric %s: date column %s.%s does not exist", name, definition.SourceTable, definition.dayColumn())
			continue
		}
		isNumeric := numericType.MatchString(columnType)
		if isNumeric != (definition.ValueKind == valueKindNumeric) {
			t.Errorf("registry metric %s: column type %s but declared value kind %q", name, columnType, definition.ValueKind)
			continue
		}
		if isNumeric {
			numeric = append(numeric, name)
		} else {
			nonNumeric = append(nonNumeric, name)
		}
	}
	if len(nonNumeric) == 0 || len(numeric) < 100 {
		t.Fatalf("numeric=%d non-numeric=%d: the classification is not exercised", len(numeric), len(nonNumeric))
	}
	names = numeric

	// One row per table holds every metric of that table: the dedup source
	// keeps the newest row per natural key, so two rows of one key would hide
	// each other's values. Sample-count columns (see sampleCountColumns) are
	// seeded to 1 when they are not metrics themselves, so a sampled average
	// has a sample to average.
	seeded := map[string]float64{}
	columns := map[string][]string{}
	values := map[string][]string{}
	add := func(table, column string, value float64) {
		columns[table] = append(columns[table], column)
		values[table] = append(values[table], fmt.Sprint(value))
	}
	for index, name := range names {
		table := supportedMetrics[name].SourceTable
		value := float64(3 + index)
		seeded[table+"."+name] = value
		add(table, name, value)
	}
	// A ratio the reader recomputes from table-local counts needs both counts.
	for _, name := range names {
		definition := supportedMetrics[name]
		if definition.Numerator == "" {
			continue
		}
		for column, value := range map[string]float64{definition.Numerator: 4, definition.Denominator: 10} {
			if _, isMetric := seeded[definition.SourceTable+"."+column]; !isMetric {
				seeded[definition.SourceTable+"."+column] = value
				add(definition.SourceTable, column, value)
			}
		}
	}
	for table, counts := range sampleCountColumns {
		for _, count := range counts {
			if _, isMetric := seeded[table+"."+count]; !isMetric {
				add(table, count, 1)
			}
		}
	}
	for table := range columns {
		dayColumn := metricDefinition{SourceTable: table}.dayColumn()
		statement := fmt.Sprintf(
			"INSERT INTO %s (org_id, %s, %s) VALUES ('%s', '2026-01-05', %s)",
			table, dayColumn, strings.Join(columns[table], ", "), org, strings.Join(values[table], ", "))
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatalf("seed %s: %v", table, err)
		}
	}
	// What the chart of a metric must read for the seeded row: the value for a
	// sum or an average of one row, and numerator/denominator for a ratio the
	// reader recomputes from its table-local counts.
	want := func(name string) float64 {
		definition := supportedMetrics[name]
		if definition.Numerator != "" && definition.Denominator != "" {
			return seeded[definition.SourceTable+"."+definition.Numerator] /
				seeded[definition.SourceTable+"."+definition.Denominator]
		}
		return seeded[definition.SourceTable+"."+name]
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
				t.Errorf("%s (%s) as %s chart: %v", name, supportedMetrics[name].SourceTable, shape.chartType, err)
				continue
			}
			if len(result.Charts) != 1 || len(result.Charts[0].DataPoints) != 1 {
				t.Errorf("%s as %s chart: result = %#v, want one point", name, shape.chartType, result.Charts)
				continue
			}
			if got, expected := result.Charts[0].DataPoints[0].Y, want(name); got < expected-1e-9 || got > expected+1e-9 {
				t.Errorf("%s as %s chart: y = %v, want %v", name, shape.chartType, got, expected)
			}
		}
	}
	// A chart of a non-numeric column is refused as a request error that names
	// the metric, not reported as an outage.
	for _, name := range nonNumeric {
		loader := reportLoaderFunc(func(context.Context, QueryInput) (ReportDefinition, error) {
			return ReportDefinition{
				Plan: Plan{PlanID: "plan-1", ReportType: "weekly_health", OrganizationID: org},
				Charts: []ChartSpec{{
					ChartID: "chart-1", PlanID: "plan-1", ChartType: "scorecard", Metric: name,
					TimeRangeStart: "2026-01-01", TimeRangeEnd: "2026-01-31", OrganizationID: org,
				}},
			}, nil
		})
		adapter, err := NewClickHouseQueryAdapter(loader, conn)
		if err != nil {
			t.Fatal(err)
		}
		_, err = adapter.Query(ctx, QueryInput{ReportID: "report-1", RunID: "run-1"})
		var refused *ChartMetricError
		switch {
		case !errors.As(err, &refused) || refused.Metric != name:
			t.Errorf("%s: err = %v, want a ChartMetricError naming it", name, err)
		case !errors.Is(err, ErrContractMismatch) || errors.Is(err, ErrDependencyUnavailable):
			t.Errorf("%s: refusal must be an invalid-request error, not an outage: %v", name, err)
		case !strings.Contains(err.Error(), name):
			t.Errorf("%s: message %q does not name the metric", name, err.Error())
		}
	}
}

// TestClickHouseQueryAdapterFailedChartIsLoudAndNotZero holds the other half of
// the defect: a chart read that fails must fail the report (never a measured
// 0 or an empty chart) with the bounded error, and the log must carry the
// metric name and the real cause. The source table of the chart is dropped, so
// the server refuses the query.
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

	if err := conn.Exec(ctx, "DROP TABLE file_complexity_snapshots"); err != nil {
		t.Fatal(err)
	}
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
	for _, want := range []string{"report.chart_read_failed", "metric=functions_count", "stage=query", "file_complexity_snapshots"} {
		if !strings.Contains(logs.String(), want) {
			t.Fatalf("log %q lacks %q", logs.String(), want)
		}
	}
}
