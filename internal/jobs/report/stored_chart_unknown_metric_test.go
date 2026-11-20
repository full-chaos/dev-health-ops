package report

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// A stored chart spec whose metric is not an exact registry key does not stop the run: the spec is dropped at decode, the
// authored name is listed as uncharted, and one WARN (no name) plus one counter tick tell an operator.

type unknownMetricLoader struct{ plan []byte }

func (loader unknownMetricLoader) Load(_ context.Context, input QueryInput) (ReportDefinition, error) {
	return decodeReportDefinition(input.ReportID, "org-1", loader.plan, nil, time.Date(2026, 1, 8, 0, 0, 0, 0, time.UTC))
}

type countingConn struct {
	driver.Conn
	queried int
}

func (conn *countingConn) Query(context.Context, string, ...any) (driver.Rows, error) {
	conn.queried++
	return nil, errors.New("inert connection")
}

func storedSpecPlan(requested string, specs ...[2]string) []byte {
	entries := make([]string, 0, len(specs))
	for index, spec := range specs {
		group := "null"
		if spec[1] != "" {
			group = `"` + spec[1] + `"`
		}
		entries = append(entries, fmt.Sprintf(`{"chart_id":"c%d","plan_id":"p1","chart_type":"line","metric":%q,"group_by":%s,"org_id":"org-1"}`,
			index, spec[0], group))
	}
	return []byte(fmt.Sprintf(`{"plan_id":"p1","report_type":"weekly_health","org_id":"org-1","sections":["summary"],
"requested_metrics":[%s],"created_at":"2026-01-08T00:00:00Z","chart_specs":[%s]}`, requested, strings.Join(entries, ",")))
}

type uncharted14329Probe struct {
	logs  *bytes.Buffer
	count func() int64
}

func probeUncharted(t *testing.T) uncharted14329Probe {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	counter, err := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test").Int64Counter(unchartedStoredSpecCounterName)
	if err != nil {
		t.Fatal(err)
	}
	previousCounter, previousLogger := unchartedStoredSpecCounter, slog.Default()
	t.Cleanup(func() { unchartedStoredSpecCounter = previousCounter; slog.SetDefault(previousLogger) })
	unchartedStoredSpecCounter = counter
	logs := &bytes.Buffer{}
	slog.SetDefault(slog.New(slog.NewTextHandler(logs, nil)))
	return uncharted14329Probe{logs: logs, count: func() int64 {
		var data metricdata.ResourceMetrics
		if err := reader.Collect(context.Background(), &data); err != nil {
			t.Fatal(err)
		}
		var total int64
		for _, scope := range data.ScopeMetrics {
			for _, m := range scope.Metrics {
				if sum, ok := m.Data.(metricdata.Sum[int64]); ok && m.Name == unchartedStoredSpecCounterName {
					for _, point := range sum.DataPoints {
						total += point.Value
					}
				}
			}
		}
		return total
	}}
}

func queryStored(t *testing.T, plan []byte) (QueryResult, *countingConn, error) {
	t.Helper()
	conn := &countingConn{}
	adapter := &ClickHouseQueryAdapter{loader: unknownMetricLoader{plan}, conn: conn,
		now: func() time.Time { return time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC) }}
	result, err := adapter.Query(context.Background(), QueryInput{ReportID: "r1", RunID: "run1"})
	return result, conn, err
}

func TestStoredChartWithACustomMetricNameDoesNotStopTheRun(t *testing.T) {
	for _, name := range []string{"Release _Candidate_", "Pipelines_Count"} {
		for _, group := range []string{"day", "team", ""} {
			t.Run(name+"/"+group, func(t *testing.T) {
				probe := probeUncharted(t)
				result, conn, err := queryStored(t, storedSpecPlan(`"`+name+`"`, [2]string{name, group}))
				if err != nil {
					t.Fatalf("run stopped: %v", err)
				}
				want := []UnchartedMetric{{Label: name, Reason: unchartedNoMetric}}
				if len(result.Charts) != 0 || fmt.Sprint(result.Plan.UnchartedMetrics) != fmt.Sprint(want) || conn.queried != 0 {
					t.Fatalf("charts=%d uncharted=%v queried=%d, want 0 charts, %v, 0 queries", len(result.Charts), result.Plan.UnchartedMetrics, conn.queried, want)
				}
				if probe.count() != 1 || strings.Count(probe.logs.String(), "stored chart spec dropped") != 1 ||
					strings.Contains(probe.logs.String(), name) || !strings.Contains(probe.logs.String(), "plan_id=p1") {
					t.Fatalf("want one tick and one WARN with the plan id and no name: count=%d logs=%q", probe.count(), probe.logs.String())
				}
				artifact, err := NewDeterministicRenderer().Render(context.Background(), result)
				if err != nil {
					t.Fatal(err)
				}
				if strings.Count(artifact.Markdown, "## Requested Metrics Without a Chart\n") != 1 ||
					strings.Count(artifact.Markdown, ": no metric matches this label") != 1 {
					t.Fatalf("report does not list the name once:\n%s", artifact.Markdown)
				}
			})
		}
	}
}

func TestStoredChartWithAnExactRegistryKeyStillReachesTheQuery(t *testing.T) {
	probe := probeUncharted(t)
	_, conn, err := queryStored(t, storedSpecPlan(`"pipelines_count"`, [2]string{"pipelines_count", "day"}))
	if err == nil || conn.queried != 1 || probe.count() != 0 || strings.Contains(probe.logs.String(), "stored chart spec dropped") {
		t.Fatalf("control: err=%v queried=%d ticks=%d logs=%q, want the query reached and no drop", err, conn.queried, probe.count(), probe.logs.String())
	}
}

func TestStoredChartKeepsItsKeyAndAuthoredTitle(t *testing.T) {
	plan := []byte(strings.Replace(string(storedSpecPlan(`"pipelines_count"`, [2]string{"pipelines_count", "day"})),
		`"org_id":"org-1"}]`, `"org_id":"org-1","title":"Release cadence"}]`, 1))
	definition, err := decodeReportDefinition("r1", "org-1", plan, nil, time.Date(2026, 1, 8, 0, 0, 0, 0, time.UTC))
	if err != nil || len(definition.Charts) != 1 || definition.Charts[0].Metric != "pipelines_count" ||
		definition.Charts[0].Title != "Release cadence" || len(definition.Plan.UnchartedMetrics) != 0 {
		t.Fatalf("definition=%#v err=%v", definition, err)
	}
}

func TestStoredPlanMixesAKeyAndCustomNames(t *testing.T) {
	probe := probeUncharted(t)
	plan := storedSpecPlan(`"Release _Candidate_","pipelines_count","Release _Candidate_"`,
		[2]string{"Release _Candidate_", "day"}, [2]string{"pipelines_count", "day"}, [2]string{"Release _Candidate_", "team"}, [2]string{" ", "day"})
	definition, err := decodeReportDefinition("r1", "org-1", plan, nil, time.Date(2026, 1, 8, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if len(definition.Charts) != 1 || definition.Charts[0].Metric != "pipelines_count" || definition.Charts[0].ChartID != "c1" {
		t.Fatalf("charts = %#v, want only the registry key", definition.Charts)
	}
	if got := fmt.Sprint(definition.Plan.UnchartedMetrics); got != fmt.Sprint([]UnchartedMetric{{Label: "Release _Candidate_", Reason: unchartedNoMetric}}) {
		t.Fatalf("uncharted = %s, want the custom name once and no blank name", got)
	}
	if probe.count() != 3 {
		t.Fatalf("ticks = %d, want 1 per dropped spec (3)", probe.count())
	}
	artifact, err := NewDeterministicRenderer().Render(context.Background(), QueryResult{Plan: definition.Plan,
		Charts: []ChartResult{{Spec: definition.Charts[0], Title: "Pipelines count"}}})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(artifact.Markdown, "- Release \\_Candidate\\_:") != 1 || strings.Contains(artifact.Markdown, "ArtifactID: c0") {
		t.Fatalf("render:\n%s", artifact.Markdown)
	}
	for _, record := range artifact.Provenance {
		if record.ArtifactType == "chart" && record.ArtifactID != "c1" {
			t.Fatalf("provenance holds a chart for a dropped spec: %#v", record)
		}
	}
}

func TestStoredChartWithABlankMetricIsNotListed(t *testing.T) {
	probe := probeUncharted(t)
	definition, err := decodeReportDefinition("r1", "org-1", storedSpecPlan(`""`, [2]string{" ", "day"}), nil, time.Date(2026, 1, 8, 0, 0, 0, 0, time.UTC))
	if err != nil || len(definition.Charts) != 0 || len(definition.Plan.UnchartedMetrics) != 0 || probe.count() != 1 {
		t.Fatalf("definition=%#v err=%v ticks=%d", definition, err, probe.count())
	}
	if _, err := decodeReportDefinition("r1", "org-1", storedSpecPlan(`""`, [2]string{"", "day"}), nil, time.Now()); !errors.Is(err, ErrContractMismatch) {
		t.Fatalf("an empty metric must stay a contract mismatch, got %v", err)
	}
}
