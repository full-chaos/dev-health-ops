package providersync

import (
	"bytes"
	"context"
	"log/slog"
	"reflect"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func plannerKeptCounts(t *testing.T) map[string]int64 {
	t.Helper()
	var collected metricdata.ResourceMetrics
	if err := meterReader.Collect(context.Background(), &collected); err != nil {
		t.Fatal(err)
	}
	out := map[string]int64{}
	for _, scope := range collected.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != githubTestsPlannerKeptAggregateName {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("%s is %T, want an int64 sum", m.Name, m.Data)
			}
			for _, point := range sum.DataPoints {
				outcome, _ := point.Attributes.Value("outcome")
				out[outcome.AsString()] += point.Value
			}
		}
	}
	return out
}

func capturePlannerSlog(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previous) })
	return &buf
}

func assertPlannerKeep(t *testing.T, reports []githubTestsParsedReport, plan githubTestsRunReportPlan, outcome, wantIDs string) {
	t.Helper()
	before := plannerKeptCounts(t)
	buf := capturePlannerSlog(t)
	reportGitHubTestsPlannerKeeps(context.Background(), "github", "tests", "unit-1", "org/repo", "run-77", reports, plan)
	after := plannerKeptCounts(t)
	delta := map[string]int64{}
	for key, value := range after {
		if value != before[key] {
			delta[key] = value - before[key]
		}
	}
	if want := map[string]int64{outcome: int64(len(strings.Split(wantIDs, ",")))}; !reflect.DeepEqual(delta, want) {
		t.Fatalf("counter delta %v, want %v", delta, want)
	}
	line := buf.String()
	for _, want := range []string{"level=WARN", "outcome=" + outcome, "artifact_ids=" + wantIDs, "run-77"} {
		if !strings.Contains(line, want) {
			t.Fatalf("log %q lacks %q", line, want)
		}
	}
	if strings.Count(line, "\n") != 1 {
		t.Fatalf("want one log line, got %q", line)
	}
}

func TestGitHubTestsPlannerSignalsPartitionBoundExhausted(t *testing.T) {
	reports := []githubTestsParsedReport{
		boundReport("1", boundSuite{"a", 10, 0}), boundReport("2", boundSuite{"b", 10, 0}),
		boundReport("3", boundSuite{"a", 10, 0}, boundSuite{"b", 10, 0}),
	}
	free := mustPlanWithin(t, githubTestsDefaultPartitionBound, reports...)
	if len(free.BoundExhausted) != 0 {
		t.Fatalf("unbounded plan flagged %v", free.BoundExhausted)
	}
	at := mustPlanWithin(t, githubTestsPartitionBound{Work: free.PartitionWork}, reports...)
	if len(at.BoundExhausted) != 0 {
		t.Fatalf("plan at the bound flagged %v", at.BoundExhausted)
	}
	below := mustPlanWithin(t, githubTestsPartitionBound{Work: free.PartitionWork - 1}, reports...)
	if !reflect.DeepEqual(below.BoundExhausted, []int{2}) {
		t.Fatalf("BoundExhausted %v, want [2]", below.BoundExhausted)
	}
	assertPlannerKeep(t, reports, below, githubTestsPlannerPartitionBoundExhausted, "3")
}

func TestGitHubTestsPlannerSignalsDropUndoneByAdmission(t *testing.T) {
	reports := []githubTestsParsedReport{
		boundReport("1", boundSuite{"a", 120_000, 0}), boundReport("2", boundSuite{"b", 90_000, 0}),
		boundReport("3", boundSuite{"a", 120_000, 0}, boundSuite{"b", 90_000, 0}),
	}
	plan := mustPlanRunReports(t, reports...)
	if !reflect.DeepEqual(plan.UndoneByAdmission, []int{2}) || len(plan.Dropped) != 0 {
		t.Fatalf("UndoneByAdmission %v dropped %+v, want [2] and none", plan.UndoneByAdmission, plan.Dropped)
	}
	assertPlannerKeep(t, reports, plan, githubTestsPlannerDropUndoneByAdmission, "3")
}
