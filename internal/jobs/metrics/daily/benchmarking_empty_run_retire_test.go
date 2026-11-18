package daily

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
)

var benchmarkingEmptyRunDay = time.Date(2026, 9, 14, 0, 0, 0, 0, time.UTC)

const benchmarkingEmptyRunRepo = "00000000-0000-4000-8000-00000014052a"

// The six output tables of benchmarking, each with the key columns (after
// org_id) its live-key read returns before max(computed_at).
var benchmarkingEmptyRunTables = []string{
	"testops_metric_baselines", "testops_maturity_bands", "testops_metric_anomalies",
	"testops_period_comparisons", "testops_metric_correlations", "testops_benchmark_insights",
}

func benchmarkingEmptyRunStore() *fakeStore {
	store := finalizeStoreWithClaim()
	store.noPartitions = true
	store.run.FullOrg = true
	store.run.TargetDay = benchmarkingEmptyRunDay
	store.finalizeClaim.Run.TargetDay = benchmarkingEmptyRunDay
	return store
}

// benchmarkingEmptyRunConn answers the repository discovery with repos (nil:
// none) and each live-key read of a benchmarking table with live[table].
func benchmarkingEmptyRunConn(repos []uuid.UUID, live map[string][][]any) *retireTestConn {
	routes := []retireTestRoute{
		{needles: []string{"FROM repos"}, rows: func() [][]any {
			values := make([][]any, 0, len(repos))
			for _, id := range repos {
				values = append(values, []any{id})
			}
			return values
		}},
		{needles: []string{"FROM work_items WHERE org_id = ? AND repo_id = ?"}},
	}
	for _, table := range benchmarkingEmptyRunTables {
		rows := live[table]
		routes = append(routes, retireTestRoute{needles: []string{"FROM " + table}, rows: func() [][]any { return rows }})
	}
	return &retireTestConn{routes: routes}
}

func benchmarkingEmptyRunHandler(t *testing.T, store *fakeStore, conn *retireTestConn, now time.Time) (*FinalizeHandler, *jobruntime.MetricsCollector) {
	t.Helper()
	handler, err := NewFinalizeHandler(store)
	if err != nil {
		t.Fatal(err)
	}
	benchmarking, err := NewBenchmarkingFinalizeExecutor(store, conn, nil)
	if err != nil {
		t.Fatal(err)
	}
	benchmarking.nowUTC = func() time.Time { return now }
	registered := map[string]NativeFinalizeFamilyExecutor{}
	for _, name := range pythonRecognisedFinalizeFamilies {
		registered[name] = &stubFinalizeFamily{}
	}
	registered[BenchmarkingFamilyName] = benchmarking
	if err := handler.SetNativeFinalizeFamilies(registered); err != nil {
		t.Fatal(err)
	}
	collector, err := jobruntime.NewMetricsCollector(jobruntime.MetricDimensions{})
	if err != nil {
		t.Fatal(err)
	}
	handler.SetNativeFinalizeFamilyObserver(collector)
	return handler, collector
}

func rowHolds(row []any, values ...any) bool {
	for _, want := range values {
		found := false
		for _, value := range row {
			if reflect.DeepEqual(value, want) {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

// On a run of the whole organization with no partition and no repository at
// the finalize, benchmarking computes nothing and writes one retraction row
// (retracted = 1) over each live key of the day, strictly newer than the
// stored row. A key whose newest row is already a retraction is not live.
func TestBenchmarkingRetiresEveryLiveKeyOfTheDayOnAWholeOrganizationRunWithNoPartition(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	stored := now.Add(-time.Hour)
	ahead := now.Add(10 * time.Second)
	store := benchmarkingEmptyRunStore()
	conn := benchmarkingEmptyRunConn(nil, map[string][][]any{
		"testops_maturity_bands": {
			{"pass_rate", "repo", benchmarkingEmptyRunRepo, stored},
			{"line_coverage_pct", "repo", benchmarkingEmptyRunRepo, ahead},
		},
		"testops_benchmark_insights": {{"insight-1", stored}},
	})
	handler, collector := benchmarkingEmptyRunHandler(t, store, conn, now)

	workErr := handler.Work(context.Background(), finalizeExecution())

	bands := conn.appended("testops_maturity_bands")
	insights := conn.appended("testops_benchmark_insights")
	t.Logf("finalize_err=%v maturity_bands=%v insights=%v queries=%d", workErr, bands, insights, len(conn.queries))
	if workErr != nil {
		t.Fatalf("finalize = %v, want nil", workErr)
	}
	for _, batch := range conn.batches {
		if !strings.Contains(batch.query, "retracted") {
			t.Errorf("a write that is not a retraction on a run with no repository: %s", batch.query)
		}
	}
	for _, want := range []struct {
		rows    [][]any
		key     []any
		version time.Time
	}{
		{bands, []any{"pass_rate", "repo", benchmarkingEmptyRunRepo}, now},
		{bands, []any{"line_coverage_pct", "repo", benchmarkingEmptyRunRepo}, ahead.Add(time.Second)},
		{insights, []any{"insight-1"}, now},
	} {
		found := false
		for _, row := range want.rows {
			if rowHolds(row, want.key...) {
				found = true
				if !rowHolds(row, uint8(1), testOrgID) {
					t.Errorf("row of key %v is not a retraction of the organization: %#v", want.key, row)
				}
				if !rowHolds(row, want.version) {
					t.Errorf("row of key %v: want computed_at %s (strictly newer than the stored row), got %#v", want.key, want.version, row)
				}
			}
		}
		if !found {
			t.Errorf("key %v: no retraction row; the benchmarking row of the day stays live", want.key)
		}
	}
	exposition := collector.PrometheusText()
	if want := `worker_daily_metrics_native_family_outcome_total{family="` + BenchmarkingFamilyName + `",outcome="computed"} 1`; !strings.Contains(exposition, want) {
		t.Errorf("exposition is missing %q", want)
	}
}

// A repository that is back at the finalize (written after the dispatch found
// none) keeps every benchmarking row: the family retires nothing, and the skip
// is counted and logged. Guard: it holds on main too.
func TestBenchmarkingKeepsItsRowsWhenARepositoryIsBackAtTheFinalizeOfAnEmptyRun(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	store := benchmarkingEmptyRunStore()
	conn := benchmarkingEmptyRunConn([]uuid.UUID{uuid.MustParse(benchmarkingEmptyRunRepo)}, map[string][][]any{
		"testops_maturity_bands": {{"pass_rate", "repo", benchmarkingEmptyRunRepo, now.Add(-time.Hour)}},
	})
	handler, collector := benchmarkingEmptyRunHandler(t, store, conn, now)
	if err := handler.Work(context.Background(), finalizeExecution()); err != nil {
		t.Fatalf("finalize = %v, want nil", err)
	}
	if len(conn.batches) != 0 {
		t.Errorf("%d write(s) with a repository present, want none", len(conn.batches))
	}
	if want := `worker_daily_metrics_native_family_outcome_total{family="` + BenchmarkingFamilyName + `",outcome="skipped_empty_run"} 1`; !strings.Contains(collector.PrometheusText(), want) {
		t.Errorf("exposition is missing %q", want)
	}
	if store.finalizeCompletions != 1 {
		t.Errorf("completions = %d, want 1", store.finalizeCompletions)
	}
}
