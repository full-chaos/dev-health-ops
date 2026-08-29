package analytics

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graph/model"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graphqldate"
)

// mustGraphQLDate is mustDate's *testing.T-free sibling, for use inside
// struct-literal helper functions (tsInput/bdInput below) where a *T
// isn't threaded through. Panics on parse failure -- the literal date
// strings below are fixed and known-valid, so this never fires in
// practice; it exists only so a future typo fails loudly instead of
// silently producing a zero Date.
func mustGraphQLDate(s string) graphqldate.Date {
	d, err := graphqldate.Parse(s)
	if err != nil {
		panic(err)
	}
	return d
}

// routingFakeClient dispatches by matching the statement against a set of
// (substring -> response|error) rules, checked in REGISTRATION order
// (first match wins) and mutex guarded -- Resolve's Phase 1 fires the
// whole timeseries+breakdowns batch concurrently, so a fake keyed on
// call order would be flaky.
type routingFakeClient struct {
	mu    sync.Mutex
	rules []routingRule
	calls []string
}

type routingRule struct {
	match    string
	response *fakeRowScanner
	err      error
}

func (f *routingFakeClient) on(match string, response *fakeRowScanner) *routingFakeClient {
	f.rules = append(f.rules, routingRule{match: match, response: response})
	return f
}

func (f *routingFakeClient) onErr(match string, err error) *routingFakeClient {
	f.rules = append(f.rules, routingRule{match: match, err: err})
	return f
}

func (f *routingFakeClient) Query(_ context.Context, statement string, _ []clickhouse.Binding) (clickhouse.RowScanner, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, r := range f.rules {
		if strings.Contains(statement, r.match) {
			f.calls = append(f.calls, r.match)
			if r.err != nil {
				return nil, r.err
			}
			return r.response, nil
		}
	}
	return nil, errors.New("routingFakeClient: no rule matches statement: " + statement)
}

func tsInput(dim model.DimensionInput, measure model.MeasureInput) model.TimeseriesRequestInput {
	return model.TimeseriesRequestInput{
		Dimension: dim,
		Measure:   measure,
		Interval:  model.BucketIntervalInputDay,
		DateRange: &model.DateRangeInput{StartDate: mustGraphQLDate("2026-01-01"), EndDate: mustGraphQLDate("2026-01-07")},
	}
}

func bdInput(dim model.DimensionInput, measure model.MeasureInput) model.BreakdownRequestInput {
	return model.BreakdownRequestInput{
		Dimension: dim,
		Measure:   measure,
		DateRange: &model.DateRangeInput{StartDate: mustGraphQLDate("2026-01-01"), EndDate: mustGraphQLDate("2026-01-07")},
		TopN:      10,
	}
}

func boolPtr(b bool) *bool { return &b }

func TestValidateSubRequestCount_OverLimit(t *testing.T) {
	err := validateSubRequestCount(6, 5, true, false) // 6+5+1 = 12 > 10
	if err == nil {
		t.Fatal("expected error over max_sub_requests")
	}
}

func TestResolve_RejectsInvestmentTrue(t *testing.T) {
	client := &routingFakeClient{}
	batch := model.AnalyticsRequestInput{
		Timeseries:    []model.TimeseriesRequestInput{tsInput(model.DimensionInputRepo, model.MeasureInputCount)},
		UseInvestment: boolPtr(true),
	}
	_, err := Resolve(context.Background(), client, "org-1", batch)
	if err == nil {
		t.Fatal("expected rejection when batch.useInvestment=true")
	}
}

func TestResolve_HappyPath_TimeseriesAndBreakdown(t *testing.T) {
	client := &routingFakeClient{}
	// Breakdown rule FIRST: a REPO/TEAM breakdown's SQL also contains the
	// same "<col> AS dimension_value" substring a timeseries query for the
	// same dimension would -- routing on "LIMIT {top_n:UInt32}" (present
	// in every breakdown query, never in timeseries) first disambiguates
	// them regardless of dimension overlap.
	client.on("LIMIT {top_n:UInt32}", &fakeRowScanner{rows: [][]any{
		{"team-x", 9.0},
	}})
	client.on("date_trunc", &fakeRowScanner{rows: [][]any{
		{mustGraphQLDate("2026-01-01"), "repo-a", 3.0},
	}})

	batch := model.AnalyticsRequestInput{
		Timeseries: []model.TimeseriesRequestInput{tsInput(model.DimensionInputRepo, model.MeasureInputCount)},
		Breakdowns: []model.BreakdownRequestInput{bdInput(model.DimensionInputTeam, model.MeasureInputThroughput)},
	}
	result, err := Resolve(context.Background(), client, "org-1", batch)
	if err != nil {
		t.Fatalf("Resolve error = %v", err)
	}
	if len(result.Timeseries) != 1 || result.Timeseries[0].DimensionValue != "repo-a" {
		t.Fatalf("unexpected timeseries: %+v", result.Timeseries)
	}
	if len(result.Breakdowns) != 1 || len(result.Breakdowns[0].Items) != 1 || result.Breakdowns[0].Items[0].Key != "team-x" {
		t.Fatalf("unexpected breakdowns: %+v", result.Breakdowns)
	}
	if result.EvidenceQualityStats != nil || result.EvidenceQualityDistribution != nil {
		t.Fatalf("expected nil evidence-quality fields (Phase 4 is investment-path only), got stats=%v dist=%v", result.EvidenceQualityStats, result.EvidenceQualityDistribution)
	}
}

// TestResolve_IndexOrderedFirstError is the sharpest behavior this port
// preserves: when MULTIPLE sub-requests fail concurrently, the surfaced
// error is the first one BY INDEX (timeseries before breakdowns, then by
// position within each), never whichever happened to fail first in
// wall-clock time. Run several times to catch goroutine-scheduling
// flakiness a single run could miss -- an errgroup-style
// "first error in real time" implementation would intermittently surface
// the breakdown or ts[1] error instead, which is exactly the divergence
// this port must not have.
func TestResolve_IndexOrderedFirstError(t *testing.T) {
	for i := 0; i < 20; i++ {
		client := &routingFakeClient{}
		// Breakdown rule first, same disambiguation reasoning as the
		// happy-path test above.
		client.onErr("LIMIT {top_n:UInt32}", errors.New("bd0 fails"))
		client.onErr("repo_id AS dimension_value", errors.New("ts0 fails"))
		client.onErr("team_id AS dimension_value", errors.New("ts1 fails"))

		batch := model.AnalyticsRequestInput{
			Timeseries: []model.TimeseriesRequestInput{
				tsInput(model.DimensionInputRepo, model.MeasureInputCount), // index 0
				tsInput(model.DimensionInputTeam, model.MeasureInputCount), // index 1
			},
			Breakdowns: []model.BreakdownRequestInput{
				bdInput(model.DimensionInputRepo, model.MeasureInputCount), // index 0
			},
		}
		_, err := Resolve(context.Background(), client, "org-1", batch)
		if err == nil {
			t.Fatalf("run %d: expected an error", i)
		}
		if !strings.Contains(err.Error(), "timeseries[0]") {
			t.Fatalf("run %d: expected the timeseries[0] error to win, got: %v", i, err)
		}
	}
}

func TestResolve_Sankey_ExecuteFailureSwallowsToEmpty(t *testing.T) {
	client := &routingFakeClient{}
	client.onErr("'TEAM' AS dimension", errors.New("nodes query failed"))
	client.on("'TEAM' AS source_dimension", &fakeRowScanner{})

	batch := model.AnalyticsRequestInput{
		Sankey: &model.SankeyRequestInput{
			Path:      []model.DimensionInput{model.DimensionInputTeam, model.DimensionInputRepo},
			Measure:   model.MeasureInputCount,
			DateRange: &model.DateRangeInput{StartDate: mustGraphQLDate("2026-01-01"), EndDate: mustGraphQLDate("2026-01-07")},
			MaxNodes:  100,
			MaxEdges:  500,
		},
	}
	result, err := Resolve(context.Background(), client, "org-1", batch)
	if err != nil {
		t.Fatalf("Resolve error = %v -- sankey execution failure must SWALLOW, not propagate", err)
	}
	if result.Sankey == nil {
		t.Fatal("expected a non-nil (degraded-empty) SankeyResult")
	}
	if result.Sankey.Nodes != nil || result.Sankey.Edges != nil {
		t.Fatalf("expected empty nodes/edges after swallowed execute failure, got %+v", result.Sankey)
	}
	if result.Sankey.Unit != model.SankeyValueUnitWorkUnits {
		t.Fatalf("unit = %v, want WORK_UNITS for a COUNT measure", result.Sankey.Unit)
	}
}

func TestResolve_Sankey_CompileFailureIsFatal(t *testing.T) {
	client := &routingFakeClient{}
	batch := model.AnalyticsRequestInput{
		Sankey: &model.SankeyRequestInput{
			Path:      []model.DimensionInput{model.DimensionInputTeam}, // < 2 dimensions -> fatal compile error
			Measure:   model.MeasureInputCount,
			DateRange: &model.DateRangeInput{StartDate: mustGraphQLDate("2026-01-01"), EndDate: mustGraphQLDate("2026-01-07")},
			MaxNodes:  100,
			MaxEdges:  500,
		},
	}
	_, err := Resolve(context.Background(), client, "org-1", batch)
	if err == nil {
		t.Fatal("expected a fatal error for an invalid sankey path -- compile errors must NOT be swallowed")
	}
}

func TestResolve_FlowMatrix_ExecuteFailureSwallowsToEmpty(t *testing.T) {
	client := &routingFakeClient{}
	client.onErr("work_item_cycle_times AS wct FINAL", errors.New("boom"))

	batch := model.AnalyticsRequestInput{
		FlowMatrix: &model.FlowMatrixRequestInput{
			Dimension: model.DimensionInputTeam,
			Measure:   model.MeasureInputCount,
			DateRange: &model.DateRangeInput{StartDate: mustGraphQLDate("2026-01-01"), EndDate: mustGraphQLDate("2026-01-07")},
			MaxNodes:  100,
			MaxEdges:  500,
		},
	}
	result, err := Resolve(context.Background(), client, "org-1", batch)
	if err != nil {
		t.Fatalf("Resolve error = %v -- flowMatrix execution failure must SWALLOW, not propagate", err)
	}
	if result.FlowMatrix == nil {
		t.Fatal("expected a non-nil (degraded-empty) FlowMatrixResult")
	}
}

func TestResolve_FlowMatrix_FilteredSameDimensionCompileFailureIsFatal(t *testing.T) {
	client := &routingFakeClient{}
	batch := model.AnalyticsRequestInput{
		FlowMatrix: &model.FlowMatrixRequestInput{
			Dimension: model.DimensionInputTeam,
			Measure:   model.MeasureInputCount,
			DateRange: &model.DateRangeInput{StartDate: mustGraphQLDate("2026-01-01"), EndDate: mustGraphQLDate("2026-01-07")},
			MaxNodes:  100,
			MaxEdges:  500,
		},
		Filters: &model.FilterInput{
			Scope: &model.ScopeFilterInput{Level: model.ScopeLevelInputTeam, Ids: []string{"t1"}},
		},
	}
	_, err := Resolve(context.Background(), client, "org-1", batch)
	if err == nil {
		t.Fatal("expected the CHAOS-2487 filtered-same-dimension rejection to be fatal, not swallowed")
	}
}
