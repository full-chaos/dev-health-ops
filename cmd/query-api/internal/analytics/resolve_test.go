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

// TestResolve_RejectsInvestmentTrue: the FIRST version of this test only
// checked `err != nil` against an EMPTY routingFakeClient (no rules
// registered) -- removal-checked (as instructed) by deleting
// CompileTimeseries's own useInvestment guard, and the test STAYED
// GREEN, because with no compile-time rejection the query proceeds to
// Execute, which then fails anyway with "no rule matches statement" from
// the empty fake -- a genuine error, just the WRONG one, proving nothing
// about the guard this test claims to cover. Fixed by asserting the
// SPECIFIC guard message, which a routing-fake miss can never produce.
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
	if !strings.Contains(err.Error(), "investment path not yet ported") {
		t.Fatalf("expected the investment-path-not-yet-ported guard to fire, got a DIFFERENT error (possibly a fake-routing miss, not the guard): %v", err)
	}
}

// TestResolve_FlowMatrix_RealClientShape_BatchUseInvestmentTrueDoesNotReject
// mirrors web/src/lib/graphql/hooks/useChordFlow.ts's ACTUAL call shape
// verbatim, not an assumption -- confirmed by reading the live caller:
// EVERY useChordFlow request sets BOTH `flowMatrix.useInvestment: true`
// AND the batch-level `useInvestment: true`, for TEAM/REPO/WORK_TYPE
// alike (GROUPING_TO_DIMENSION maps chord groupings 1:1 onto exactly
// those three). compile_flow_matrix's TEAM/REPO/WORK_TYPE branch never
// reads use_investment at all (compiler.py:495-518), so this is not an
// edge case Python happens to handle -- it is the ONLY shape real
// FLOW_MATRIX_QUERY traffic sends. This test proves Resolve's top-level
// `useInvestment` (used only by timeseries/breakdown/sankey) does NOT
// leak into flowMatrix handling and wrongly reject it.
func TestResolve_FlowMatrix_RealClientShape_BatchUseInvestmentTrueDoesNotReject(t *testing.T) {
	client := &routingFakeClient{}
	client.on("work_item_cycle_times AS wct FINAL", &fakeRowScanner{})

	fmUseInvestment := true
	batch := model.AnalyticsRequestInput{
		UseInvestment: boolPtr(true),
		FlowMatrix: &model.FlowMatrixRequestInput{
			Dimension:     model.DimensionInputTeam,
			Measure:       model.MeasureInputCount,
			DateRange:     &model.DateRangeInput{StartDate: mustGraphQLDate("2026-01-01"), EndDate: mustGraphQLDate("2026-01-07")},
			MaxNodes:      50,
			MaxEdges:      200,
			UseInvestment: &fmUseInvestment,
		},
	}
	result, err := Resolve(context.Background(), client, "org-1", batch)
	if err != nil {
		t.Fatalf("Resolve error = %v -- the real useChordFlow.ts call shape (batch.useInvestment=true, flowMatrix-only) must NOT be rejected", err)
	}
	if result.FlowMatrix == nil {
		t.Fatal("expected a non-nil FlowMatrixResult")
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

// TestResolve_FlowMatrixDegradation_IsReported pins the ONLY observable
// a swallowed execute error has.
//
// resolveFlowMatrix mirrors analytics.py:959-961: it catches the execute
// error and returns an empty FlowMatrixResult. The returned value is
// therefore byte-identical whether the query failed or the org genuinely
// has no flow -- so asserting on the RESULT cannot detect a regression
// that drops the reporting. That indistinguishability is the defect
// telemetry.go exists to close, which makes the report itself the thing
// under test.
//
// It is also masked from above, the same double-layer shape found earlier
// in this port: schema.resolvers.go's startAnalyticsSpan closes its span
// with outcome "ok" for a degraded response (correctly -- Python returns
// a success response too), so no resolver-level assertion can see this
// either. It has to be tested here, at its own level.
func TestResolve_FlowMatrixDegradation_IsReported(t *testing.T) {
	// The fake error MUST model the real client's error shape, not a
	// convenient one. dev-health-go/clickhouse wraps every driver
	// failure as *operationError, whose Error() is the FIXED string
	// "ClickHouse query failed" with the cause reachable only via
	// Unwrap() (client.go:212-213). An errors.New("clickhouse exploded")
	// fake -- which is what this test used first -- is KINDER than
	// reality: its Error() carries the real text, so it would validate
	// telemetry that records err.Error() alone, even though against the
	// real client that records a constant string carrying no cause.
	// The fake being easier than the dependency is what makes the test
	// measure the fake. (Lane A's codex P1, same class.)
	driverErr := errors.New("code: 60, message: Table default.work_item_cycle_times does not exist")
	boom := &fakeOperationError{operation: "query", cause: driverErr}
	client := &routingFakeClient{}
	client.onErr("work_item_cycle_times", boom)

	type report struct {
		phase string
		err   error
	}
	var got []report
	orig := recordDegradation
	recordDegradation = func(_ context.Context, phase string, err error) {
		got = append(got, report{phase: phase, err: err})
	}
	t.Cleanup(func() { recordDegradation = orig })

	fmUseInvestment := true
	batch := model.AnalyticsRequestInput{
		UseInvestment: boolPtr(true),
		FlowMatrix: &model.FlowMatrixRequestInput{
			Dimension:     model.DimensionInputTeam,
			Measure:       model.MeasureInputCount,
			DateRange:     &model.DateRangeInput{StartDate: mustGraphQLDate("2026-01-01"), EndDate: mustGraphQLDate("2026-01-07")},
			MaxNodes:      50,
			MaxEdges:      200,
			UseInvestment: &fmUseInvestment,
		},
	}

	result, err := Resolve(context.Background(), client, "org-1", batch)
	// Parity first: the swallow must still swallow.
	if err != nil {
		t.Fatalf("Resolve error = %v -- a failed flowMatrix execute must degrade, not propagate (analytics.py:959-961)", err)
	}
	if result.FlowMatrix == nil {
		t.Fatal("expected a non-nil degraded FlowMatrixResult")
	}
	if len(result.FlowMatrix.Nodes) != 0 || len(result.FlowMatrix.Edges) != 0 {
		t.Fatalf("expected the degraded result to be empty, got %d nodes / %d edges", len(result.FlowMatrix.Nodes), len(result.FlowMatrix.Edges))
	}

	// The part the result cannot show.
	if len(got) != 1 {
		t.Fatalf("expected exactly 1 degradation report, got %d -- a swallowed failure that reports nothing is indistinguishable from an empty org", len(got))
	}
	if got[0].phase != "flowMatrix" {
		t.Fatalf("degradation phase = %q, want %q", got[0].phase, "flowMatrix")
	}
	if !errors.Is(got[0].err, boom) {
		t.Fatalf("degradation error = %v, want it to wrap %v -- the report must carry the cause Python's logger.error carried", got[0].err, boom)
	}

	// The whole point: the DRIVER's message must still be recoverable.
	// err.Error() cannot carry it (fixed string), so anything that
	// records only the message would lose the table name and code 60.
	if !errors.Is(got[0].err, driverErr) {
		t.Fatalf("degradation error does not unwrap to the driver cause -- telemetry would record only the fixed %q and distinguish nothing", boom.Error())
	}
	if strings.Contains(got[0].err.Error(), "code: 60") {
		t.Fatal("fake is not modelling the real client: operationError.Error() must NOT leak the driver text, or this test proves nothing about the real one")
	}
	if cause := rootCause(got[0].err); !strings.Contains(cause.Error(), "code: 60") {
		t.Fatalf("rootCause = %q, want it to recover the driver message telemetry needs", cause.Error())
	}
}

// fakeOperationError mirrors dev-health-go/clickhouse's unexported
// *operationError semantics exactly (client.go:207-213): a fixed
// Error() string that omits the cause, plus Unwrap() to reach it.
type fakeOperationError struct {
	operation string
	cause     error
}

func (e *fakeOperationError) Error() string { return "ClickHouse " + e.operation + " failed" }
func (e *fakeOperationError) Unwrap() error { return e.cause }
