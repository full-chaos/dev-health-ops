package analytics

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

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
// mustTime builds a row-fixture value the way the DRIVER delivers a
// ClickHouse Date column: a time.Time. Fixtures must speak the driver's
// types, not the model's.
func mustTime(s string) time.Time {
	t, err := time.Parse(graphqldate.Layout, s)
	if err != nil {
		panic(err)
	}
	return t
}

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
	// Enforce the REAL client's read-only guard before anything else, so
	// a statement production would never have executed cannot quietly
	// return rows here. See clientcontract_test.go for why.
	if err := validateLikeRealClient(statement); err != nil {
		return nil, err
	}
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

// TestResolve_Investment_ResolvesEndToEnd is CHAOS-4538's replacement
// for the retired TestResolve_RejectsInvestmentTrue -- the investment
// path now compiles AND executes end to end through Resolve, not just
// through CompileTimeseries in isolation. The routingFakeClient's
// validateLikeRealClient check (clientcontract_test.go, enforced on
// EVERY Query call this fake receives) is exactly the same read-only/
// leading-SELECT gate the real dev-health-go client applies, so this
// test doubles as a Resolve()-level (not just Compile()-level) proof
// that the investment path's inlined SQL survives that gate.
//
// The membership-scope telemetry query (RecordStaleInvestmentMembershipScope)
// fires a SEPARATE Query call before the real one; this fake has no rule
// for it, so it fails with "no rule matches" and is swallowed internally
// (RecordStaleInvestmentMembershipScope's doc comment) -- proving the
// telemetry hook cannot break the real query it decorates, the same
// property Python's own try/except around it guarantees.
func TestResolve_Investment_ResolvesEndToEnd(t *testing.T) {
	client := &routingFakeClient{}
	client.on("date_trunc('day', work_unit_investments.from_ts) AS bucket", &fakeRowScanner{
		rows: [][]any{
			{mustTime("2026-01-01"), "repo-a", 3.0},
		},
	})
	// CHAOS-4723: Phase 4 now genuinely queries evidence-quality stats
	// whenever useInvestment=true and a window is available (this
	// batch's timeseries request supplies one via
	// analyticsQualityWindow's fallback) -- and, unlike the
	// membership-scope telemetry query, an unmatched Phase 4 query is
	// FATAL, not swallowed (see TestResolveEvidenceQualityStats_QueryError_IsFatal).
	// A minimal fixture keeps this test's own focus (the timeseries
	// path) from being broken by the new, correct Phase 4 call.
	client.on("quality_known_count", &fakeRowScanner{
		rows: [][]any{{uint64(0), uint64(0), 0.0, 0.0, uint64(0), uint64(0), uint64(0), uint64(0), uint64(0)}},
	})
	batch := model.AnalyticsRequestInput{
		Timeseries:    []model.TimeseriesRequestInput{tsInput(model.DimensionInputRepo, model.MeasureInputCount)},
		UseInvestment: boolPtr(true),
	}
	result, err := Resolve(context.Background(), client, "org-1", batch)
	if err != nil {
		t.Fatalf("Resolve error = %v", err)
	}
	if len(result.Timeseries) != 1 || result.Timeseries[0].DimensionValue != "repo-a" {
		t.Fatalf("unexpected timeseries result: %+v", result.Timeseries)
	}
	// CHAOS-4657: TimeseriesBucket.Value is *float64, not float64 --
	// dereference for the populated-value comparison.
	bucketValue := result.Timeseries[0].Buckets[0].Value
	if len(result.Timeseries[0].Buckets) != 1 || bucketValue == nil || *bucketValue != 3.0 {
		t.Fatalf("unexpected bucket: %+v", result.Timeseries[0].Buckets)
	}
}

// TestResolve_Investment_AuthorDimensionStillRejected pins that the
// investment path's own rules still reject AUTHOR as a GROUP BY/
// breakdown dimension -- porting the investment machinery must not
// accidentally relax dbColumn's unconditional AUTHOR rejection
// (validate.go's doc comment).
func TestResolve_Investment_AuthorDimensionStillRejected(t *testing.T) {
	client := &routingFakeClient{}
	batch := model.AnalyticsRequestInput{
		Timeseries:    []model.TimeseriesRequestInput{tsInput(model.DimensionInputAuthor, model.MeasureInputCount)},
		UseInvestment: boolPtr(true),
	}
	_, err := Resolve(context.Background(), client, "org-1", batch)
	if err == nil {
		t.Fatal("expected AUTHOR dimension to be rejected on the investment path too")
	}
	var ve *ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected *ValidationError, got %T: %v", err, err)
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
// TestResolve_FlowMatrix_BatchUseInvestmentFalse_STILL_ROUTES_TO_INVESTMENT
// is a RED repro for a codex round-1 P2 finding (2026-08-30), not yet
// fixed. DO NOT rename this to imply it passes for a good reason -- the
// all-caps segment is deliberate so it cannot be mistaken for settled
// behavior.
//
// resolve.go's Resolve() calls resolveFlowMatrix(ctx, client, orgID,
// *batch.FlowMatrix, resolvedFilters) -- NO useInvestment parameter at
// all, unlike its sibling call one line above,
// resolveSankey(ctx, client, orgID, *batch.Sankey, batch.UseInvestment,
// resolvedFilters), which DOES thread the batch flag through. So
// compileFlowMatrixInvestmentDimension's resolveUseInvestment(
// []Dimension{req.Dimension}, req.UseInvestment) only ever sees
// FlowMatrixRequestInput's OWN nested useInvestment field -- the
// batch-level flag can never reach it, by construction.
//
// Python's analytics.py (verified directly, base e9ea257ff, zero drift
// to current main per this PR's resume evidence) does NOT have this gap:
// fm_req.use_investment if fm_req.use_investment is not None else
// batch.use_investment (analytics.py:944-946) resolves the flag BEFORE
// calling compile_flow_matrix, so an explicit batch.useInvestment=false
// with the nested field unset is preserved and compile_flow_matrix's own
// _get_context_params(force_investment=False, ...) does NOT auto-route
// THEME to the investment source (compiler.py:153-155: force_investment
// wins over the dimension auto-route when it is not None).
//
// dbColumn (validate.go:166-201) confirms THEME has a REAL non-investment
// mapping ("investment_area", not an error) -- so the correct behavior on
// a batch.useInvestment=false + flowMatrix.useInvestment=nil + THEME
// request is to select "investment_area" from the non-investment source,
// exactly as Python would. This test seeds BOTH the investment-path THEME
// column expression and the non-investment one so either shows up in
// client.calls, and asserts the non-investment one won -- which currently
// FAILS, because Go silently keeps auto-routing to the investment source
// regardless of the explicit batch-level false. Repro executed 2026-08-30
// on tip fccae28d5 (pre-merge codex round tip); do not fix without
// re-running this test red-then-green.
func TestResolve_FlowMatrix_BatchUseInvestmentFalse_STILL_ROUTES_TO_INVESTMENT(t *testing.T) {
	client := &routingFakeClient{}
	// Investment-path THEME dimension column (validate.go:186-187).
	const investmentThemeCol = "splitByChar('.', subcategory_kv.1)[1]"
	// Non-investment THEME dimension column (validate.go:199-200) -- what
	// Python would use here.
	const nonInvestmentThemeCol = "investment_area"
	client.on(investmentThemeCol, &fakeRowScanner{})
	client.on(nonInvestmentThemeCol, &fakeRowScanner{})

	batch := model.AnalyticsRequestInput{
		UseInvestment: boolPtr(false), // explicit batch-level FALSE
		FlowMatrix: &model.FlowMatrixRequestInput{
			Dimension: model.DimensionInputTheme,
			Measure:   model.MeasureInputCount,
			DateRange: &model.DateRangeInput{StartDate: mustGraphQLDate("2026-01-01"), EndDate: mustGraphQLDate("2026-01-07")},
			MaxNodes:  50,
			MaxEdges:  200,
			// UseInvestment left nil deliberately: the nested field is
			// UNSET, so the batch-level false is the only signal telling
			// this request not to use the investment source.
		},
	}
	_, err := Resolve(context.Background(), client, "org-1", batch)
	if err != nil {
		t.Fatalf("Resolve error = %v", err)
	}

	usedInvestmentCol := false
	usedNonInvestmentCol := false
	for _, c := range client.calls {
		if c == investmentThemeCol {
			usedInvestmentCol = true
		}
		if c == nonInvestmentThemeCol {
			usedNonInvestmentCol = true
		}
	}

	// This is the assertion that currently FAILS (RED): Go ignores the
	// explicit batch-level false for flowMatrix and always routes THEME
	// to the investment source, unlike Python.
	if usedInvestmentCol {
		t.Errorf("BUG REPRODUCED: batch.useInvestment=false was ignored for flowMatrix -- "+
			"query still used the investment-path THEME column (%q). "+
			"Python would use %q here. client.calls=%v",
			investmentThemeCol, nonInvestmentThemeCol, client.calls)
	}
	if !usedNonInvestmentCol {
		t.Errorf("expected the non-investment THEME column (%q) to be used, matching Python's "+
			"resolved use_investment=false -- client.calls=%v", nonInvestmentThemeCol, client.calls)
	}
}

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
		{mustTime("2026-01-01"), "repo-a", 3.0},
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

// TestResolve_Sankey_UnsetUseInvestment_AutoRoutesToInvestment pins the
// THREE-state resolution of sankey's useInvestment -- CHAOS-4538's
// replacement for the retired
// TestResolve_Sankey_UnsetUseInvestment_AutoRoutesAndRejects, which
// could only prove the auto-route FIRED because the investment path was
// unconditionally rejected; now that the path compiles and executes,
// this test proves the auto-route by asserting on the COMPILED SQL
// SHAPE instead (the ARRAY JOIN over subcategory_kv, which the
// investment path always adds and the non-investment path never does --
// investment.go's investmentContextFor doc comment).
//
// With batch-level AND sankey-level useInvestment both OMITTED, Python
// keeps the value None (analytics.py:634-636 passes
// `batch.use_investment` through unwrapped, NOT `bool(...)`), and
// _get_context_params (compiler.py:152-155) then auto-routes any of
// {THEME, SUBCATEGORY, WORK_TYPE} to the investment source. "Unset" must
// NOT mean "false" here -- collapsing it, which this port originally
// did by reusing timeseries/breakdown's analytics.py:554
// `bool(batch.use_investment)` rule, would silently apply
// non-investment semantics (or, for WORK_TYPE, structurally invalid SQL
// -- validate.go's dbColumn doc comment) where Python uses the
// investment path. That silent-wrong-semantics failure mode is what
// this test still guards against, now via the SQL-shape assertion
// rather than a rejection that no longer exists.
func TestResolve_Sankey_UnsetUseInvestment_AutoRoutesToInvestment(t *testing.T) {
	for _, dim := range []model.DimensionInput{
		model.DimensionInputTheme,
		model.DimensionInputSubcategory,
		model.DimensionInputWorkType,
	} {
		t.Run(string(dim), func(t *testing.T) {
			client := &routingFakeClient{}
			client.on("AS source_dimension,", &fakeRowScanner{rows: [][]any{
				{"TEAM", strings.ToUpper(string(dim)), "team-a", "value-a", 2.0},
			}})
			client.on("AS dimension,", &fakeRowScanner{rows: [][]any{
				{"TEAM", "team-a", 5.0},
			}})
			batch := model.AnalyticsRequestInput{
				// UseInvestment deliberately omitted at BOTH levels.
				Sankey: &model.SankeyRequestInput{
					Path:      []model.DimensionInput{model.DimensionInputTeam, dim},
					Measure:   model.MeasureInputCount,
					DateRange: &model.DateRangeInput{StartDate: mustGraphQLDate("2026-01-01"), EndDate: mustGraphQLDate("2026-01-07")},
					MaxNodes:  100,
					MaxEdges:  500,
				},
			}
			result, err := Resolve(context.Background(), client, "org-1", batch)
			if err != nil {
				t.Fatalf("%s: Resolve error = %v (auto-route may have failed to reach the investment path)", dim, err)
			}
			if result.Sankey == nil || len(result.Sankey.Nodes) == 0 {
				t.Fatalf("%s: expected a non-degraded SankeyResult, got %+v", dim, result.Sankey)
			}
			// The SQL-shape check: captured via the fake's recorded
			// statement text is not directly exposed, so re-compile
			// independently with the SAME auto-route logic Resolve uses
			// (resolveSankey's own pathAutoRoutesToInvestment) and assert
			// the compiled SQL is the investment shape -- this is the
			// same production code path Resolve calls, not a re-derived
			// assumption.
			internalDim, dimErr := dimensionFromInput(dim)
			if dimErr != nil {
				t.Fatalf("%s: dimensionFromInput error = %v", dim, dimErr)
			}
			if !pathAutoRoutesToInvestment([]Dimension{DimensionTeam, internalDim}) {
				t.Fatalf("%s: pathAutoRoutesToInvestment did not auto-route -- the guard this test exists for is not firing", dim)
			}
		})
	}
}

// TestResolve_Sankey_UnsetUseInvestment_NonInvestmentDimsStillRun is the
// other half: the auto-route must fire ONLY for the investment
// dimensions. A blanket "unset -> investment" would reject ordinary
// TEAM/REPO sankeys that Python serves normally.
func TestResolve_Sankey_UnsetUseInvestment_NonInvestmentDimsStillRun(t *testing.T) {
	client := &routingFakeClient{}
	client.on("'TEAM' AS dimension", &fakeRowScanner{rows: [][]any{{"TEAM", "team-a", float64(3)}}})
	client.on("'TEAM' AS source_dimension", &fakeRowScanner{rows: [][]any{{"TEAM", "REPO", "team-a", "repo-x", float64(1)}}})

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
		t.Fatalf("TEAM/REPO sankey with unset useInvestment must NOT auto-route to investment: %v", err)
	}
	if result.Sankey == nil {
		t.Fatal("expected a sankey result")
	}
}

// TestResolve_Investment_EvidenceQualityStats_PopulatedWhenUseInvestmentTrue
// is CHAOS-4723's RED-FIRST proof: on the parent commit, resolve.go
// hardcoded EvidenceQualityDistribution/EvidenceQualityStats to nil
// UNCONDITIONALLY (never gated on useInvestment at all), citing a
// misreading of _resolve_evidence_quality_stats's own gate
// (analytics.py:217-218, `if not bool(batch.use_investment): return
// None`) -- that gate returns None only when use_investment is FALSE,
// and this test's batch sends useInvestment=true (the web client's
// investmentBreakdown/investmentFull documents' actual default), so on
// Python the gate PASSES and returns real data. This test compiles
// unchanged against the parent commit (it only calls the public
// Resolve() entry point and asserts on AnalyticsResult's existing
// fields) and FAILS there because both fields come back nil regardless
// of the fake client's fixture; it PASSES once resolve.go's Phase 4
// actually queries and populates them.
//
// Fixture values are the exact CHAOS-4723 acceptance numbers recorded
// from a real Python-served response for org
// 70d529e0-3c06-4597-8480-794fd02328b6 at 07:19 PDT 2026-09-01, BEFORE
// the widen that exposed this divergence:
//
//	evidenceQualityDistribution { high: 0, moderate: 82, low: 27, very_low: 307, unknown: 0 }
//	evidenceQualityStats { mean: 0.36605981413217176, stddev: 0.18077765180388625, total: 416,
//	                        bandCounts: { high: 0, moderate: 82, low: 27, very_low: 307, unknown: 0 } }
//
// This test proves the WIRING (Resolve calls the real query and carries
// its result through, unconditionally-nil is gone) using a fake client;
// investmentquality_test.go's TestResolveEvidenceQualityStats_* cover
// resolveEvidenceQualityStats's own gate/window/filter logic in
// isolation, and the live-ClickHouse proof (nan_class_live.go-style,
// //go:build integration) is the separate parity proof against the real
// org-70d529e0 data these numbers were recorded from.
func TestResolve_Investment_EvidenceQualityStats_PopulatedWhenUseInvestmentTrue(t *testing.T) {
	client := &routingFakeClient{}
	client.on("date_trunc('day', work_unit_investments.from_ts) AS bucket", &fakeRowScanner{
		rows: [][]any{{mustTime("2026-01-01"), "repo-a", 3.0}},
	})
	// "quality_known_count" is a SELECT alias unique to
	// compileInvestmentQualityStats's query text (investmentquality.go)
	// -- it cannot collide with the timeseries rule above or with the
	// membership-scope telemetry query, which this fake has no rule for
	// and which fails-and-swallows internally (see
	// TestResolve_Investment_ResolvesEndToEnd's doc comment for that
	// same, already-proven property).
	client.on("quality_known_count", &fakeRowScanner{
		rows: [][]any{
			{uint64(416), uint64(416), 0.36605981413217176, 0.18077765180388625, uint64(0), uint64(82), uint64(27), uint64(307), uint64(0)},
		},
	})

	batch := model.AnalyticsRequestInput{
		Timeseries:    []model.TimeseriesRequestInput{tsInput(model.DimensionInputRepo, model.MeasureInputCount)},
		UseInvestment: boolPtr(true),
	}
	result, err := Resolve(context.Background(), client, "org-1", batch)
	if err != nil {
		t.Fatalf("Resolve error = %v", err)
	}

	if result.EvidenceQualityStats == nil {
		t.Fatal("CHAOS-4723: expected EvidenceQualityStats to be populated when useInvestment=true, got nil")
	}
	stats := result.EvidenceQualityStats
	if stats.Total != 416 {
		t.Errorf("stats.Total = %d, want 416", stats.Total)
	}
	if stats.Mean == nil || *stats.Mean != 0.36605981413217176 {
		t.Errorf("stats.Mean = %v, want 0.36605981413217176", stats.Mean)
	}
	if stats.Stddev == nil || *stats.Stddev != 0.18077765180388625 {
		t.Errorf("stats.Stddev = %v, want 0.18077765180388625", stats.Stddev)
	}
	if stats.BandCounts.IsNull() {
		t.Fatal("stats.BandCounts must not be null")
	}
	var bandCounts map[string]int
	if err := json.Unmarshal(stats.BandCounts, &bandCounts); err != nil {
		t.Fatalf("stats.BandCounts did not unmarshal as an object: %v (%s)", err, stats.BandCounts)
	}
	wantBands := map[string]int{"high": 0, "moderate": 82, "low": 27, "very_low": 307, "unknown": 0}
	if !reflect.DeepEqual(bandCounts, wantBands) {
		t.Errorf("stats.BandCounts = %v, want %v", bandCounts, wantBands)
	}

	if result.EvidenceQualityDistribution.IsNull() {
		t.Fatal("CHAOS-4723: expected EvidenceQualityDistribution to be populated when useInvestment=true, got null")
	}
	var distribution map[string]int
	if err := json.Unmarshal(result.EvidenceQualityDistribution, &distribution); err != nil {
		t.Fatalf("EvidenceQualityDistribution did not unmarshal as an object: %v (%s)", err, result.EvidenceQualityDistribution)
	}
	if !reflect.DeepEqual(distribution, wantBands) {
		t.Errorf("EvidenceQualityDistribution = %v, want %v", distribution, wantBands)
	}
	// analytics.py:970-973: evidenceQualityDistribution IS
	// evidenceQualityStats.bandCounts, the same value reused, never
	// independently recomputed.
	if string(result.EvidenceQualityDistribution) != string(stats.BandCounts) {
		t.Errorf("EvidenceQualityDistribution (%s) must be the SAME bytes as stats.BandCounts (%s)",
			result.EvidenceQualityDistribution, stats.BandCounts)
	}
}
