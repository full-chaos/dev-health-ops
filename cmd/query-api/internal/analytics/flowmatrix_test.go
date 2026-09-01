package analytics

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graph/model"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graphqldate"
)

// --- fakes ---------------------------------------------------------------

// fakeRowScanner is one scripted response. errAfter lets a test express a
// MID-STREAM failure -- some rows Scan() successfully, THEN iteration
// fails -- distinct from a pre-failed scanner that yields zero rows
// before the first Next(). The two are NOT interchangeable as a test of
// "does the caller discard a partial slice on error": a pre-failed
// scanner (errAfter=0, the default) passes whether or not the caller has
// the CHAOS-4534-sibling partial-row bug (Lane B's finding, this
// package's BRIEF.md "PARTIAL-ROW CLASS" section) -- only errAfter>0
// actually distinguishes correct code from buggy code.
type fakeRowScanner struct {
	rows     [][]any
	cursor   int
	err      error
	errAfter int
}

func (f *fakeRowScanner) Next() bool {
	if f == nil {
		return false
	}
	if f.err != nil && f.cursor >= f.errAfter {
		return false
	}
	return f.cursor < len(f.rows)
}

func (f *fakeRowScanner) Scan(dest ...any) error {
	row := f.rows[f.cursor]
	f.cursor++
	if len(dest) != len(row) {
		return errors.New("flowmatrix test: scan arity mismatch")
	}
	for i, d := range dest {
		switch ptr := d.(type) {
		case *string:
			v, ok := row[i].(string)
			if !ok {
				return fmt.Errorf("scan col %d: destination *string but fixture holds %T -- the real driver errors on a type mismatch, it does not convert", i, row[i])
			}
			*ptr = v
		case *uint64:
			v, ok := row[i].(uint64)
			if !ok {
				return fmt.Errorf("scan col %d: destination *uint64 but fixture holds %T -- ClickHouse returns UInt64 for count()/uniqExact()/SUM(UInt*); the native driver will NOT scan it into another type", i, row[i])
			}
			*ptr = v
		case *int64:
			// Added for investmentmembershiptelemetry.go's lag_seconds
			// (a toInt64(...) SQL expression, not the UInt64-producing
			// count()/uniqExact()/SUM(UInt*) shape every other scan in
			// this package uses -- it is the only *int64 destination in
			// the whole port). Verified against the real driver before
			// adding this case, not assumed: clickhouse-go/v2's
			// lib/column/column_gen.go has a dedicated generated Int64
			// column type with `case *int64:` scan destinations
			// (v2.47.0, this module's pinned version) -- Int64 -> *int64
			// is a standard supported pairing, unlike UInt64 (which
			// specifically requires *uint64 because a real UInt64 can
			// exceed int64's range).
			v, ok := row[i].(int64)
			if !ok {
				return fmt.Errorf("scan col %d: destination *int64 but fixture holds %T -- ClickHouse returns Int64 for toInt64(...); the native driver will NOT scan it into another type", i, row[i])
			}
			*ptr = v
		case *float64:
			// CHAOS-4650: a nil fixture cell stands in for a NULL
			// Nullable(Float64) value. Verified against the real
			// driver, not assumed: nullable.go's Nullable.ScanRow only
			// recognises **T (double-pointer) cases in its NULL switch
			// (case **uint64/**float64/etc, all `*v = nil`) -- a bare
			// *float64 matches none of them, is not an sql.Scanner
			// either, so the NULL branch falls through to `return nil`
			// WITHOUT writing to *ptr at all. The destination is left
			// at its zero-initialised value (Go's `var value float64`
			// defaults to 0.0) -- silently, no error. THIS is the
			// documented CHAOS-4650 mechanism (SQL NULL scanned into a
			// non-nullable float64 silently reads back as 0.0); a real
			// type-mismatch (fixture holds neither float64 nor nil) is
			// still an error below, matching every other case's
			// contract.
			if row[i] == nil {
				continue
			}
			v, ok := row[i].(float64)
			if !ok {
				return fmt.Errorf("scan col %d: destination *float64 but fixture holds %T -- an aggregate returning UInt64 cannot be scanned into float64 by the native driver (see reviewedges.go:145's UInt32 note)", i, row[i])
			}
			*ptr = v
		case **float64:
			// CHAOS-4650: the nullable-aware destination breakdown.go's
			// executeBreakdownRaw now scans category-2 AT-RISK measures
			// into (var value *float64; rows.Scan(&dimValue, &value)).
			// Verified against the real driver, not assumed:
			// clickhouse-go v2.47.0's lib/column/nullable.go
			// Nullable.ScanRow intercepts a NULL row FIRST and only
			// recognises `case **float64: *v = nil` as a nullable
			// destination (a bare *float64 never sees the NULL at
			// all -- Float64.ScanRow's `case *float64` branch is
			// unreachable for a null cell); a non-NULL row falls
			// through to Float64.ScanRow, whose `case **float64`
			// allocates a fresh float64 and sets *d = &value. A
			// fixture row cell of literal `nil` stands in for the
			// NULL case; anything else must be a float64, matching a
			// real non-NULL Nullable(Float64) row.
			if row[i] == nil {
				*ptr = nil
				continue
			}
			v, ok := row[i].(float64)
			if !ok {
				return fmt.Errorf("scan col %d: destination **float64 but fixture holds %T (want float64 or nil)", i, row[i])
			}
			allocated := v
			*ptr = &allocated
		case *time.Time:
			// The native driver's Date.ScanRow accepts *time.Time,
			// **time.Time or sql.Scanner -- and NOTHING else. It used to
			// accept *graphqldate.Date here, which the real driver
			// rejects (graphqldate.Date has no Scan method), so every
			// non-empty timeseries result failed in production while the
			// tests passed. A fake kinder than the driver cannot fail on
			// the class the driver enforces.
			v, ok := row[i].(time.Time)
			if !ok {
				return fmt.Errorf("scan col %d: destination *time.Time but fixture holds %T -- a ClickHouse Date/DateTime arrives as time.Time", i, row[i])
			}
			*ptr = v
		default:
			// The real driver accepts a FIXED set of destinations. Anything
			// else is a production failure, not a test-fixture gap -- most
			// notably *graphqldate.Date, which has no Scan method and is
			// therefore not an sql.Scanner. Scan the driver's type
			// (time.Time) and convert after, as reviewedges.go:152 does.
			return fmt.Errorf("scan col %d: destination %T is not one of the types the real ClickHouse driver accepts (*string, *uint64, *int64, *float64, **float64, *time.Time, sql.Scanner)", i, d)
		}
	}
	return nil
}

func (f *fakeRowScanner) Err() error {
	if f == nil {
		return nil
	}
	return f.err
}
func (f *fakeRowScanner) Close() error { return nil }

// fakeClient dispatches by inspecting the statement text for "dimension,"
// (nodes queries alias their value-bearing column set as `... AS
// dimension, ... AS node_id, ...`) vs "source_dimension," (edges queries)
// rather than by call ORDER -- ExecuteFlowMatrix runs both queries
// concurrently via goroutines, so ordering between the two Query calls is
// not guaranteed and a fake keyed on call index would be flaky.
type fakeClient struct {
	mu             sync.Mutex
	nodesResponse  *fakeRowScanner
	edgesResponse  *fakeRowScanner
	nodesErr       error
	edgesErr       error
	nodesCalled    bool
	edgesCalled    bool
	nodesStatement string
	edgesStatement string
	nodesBindings  []clickhouse.Binding
	edgesBindings  []clickhouse.Binding
}

func (f *fakeClient) Query(_ context.Context, statement string, bindings []clickhouse.Binding) (clickhouse.RowScanner, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if strings.Contains(statement, "AS source_dimension,") {
		f.edgesCalled = true
		f.edgesStatement = statement
		f.edgesBindings = bindings
		if f.edgesErr != nil {
			return nil, f.edgesErr
		}
		return f.edgesResponse, nil
	}
	f.nodesCalled = true
	f.nodesStatement = statement
	f.nodesBindings = bindings
	if f.nodesErr != nil {
		return nil, f.nodesErr
	}
	return f.nodesResponse, nil
}

func mustDate(t *testing.T, s string) graphqldate.Date {
	t.Helper()
	d, err := graphqldate.Parse(s)
	if err != nil {
		t.Fatalf("graphqldate.Parse(%q): %v", s, err)
	}
	return d
}

// --- dimensionFromInput / measureFromInput --------------------------------

func TestDimensionFromInput_AllValues(t *testing.T) {
	cases := map[model.DimensionInput]Dimension{
		model.DimensionInputTeam:        DimensionTeam,
		model.DimensionInputRepo:        DimensionRepo,
		model.DimensionInputAuthor:      DimensionAuthor,
		model.DimensionInputWorkType:    DimensionWorkType,
		model.DimensionInputTheme:       DimensionTheme,
		model.DimensionInputSubcategory: DimensionSubcategory,
	}
	for in, want := range cases {
		got, err := dimensionFromInput(in)
		if err != nil {
			t.Fatalf("dimensionFromInput(%v) error = %v", in, err)
		}
		if got != want {
			t.Fatalf("dimensionFromInput(%v) = %v, want %v", in, got, want)
		}
	}
}

func TestDimensionFromInput_Invalid(t *testing.T) {
	_, err := dimensionFromInput(model.DimensionInput("BOGUS"))
	if err == nil {
		t.Fatal("expected error for invalid dimension")
	}
	var ve *ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected *ValidationError, got %T: %v", err, err)
	}
}

func TestMeasureFromInput_AllValues(t *testing.T) {
	cases := map[model.MeasureInput]Measure{
		model.MeasureInputCount:                MeasureCount,
		model.MeasureInputChurnLoc:             MeasureChurnLOC,
		model.MeasureInputPrReworkRatio:        MeasurePRReworkRatio,
		model.MeasureInputCycleTimeHours:       MeasureCycleTimeHours,
		model.MeasureInputThroughput:           MeasureThroughput,
		model.MeasureInputPipelineSuccessRate:  MeasurePipelineSuccessRate,
		model.MeasureInputPipelineFailureRate:  MeasurePipelineFailureRate,
		model.MeasureInputPipelineDurationP95:  MeasurePipelineDurationP95,
		model.MeasureInputPipelineQueueTime:    MeasurePipelineQueueTime,
		model.MeasureInputPipelineRerunRate:    MeasurePipelineRerunRate,
		model.MeasureInputTestPassRate:         MeasureTestPassRate,
		model.MeasureInputTestFailureRate:      MeasureTestFailureRate,
		model.MeasureInputTestFlakeRate:        MeasureTestFlakeRate,
		model.MeasureInputTestSuiteDurationP95: MeasureTestSuiteDurationP95,
		model.MeasureInputCoverageLinePct:      MeasureCoverageLinePct,
		model.MeasureInputCoverageBranchPct:    MeasureCoverageBranchPct,
		model.MeasureInputCoverageDeltaPct:     MeasureCoverageDeltaPct,
		model.MeasureInputFlagFrictionDelta:    MeasureFlagFrictionDelta,
		model.MeasureInputFlagErrorRateDelta:   MeasureFlagErrorRateDelta,
		model.MeasureInputFlagCoverageRatio:    MeasureFlagCoverageRatio,
		model.MeasureInputFlagActivationRate:   MeasureFlagActivationRate,
	}
	if len(cases) != len(model.AllMeasureInput) {
		t.Fatalf("test covers %d measures, schema declares %d -- AllMeasureInput drifted, update this test", len(cases), len(model.AllMeasureInput))
	}
	for in, want := range cases {
		got, err := measureFromInput(in)
		if err != nil {
			t.Fatalf("measureFromInput(%v) error = %v", in, err)
		}
		if got != want {
			t.Fatalf("measureFromInput(%v) = %v, want %v", in, got, want)
		}
	}
}

// --- CompileFlowMatrix -----------------------------------------------------

// TestCompileFlowMatrix_ExposedReadsCarryFinal is the CHAOS-4516 regression
// guard the PR promises: every one of the 6 flow-matrix templates must
// read `work_item_cycle_times AS wct FINAL` -- NOT just contain the word
// FINAL somewhere (BRIEF.md's documented trap: `FINAL` on the JOINED
// `work_items` table reads as present under a bare substring search but
// binds to the wrong alias). Covers both the 3 sites this port fixes
// (repo nodes, work_type nodes, the work_type CTE feeding work_type
// edges) and the 3 sites that were already correct (team nodes, team
// edges, the repo CTE feeding repo edges) so a future edit cannot
// silently regress either group.
func TestCompileFlowMatrix_ExposedReadsCarryFinal(t *testing.T) {
	const want = "work_item_cycle_times AS wct FINAL"
	templates := map[string]string{
		"team nodes":            flowMatrixTeamNodesTemplate,
		"team edges":            flowMatrixTeamEdgesTemplate,
		"repo nodes":            flowMatrixRepoNodesTemplate,
		"repo edges (CTE)":      flowMatrixRepoEdgesTemplate,
		"work_type nodes":       flowMatrixWorkTypeNodesTemplate,
		"work_type edges (CTE)": flowMatrixWorkTypeEdgesTemplate,
	}
	for name, tpl := range templates {
		if !strings.Contains(tpl, want) {
			t.Errorf("%s: query text does not contain %q -- work_item_cycle_times read is exposed (CHAOS-4516 regression)", name, want)
		}
	}
}

func TestCompileFlowMatrix_TeamRepoWorkType_BindingsAndTemplate(t *testing.T) {
	req := FlowMatrixRequest{
		Measure:   MeasureCount,
		StartDate: mustDate(t, "2026-01-01"),
		EndDate:   mustDate(t, "2026-01-31"),
		MaxNodes:  50,
		MaxEdges:  200,
	}

	cases := []struct {
		dim          Dimension
		wantNodesSQL string
		wantEdgesSQL string
	}{
		{DimensionTeam, flowMatrixTeamNodesTemplate, flowMatrixTeamEdgesTemplate},
		{DimensionRepo, flowMatrixRepoNodesTemplate, flowMatrixRepoEdgesTemplate},
		{DimensionWorkType, flowMatrixWorkTypeNodesTemplate, flowMatrixWorkTypeEdgesTemplate},
	}
	for _, tc := range cases {
		t.Run(string(tc.dim), func(t *testing.T) {
			req.Dimension = tc.dim
			nodes, edges, err := CompileFlowMatrix(req, "org-1", 30, nil)
			if err != nil {
				t.Fatalf("CompileFlowMatrix(%v) error = %v", tc.dim, err)
			}
			if nodes.sql != tc.wantNodesSQL {
				t.Errorf("nodes SQL mismatch for %v", tc.dim)
			}
			if edges.sql != tc.wantEdgesSQL {
				t.Errorf("edges SQL mismatch for %v", tc.dim)
			}

			nodesBindings := bindingMap(nodes.bindings)
			if nodesBindings["org_id"] != "org-1" {
				t.Errorf("nodes org_id binding = %v, want org-1", nodesBindings["org_id"])
			}
			if nodesBindings["limit_per_dim"] != 50 {
				t.Errorf("nodes limit_per_dim binding = %v, want 50", nodesBindings["limit_per_dim"])
			}
			edgesBindings := bindingMap(edges.bindings)
			if edgesBindings["max_edges"] != 200 {
				t.Errorf("edges max_edges binding = %v, want 200", edgesBindings["max_edges"])
			}
		})
	}
}

func bindingMap(bindings []clickhouse.Binding) map[string]any {
	out := make(map[string]any, len(bindings))
	for _, b := range bindings {
		out[b.Name] = b.Value
	}
	return out
}

// TestCompileFlowMatrix_RejectsActiveFiltersForSameDimension ports
// _reject_filtered_same_dimension_flow_matrix's CHAOS-2487 rejection
// (compiler.py:537-550): a same-dimension flow matrix with an active
// (non-org) scope filter must fail loudly, never silently return
// unfiltered org-wide data.
func TestCompileFlowMatrix_RejectsActiveFiltersForSameDimension(t *testing.T) {
	req := FlowMatrixRequest{
		Dimension: DimensionRepo,
		Measure:   MeasureCount,
		StartDate: mustDate(t, "2026-01-01"),
		EndDate:   mustDate(t, "2026-01-31"),
		MaxNodes:  50,
		MaxEdges:  200,
	}
	filters := &model.FilterInput{
		Scope: &model.ScopeFilterInput{
			Level: model.ScopeLevelInputTeam,
			Ids:   []string{"team-1"},
		},
	}
	_, _, err := CompileFlowMatrix(req, "org-1", 30, filters)
	if err == nil {
		t.Fatal("expected rejection for filtered same-dimension flow matrix")
	}
	var ve *ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected *ValidationError, got %T: %v", err, err)
	}
}

func TestCompileFlowMatrix_OrgScopeIsNotActive(t *testing.T) {
	// hasActiveFilters treats scope.level == ORG as inactive even with ids
	// present (compiler.py:278: "level.value != 'org'") -- confirm the
	// same-dimension flow matrix is NOT rejected for an org-scoped filter.
	req := FlowMatrixRequest{
		Dimension: DimensionRepo,
		Measure:   MeasureCount,
		StartDate: mustDate(t, "2026-01-01"),
		EndDate:   mustDate(t, "2026-01-31"),
		MaxNodes:  50,
		MaxEdges:  200,
	}
	filters := &model.FilterInput{
		Scope: &model.ScopeFilterInput{
			Level: model.ScopeLevelInputOrg,
			Ids:   []string{"org-1"},
		},
	}
	if _, _, err := CompileFlowMatrix(req, "org-1", 30, filters); err != nil {
		t.Fatalf("org-scoped filter should not be rejected: %v", err)
	}
}

// TestCompileFlowMatrix_AuthorDimensionRejectedWithValidationError is
// CHAOS-4538's replacement for the retired
// TestCompileFlowMatrix_AuthorThemeSubcategoryNotYetPorted's AUTHOR case.
// AUTHOR is no longer a "not yet ported" stub -- it now runs through the
// real compileFlowMatrixInvestmentDimension path and is rejected by
// dbColumn's unconditional AUTHOR rejection (validate.go doc comment:
// neither source table has a scalar per-row author identity column),
// which returns a real *ValidationError, matching Python. Pin that shape
// specifically so a future change that relaxes it back to a plain
// "not yet implemented" error is caught.
func TestCompileFlowMatrix_AuthorDimensionRejectedWithValidationError(t *testing.T) {
	req := FlowMatrixRequest{
		Dimension: DimensionAuthor,
		Measure:   MeasureCount,
		StartDate: mustDate(t, "2026-01-01"),
		EndDate:   mustDate(t, "2026-01-31"),
		MaxNodes:  50,
		MaxEdges:  200,
	}
	_, _, err := CompileFlowMatrix(req, "org-1", 30, nil)
	if err == nil {
		t.Fatal("expected AUTHOR dimension to be rejected")
	}
	var ve *ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected a real *ValidationError (matching Python's dbColumn rejection), got %T: %v", err, err)
	}
}

// TestCompileFlowMatrix_ThemeSubcategory_CompilesInlinedSource is
// CHAOS-4538's replacement for the retired
// TestCompileFlowMatrix_AuthorThemeSubcategoryNotYetPorted's THEME/
// SUBCATEGORY cases. THEME and SUBCATEGORY both auto-route to the
// investment path (resolveUseInvestment's investment_dims set,
// compileFlowMatrixInvestmentDimension's doc comment) and now compile
// successfully instead of returning the retired stub error. Pin the same
// structural shape TestCompileTimeseries_Investment_CompilesInlinedSource
// pins: no leading WITH (clickhouse/client.go:190).
func TestCompileFlowMatrix_ThemeSubcategory_CompilesInlinedSource(t *testing.T) {
	for _, dim := range []Dimension{DimensionTheme, DimensionSubcategory} {
		t.Run(string(dim), func(t *testing.T) {
			req := FlowMatrixRequest{
				Dimension: dim,
				Measure:   MeasureCount,
				StartDate: mustDate(t, "2026-01-01"),
				EndDate:   mustDate(t, "2026-01-31"),
				MaxNodes:  50,
				MaxEdges:  200,
			}
			nodes, edges, err := CompileFlowMatrix(req, "org-1", 30, nil)
			if err != nil {
				t.Fatalf("dimension %v: CompileFlowMatrix error = %v", dim, err)
			}
			for _, q := range []struct {
				name string
				sql  string
			}{{"nodes", nodes.sql}, {"edges", edges.sql}} {
				trimmed := strings.TrimSpace(q.sql)
				if !strings.HasPrefix(trimmed, "SELECT") {
					t.Fatalf("dimension %v %s SQL must start with a literal SELECT, got prefix: %q", dim, q.name, trimmed[:min(40, len(trimmed))])
				}
				if strings.Contains(q.sql, "\nWITH ") || strings.HasPrefix(trimmed, "WITH") {
					t.Errorf("dimension %v %s SQL must never contain a top-level WITH clause, got: %s", dim, q.name, q.sql)
				}
			}
		})
	}
}

// --- ExecuteFlowMatrix -----------------------------------------------------

func TestExecuteFlowMatrix_Success(t *testing.T) {
	client := &fakeClient{
		nodesResponse: &fakeRowScanner{rows: [][]any{
			{"TEAM", "team-a", float64(7)},
			{"TEAM", "team-b", float64(3)},
		}},
		edgesResponse: &fakeRowScanner{rows: [][]any{
			{"TEAM", "TEAM", "team-a", "team-b", float64(2)},
		}},
	}
	nodesQ := compiledQuery{sql: flowMatrixTeamNodesTemplate}
	edgesQ := compiledQuery{sql: flowMatrixTeamEdgesTemplate}

	nodes, edges, err := ExecuteFlowMatrix(context.Background(), client, nodesQ, edgesQ)
	if err != nil {
		t.Fatalf("ExecuteFlowMatrix error = %v", err)
	}
	if !client.nodesCalled || !client.edgesCalled {
		t.Fatalf("expected both nodes and edges queries to run, nodesCalled=%v edgesCalled=%v", client.nodesCalled, client.edgesCalled)
	}
	if len(nodes) != 2 {
		t.Fatalf("got %d nodes, want 2", len(nodes))
	}
	// f"{dim}:{node_id}" shape, analytics.py:297.
	// CHAOS-4701: SankeyNode.Value/SankeyEdge.Value are *float64, not
	// float64 -- dereference for the populated-value comparison (same
	// discipline as resolve_test.go's TestResolve_Investment_ResolvesEndToEnd,
	// CHAOS-4657).
	if nodes[0].ID != "TEAM:team-a" || nodes[0].Label != "team-a" || nodes[0].Dimension != "TEAM" || nodes[0].Value == nil || *nodes[0].Value != 7 {
		t.Fatalf("unexpected node[0]: %+v", nodes[0])
	}
	if len(edges) != 1 {
		t.Fatalf("got %d edges, want 1", len(edges))
	}
	if edges[0].Source != "TEAM:team-a" || edges[0].Target != "TEAM:team-b" || edges[0].Value == nil || *edges[0].Value != 2 {
		t.Fatalf("unexpected edge[0]: %+v", edges[0])
	}
}

func TestExecuteFlowMatrix_NodesErrorPropagates(t *testing.T) {
	client := &fakeClient{
		nodesErr:      errors.New("boom"),
		edgesResponse: &fakeRowScanner{},
	}
	nodesQ := compiledQuery{sql: flowMatrixTeamNodesTemplate}
	edgesQ := compiledQuery{sql: flowMatrixTeamEdgesTemplate}

	_, _, err := ExecuteFlowMatrix(context.Background(), client, nodesQ, edgesQ)
	if err == nil {
		t.Fatal("expected error to propagate from ExecuteFlowMatrix")
	}
	// Both queries still run -- no cancel-on-first-error, matching
	// _execute_sankey_inner's asyncio.gather semantics.
	if !client.edgesCalled {
		t.Fatal("edges query should still have been issued despite the nodes error")
	}
}

func TestExecuteFlowMatrix_EdgesErrorPropagates(t *testing.T) {
	client := &fakeClient{
		nodesResponse: &fakeRowScanner{},
		edgesErr:      errors.New("boom"),
	}
	nodesQ := compiledQuery{sql: flowMatrixTeamNodesTemplate}
	edgesQ := compiledQuery{sql: flowMatrixTeamEdgesTemplate}

	_, _, err := ExecuteFlowMatrix(context.Background(), client, nodesQ, edgesQ)
	if err == nil {
		t.Fatal("expected error to propagate from ExecuteFlowMatrix")
	}
	if !client.nodesCalled {
		t.Fatal("nodes query should still have been issued despite the edges error")
	}
}

// TestExecuteFlowMatrix_MidStreamNodesFailureDiscardsPartialRows is the
// PARTIAL-ROW CLASS regression guard (BRIEF.md, found live in Lane B's
// fetchPeriodRows): a scanner that yields ONE row successfully and THEN
// fails must not leave that row feeding the caller -- ExecuteFlowMatrix
// must return nil/empty, never a 1-row slice that looks like a complete,
// merely-small result.
func TestExecuteFlowMatrix_MidStreamNodesFailureDiscardsPartialRows(t *testing.T) {
	client := &fakeClient{
		nodesResponse: &fakeRowScanner{
			rows:     [][]any{{"TEAM", "team-a", float64(7)}, {"TEAM", "team-b", float64(3)}},
			err:      errors.New("mid-stream failure"),
			errAfter: 1, // first row scans fine, second Next() call fails
		},
		edgesResponse: &fakeRowScanner{},
	}
	nodesQ := compiledQuery{sql: flowMatrixTeamNodesTemplate}
	edgesQ := compiledQuery{sql: flowMatrixTeamEdgesTemplate}

	nodes, _, err := ExecuteFlowMatrix(context.Background(), client, nodesQ, edgesQ)
	if err == nil {
		t.Fatal("expected mid-stream failure to surface as an error")
	}
	if nodes != nil {
		t.Fatalf("expected nil nodes on mid-stream failure, got %d partial rows: %+v", len(nodes), nodes)
	}
}

func TestExecuteFlowMatrix_MidStreamEdgesFailureDiscardsPartialRows(t *testing.T) {
	client := &fakeClient{
		nodesResponse: &fakeRowScanner{},
		edgesResponse: &fakeRowScanner{
			rows:     [][]any{{"TEAM", "TEAM", "team-a", "team-b", float64(2)}, {"TEAM", "TEAM", "team-b", "team-c", float64(1)}},
			err:      errors.New("mid-stream failure"),
			errAfter: 1,
		},
	}
	nodesQ := compiledQuery{sql: flowMatrixTeamNodesTemplate}
	edgesQ := compiledQuery{sql: flowMatrixTeamEdgesTemplate}

	_, edges, err := ExecuteFlowMatrix(context.Background(), client, nodesQ, edgesQ)
	if err == nil {
		t.Fatal("expected mid-stream failure to surface as an error")
	}
	if edges != nil {
		t.Fatalf("expected nil edges on mid-stream failure, got %d partial rows: %+v", len(edges), edges)
	}
}

// TestQueryNodes_MidStreamFailureDiscardsPartialRows tests queryNodes
// DIRECTLY, not through ExecuteFlowMatrix -- ExecuteFlowMatrix has its
// OWN unconditional discard on nodesErr/edgesErr ("return nil, nil,
// err"), which masks a regression in queryNodes' own guard from any test
// that only goes through ExecuteFlowMatrix (empirically confirmed: with
// queryNodes' guard removed, the ExecuteFlowMatrix-level test still
// passed, because the outer function discards regardless of what
// queryNodes returns). This test exercises the actual scanning function,
// closing that gap.
func TestQueryNodes_MidStreamFailureDiscardsPartialRows(t *testing.T) {
	client := &fakeClient{
		nodesResponse: &fakeRowScanner{
			rows:     [][]any{{"TEAM", "team-a", float64(7)}, {"TEAM", "team-b", float64(3)}},
			err:      errors.New("mid-stream failure"),
			errAfter: 1,
		},
	}
	q := compiledQuery{sql: flowMatrixTeamNodesTemplate}
	nodes, err := queryNodes(context.Background(), client, q)
	if err == nil {
		t.Fatal("expected mid-stream failure to surface as an error")
	}
	if nodes != nil {
		t.Fatalf("expected nil nodes on mid-stream failure, got %d partial rows: %+v", len(nodes), nodes)
	}
}

func TestQueryEdges_MidStreamFailureDiscardsPartialRows(t *testing.T) {
	client := &fakeClient{
		edgesResponse: &fakeRowScanner{
			rows:     [][]any{{"TEAM", "TEAM", "team-a", "team-b", float64(2)}, {"TEAM", "TEAM", "team-b", "team-c", float64(1)}},
			err:      errors.New("mid-stream failure"),
			errAfter: 1,
		},
	}
	q := compiledQuery{sql: flowMatrixTeamEdgesTemplate}
	edges, err := queryEdges(context.Background(), client, q)
	if err == nil {
		t.Fatal("expected mid-stream failure to surface as an error")
	}
	if edges != nil {
		t.Fatalf("expected nil edges on mid-stream failure, got %d partial rows: %+v", len(edges), edges)
	}
}

// TestQueryNodes_AllNullValueYieldsNilValue_NotZero is the CHAOS-4701
// regression guard for queryNodes' nullable scan -- same shape and same
// both-directions requirement as CHAOS-4650's breakdown sibling
// (breakdown_test.go's TestExecuteBreakdown_AllNullGroupYieldsNilValue_NotZero)
// and CHAOS-4657's timeseries sibling
// (timeseries_test.go's TestExecuteTimeseries_AllNullBucketYieldsNilValue_NotZero).
// Proven in BOTH directions in the SAME result set: a null-only
// assertion cannot catch a change that nils out EVERY value, which is
// the WORSE bug (a populated node/edge silently losing its real value).
// One row (team-null) whose measure column is SQL NULL must come back
// as model.SankeyNode.Value/SankeyEdge.Value == nil (JSON null on the
// wire, not the literal 0); another row (team-real) in the SAME result
// set must still carry its real float64.
//
// RED-on-baseline (executed against origin/main's parent commit,
// e237f87bc403, in a detached worktree, BEFORE this fix): queryNodes'
// pre-fix `var value float64` (bare, non-pointer) destination does not
// compile against this test at all -- the fake's **float64 case is a
// NEW addition needed by the fix, so the parent commit's queryNodes
// passes a bare *float64 to Scan, which the fakeRowScanner's existing
// *float64 case accepts, silently reading the NULL fixture cell as the
// zero-initialised 0.0 (same mechanism nullable.go's real driver
// documents -- see fakeRowScanner's *float64 case comment above). The
// pre-fix code makes team-null's Value compare equal to 0 (the zero
// value), not nil -- indistinguishable from a genuinely measured zero,
// which is exactly the silent collapse this ticket exists to remove.
func TestQueryNodes_AllNullValueYieldsNilValue_NotZero(t *testing.T) {
	client := &fakeClient{
		nodesResponse: &fakeRowScanner{rows: [][]any{
			{"TEAM", "team-null", nil},          // SQL NULL -- the all-NULL-group shape
			{"TEAM", "team-real", float64(9.5)}, // populated -- the other direction
		}},
	}
	q := compiledQuery{sql: flowMatrixTeamNodesTemplate}
	nodes, err := queryNodes(context.Background(), client, q)
	if err != nil {
		t.Fatalf("queryNodes error = %v", err)
	}
	if len(nodes) != 2 {
		t.Fatalf("got %d nodes, want 2", len(nodes))
	}
	nullNode := nodes[0]
	realNode := nodes[1]
	if nullNode.ID != "TEAM:team-null" || nullNode.Value != nil {
		t.Fatalf("expected team-null's Value to be nil (SQL NULL scanned nullable, not silently 0.0), got %+v", nullNode)
	}
	if realNode.ID != "TEAM:team-real" || realNode.Value == nil || *realNode.Value != 9.5 {
		t.Fatalf("expected team-real's Value to be the real populated 9.5 -- a fix that nils out EVERY value would also pass a null-only test, got %+v", realNode)
	}
}

// TestQueryEdges_AllNullValueYieldsNilValue_NotZero is queryEdges' half
// of TestQueryNodes_AllNullValueYieldsNilValue_NotZero -- same shape,
// same both-directions discipline, applied to the edges scan.
func TestQueryEdges_AllNullValueYieldsNilValue_NotZero(t *testing.T) {
	client := &fakeClient{
		edgesResponse: &fakeRowScanner{rows: [][]any{
			{"TEAM", "TEAM", "team-a", "team-null", nil},           // SQL NULL
			{"TEAM", "TEAM", "team-a", "team-real", float64(4.25)}, // populated
		}},
	}
	q := compiledQuery{sql: flowMatrixTeamEdgesTemplate}
	edges, err := queryEdges(context.Background(), client, q)
	if err != nil {
		t.Fatalf("queryEdges error = %v", err)
	}
	if len(edges) != 2 {
		t.Fatalf("got %d edges, want 2", len(edges))
	}
	nullEdge := edges[0]
	realEdge := edges[1]
	if nullEdge.Target != "TEAM:team-null" || nullEdge.Value != nil {
		t.Fatalf("expected the team-null edge's Value to be nil (SQL NULL scanned nullable, not silently 0.0), got %+v", nullEdge)
	}
	if realEdge.Target != "TEAM:team-real" || realEdge.Value == nil || *realEdge.Value != 4.25 {
		t.Fatalf("expected the team-real edge's Value to be the real populated 4.25 -- a fix that nils out EVERY value would also pass a null-only test, got %+v", realEdge)
	}
}
