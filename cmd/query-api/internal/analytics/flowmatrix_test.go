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

// --- fakes ---------------------------------------------------------------

// fakeRowScanner is one scripted response.
type fakeRowScanner struct {
	rows   [][]any
	cursor int
	err    error
}

func (f *fakeRowScanner) Next() bool {
	if f == nil {
		return false
	}
	if f.err != nil {
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
			*ptr = row[i].(string)
		case *uint64:
			*ptr = row[i].(uint64)
		case *float64:
			*ptr = row[i].(float64)
		case *graphqldate.Date:
			*ptr = row[i].(graphqldate.Date)
		default:
			return errors.New("flowmatrix test: unsupported scan destination")
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

func TestCompileFlowMatrix_AuthorThemeSubcategoryNotYetPorted(t *testing.T) {
	for _, dim := range []Dimension{DimensionAuthor, DimensionTheme, DimensionSubcategory} {
		req := FlowMatrixRequest{
			Dimension: dim,
			Measure:   MeasureCount,
			StartDate: mustDate(t, "2026-01-01"),
			EndDate:   mustDate(t, "2026-01-31"),
			MaxNodes:  50,
			MaxEdges:  200,
		}
		_, _, err := CompileFlowMatrix(req, "org-1", 30, nil)
		if err == nil {
			t.Fatalf("dimension %v: expected 'not yet ported' error, got nil", dim)
		}
		var ve *ValidationError
		if errors.As(err, &ve) {
			t.Fatalf("dimension %v: expected a plain not-yet-implemented error, got a ValidationError (%v) -- do not let this read as a real input rejection", dim, err)
		}
	}
}

// --- ExecuteFlowMatrix -----------------------------------------------------

func TestExecuteFlowMatrix_Success(t *testing.T) {
	client := &fakeClient{
		nodesResponse: &fakeRowScanner{rows: [][]any{
			{"TEAM", "team-a", uint64(7)},
			{"TEAM", "team-b", uint64(3)},
		}},
		edgesResponse: &fakeRowScanner{rows: [][]any{
			{"TEAM", "TEAM", "team-a", "team-b", uint64(2)},
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
	if nodes[0].ID != "TEAM:team-a" || nodes[0].Label != "team-a" || nodes[0].Dimension != "TEAM" || nodes[0].Value != 7 {
		t.Fatalf("unexpected node[0]: %+v", nodes[0])
	}
	if len(edges) != 1 {
		t.Fatalf("got %d edges, want 1", len(edges))
	}
	if edges[0].Source != "TEAM:team-a" || edges[0].Target != "TEAM:team-b" || edges[0].Value != 2 {
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
