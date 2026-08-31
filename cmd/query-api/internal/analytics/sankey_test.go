package analytics

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graph/model"
)

func TestValidateSankeyPath_TooShort(t *testing.T) {
	for _, path := range [][]model.DimensionInput{nil, {model.DimensionInputTeam}} {
		_, err := validateSankeyPath(path)
		if err == nil {
			t.Fatalf("path %v: expected error for < 2 dimensions", path)
		}
		var ve *ValidationError
		if !errors.As(err, &ve) {
			t.Fatalf("path %v: expected *ValidationError, got %T", path, err)
		}
	}
}

func TestValidateSankeyPath_Duplicate(t *testing.T) {
	_, err := validateSankeyPath([]model.DimensionInput{model.DimensionInputTeam, model.DimensionInputTeam})
	if err == nil {
		t.Fatal("expected error for duplicate dimension")
	}
}

func TestValidateSankeyPath_Valid(t *testing.T) {
	dims, err := validateSankeyPath([]model.DimensionInput{model.DimensionInputTeam, model.DimensionInputRepo, model.DimensionInputWorkType})
	if err != nil {
		t.Fatalf("validateSankeyPath error = %v", err)
	}
	want := []Dimension{DimensionTeam, DimensionRepo, DimensionWorkType}
	if len(dims) != len(want) {
		t.Fatalf("got %v, want %v", dims, want)
	}
	for i := range want {
		if dims[i] != want[i] {
			t.Fatalf("dims[%d] = %v, want %v", i, dims[i], want[i])
		}
	}
}

// TestCompileSankey_Investment_CompilesInlinedSource is CHAOS-4538's
// replacement for the retired TestCompileSankey_RejectsInvestment -- see
// TestCompileTimeseries_Investment_CompilesInlinedSource's doc comment
// for the "no leading WITH" reasoning this pins identically. A TEAM+REPO
// path exercises BOTH investmentContextFor joins (team vote AND repo
// join) in one compile, matching how a real chord/Sankey request would
// combine them.
func TestCompileSankey_Investment_CompilesInlinedSource(t *testing.T) {
	req := SankeyRequest{
		Path:      []Dimension{DimensionTeam, DimensionRepo},
		Measure:   MeasureCount,
		StartDate: mustDate(t, "2026-01-01"),
		EndDate:   mustDate(t, "2026-01-31"),
		MaxNodes:  100,
		MaxEdges:  500,
	}
	nodes, edges, err := CompileSankey(req, "org-1", 30, true, nil)
	if err != nil {
		t.Fatalf("CompileSankey error = %v", err)
	}
	if len(edges) != 1 {
		t.Fatalf("expected 1 edges query for a 2-dimension path, got %d", len(edges))
	}
	for name, q := range map[string]compiledQuery{"nodes": nodes, "edges": edges[0]} {
		trimmed := strings.TrimSpace(q.sql)
		if !strings.HasPrefix(trimmed, "SELECT") {
			t.Errorf("%s: investment-path SQL must start with a literal SELECT, got prefix: %q", name, trimmed[:min(40, len(trimmed))])
		}
		if strings.Contains(q.sql, "\nWITH ") || strings.HasPrefix(trimmed, "WITH") {
			t.Errorf("%s: investment-path SQL must never contain a top-level WITH clause, got: %s", name, q.sql)
		}
		if !strings.Contains(q.sql, "(argMax(tuple(repo_id), computed_at)).1") {
			t.Errorf("%s: expected CHAOS-4547 tuple-wrap fix for repo_id, got: %s", name, q.sql)
		}
		if !strings.Contains(q.sql, "LEFT JOIN repos AS r ON toString(r.id) = toString(repo_id)") {
			t.Errorf("%s: expected the REPO-dimension repo join, got: %s", name, q.sql)
		}
		if !strings.Contains(q.sql, "(argMax(tuple(resolved_team), (cnt, resolved_team_id))).1 AS team_label") {
			t.Errorf("%s: expected CHAOS-4547 site-3 tuple-wrap fix on the team vote, got: %s", name, q.sql)
		}
	}
}

func TestCompileSankey_RejectsAuthorInPath(t *testing.T) {
	req := SankeyRequest{
		Path:      []Dimension{DimensionAuthor, DimensionRepo},
		Measure:   MeasureCount,
		StartDate: mustDate(t, "2026-01-01"),
		EndDate:   mustDate(t, "2026-01-31"),
		MaxNodes:  100,
		MaxEdges:  500,
	}
	_, _, err := CompileSankey(req, "org-1", 30, false, nil)
	if err == nil {
		t.Fatal("expected rejection for AUTHOR in path")
	}
	var ve *ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected *ValidationError, got %T: %v", err, err)
	}
}

func TestCompileSankey_ThreeDimensionPath(t *testing.T) {
	req := SankeyRequest{
		Path:      []Dimension{DimensionTeam, DimensionRepo, DimensionWorkType},
		Measure:   MeasureCount,
		StartDate: mustDate(t, "2026-01-01"),
		EndDate:   mustDate(t, "2026-01-31"),
		MaxNodes:  90,  // -> limit_per_dim = 90/3 = 30
		MaxEdges:  200, // -> max_edges per pair = 200/2 = 100
	}
	nodes, edges, err := CompileSankey(req, "org-1", 30, false, nil)
	if err != nil {
		t.Fatalf("CompileSankey error = %v", err)
	}

	// One nodes query, UNION ALL of 3 branches.
	if strings.Count(nodes.sql, "UNION ALL") != 2 {
		t.Errorf("expected 2 UNION ALL joins for 3 dimensions, got SQL: %s", nodes.sql)
	}
	for _, want := range []string{"'TEAM' AS dimension", "'REPO' AS dimension", "'WORK_TYPE' AS dimension"} {
		if !strings.Contains(nodes.sql, want) {
			t.Errorf("nodes SQL missing %q: %s", want, nodes.sql)
		}
	}
	nodesBindings := bindingMap(nodes.bindings)
	if nodesBindings["limit_per_dim"] != 30 {
		t.Errorf("limit_per_dim = %v, want 30", nodesBindings["limit_per_dim"])
	}

	// Two edges queries: TEAM->REPO, REPO->WORK_TYPE.
	if len(edges) != 2 {
		t.Fatalf("got %d edges queries, want 2 (len(path)-1)", len(edges))
	}
	if !strings.Contains(edges[0].sql, "'TEAM' AS source_dimension") || !strings.Contains(edges[0].sql, "'REPO' AS target_dimension") {
		t.Errorf("edges[0] SQL unexpected: %s", edges[0].sql)
	}
	if !strings.Contains(edges[1].sql, "'REPO' AS source_dimension") || !strings.Contains(edges[1].sql, "'WORK_TYPE' AS target_dimension") {
		t.Errorf("edges[1] SQL unexpected: %s", edges[1].sql)
	}
	e0Bindings := bindingMap(edges[0].bindings)
	if e0Bindings["max_edges"] != 100 {
		t.Errorf("edges[0] max_edges = %v, want 100", e0Bindings["max_edges"])
	}
}

func TestCompileSankey_MaxNodesSmallerThanDimensionCountClampsToOne(t *testing.T) {
	// max(1, max_nodes // len(dimensions)) -- compiler.py:413.
	req := SankeyRequest{
		Path:      []Dimension{DimensionTeam, DimensionRepo, DimensionWorkType},
		Measure:   MeasureCount,
		StartDate: mustDate(t, "2026-01-01"),
		EndDate:   mustDate(t, "2026-01-31"),
		MaxNodes:  2, // 2/3 == 0 in integer division -> must clamp to 1
		MaxEdges:  10,
	}
	nodes, _, err := CompileSankey(req, "org-1", 30, false, nil)
	if err != nil {
		t.Fatalf("CompileSankey error = %v", err)
	}
	if bindingMap(nodes.bindings)["limit_per_dim"] != 1 {
		t.Errorf("limit_per_dim should clamp to 1, got %v", bindingMap(nodes.bindings)["limit_per_dim"])
	}
}

// --- ExecuteSankeyQueries ---------------------------------------------

// concurrentFakeClient dispatches by matching the statement against a
// caller-provided set of canned (substring -> response) pairs, guarded
// by a mutex -- needed because ExecuteSankeyQueries fires an arbitrary
// number of concurrent Query calls (unlike flow-matrix's fixed 2).
type concurrentFakeClient struct {
	mu        sync.Mutex
	responses map[string]*fakeRowScanner
	errs      map[string]error
	calls     []string
}

func (f *concurrentFakeClient) Query(_ context.Context, statement string, _ []clickhouse.Binding) (clickhouse.RowScanner, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for key, err := range f.errs {
		if strings.Contains(statement, key) {
			f.calls = append(f.calls, key)
			return nil, err
		}
	}
	for key, resp := range f.responses {
		if strings.Contains(statement, key) {
			f.calls = append(f.calls, key)
			return resp, nil
		}
	}
	return nil, errors.New("concurrentFakeClient: no canned response matches statement")
}

func TestExecuteSankeyQueries_MultiEdgeSuccess(t *testing.T) {
	client := &concurrentFakeClient{
		responses: map[string]*fakeRowScanner{
			"'TEAM' AS dimension":        {rows: [][]any{{"TEAM", "team-a", float64(3)}}},
			"'TEAM' AS source_dimension": {rows: [][]any{{"TEAM", "REPO", "team-a", "repo-x", float64(1)}}},
			"'REPO' AS source_dimension": {rows: [][]any{{"REPO", "WORK_TYPE", "repo-x", "bug", float64(2)}}},
		},
	}
	nodesQ := []compiledQuery{{sql: "SELECT 'TEAM' AS dimension, ..."}}
	edgesQ := []compiledQuery{
		{sql: "SELECT 'TEAM' AS source_dimension, 'REPO' AS target_dimension, ..."},
		{sql: "SELECT 'REPO' AS source_dimension, 'WORK_TYPE' AS target_dimension, ..."},
	}
	nodes, edges, err := ExecuteSankeyQueries(context.Background(), client, nodesQ, edgesQ)
	if err != nil {
		t.Fatalf("ExecuteSankeyQueries error = %v", err)
	}
	if len(nodes) != 1 {
		t.Fatalf("got %d nodes, want 1", len(nodes))
	}
	if len(edges) != 2 {
		t.Fatalf("got %d edges, want 2 (one per hop)", len(edges))
	}
}

func TestExecuteSankeyQueries_OneEdgeErrorPropagates(t *testing.T) {
	client := &concurrentFakeClient{
		responses: map[string]*fakeRowScanner{
			"'TEAM' AS dimension":        {rows: nil},
			"'TEAM' AS source_dimension": {rows: nil},
		},
		errs: map[string]error{
			"'REPO' AS source_dimension": errors.New("boom"),
		},
	}
	nodesQ := []compiledQuery{{sql: "SELECT 'TEAM' AS dimension, ..."}}
	edgesQ := []compiledQuery{
		{sql: "SELECT 'TEAM' AS source_dimension, ..."},
		{sql: "SELECT 'REPO' AS source_dimension, ..."},
	}
	_, _, err := ExecuteSankeyQueries(context.Background(), client, nodesQ, edgesQ)
	if err == nil {
		t.Fatal("expected error to propagate")
	}
}
