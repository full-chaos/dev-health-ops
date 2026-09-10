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
		if !strings.Contains(q.sql, "LEFT JOIN repos AS r FINAL ON toString(r.id) = toString(repo_id) AND r.org_id = {org_id:String}") {
			t.Errorf("%s: expected the REPO-dimension repo join, deduped with FINAL (CHAOS-4773), got: %s", name, q.sql)
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

// TestCompileSankey_NodesOrderByDimOrderMatchesPathPositionNotAlphabet is
// CHAOS-5546's red-first proof for the fix, pinned at the SQL-text level
// (no engine needed for this half of the claim): a plain `UNION ALL` of
// the per-dimension branches has NO guaranteed row order without an
// explicit outer ORDER BY -- confirmed live against the real ClickHouse
// stack (the same compiled SQL, bindings resolved, ran 4 times in a row
// and came back in 3 different branch orderings). The fix tags every
// branch with its own index in req.Path (`dim_order`) and wraps the
// union in an outer `ORDER BY dim_order ASC, ...`.
//
// The path here is deliberately WORK_TYPE, THEME, TEAM -- neither
// alphabetical (TEAM < THEME < WORK_TYPE) nor the reverse -- so a test
// that only checked "some sort order exists" couldn't pass by accident
// of the dimensions already sorting themselves; it must actually track
// req.Path's own index.
func TestCompileSankey_NodesOrderByDimOrderMatchesPathPositionNotAlphabet(t *testing.T) {
	req := SankeyRequest{
		Path:      []Dimension{DimensionWorkType, DimensionTheme, DimensionTeam},
		Measure:   MeasureCount,
		StartDate: mustDate(t, "2026-01-01"),
		EndDate:   mustDate(t, "2026-01-31"),
		MaxNodes:  90,
		MaxEdges:  200,
	}
	nodes, _, err := CompileSankey(req, "org-1", 30, true, nil)
	if err != nil {
		t.Fatalf("CompileSankey error = %v", err)
	}

	// Each branch is tagged with its OWN index in req.Path, not an
	// alphabetical or measure-derived rank.
	wantDimOrder := map[string]int{"WORK_TYPE": 0, "THEME": 1, "TEAM": 2}
	for dim, order := range wantDimOrder {
		tag := fmt.Sprintf("'%s' AS dimension,\n    toString(", dim)
		idx := strings.Index(nodes.sql, tag)
		if idx == -1 {
			t.Fatalf("branch for dimension %s not found in SQL: %s", dim, nodes.sql)
		}
		wantTag := fmt.Sprintf("%d AS dim_order", order)
		if !strings.Contains(nodes.sql[idx:idx+800], wantTag) {
			t.Errorf("dimension %s branch missing %q (path index), got: %s", dim, wantTag, nodes.sql[idx:idx+800])
		}
	}

	// The outer query must sort on dim_order FIRST -- this is what turns
	// the union's undefined branch order into "grouped by requested path
	// position", and the value/node_id tie-break preserves the existing
	// per-branch top-N ordering globally instead of leaving it to
	// whatever physical row order the union happens to hand back.
	if !strings.Contains(nodes.sql, "ORDER BY dim_order ASC, value DESC, node_id ASC") {
		t.Errorf("expected an outer ORDER BY dim_order ASC, value DESC, node_id ASC, got: %s", nodes.sql)
	}

	// dim_order must never leak into the projected columns -- queryNodes
	// scans exactly 3 columns (dimension, node_id, value) and is shared
	// with flow-matrix's own node query.
	if !strings.HasPrefix(strings.TrimSpace(nodes.sql), "SELECT dimension, node_id, value") {
		t.Errorf("expected the outer SELECT to project exactly (dimension, node_id, value), got: %s", nodes.sql)
	}
}

// TestExecuteSankeyQueries_NodeOrderPreservesQueryRowOrder pins the
// OTHER half of the determinism claim: ExecuteSankeyQueries itself must
// never reorder the rows a query returns -- ordering is entirely the
// compiled SQL's responsibility (the test above), and the aggregation
// loop here must just copy rows through in the order the client handed
// them back, regardless of which goroutine (there is normally exactly
// one nodes query, but this also covers callers that pass more than
// one) finishes first. nodesResults is written into by INDEX
// (`nodesResults[i], nodesErrs[i] = ...`), so slow-query-finishes-first
// must not perturb the final concatenation order.
func TestExecuteSankeyQueries_NodeOrderPreservesQueryRowOrder(t *testing.T) {
	slow := &fakeRowScanner{rows: [][]any{{"TEAM", "slow-node", float64(1)}}}
	fast := &fakeRowScanner{rows: [][]any{{"REPO", "fast-node", float64(2)}}}
	client := &orderedDelayClient{
		byStatement: map[string]*fakeRowScanner{
			"slow-query": slow,
			"fast-query": fast,
		},
		delay: map[string]time.Duration{
			"slow-query": 30 * time.Millisecond,
			"fast-query": 0,
		},
	}
	nodesQ := []compiledQuery{
		{sql: "slow-query"}, // index 0 -- must stay first in the output
		{sql: "fast-query"}, // index 1, even though it returns first
	}
	nodes, _, err := ExecuteSankeyQueries(context.Background(), client, nodesQ, nil)
	if err != nil {
		t.Fatalf("ExecuteSankeyQueries error = %v", err)
	}
	if len(nodes) != 2 || nodes[0].Label != "slow-node" || nodes[1].Label != "fast-node" {
		t.Fatalf("node order = %v, want [slow-node, fast-node] (index order, not completion order)", nodes)
	}
}

// TestExecuteSankeyQueries_EdgeOrderPreservesQueryRowOrder is
// CHAOS-5546 r1's P3 fix pin: the same completion-order-independence
// claim as the node test above, but for edges -- there IS more than one
// edges query per real request (one per path hop, CompileSankey's own
// doc comment), so a hop's SQL finishing before an EARLIER hop's must
// not reorder the concatenated edge list. edgesResults is written by
// INDEX exactly like nodesResults; before this test, reversing the
// concatenation loop (edgesErrs range order) still passed every existing
// edge test, because none of them varied completion timing.
func TestExecuteSankeyQueries_EdgeOrderPreservesQueryRowOrder(t *testing.T) {
	slow := &fakeRowScanner{rows: [][]any{{"TEAM", "REPO", "team-a", "repo-x", float64(1)}}}
	fast := &fakeRowScanner{rows: [][]any{{"REPO", "WORK_TYPE", "repo-x", "bug", float64(2)}}}
	client := &orderedDelayClient{
		byStatement: map[string]*fakeRowScanner{
			"slow-hop-query": slow,
			"fast-hop-query": fast,
		},
		delay: map[string]time.Duration{
			"slow-hop-query": 30 * time.Millisecond,
			"fast-hop-query": 0,
		},
	}
	edgesQ := []compiledQuery{
		{sql: "slow-hop-query"}, // hop 0 (TEAM->REPO) -- must stay first
		{sql: "fast-hop-query"}, // hop 1 (REPO->WORK_TYPE), even though it returns first
	}
	_, edges, err := ExecuteSankeyQueries(context.Background(), client, nil, edgesQ)
	if err != nil {
		t.Fatalf("ExecuteSankeyQueries error = %v", err)
	}
	if len(edges) != 2 || edges[0].Source != "TEAM:team-a" || edges[1].Source != "REPO:repo-x" {
		t.Fatalf("edge order = %+v, want hop 0 (TEAM:team-a->...) before hop 1 (REPO:repo-x->...), index order not completion order", edges)
	}
}

// orderedDelayClient dispatches by exact statement match after an
// artificial per-statement delay, so the SLOWEST query is queued FIRST
// and must still land at its own index in the result.
type orderedDelayClient struct {
	byStatement map[string]*fakeRowScanner
	delay       map[string]time.Duration
}

func (c *orderedDelayClient) Query(_ context.Context, statement string, _ []clickhouse.Binding) (clickhouse.RowScanner, error) {
	time.Sleep(c.delay[statement])
	resp, ok := c.byStatement[statement]
	if !ok {
		return nil, fmt.Errorf("orderedDelayClient: no canned response for %q", statement)
	}
	return resp, nil
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
