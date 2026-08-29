package analytics

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graph/model"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graphqldate"
)

// SankeyRequest is the Go port of compiler.py's SankeyRequest dataclass
// (compiler.py:106-116), non-investment path only -- same scope split as
// TimeseriesRequest/BreakdownRequest.
type SankeyRequest struct {
	Path      []Dimension
	Measure   Measure
	StartDate graphqldate.Date
	EndDate   graphqldate.Date
	MaxNodes  int
	MaxEdges  int
}

// validateSankeyPath ports validate_sankey_path (sql/validate.py:286-325):
// at least 2 dimensions, no duplicates (case-insensitive on the wire
// string, moot here since model.DimensionInput values are already
// canonical uppercase, but the duplicate check itself is real business
// logic, not just enum validation).
func validateSankeyPath(path []model.DimensionInput) ([]Dimension, error) {
	if len(path) < 2 {
		return nil, newValidationError("path", path, "Sankey path must contain at least 2 dimensions")
	}
	seen := make(map[model.DimensionInput]bool, len(path))
	out := make([]Dimension, 0, len(path))
	for _, d := range path {
		if seen[d] {
			return nil, newValidationError("path", path, "Duplicate dimension in Sankey path: %q", d)
		}
		seen[d] = true
		dim, err := dimensionFromInput(d)
		if err != nil {
			return nil, err
		}
		out = append(out, dim)
	}
	return out, nil
}

// SankeyRequestFromInput converts the GraphQL input.
func SankeyRequestFromInput(input model.SankeyRequestInput) (SankeyRequest, error) {
	path, err := validateSankeyPath(input.Path)
	if err != nil {
		return SankeyRequest{}, err
	}
	measure, err := measureFromInput(input.Measure)
	if err != nil {
		return SankeyRequest{}, err
	}
	if input.DateRange == nil {
		return SankeyRequest{}, newValidationError("dateRange", nil, "sankey.dateRange is required")
	}
	maxNodes, maxEdges := input.MaxNodes, input.MaxEdges
	return SankeyRequest{
		Path:      path,
		Measure:   measure,
		StartDate: input.DateRange.StartDate,
		EndDate:   input.DateRange.EndDate,
		MaxNodes:  maxNodes,
		MaxEdges:  maxEdges,
	}, nil
}

// CompileSankey ports compile_sankey (compiler.py:388-447) for the
// non-investment path -- same wholesale useInvestment rejection as
// CompileTimeseries/CompileBreakdown, and for the same reason (every
// investment-path sankey dimension needs _get_context_params's
// investment branch, not yet ported).
//
// Returns exactly 1 nodes query (a single UNION ALL across every
// dimension in the path -- sankey_nodes_template folds all dimensions
// into ONE query, unlike edges) and len(path)-1 edges queries, one per
// adjacent dimension pair -- mirrors Python's
// `[(nodes_sql, nodes_params)], edges_queries` return shape
// (compiler.py:447) exactly, including that nodes is always a
// single-element list while edges can be longer.
func CompileSankey(req SankeyRequest, orgID string, timeoutSeconds int, useInvestment bool, filters *model.FilterInput) (nodes compiledQuery, edges []compiledQuery, err error) {
	if useInvestment {
		return compiledQuery{}, nil, fmt.Errorf("analytics: CompileSankey: investment path not yet ported (CHAOS-4506 follow-up)")
	}
	for _, dim := range req.Path {
		if dim == DimensionAuthor {
			// dbColumn below would reject this per-dimension anyway, but
			// checking here first gives a clearer, path-scoped error
			// rather than pointing at whichever UNION branch happened to
			// build first.
			return compiledQuery{}, nil, newValidationError("path", string(dim),
				"author is not a supported breakdown/grouping dimension; "+
					"filter by who.developers or scope.level=developer instead "+
					"of grouping by author.")
		}
	}

	fc, err := translateFilters(filters, false, filterColumns{Team: "team_id", Repo: "repo_id", Author: "author_email"})
	if err != nil {
		return compiledQuery{}, nil, err
	}

	measureExpr, err := dbExpression(req.Measure, false, false)
	if err != nil {
		return compiledQuery{}, nil, err
	}
	// Force a uniform Float64 result type. ClickHouse returns UInt64 for
	// the SUM()-based measures (COUNT, THROUGHPUT, CHURN_LOC) and Float64
	// for the AVG/ratio ones, and the native driver will NOT convert
	// between them at scan time -- it errors, exactly as
	// reviewedges.go:145 documents for UInt32. Coercing in SQL keeps ONE
	// scan type for every measure. Python does the same coercion one
	// layer later with `float(row["value"])`, so values are unchanged.
	measureExpr = "toFloat64(" + measureExpr + ")"
	source, alias, dateFilter := nonInvestmentSourceAndDateFilter(req.Measure)

	// limit_per_dim = max(1, max_nodes // len(dimensions)), compiler.py:413.
	limitPerDim := req.MaxNodes / len(req.Path)
	if limitPerDim < 1 {
		limitPerDim = 1
	}

	var unionParts []string
	for _, dim := range req.Path {
		dimCol, dimErr := dbColumn(dim, false)
		if dimErr != nil {
			return compiledQuery{}, nil, dimErr
		}
		unionParts = append(unionParts, fmt.Sprintf(`
SELECT
    '%s' AS dimension,
    toString(%s) AS node_id,
    %s AS value
FROM %s
WHERE %s
  AND %s.org_id = {org_id:String}
%s
GROUP BY node_id
ORDER BY value DESC, node_id ASC
LIMIT {limit_per_dim:UInt32}
`, strings.ToUpper(string(dim)), dimCol, measureExpr, source, dateFilter, alias, fc.sql))
	}
	nodesSQL := fmt.Sprintf("\n%s\nSETTINGS max_execution_time = {timeout:UInt64}\n", strings.Join(unionParts, " UNION ALL "))
	nodesBindings := []clickhouse.Binding{
		{Name: "org_id", Value: orgID},
		{Name: "start_date", Value: req.StartDate.Time()},
		{Name: "end_date", Value: req.EndDate.Time()},
		{Name: "limit_per_dim", Value: limitPerDim},
		{Name: "timeout", Value: timeoutSeconds},
	}
	nodesBindings = append(nodesBindings, fc.bindings...)
	nodes = compiledQuery{sql: nodesSQL, bindings: nodesBindings}

	// One edges query per adjacent (source_dim, target_dim) pair,
	// compiler.py:428-445. max_edges divided evenly across pairs --
	// integer division, matching Python's `//`.
	maxEdgesPerPair := req.MaxEdges / (len(req.Path) - 1)
	for i := 0; i < len(req.Path)-1; i++ {
		sourceDim, targetDim := req.Path[i], req.Path[i+1]
		sourceCol, colErr := dbColumn(sourceDim, false)
		if colErr != nil {
			return compiledQuery{}, nil, colErr
		}
		targetCol, colErr := dbColumn(targetDim, false)
		if colErr != nil {
			return compiledQuery{}, nil, colErr
		}
		edgeSQL := fmt.Sprintf(`
SELECT
    '%s' AS source_dimension,
    '%s' AS target_dimension,
    toString(%s) AS source,
    toString(%s) AS target,
    %s AS value
FROM %s
WHERE %s
  AND %s.org_id = {org_id:String}
%s
  AND %s IS NOT NULL
  AND %s IS NOT NULL
GROUP BY source, target
ORDER BY value DESC, source ASC, target ASC
LIMIT {max_edges:UInt32}
SETTINGS max_execution_time = {timeout:UInt64}
`, strings.ToUpper(string(sourceDim)), strings.ToUpper(string(targetDim)), sourceCol, targetCol, measureExpr, source, dateFilter, alias, fc.sql, sourceCol, targetCol)

		edgeBindings := []clickhouse.Binding{
			{Name: "org_id", Value: orgID},
			{Name: "start_date", Value: req.StartDate.Time()},
			{Name: "end_date", Value: req.EndDate.Time()},
			{Name: "max_edges", Value: maxEdgesPerPair},
			{Name: "timeout", Value: timeoutSeconds},
		}
		edgeBindings = append(edgeBindings, fc.bindings...)
		edges = append(edges, compiledQuery{sql: edgeSQL, bindings: edgeBindings})
	}

	return nodes, edges, nil
}

// ExecuteSankeyQueries ports _execute_sankey_inner (analytics.py:275-331)
// in full generality -- unlike flow-matrix's ExecuteFlowMatrix (always
// exactly 1 nodes + 1 edges query), sankey can have 1 nodes query and
// MULTIPLE edges queries (one per path hop). Every query in BOTH lists
// runs concurrently (Python: nodes_task/edges_task each wrap their own
// asyncio.gather over their query list, then both tasks are gathered
// together, analytics.py:328-330) -- no cancellation on first failure,
// same reasoning as ExecuteFlowMatrix's doc comment. Returns a real
// error on failure; the caller (once the top-level orchestrator exists)
// is responsible for catching it and degrading to an empty SankeyResult,
// matching analytics.py:646-656's swallow boundary -- this function does
// NOT swallow on its own, same compile-fatal/execute-swallow split as
// flow-matrix.
func ExecuteSankeyQueries(ctx context.Context, client QueryClient, nodesQueries []compiledQuery, edgesQueries []compiledQuery) ([]model.SankeyNode, []model.SankeyEdge, error) {
	var wg sync.WaitGroup
	nodesResults := make([][]model.SankeyNode, len(nodesQueries))
	nodesErrs := make([]error, len(nodesQueries))
	edgesResults := make([][]model.SankeyEdge, len(edgesQueries))
	edgesErrs := make([]error, len(edgesQueries))

	wg.Add(len(nodesQueries) + len(edgesQueries))
	for i, q := range nodesQueries {
		go func(i int, q compiledQuery) {
			defer wg.Done()
			nodesResults[i], nodesErrs[i] = queryNodes(ctx, client, q)
		}(i, q)
	}
	for i, q := range edgesQueries {
		go func(i int, q compiledQuery) {
			defer wg.Done()
			edgesResults[i], edgesErrs[i] = queryEdges(ctx, client, q)
		}(i, q)
	}
	wg.Wait()

	var nodes []model.SankeyNode
	for i, err := range nodesErrs {
		if err != nil {
			return nil, nil, fmt.Errorf("analytics: sankey nodes[%d]: %w", i, err)
		}
		nodes = append(nodes, nodesResults[i]...)
	}
	var edges []model.SankeyEdge
	for i, err := range edgesErrs {
		if err != nil {
			return nil, nil, fmt.Errorf("analytics: sankey edges[%d]: %w", i, err)
		}
		edges = append(edges, edgesResults[i]...)
	}
	return nodes, edges, nil
}
