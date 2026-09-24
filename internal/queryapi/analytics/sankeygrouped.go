package analytics

import (
	"context"
	"fmt"
	"strings"

	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

// sankeyShape is everything CompileSankey and CompileSankeyGrouped share:
// one source, one filter translation, one measure, one pair of limits.
// Both compilers read it, so the per-dimension queries and the single
// grouped query cannot drift apart on what they aggregate.
type sankeyShape struct {
	fc              filterClause
	source          string
	alias           string
	dateFilter      string
	extraClauses    string
	measureExpr     string
	limitPerDim     int
	maxEdgesPerPair int
}

func compileSankeyShape(req SankeyRequest, useInvestment bool, filters *model.FilterInput) (sankeyShape, error) {
	for _, dim := range req.Path {
		if dim == DimensionAuthor {
			// dbColumn would reject this per-dimension anyway, but
			// checking here first gives a clearer, path-scoped error
			// rather than pointing at whichever dimension happened to
			// build first.
			return sankeyShape{}, newValidationError("path", string(dim),
				"author is not a supported breakdown/grouping dimension; "+
					"filter by who.developers or scope.level=developer instead "+
					"of grouping by author.")
		}
	}

	fc, err := translateFilters(filters, useInvestment, defaultFilterColumns())
	if err != nil {
		return sankeyShape{}, err
	}

	shape := sankeyShape{fc: fc}
	if useInvestment {
		ictx := investmentContextFor(req.Path, needsTeamJoin(filters), needsAuthorJoin(filters))
		shape.source, shape.alias, shape.dateFilter, shape.extraClauses = ictx.Source, ictx.Alias, ictx.DateFilter, ictx.ExtraClauses
		shape.measureExpr, err = dbExpression(req.Measure, true, ictx.UseRepoAllocation)
		if err != nil {
			return sankeyShape{}, err
		}
	} else {
		shape.source, shape.alias, shape.dateFilter = nonInvestmentSourceAndDateFilter(req.Measure)
		shape.measureExpr, err = dbExpression(req.Measure, false, false)
		if err != nil {
			return sankeyShape{}, err
		}
	}
	// Force a uniform Float64 result type. ClickHouse returns UInt64 for
	// the SUM()-based measures (COUNT, THROUGHPUT, CHURN_LOC) and Float64
	// for the AVG/ratio ones, and the native driver will NOT convert
	// between them at scan time -- it errors, exactly as
	// reviewedges.go:145 documents for UInt32. Coercing in SQL keeps ONE
	// scan type for every measure. Python does the same coercion one
	// layer later with `float(row["value"])`, so values are unchanged.
	shape.measureExpr = "toFloat64(" + shape.measureExpr + ")"

	// limit_per_dim = max(1, max_nodes // len(dimensions)), compiler.py:413.
	shape.limitPerDim = req.MaxNodes / len(req.Path)
	if shape.limitPerDim < 1 {
		shape.limitPerDim = 1
	}
	// max_edges divided evenly across adjacent pairs -- integer division,
	// matching Python's `//` (compiler.py:428-445).
	shape.maxEdgesPerPair = req.MaxEdges / (len(req.Path) - 1)
	return shape, nil
}

// sankeyGroupedQuery is the single-statement form of a sankey: every
// node dimension and every adjacent edge pair aggregated in ONE scan of
// the source, instead of one UNION ALL arm per dimension plus one query
// per edge pair.
//
// Why: on the investment path the source is a deep subquery (latest
// investments, membership scope, repo allocation, per-unit team
// attribution). ClickHouse evaluates that subquery once per UNION arm and
// once per edges query, so a TEAM>THEME>REPO sankey scanned it five
// times per request, and the Investment page sends three such requests
// at once. GROUPING SETS aggregates every set from one evaluation.
//
// Output is identical to CompileSankey + ExecuteSankeyQueries: same
// group keys, same measure, same per-set ORDER BY, same per-set limits,
// same node/edge order. The differential test in
// sankeygrouped_seeded_integration_test.go runs both forms on the same
// data and requires equal results.
type sankeyGroupedQuery struct {
	query           compiledQuery
	path            []Dimension
	limitPerDim     int
	maxEdgesPerPair int
}

// sankeyKeyAlias names the i-th path dimension's group key. The prefix
// keeps it clear of every source column name.
func sankeyKeyAlias(i int) string { return fmt.Sprintf("sankey_key_%d", i) }

// sankeyNodeSetID and sankeyEdgeSetID return the value ClickHouse's
// grouping(k0, ..., kN-1) gives a row of that grouping set under
// force_grouping_standard_compatibility = 1 (set explicitly in the
// query): one bit per argument, the first argument in the most
// significant bit, and a bit is 1 when that key is NOT in the set.
func sankeyNodeSetID(n, i int) uint64 {
	return (uint64(1)<<n - 1) &^ (uint64(1) << (n - 1 - i))
}

func sankeyEdgeSetID(n, i int) uint64 {
	return sankeyNodeSetID(n, i) &^ (uint64(1) << (n - 2 - i))
}

// CompileSankeyGrouped compiles req into one GROUPING SETS query. It
// accepts exactly what CompileSankey accepts and rejects exactly what it
// rejects (both read compileSankeyShape).
func CompileSankeyGrouped(req SankeyRequest, orgID string, timeoutSeconds int, useInvestment bool, filters *model.FilterInput) (sankeyGroupedQuery, error) {
	shape, err := compileSankeyShape(req, useInvestment, filters)
	if err != nil {
		return sankeyGroupedQuery{}, err
	}
	n := len(req.Path)

	keys := make([]string, n)
	keyExprs := make([]string, n)
	for i, dim := range req.Path {
		dimCol, dimErr := dbColumn(dim, useInvestment)
		if dimErr != nil {
			return sankeyGroupedQuery{}, dimErr
		}
		keys[i] = sankeyKeyAlias(i)
		keyExprs[i] = fmt.Sprintf("toString(%s) AS %s", dimCol, keys[i])
	}

	// Node sets first, then edge sets: the order here does not reach the
	// output (rows are routed by their grouping id), it only keeps the
	// SQL readable.
	sets := make([]string, 0, 2*n-1)
	for i := 0; i < n; i++ {
		sets = append(sets, "("+keys[i]+")")
	}
	// The per-edge-query `AND <col> IS NOT NULL` filter becomes a
	// post-aggregation filter on the edge set: toString(col) is NULL
	// exactly when col is NULL, and a group whose keys are non-NULL holds
	// exactly the rows the pre-aggregation filter kept, so every
	// remaining edge aggregates the same rows. Node sets keep NULL keys,
	// as the node arms never filtered them (executeSankeyGrouped reads a
	// NULL node key as "", as the per-dimension scan did).
	var edgeNullFilters []string
	for i := 0; i < n-1; i++ {
		sets = append(sets, "("+keys[i]+", "+keys[i+1]+")")
		edgeNullFilters = append(edgeNullFilters, fmt.Sprintf(
			"NOT (grouping_set = %d AND (%s IS NULL OR %s IS NULL))",
			sankeyEdgeSetID(n, i), keys[i], keys[i+1]))
	}

	// ORDER BY value DESC then every key ASC: inside one set the keys
	// outside the set are constant, so this is exactly the node arm's
	// `value DESC, node_id ASC` and the edge query's
	// `value DESC, source ASC, target ASC`. LIMIT BY takes one count, so
	// it keeps the larger of the two limits and executeSankeyGrouped
	// trims each set to its own.
	orderKeys := make([]string, n)
	for i, k := range keys {
		orderKeys[i] = k + " ASC"
	}
	perSetLimit := shape.limitPerDim
	if shape.maxEdgesPerPair > perSetLimit {
		perSetLimit = shape.maxEdgesPerPair
	}

	sql := fmt.Sprintf(`
SELECT grouping_set, %s, value
FROM (
SELECT
    grouping(%s) AS grouping_set,
    %s,
    %s AS value
FROM %s
%s
WHERE %s
  AND %s.org_id = {org_id:String}
%s
GROUP BY GROUPING SETS (%s)
)
WHERE %s
ORDER BY grouping_set ASC, value DESC, %s
LIMIT {per_set_limit:UInt32} BY grouping_set
%s, force_grouping_standard_compatibility = 1
`, strings.Join(keys, ", "),
		strings.Join(keys, ", "),
		strings.Join(keyExprs, ",\n    "),
		shape.measureExpr,
		shape.source, shape.extraClauses, shape.dateFilter, shape.alias, shape.fc.sql,
		strings.Join(sets, ", "),
		strings.Join(edgeNullFilters, "\n  AND "),
		strings.Join(orderKeys, ", "),
		settingsMaxExecutionTime(timeoutSeconds))

	bindings := []clickhouse.Binding{
		{Name: "org_id", Value: orgID},
		{Name: "start_date", Value: dateBindingValue(req.StartDate.Time())},
		{Name: "end_date", Value: dateBindingValue(req.EndDate.Time())},
		{Name: "per_set_limit", Value: perSetLimit},
	}
	bindings = append(bindings, shape.fc.bindings...)

	return sankeyGroupedQuery{
		query:           compiledQuery{sql: sql, bindings: bindings},
		path:            req.Path,
		limitPerDim:     shape.limitPerDim,
		maxEdgesPerPair: shape.maxEdgesPerPair,
	}, nil
}

// executeSankeyGrouped runs q and returns the nodes and edges
// ExecuteSankeyQueries returns for the same request: nodes in path
// order, then value DESC, node_id ASC inside a dimension; edges in pair
// order, then value DESC, source ASC, target ASC inside a pair.
func executeSankeyGrouped(ctx context.Context, client QueryClient, q sankeyGroupedQuery) ([]model.SankeyNode, []model.SankeyEdge, error) {
	n := len(q.path)
	nodeSet := make(map[uint64]int, n)
	edgeSet := make(map[uint64]int, n-1)
	for i := 0; i < n; i++ {
		nodeSet[sankeyNodeSetID(n, i)] = i
	}
	for i := 0; i < n-1; i++ {
		edgeSet[sankeyEdgeSetID(n, i)] = i
	}
	dimNames := make([]string, n)
	for i, dim := range q.path {
		dimNames[i] = strings.ToUpper(string(dim))
	}

	rows, err := client.Query(ctx, q.query.sql, q.query.bindings)
	if err != nil {
		return nil, nil, fmt.Errorf("query: %w", err)
	}
	defer rows.Close()

	nodesByDim := make([][]model.SankeyNode, n)
	edgesByPair := make([][]model.SankeyEdge, n-1)
	for rows.Next() {
		var set uint64
		keys := make([]*string, n)
		// *float64: see queryNodes's doc comment (CHAOS-4701).
		var value *float64
		dest := make([]any, 0, n+2)
		dest = append(dest, &set)
		for i := range keys {
			dest = append(dest, &keys[i])
		}
		dest = append(dest, &value)
		if scanErr := rows.Scan(dest...); scanErr != nil {
			return nil, nil, fmt.Errorf("scan: %w", scanErr)
		}
		if i, ok := nodeSet[set]; ok {
			// A NULL node key (e.g. a NULL team_id on
			// investment_metrics_daily) reads as "": the per-dimension
			// node query scans node_id into a plain string, and the
			// driver leaves a plain string destination untouched on a
			// NULL cell. Node sets are not NULL-filtered, as the node
			// arms never were.
			nodeKey := ""
			if keys[i] != nil {
				nodeKey = *keys[i]
			}
			if len(nodesByDim[i]) < q.limitPerDim {
				nodesByDim[i] = append(nodesByDim[i], model.SankeyNode{
					ID:        dimNames[i] + ":" + nodeKey,
					Label:     nodeKey,
					Dimension: dimNames[i],
					Value:     value,
				})
			}
			continue
		}
		if i, ok := edgeSet[set]; ok {
			// The SQL already drops NULL-keyed edge groups; a NULL here
			// means that filter and this router disagree on set ids.
			if keys[i] == nil || keys[i+1] == nil {
				return nil, nil, fmt.Errorf("scan: NULL edge key in grouping set %d", set)
			}
			if len(edgesByPair[i]) < q.maxEdgesPerPair {
				edgesByPair[i] = append(edgesByPair[i], model.SankeyEdge{
					Source: dimNames[i] + ":" + *keys[i],
					Target: dimNames[i+1] + ":" + *keys[i+1],
					Value:  value,
				})
			}
			continue
		}
		return nil, nil, fmt.Errorf("scan: unexpected grouping set %d for a %d-dimension path", set, n)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("rows: %w", err)
	}

	var nodes []model.SankeyNode
	for _, dimNodes := range nodesByDim {
		nodes = append(nodes, dimNodes...)
	}
	var edges []model.SankeyEdge
	for _, pairEdges := range edgesByPair {
		edges = append(edges, pairEdges...)
	}
	return nodes, edges, nil
}

// pathLabel renders a sankey path for log lines, e.g. "TEAM>THEME>REPO".
func pathLabel(path []Dimension) string {
	parts := make([]string, len(path))
	for i, dim := range path {
		parts[i] = strings.ToUpper(string(dim))
	}
	return strings.Join(parts, ">")
}
