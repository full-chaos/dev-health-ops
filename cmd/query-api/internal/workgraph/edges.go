package workgraph

import (
	"context"
	"fmt"
	"strings"

	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graph/model"
)

// MaxEdgesLimit is the hard ceiling on filters.limit for workGraphEdges.
// Python's WorkGraphEdgeFilterInput.limit (models/inputs.py:320) is an
// UNCLAMPED `int = 1000` default with no server-side max enforced anywhere
// in resolve_work_graph_edges -- confirmed by reading work_graph.py:1118
// (`limit = filters.limit if filters else 1000`) top to bottom for a clamp
// that does not exist. This port does not invent one either; MaxEdgesLimit
// exists only as a defensive backstop against a pathological/malicious
// limit value reaching a live query, applied ONLY when limit is
// non-positive (mirroring nothing in Python -- see clampEdgesLimit).
const MaxEdgesLimit = 100000

// edgeRow is one row of either the primary (deduped) edges query or the
// dependency-edges splice query, in the SAME shape _row_to_edge
// (work_graph.py:419-471) reads a Python dict as.
type edgeRow struct {
	edgeID     string
	sourceType string
	sourceID   string
	targetType string
	targetID   string
	edgeType   string
	repoID     string
	provider   string
	provenance string
	confidence float64
	evidence   string
}

func (r edgeRow) toEndpoint() edgeEndpoint {
	return edgeEndpoint{sourceID: r.sourceID, sourceType: r.sourceType, targetID: r.targetID, targetType: r.targetType}
}

// identityKey is the ReplacingMergeTree dedup identity for work_graph_edges
// -- (source_type, source_id, edge_type, target_type, target_id), the
// table's own ORDER BY key (migration 014_work_graph.sql) -- and also the
// splice-dedup key work_graph.py:1205-1227 builds to decide which
// dependency rows are "not already present" in the primary rows.
type identityKey struct {
	sourceType, sourceID, edgeType, targetType, targetID string
}

func (r edgeRow) identity() identityKey {
	return identityKey{r.sourceType, r.sourceID, r.edgeType, r.targetType, r.targetID}
}

// fetchDedupedEdgeRows is the CHAOS-4515 / CHAOS-4504 GO-SIDE DEDUP fix:
// work_graph_edges is ReplacingMergeTree(last_synced) and Python's raw read
// (work_graph.py:1164-1181, no FINAL/argMax) can surface duplicate pre-merge
// versions of one logical edge, tied on the ENTIRE ORDER BY key (confidence,
// edge_id) because edge_id is a deterministic hash of the identity columns
// -- no tie-break on the row's own keys can separate two versions of the
// same edge. Python is frozen (chris ruling, 06:52 PT 08-29); this Go query
// applies the fix Python does not have: argMax(<col>, last_synced) collapse
// per identity BEFORE ORDER BY/LIMIT, mirroring the in-repo reference
// src/dev_health_ops/work_graph/investment/queries.py:38-91
// (fetch_work_graph_edges) -- GROUP BY the identity (org_id plus the 5
// ORDER-BY columns), argMax every other column, any(edge_id) (edge_id is a
// pure function of the identity columns, so it is invariant across
// duplicate physical rows -- any() is exact, not an approximation).
//
// THIS IS A DELIBERATE DIVERGENCE from Python, not a bug: see workgraph.go's
// package doc comment "THE COMPARATOR DIVERGES BY DESIGN". Never revert this
// to a raw (un-deduped) read to make the dual-run comparator match --
// a match under a genuine duplicate-version condition means this fix did
// not take effect.
//
// The dedup GROUP BY plus the theme-membership EXISTS clause (when a theme
// filter is active) are BOTH pushed into the same WHERE, applied
// pre-aggregation -- source_type/source_id/target_type/target_id/edge_type
// are identity columns, stable across duplicate versions of one edge, so
// filtering on them before the GROUP BY is exact (a duplicate version can
// never change which identity group a row belongs to).
func fetchDedupedEdgeRows(ctx context.Context, client QueryClient, orgID string, scope *filterScope, limit int) ([]edgeRow, error) {
	where := buildWorkGraphWhere(orgID, scope, true)
	query := fmt.Sprintf(`
        SELECT
            any(edge_id) AS edge_id,
            source_type,
            source_id,
            target_type,
            target_id,
            edge_type,
            ifNull(toString(argMax(repo_id, last_synced)), '') AS repo_id,
            ifNull(argMax(provider, last_synced), '') AS provider,
            argMax(provenance, last_synced) AS provenance,
            argMax(confidence, last_synced) AS confidence,
            argMax(evidence, last_synced) AS evidence
        FROM work_graph_edges
        %s
        GROUP BY org_id, source_type, source_id, edge_type, target_type, target_id
        ORDER BY confidence DESC, edge_id ASC
        LIMIT {limit:UInt64}
    `, where.sql)

	bindings := append(where.bindings, clickhouse.Binding{Name: "limit", Value: limit})

	rows, err := client.Query(ctx, query, bindings)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []edgeRow
	for rows.Next() {
		var r edgeRow
		if scanErr := rows.Scan(&r.edgeID, &r.sourceType, &r.sourceID, &r.targetType, &r.targetID, &r.edgeType, &r.repoID, &r.provider, &r.provenance, &r.confidence, &r.evidence); scanErr != nil {
			return nil, fmt.Errorf("workgraph: edges scan: %w", scanErr)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("workgraph: edges rows: %w", err)
	}
	return out, nil
}

// queryDependencyEdges mirrors work_graph.py:1012-1097's
// _query_dependency_edges. Two early exits, BOTH preserved exactly:
//  1. no edge_type/edge_types filter narrows to a dependency-eligible type
//     (dependencyEdgeFilterValues) -- the unfiltered path never reaches
//     here with a non-empty set, so the splice never activates unfiltered.
//  2. ANY repo scope is active (scope.repoIDs non-empty, i.e. filters.repo_ids
//     was non-empty POST-resolution -- work_graph.py:1023-1024 reads
//     filters.repo_ids after _resolve_filter_repo_scope has already
//     overwritten it in place) -- dependency edges are skipped entirely
//     when a repo filter is in play.
//
// work_item_dependencies is read via FINAL (work_graph.py:1089) -- already
// correct per lane-4516-group1's verified finding, not touched here.
//
// repo_id/provider are rendered as plain empty strings rather than
// Python's `CAST(NULL, 'Nullable(UUID)')`/`CAST(NULL, 'Nullable(String)')`
// -- behaviorally identical (both end up as "no repo/provider" in
// rowToEdge, which treats an empty string as absent) and avoids a nullable
// scan for a column that is always empty on this path.
func queryDependencyEdges(ctx context.Context, client QueryClient, orgID string, filters *model.WorkGraphEdgeFilterInput, scope *filterScope, limit int) ([]edgeRow, error) {
	edgeTypes := dependencyEdgeFilterValues(filters)
	if len(edgeTypes) == 0 {
		return nil, nil
	}
	if len(scope.repoIDs) > 0 {
		return nil, nil
	}

	mappedEdgeType := dependencyEdgeTypeSQL()
	sourceTypeSQL := dependencyNodeTypeSQL("source_work_item_id")
	targetTypeSQL := dependencyNodeTypeSQL("target_work_item_id")

	clauses := []string{"org_id = {org_id:String}", "mapped_edge_type IN {edge_types:Array(String)}"}
	bindings := []clickhouse.Binding{
		{Name: "org_id", Value: orgID},
		{Name: "edge_types", Value: edgeTypes},
	}
	bindings = addMembershipScopeBindings(bindings, scope)

	if filters != nil && filters.SourceType != nil {
		clauses = append(clauses, "source_type = {source_type:String}")
		bindings = append(bindings, clickhouse.Binding{Name: "source_type", Value: lowerNodeTypeInput(*filters.SourceType)})
	}
	if filters != nil && filters.TargetType != nil {
		clauses = append(clauses, "target_type = {target_type:String}")
		bindings = append(bindings, clickhouse.Binding{Name: "target_type", Value: lowerNodeTypeInput(*filters.TargetType)})
	}
	if filters != nil && filters.NodeID != nil && *filters.NodeID != "" {
		clauses = append(clauses, "(source_work_item_id = {node_id:String} OR target_work_item_id = {node_id:String})")
		bindings = append(bindings, clickhouse.Binding{Name: "node_id", Value: *filters.NodeID})
	}

	var theme, subcategory *string
	if filters != nil {
		theme, subcategory = filters.Theme, filters.Subcategory
	}
	tf := themeFilter{theme: theme, subcategory: subcategory}
	if tf.active() {
		clauses = append(clauses, dependencyThemeMembershipExistsClause(scope, tf))
		bindings = append(bindings, tf.bindings()...)
		bindings = append(bindings, clickhouse.Binding{Name: "wanted_count", Value: tf.wantedCount()})
	}

	bindings = append(bindings, clickhouse.Binding{Name: "limit", Value: limit})

	query := fmt.Sprintf(`
        SELECT
            concat(
                'wid:',
                hex(MD5(concat(source_type, ':', source_work_item_id, ':', mapped_edge_type, ':', target_type, ':', target_work_item_id)))
            ) AS edge_id,
            source_type,
            source_work_item_id AS source_id,
            target_type,
            target_work_item_id AS target_id,
            mapped_edge_type AS edge_type,
            '' AS repo_id,
            '' AS provider,
            'native' AS provenance,
            1.0 AS confidence,
            if(relationship_type_raw != '', relationship_type_raw, relationship_type) AS evidence
        FROM (
            SELECT
                org_id,
                source_work_item_id,
                target_work_item_id,
                relationship_type,
                relationship_type_raw,
                %s AS mapped_edge_type,
                %s AS source_type,
                %s AS target_type,
                last_synced
            FROM work_item_dependencies FINAL
            WHERE org_id = {org_id:String}
        )
        WHERE %s
        ORDER BY last_synced DESC, edge_id ASC
        LIMIT {limit:UInt64}
    `, mappedEdgeType, sourceTypeSQL, targetTypeSQL, strings.Join(clauses, " AND "))

	rows, err := client.Query(ctx, query, bindings)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []edgeRow
	for rows.Next() {
		var r edgeRow
		if scanErr := rows.Scan(&r.edgeID, &r.sourceType, &r.sourceID, &r.targetType, &r.targetID, &r.edgeType, &r.repoID, &r.provider, &r.provenance, &r.confidence, &r.evidence); scanErr != nil {
			return nil, fmt.Errorf("workgraph: dependency edges scan: %w", scanErr)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("workgraph: dependency edges rows: %w", err)
	}
	return out, nil
}

// spliceDependencyEdges mirrors work_graph.py:1204-1229's splice EXACTLY:
// dependency rows whose identity is not already present among the primary
// rows are appended to the END of primary (in the dependency query's OWN
// order -- last_synced DESC, edge_id ASC), and the concatenation is
// truncated to limit. THE RESULT IS NEVER RE-SORTED. This is the "splice
// trap" BRIEF.md names: a port that sorts the merged slice by confidence
// diverges from Python. Do not add a sort here.
func spliceDependencyEdges(primary, dependency []edgeRow, limit int) []edgeRow {
	if len(dependency) == 0 {
		return primary
	}
	existing := make(map[identityKey]struct{}, len(primary))
	for _, r := range primary {
		existing[r.identity()] = struct{}{}
	}
	combined := make([]edgeRow, len(primary), len(primary)+len(dependency))
	copy(combined, primary)
	for _, r := range dependency {
		if _, ok := existing[r.identity()]; ok {
			continue
		}
		combined = append(combined, r)
	}
	if len(combined) > limit {
		combined = combined[:limit]
	}
	return combined
}

// clampEdgesLimit applies MaxEdgesLimit's defensive backstop -- see that
// constant's doc comment for why this is NOT a Python-parity clamp.
func clampEdgesLimit(limit int) int {
	if limit <= 0 {
		return 1
	}
	if limit > MaxEdgesLimit {
		return MaxEdgesLimit
	}
	return limit
}

func emptyEdgesResult(degradedReason *string, scope *filterScope) *model.WorkGraphEdgesResult {
	var partialScope *string
	if scope.usesScopedPartial() {
		s := "repo"
		partialScope = &s
	}
	return &model.WorkGraphEdgesResult{
		Edges:      []model.WorkGraphEdgeResult{},
		TotalCount: 0,
		PageInfo: &model.PageInfo{
			HasNextPage:     false,
			HasPreviousPage: false,
		},
		DegradedReason: degradedReason,
		IsPartial:      scope.usesScopedPartial(),
		PartialScope:   partialScope,
		PartialRepoIds: scope.partialRepoIDs(),
	}
}

// ResolveEdges mirrors work_graph.py:1100-1265's resolve_work_graph_edges.
// orgID must already be the AUTHORIZED org (see this package's doc comment
// and schema.resolvers.go's WorkGraphEdges wiring) -- Python's
// require_org_id(context) + "authorized org always wins" behavior, same
// convention every Wave 1-3 package's Resolve documents.
func ResolveEdges(ctx context.Context, client QueryClient, orgID string, filters *model.WorkGraphEdgeFilterInput) (*model.WorkGraphEdgesResult, error) {
	if client == nil {
		return nil, fmt.Errorf("workgraph: clickhouse client is required")
	}

	var repoRefs []string
	if filters != nil {
		repoRefs = filters.RepoIds
	}
	resolvedRepoIDs, shortCircuit, err := resolveRepoScope(ctx, client, orgID, repoRefs)
	if err != nil {
		return nil, err
	}
	scope := newFilterScope(filters, resolvedRepoIDs)
	if shortCircuit {
		return emptyEdgesResult(nil, scope), nil
	}

	limit := 1000
	if filters != nil {
		limit = filters.Limit
	}
	limit = clampEdgesLimit(limit)

	var theme, subcategory *string
	if filters != nil {
		theme, subcategory = filters.Theme, filters.Subcategory
	}
	themeFilterActive := (theme != nil && *theme != "") || (subcategory != nil && *subcategory != "")

	if themeSubcategoryConflict(theme, subcategory) {
		return emptyEdgesResult(nil, scope), nil
	}

	rows, err := fetchDedupedEdgeRows(ctx, client, orgID, scope, limit)
	if err != nil {
		if themeFilterActive && isMissingMembershipTableError(err) {
			reason := MembershipNotMaterialized
			return emptyEdgesResult(&reason, scope), nil
		}
		return nil, fmt.Errorf("workgraph: edges query: %w", err)
	}

	// NOT wrapped in the narrow membership-table catch above -- Python's
	// try/except covers ONLY the primary query call (work_graph.py:1183-1201);
	// _query_dependency_edges (work_graph.py:1203) runs OUTSIDE it, so a
	// missing-membership-table error from the dependency query's
	// theme-EXISTS clause (only reachable when BOTH a theme filter AND an
	// edge_type filter are active) propagates unguarded, exactly as here.
	dependencyRows, err := queryDependencyEdges(ctx, client, orgID, filters, scope, limit)
	if err != nil {
		return nil, fmt.Errorf("workgraph: dependency edges query: %w", err)
	}
	rows = spliceDependencyEdges(rows, dependencyRows, limit)

	var degradedReason *string
	if themeFilterActive && len(rows) == 0 {
		degradedReason = detectMembershipDegradedReason(ctx, client, orgID, scope)
	}

	endpoints := make([]edgeEndpoint, len(rows))
	for i, r := range rows {
		endpoints[i] = r.toEndpoint()
	}
	resolved := batchResolveDisplayNames(ctx, client, orgID, endpoints)

	membership, err := batchResolveMembership(ctx, client, orgID, endpoints, scope)
	if err != nil {
		return nil, fmt.Errorf("workgraph: batch membership: %w", err)
	}

	edges := make([]model.WorkGraphEdgeResult, len(rows))
	for i, r := range rows {
		edges[i] = rowToEdge(r, resolved, membership)
	}

	pageInfo := &model.PageInfo{
		HasNextPage:     len(edges) == limit,
		HasPreviousPage: false,
	}
	if len(edges) > 0 {
		start := edges[0].EdgeID
		end := edges[len(edges)-1].EdgeID
		pageInfo.StartCursor = &start
		pageInfo.EndCursor = &end
	}

	var partialScope *string
	if scope.usesScopedPartial() {
		s := "repo"
		partialScope = &s
	}

	return &model.WorkGraphEdgesResult{
		Edges:          edges,
		TotalCount:     len(edges),
		PageInfo:       pageInfo,
		DegradedReason: degradedReason,
		IsPartial:      scope.usesScopedPartial(),
		PartialScope:   partialScope,
		PartialRepoIds: scope.partialRepoIDs(),
	}, nil
}
