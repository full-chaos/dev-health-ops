package workgraph

import (
	"context"
	"fmt"

	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graph/model"
)

func emptyArtifactsResult(degradedReason *string, scope *filterScope) *model.WorkGraphArtifactsResult {
	var partialScope *string
	if scope.usesScopedPartial() {
		s := "repo"
		partialScope = &s
	}
	return &model.WorkGraphArtifactsResult{
		Rows:           []model.WorkGraphArtifactRow{},
		DegradedReason: degradedReason,
		IsPartial:      scope.usesScopedPartial(),
		PartialScope:   partialScope,
		PartialRepoIds: scope.partialRepoIDs(),
	}
}

// ResolveArtifacts mirrors work_graph.py:1377-1502's
// resolve_work_graph_artifacts: top-N nodes by degree over the FULL edge
// set (a UNION ALL of source/target projections). Uses uniqExact(edge_id)
// for degree, NOT count() -- same dedup-tolerant-by-construction property
// ResolveFlow documents (work_graph.py:1430-1435), confirmed by reading the
// query verbatim; no argMax fix needed here either. A self-referential edge
// (source==target) contributes the SAME edge_id to both UNION ALL
// projections of that node, so uniqExact makes it count once (degree 1),
// not twice.
//
// filters.limit is the top-N (default 1000, web passes 50). Applies ONLY
// the graph-wide filters, same as ResolveFlow. Display names reuse
// batchResolveDisplayNames by shaping each aggregate row as a pseudo-edge
// whose source endpoint IS the node and whose target endpoint is blank
// (ignored by batchResolveDisplayNames's collect closure, which returns
// immediately on an empty entityID) -- work_graph.py:1465-1477's exact
// "reuse the edge-list batch resolver" approach, one query per entity type,
// no N+1.
func ResolveArtifacts(ctx context.Context, client QueryClient, orgID string, filters *model.WorkGraphEdgeFilterInput) (*model.WorkGraphArtifactsResult, error) {
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
		return emptyArtifactsResult(nil, scope), nil
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
		return emptyArtifactsResult(nil, scope), nil
	}

	// The where clause (including the correlated theme EXISTS clause
	// referencing work_graph_edges.{source,target}_{type,id}) is rendered
	// TWICE -- once per UNION ALL branch, exactly as work_graph.py:1424-1428
	// documents. buildWorkGraphWhere is deterministic (same orgID/scope in),
	// so calling it twice yields byte-identical SQL text; bindings must be
	// supplied ONCE per placeholder name (the native ClickHouse driver
	// resolves each named placeholder from one shared parameter map, so
	// re-using the same binding set for both occurrences is correct, not a
	// duplicate-bind bug).
	where := buildWorkGraphWhere(orgID, scope, false)
	query := fmt.Sprintf(`
        SELECT node_type, node_id, uniqExact(edge_id) AS degree, any(evidence) AS evidence
        FROM (
            SELECT source_type AS node_type, source_id AS node_id, edge_id, evidence
            FROM work_graph_edges
            %s
            UNION ALL
            SELECT target_type AS node_type, target_id AS node_id, edge_id, evidence
            FROM work_graph_edges
            %s
        )
        GROUP BY node_type, node_id
        ORDER BY degree DESC, node_id ASC
        LIMIT {limit:UInt64}
    `, where.sql, where.sql)

	bindings := append(where.bindings, clickhouse.Binding{Name: "limit", Value: limit})

	rows, err := client.Query(ctx, query, bindings)
	if err != nil {
		if themeFilterActive && isMissingMembershipTableError(err) {
			reason := MembershipNotMaterialized
			return emptyArtifactsResult(&reason, scope), nil
		}
		return nil, fmt.Errorf("workgraph: artifacts query: %w", err)
	}
	defer rows.Close()

	type artifactRow struct {
		nodeType, nodeID, evidence string
		degree                     uint64
	}
	var rawRows []artifactRow
	for rows.Next() {
		var r artifactRow
		if scanErr := rows.Scan(&r.nodeType, &r.nodeID, &r.degree, &r.evidence); scanErr != nil {
			return nil, fmt.Errorf("workgraph: artifacts scan: %w", scanErr)
		}
		rawRows = append(rawRows, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("workgraph: artifacts rows: %w", err)
	}

	pseudoRows := make([]edgeEndpoint, len(rawRows))
	for i, r := range rawRows {
		pseudoRows[i] = edgeEndpoint{sourceID: r.nodeID, sourceType: r.nodeType}
	}
	resolved := batchResolveDisplayNames(ctx, client, orgID, pseudoRows)

	artifactRows := make([]model.WorkGraphArtifactRow, len(rawRows))
	for i, r := range rawRows {
		row := model.WorkGraphArtifactRow{
			NodeType:    mapNodeType(r.nodeType),
			NodeID:      r.nodeID,
			DisplayName: displayNameFor(r.nodeID, resolved),
			Degree:      int(r.degree),
		}
		if r.evidence != "" {
			ev := r.evidence
			row.Evidence = &ev
		}
		artifactRows[i] = row
	}

	var degradedReason *string
	if themeFilterActive && len(artifactRows) == 0 {
		degradedReason = detectMembershipDegradedReason(ctx, client, orgID, scope)
	}

	var partialScope *string
	if scope.usesScopedPartial() {
		s := "repo"
		partialScope = &s
	}

	return &model.WorkGraphArtifactsResult{
		Rows:           artifactRows,
		DegradedReason: degradedReason,
		IsPartial:      scope.usesScopedPartial(),
		PartialScope:   partialScope,
		PartialRepoIds: scope.partialRepoIDs(),
	}, nil
}
