package workgraph

import (
	"context"
	"fmt"
	"sort"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graph/model"
)

func emptyFlowResult(degradedReason *string, scope *filterScope) *model.WorkGraphFlowResult {
	var partialScope *string
	if scope.usesScopedPartial() {
		s := "repo"
		partialScope = &s
	}
	return &model.WorkGraphFlowResult{
		Rows:           []model.WorkGraphFlowRow{},
		DegradedReason: degradedReason,
		IsPartial:      scope.usesScopedPartial(),
		PartialScope:   partialScope,
		PartialRepoIds: scope.partialRepoIDs(),
	}
}

// ResolveFlow mirrors work_graph.py:1268-1374's resolve_work_graph_flow: a
// true server-side aggregate over the FULL edge set, GROUPed by
// (source_type, target_type). Uses uniqExact(edge_id), NOT count() --
// work_graph_edges is ReplacingMergeTree(last_synced) and an un-merged
// duplicate physical row would inflate a raw count(), but edge_id is
// unique per logical edge and falls in exactly one (source_type,
// target_type) group, so uniqExact(edge_id) is exact WITHOUT the
// argMax collapse edges.go's fetchDedupedEdgeRows needs -- confirmed
// dedup-tolerant by reading the query verbatim (work_graph.py:1317-1322),
// matching lane-4516-group1's finding that this sibling read is NOT
// exposed. Do not add an argMax fix here; there is nothing to fix.
//
// Applies ONLY the graph-wide filters (org_id + repo_ids +
// theme/subcategory) via buildWorkGraphWhere(includeEdgeFilters=false) --
// edge-list-only filters (source_type/target_type/edge_type/edge_types/
// node_id) do NOT apply here, exactly as work_graph.py:1306-1308 documents.
func ResolveFlow(ctx context.Context, client QueryClient, orgID string, filters *model.WorkGraphEdgeFilterInput) (*model.WorkGraphFlowResult, error) {
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
		return emptyFlowResult(nil, scope), nil
	}

	var theme, subcategory *string
	if filters != nil {
		theme, subcategory = filters.Theme, filters.Subcategory
	}
	themeFilterActive := (theme != nil && *theme != "") || (subcategory != nil && *subcategory != "")

	if themeSubcategoryConflict(theme, subcategory) {
		return emptyFlowResult(nil, scope), nil
	}

	where := buildWorkGraphWhere(orgID, scope, false)
	query := fmt.Sprintf(`
        SELECT source_type, target_type, uniqExact(edge_id) AS cnt
        FROM work_graph_edges
        %s
        GROUP BY source_type, target_type
    `, where.sql)

	rows, err := client.Query(ctx, query, where.bindings)
	if err != nil {
		if themeFilterActive && isMissingMembershipTableError(err) {
			reason := MembershipNotMaterialized
			return emptyFlowResult(&reason, scope), nil
		}
		return nil, fmt.Errorf("workgraph: flow query: %w", err)
	}
	defer rows.Close()

	inflow := map[model.WorkGraphNodeType]int{}
	outflow := map[model.WorkGraphNodeType]int{}
	for rows.Next() {
		var sourceType, targetType string
		var cnt uint64
		if scanErr := rows.Scan(&sourceType, &targetType, &cnt); scanErr != nil {
			return nil, fmt.Errorf("workgraph: flow scan: %w", scanErr)
		}
		st := mapNodeType(sourceType)
		tt := mapNodeType(targetType)
		outflow[st] += int(cnt)
		inflow[tt] += int(cnt)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("workgraph: flow rows: %w", err)
	}

	seen := map[model.WorkGraphNodeType]struct{}{}
	for nt := range inflow {
		seen[nt] = struct{}{}
	}
	for nt := range outflow {
		seen[nt] = struct{}{}
	}
	flowRows := make([]model.WorkGraphFlowRow, 0, len(seen))
	for nt := range seen {
		in, out := inflow[nt], outflow[nt]
		if in > 0 || out > 0 {
			flowRows = append(flowRows, model.WorkGraphFlowRow{NodeType: nt, Inflow: in, Outflow: out})
		}
	}
	sort.Slice(flowRows, func(i, j int) bool {
		di := flowRows[i].Inflow + flowRows[i].Outflow
		dj := flowRows[j].Inflow + flowRows[j].Outflow
		if di != dj {
			return di > dj
		}
		return flowRows[i].NodeType < flowRows[j].NodeType
	})

	var degradedReason *string
	if themeFilterActive && len(flowRows) == 0 {
		degradedReason = detectMembershipDegradedReason(ctx, client, orgID, scope)
	}

	var partialScope *string
	if scope.usesScopedPartial() {
		s := "repo"
		partialScope = &s
	}

	return &model.WorkGraphFlowResult{
		Rows:           flowRows,
		DegradedReason: degradedReason,
		IsPartial:      scope.usesScopedPartial(),
		PartialScope:   partialScope,
		PartialRepoIds: scope.partialRepoIDs(),
	}, nil
}
