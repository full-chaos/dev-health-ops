package workgraph

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graph/model"
)

// filterScope carries a request's ALREADY-RESOLVED repo scope alongside the
// original GraphQL filter input. Python mutates filters.repo_ids in place
// (work_graph.py:149, `filters.repo_ids = resolved`) after
// _resolve_filter_repo_scope runs; this port does not mutate the caller's
// model.WorkGraphEdgeFilterInput (Go structs shared with gqlgen's request
// pipeline should not be mutated by a resolver), so every downstream helper
// that Python reads `filters.repo_ids` from post-resolution instead takes
// this scope's repoIDs field.
type filterScope struct {
	filters *model.WorkGraphEdgeFilterInput
	// repoIDs is the RESOLVED set of repo UUIDs (post resolveRepoScope) --
	// nil/empty when no repo filter is active. Deduplicated and sorted for
	// determinism; order carries no semantic meaning downstream (every
	// consumer uses it as an IN-set or a HAVING count, never a sequence --
	// see resolveRepoScope's doc comment).
	repoIDs []string
}

func newFilterScope(filters *model.WorkGraphEdgeFilterInput, resolvedRepoIDs []string) *filterScope {
	return &filterScope{filters: filters, repoIDs: resolvedRepoIDs}
}

// usesScopedPartial mirrors work_graph.py:90-91's _uses_scoped_partial.
func (s *filterScope) usesScopedPartial() bool {
	return s.filters != nil && s.filters.AllowScopedPartial && len(s.repoIDs) > 0
}

// partialRepoIDs mirrors work_graph.py:94-97's _partial_repo_ids. repoIDs is
// already deduplicated+sorted by resolveRepoScope, matching Python's
// `sorted({str(r) for r in filters.repo_ids if r})`.
func (s *filterScope) partialRepoIDs() []string {
	if !s.usesScopedPartial() {
		return nil
	}
	return s.repoIDs
}

// resolveRepoScope mirrors work_graph.py:126-150's
// _resolve_filter_repo_scope, whose single choke point is
// api/queries/scopes.py:53-69 (resolve_repo_ids) / :19-50 (resolve_repo_id).
// Python resolves refs one at a time against the org-scoped `repos` catalog
// (a UUID-shaped ref matches repos.id, anything else matches repos.repo) --
// N sequential ClickHouse round trips for N refs. This port batches every
// ref into ONE query (BRIEF.md scope note: "there is no N+1 to reproduce,
// and you must not introduce one") while preserving the identical per-ref
// matching rule and the identical org scoping.
//
// Returns (resolvedIDs, shortCircuitEmpty, err). shortCircuitEmpty is true
// when refs was non-empty but NONE resolved to a known repo -- callers MUST
// return an empty result immediately (work_graph.py:1115-1116,
// :1290-1291, :1401-1402) rather than falling through to an unscoped
// whole-org query.
func resolveRepoScope(ctx context.Context, client QueryClient, orgID string, refs []string) ([]string, bool, error) {
	if len(refs) == 0 {
		return nil, false, nil
	}

	uuidSeen := map[string]struct{}{}
	nameSeen := map[string]struct{}{}
	var uuidRefs, nameRefs []string
	for _, ref := range refs {
		if ref == "" {
			continue
		}
		if looksLikeUUID(ref) {
			lower := strings.ToLower(strings.TrimSpace(ref))
			if _, ok := uuidSeen[lower]; !ok {
				uuidSeen[lower] = struct{}{}
				uuidRefs = append(uuidRefs, lower)
			}
			continue
		}
		if _, ok := nameSeen[ref]; !ok {
			nameSeen[ref] = struct{}{}
			nameRefs = append(nameRefs, ref)
		}
	}
	if len(uuidRefs) == 0 && len(nameRefs) == 0 {
		return nil, true, nil
	}

	var conds []string
	bindings := []clickhouse.Binding{{Name: "org_id", Value: orgID}}
	if len(uuidRefs) > 0 {
		conds = append(conds, "toString(id) IN {uuid_refs:Array(String)}")
		bindings = append(bindings, clickhouse.Binding{Name: "uuid_refs", Value: uuidRefs})
	}
	if len(nameRefs) > 0 {
		conds = append(conds, "repo IN {name_refs:Array(String)}")
		bindings = append(bindings, clickhouse.Binding{Name: "name_refs", Value: nameRefs})
	}

	query := fmt.Sprintf(`
        SELECT toString(id) AS id
        FROM repos
        WHERE org_id = {org_id:String}
          AND (%s)
    `, strings.Join(conds, " OR "))

	rows, err := client.Query(ctx, query, bindings)
	if err != nil {
		return nil, false, fmt.Errorf("workgraph: resolve repo scope: %w", err)
	}
	defer rows.Close()

	seen := map[string]struct{}{}
	var resolved []string
	for rows.Next() {
		var id string
		if scanErr := rows.Scan(&id); scanErr != nil {
			return nil, false, fmt.Errorf("workgraph: resolve repo scope scan: %w", scanErr)
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		resolved = append(resolved, id)
	}
	if err := rows.Err(); err != nil {
		return nil, false, fmt.Errorf("workgraph: resolve repo scope rows: %w", err)
	}
	sort.Strings(resolved)
	return resolved, len(resolved) == 0, nil
}

// membershipRunSubquery mirrors work_graph.py:100-113's
// _membership_run_subquery.
func membershipRunSubquery(scope *filterScope) string {
	if !scope.usesScopedPartial() {
		return latestCompleteRunSubquery
	}
	return `
        SELECT run_id AS latest_run_id
        FROM work_unit_membership_scoped_runs
        WHERE org_id = {org_id:String}
          AND scope_kind = 'repo'
          AND scope_id IN {scoped_repo_ids:Array(String)}
        GROUP BY run_id
        HAVING uniqExact(scope_id) = {scoped_repo_count:UInt64}
        ORDER BY max(completed_at) DESC, run_id DESC
        LIMIT 1
`
}

// addMembershipScopeBindings mirrors work_graph.py:116-123's
// _add_membership_scope_params.
func addMembershipScopeBindings(bindings []clickhouse.Binding, scope *filterScope) []clickhouse.Binding {
	scopedRepoIDs := scope.partialRepoIDs()
	if len(scopedRepoIDs) == 0 {
		return bindings
	}
	return append(bindings,
		clickhouse.Binding{Name: "scoped_repo_ids", Value: scopedRepoIDs},
		clickhouse.Binding{Name: "scoped_repo_count", Value: len(scopedRepoIDs)},
	)
}

// themeFilter is the resolved theme/subcategory filter state for one
// request -- (kind, category) pairs the matched membership row set must
// cover, mirroring work_graph.py:698-723's _build_theme_filter's `wanted`
// list. The Python `wanted` list has EXACTLY TWO possible non-empty shapes
// at any call site: [("theme", theme)], [("subcategory", subcategory)], or
// both together -- never more, never a different kind. This port exploits
// that fixed cardinality: instead of a generic tuple-array bind parameter
// (the clickhouse.Binding wire format this codebase's client supports has
// no tuple-array encoding -- see clickhouse/bindings.go's
// clickHouseParameter, which handles only string/[]string/time.Time/ints),
// the membership-match predicate is a fixed 2-branch OR built directly from
// the theme/subcategory strings, each still a bound parameter. Semantically
// identical to Python's `(kind, category) IN wanted` for this call site's
// provably-bounded `wanted`.
type themeFilter struct {
	theme       *string
	subcategory *string
}

func (f themeFilter) active() bool {
	return f.theme != nil && *f.theme != "" || f.subcategory != nil && *f.subcategory != ""
}

// wantedCount mirrors _build_theme_filter's `wanted_count` (len(wanted)).
func (f themeFilter) wantedCount() int {
	n := 0
	if f.theme != nil && *f.theme != "" {
		n++
	}
	if f.subcategory != nil && *f.subcategory != "" {
		n++
	}
	return n
}

// categoryMatchSQL renders the fixed 2-branch OR described in themeFilter's
// doc comment, referencing the correlated table's own category_kind/category
// columns (aliased however the caller's outer query aliases them -- callers
// pass the exact column reference, e.g. "m.category_kind"/"m.category").
func (f themeFilter) categoryMatchSQL(kindCol, categoryCol string) string {
	var branches []string
	if f.theme != nil && *f.theme != "" {
		branches = append(branches, fmt.Sprintf("(%s = 'theme' AND %s = {theme:String})", kindCol, categoryCol))
	}
	if f.subcategory != nil && *f.subcategory != "" {
		branches = append(branches, fmt.Sprintf("(%s = 'subcategory' AND %s = {subcategory:String})", kindCol, categoryCol))
	}
	return strings.Join(branches, " OR ")
}

func (f themeFilter) bindings() []clickhouse.Binding {
	var out []clickhouse.Binding
	if f.theme != nil && *f.theme != "" {
		out = append(out, clickhouse.Binding{Name: "theme", Value: *f.theme})
	}
	if f.subcategory != nil && *f.subcategory != "" {
		out = append(out, clickhouse.Binding{Name: "subcategory", Value: *f.subcategory})
	}
	return out
}

// themeMembershipExistsClause mirrors work_graph.py:634-664's
// _theme_membership_exists_clause -- a correlated EXISTS semi-join against
// work_graph_edges' own source/target endpoints, pushed into the edge
// query's WHERE so the membership filter, the repo/edge-type filters, AND
// the LIMIT all execute in one ClickHouse plan (before-LIMIT guarantee).
func themeMembershipExistsClause(scope *filterScope, tf themeFilter) string {
	return fmt.Sprintf(`
        EXISTS (
            SELECT 1
            FROM work_unit_membership AS m
            INNER JOIN (%s) AS latest_run
                ON 1 = 1
            %s
            WHERE m.org_id = {org_id:String}
              AND latest_run.latest_run_id != ''
              AND (%s)
              AND (
                (m.node_type, m.node_id) = (work_graph_edges.source_type, work_graph_edges.source_id)
                OR (m.node_type, m.node_id) = (work_graph_edges.target_type, work_graph_edges.target_id)
              )
              AND (%s)
            GROUP BY m.node_type, m.node_id
            HAVING uniqExact((m.category_kind, m.category)) = {wanted_count:UInt64}
        )
    `, membershipRunSubquery(scope), legacyNodeMaxJoin, runScopePredicate, tf.categoryMatchSQL("m.category_kind", "m.category"))
}

// dependencyThemeMembershipExistsClause mirrors work_graph.py:189-210's
// _dependency_theme_membership_exists_clause -- same shape as
// themeMembershipExistsClause but correlated against the DERIVED
// source_type/source_work_item_id/target_type/target_work_item_id columns
// the dependency-edges subquery projects (see dependency.go), not
// work_graph_edges' own columns.
func dependencyThemeMembershipExistsClause(scope *filterScope, tf themeFilter) string {
	return fmt.Sprintf(`
        EXISTS (
            SELECT 1
            FROM work_unit_membership AS m
            INNER JOIN (%s) AS latest_run
                ON 1 = 1
            %s
            WHERE m.org_id = {org_id:String}
              AND latest_run.latest_run_id != ''
              AND (%s)
              AND (
                (m.node_type, m.node_id) = (source_type, source_work_item_id)
                OR (m.node_type, m.node_id) = (target_type, target_work_item_id)
              )
              AND (%s)
            GROUP BY m.node_type, m.node_id
            HAVING uniqExact((m.category_kind, m.category)) = {wanted_count:UInt64}
        )
    `, membershipRunSubquery(scope), legacyNodeMaxJoin, runScopePredicate, tf.categoryMatchSQL("m.category_kind", "m.category"))
}

// whereBuild is buildWorkGraphWhere's return value: the rendered WHERE
// clause plus its bindings.
type whereBuild struct {
	sql      string
	bindings []clickhouse.Binding
}

// buildWorkGraphWhere mirrors work_graph.py:930-1009's
// _build_work_graph_where. Callers MUST have already screened for
// themeSubcategoryConflict and short-circuited to an empty result -- this
// builder assumes a non-conflicting theme/subcategory, exactly as the
// Python docstring states.
func buildWorkGraphWhere(orgID string, scope *filterScope, includeEdgeFilters bool) whereBuild {
	clauses := []string{"org_id = {org_id:String}"}
	bindings := []clickhouse.Binding{{Name: "org_id", Value: orgID}}
	bindings = addMembershipScopeBindings(bindings, scope)

	var theme, subcategory *string
	if scope.filters != nil {
		theme = scope.filters.Theme
		subcategory = scope.filters.Subcategory
	}

	if scope.filters != nil {
		if len(scope.repoIDs) > 0 {
			clauses = append(clauses, "repo_id IN {repo_ids:Array(String)}")
			bindings = append(bindings, clickhouse.Binding{Name: "repo_ids", Value: scope.repoIDs})
		}

		if includeEdgeFilters {
			if scope.filters.SourceType != nil {
				clauses = append(clauses, "source_type = {source_type:String}")
				bindings = append(bindings, clickhouse.Binding{Name: "source_type", Value: lowerNodeTypeInput(*scope.filters.SourceType)})
			}
			if scope.filters.TargetType != nil {
				clauses = append(clauses, "target_type = {target_type:String}")
				bindings = append(bindings, clickhouse.Binding{Name: "target_type", Value: lowerNodeTypeInput(*scope.filters.TargetType)})
			}
			if scope.filters.EdgeType != nil {
				clauses = append(clauses, "edge_type = {edge_type:String}")
				bindings = append(bindings, clickhouse.Binding{Name: "edge_type", Value: lowerEdgeTypeInput(*scope.filters.EdgeType)})
			}
			if len(scope.filters.EdgeTypes) > 0 {
				values := make([]string, len(scope.filters.EdgeTypes))
				for i, et := range scope.filters.EdgeTypes {
					values[i] = lowerEdgeTypeInput(et)
				}
				clauses = append(clauses, "edge_type IN {edge_types:Array(String)}")
				bindings = append(bindings, clickhouse.Binding{Name: "edge_types", Value: values})
			}
			if scope.filters.NodeID != nil && *scope.filters.NodeID != "" {
				clauses = append(clauses, "(source_id = {node_id:String} OR target_id = {node_id:String})")
				bindings = append(bindings, clickhouse.Binding{Name: "node_id", Value: *scope.filters.NodeID})
			}
		}
	}

	tf := themeFilter{theme: theme, subcategory: subcategory}
	if tf.active() {
		clauses = append(clauses, themeMembershipExistsClause(scope, tf))
		bindings = append(bindings, tf.bindings()...)
		bindings = append(bindings, clickhouse.Binding{Name: "wanted_count", Value: tf.wantedCount()})
	}

	return whereBuild{sql: "WHERE " + strings.Join(clauses, " AND "), bindings: bindings}
}

// dependencyEdgeFilterValues mirrors work_graph.py:213-228's
// _dependency_edge_filter_values.
func dependencyEdgeFilterValues(filters *model.WorkGraphEdgeFilterInput) []string {
	if filters == nil {
		return nil
	}
	var requested map[string]struct{}
	switch {
	case filters.EdgeType != nil && len(filters.EdgeTypes) > 0:
		plural := make(map[string]struct{}, len(filters.EdgeTypes))
		for _, et := range filters.EdgeTypes {
			plural[lowerEdgeTypeInput(et)] = struct{}{}
		}
		requested = map[string]struct{}{}
		single := lowerEdgeTypeInput(*filters.EdgeType)
		if _, ok := plural[single]; ok {
			requested[single] = struct{}{}
		}
	case filters.EdgeType != nil:
		requested = map[string]struct{}{lowerEdgeTypeInput(*filters.EdgeType): {}}
	case len(filters.EdgeTypes) > 0:
		requested = make(map[string]struct{}, len(filters.EdgeTypes))
		for _, et := range filters.EdgeTypes {
			requested[lowerEdgeTypeInput(et)] = struct{}{}
		}
	default:
		return nil
	}

	var out []string
	for v := range requested {
		if _, ok := dependencyEdgeTypes[v]; ok {
			out = append(out, v)
		}
	}
	sort.Strings(out)
	return out
}

// dependencyEdgeTypeSQL mirrors work_graph.py:170-177's
// _dependency_edge_type_sql -- a multiIf built from
// dependencyRelationshipTypeMap sorted by key (Python's `sorted(dict.items())`
// sorts by key), same default fallback 'relates'.
func dependencyEdgeTypeSQL() string {
	keys := make([]string, 0, len(dependencyRelationshipTypeMap))
	for k := range dependencyRelationshipTypeMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	cases := make([]string, 0, len(keys))
	for _, k := range keys {
		cases = append(cases, fmt.Sprintf("relationship_type = '%s', '%s'", k, dependencyRelationshipTypeMap[k]))
	}
	return fmt.Sprintf("multiIf(%s, 'relates')", strings.Join(cases, ", "))
}

// dependencyNodeTypeSQL mirrors work_graph.py:180-186's
// _dependency_node_type_sql.
func dependencyNodeTypeSQL(columnName string) string {
	return fmt.Sprintf(
		"multiIf(startsWith(%s, 'ghpr:') OR (startsWith(%s, 'gitlab:') AND position(%s, '!') > 0), 'pr', 'issue')",
		columnName, columnName, columnName,
	)
}

// detectMembershipDegradedReason mirrors work_graph.py:731-819's
// _detect_membership_degraded_reason. Never returns an error to the
// caller on probe failure -- mirrors Python's own "never raises" contract
// (a failed probe must not affect the request).
func detectMembershipDegradedReason(ctx context.Context, client QueryClient, orgID string, scope *filterScope) *string {
	scopedRepoIDs := scope.partialRepoIDs()

	var markerSQL string
	bindings := []clickhouse.Binding{{Name: "org_id", Value: orgID}}
	if len(scopedRepoIDs) > 0 {
		markerSQL = `
                    SELECT count()
                    FROM (
                        SELECT run_id
                        FROM work_unit_membership_scoped_runs
                        WHERE org_id = {org_id:String}
                          AND scope_kind = 'repo'
                          AND scope_id IN {scoped_repo_ids:Array(String)}
                        GROUP BY run_id
                        HAVING uniqExact(scope_id) = {scoped_repo_count:UInt64}
                    )
                `
		bindings = append(bindings,
			clickhouse.Binding{Name: "scoped_repo_ids", Value: scopedRepoIDs},
			clickhouse.Binding{Name: "scoped_repo_count", Value: len(scopedRepoIDs)},
		)
	} else {
		markerSQL = `
                    SELECT count()
                    FROM work_unit_membership_runs
                    WHERE org_id = {org_id:String}
                `
	}

	query := fmt.Sprintf(`
            SELECT
                (%s) AS complete_run_markers,
                (
                    SELECT count()
                    FROM work_unit_investments
                    WHERE org_id = {org_id:String}
                ) AS investment_rows
        `, markerSQL)

	rows, err := client.Query(ctx, query, bindings)
	if err != nil {
		return nil
	}
	defer rows.Close()

	if !rows.Next() {
		return nil
	}
	var completeRunMarkers, investmentRows uint64
	if scanErr := rows.Scan(&completeRunMarkers, &investmentRows); scanErr != nil {
		return nil
	}
	if completeRunMarkers == 0 && investmentRows > 0 {
		reason := MembershipNotMaterialized
		return &reason
	}
	return nil
}
