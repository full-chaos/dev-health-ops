// Package workgraph is the Go port of the three Work Graph GraphQL roots
// dev_health_ops.api.graphql.resolvers.work_graph.resolve_work_graph_edges /
// resolve_work_graph_flow / resolve_work_graph_artifacts
// (ops/src/dev_health_ops/api/graphql/resolvers/work_graph.py), CHAOS-4352
// plan Wave 4 Lane A (CHAOS-4504), copied from main SHA e91e0a0f7 (this
// worktree's tip, which already carries CHAOS-4493's fix -- see edges.go).
//
// # Scope
//
// devWorkGraphNeighbors (Ask Dev prototype) and `pr` (CHAOS-4503) are OUT of
// this port -- see BRIEF.md. Register the documents web ACTUALLY sends:
// WORK_GRAPH_EDGES_QUERY / WORK_GRAPH_FLOW_QUERY / WORK_GRAPH_ARTIFACTS_QUERY
// (web/src/lib/graphql/queries.ts:427,462,477), all three requesting
// theme/subcategory annotation (edges) and displayName (edges, artifacts) --
// so the membership-annotation and display-name-batch machinery below is in
// scope, not an optional extra.
//
// # The dedup fix (CHAOS-4515 / CHAOS-4504 "GO-SIDE DEDUP" ruling, 06:52 PT
// 08-29)
//
// work_graph_edges is ReplacingMergeTree(last_synced); Python's raw read
// (work_graph.py:1183, no FINAL/argMax) can return duplicate pre-merge
// versions of one logical edge tied on the entire ORDER BY. Python is frozen
// (chris ruling) -- the fix lands ONLY in this Go query: an argMax(...,
// last_synced) collapse per (org_id, source_type, source_id, edge_type,
// target_type, target_id) BEFORE ORDER BY/LIMIT, mirroring the already-correct
// in-repo reference src/dev_health_ops/work_graph/investment/queries.py:38-91
// (fetch_work_graph_edges). See edges.go's fetchDedupedEdgeRows doc comment.
//
// resolve_work_graph_flow and resolve_work_graph_artifacts do NOT get this
// fix: both aggregate with uniqExact(edge_id) (work_graph.py:1318, :1437),
// which counts each logical edge once regardless of duplicate physical rows
// -- dedup-tolerant by construction, confirmed by reading both queries line
// by line (matches .remember/lanes/lane-4516-group1/handoff-2026-08-29.md's
// corrected finding). Do not "fix" these -- there is nothing to fix.
//
// # THE COMPARATOR DIVERGES BY DESIGN on workGraphEdges
//
// Because this Go query carries a fix Python lacks, the stage-2 dual-run
// comparator is EXPECTED to diverge on workGraphEdges whenever seeded data
// contains an un-merged duplicate edge version -- that is the fix working,
// not a regression. See tests/api/graphql/test_go_api_dual_run_work_graph.py
// for the declared expected-divergence case. A MATCH under that exact
// condition is the suspicious result (it means the fix did not take effect
// or the seed could not produce the condition) -- never resolve a divergence
// by reverting this Go query to Python's raw-read behavior.
//
// # The splice trap (resolve_work_graph_edges only)
//
// work_graph.py:1203-1229 runs the deduped/ordered primary edges query
// (ORDER BY confidence DESC, edge_id ASC) and _query_dependency_edges
// (ORDER BY last_synced DESC, edge_id ASC -- a DIFFERENT key), then
// concatenates dependency rows not already present onto the END of the
// primary rows and truncates to limit. The combined list is NEVER
// re-sorted. edges.go's ResolveEdges reproduces this exactly: two
// independently-ordered slices, concatenated, never merged/re-sorted. This
// only activates behind a narrowing edge_type/edge_types filter (see
// dependencyEdgeFilterValues) -- the unfiltered path never calls
// queryDependencyEdges with a non-empty edge-type set, so a proof exercising
// only the unfiltered path never exercises this code path at all.
//
// # Pattern
//
// Same shape as the four Wave 1-3 packages (reviewedges, cognitiveload,
// complexitytimeseries, hotspots): a package-local QueryClient interface,
// hand-copied query text with a doc-comment citing the exact Python
// file:line, and a context-free Resolve* entrypoint per operation taking the
// caller's ALREADY-AUTHORIZED org id (never a client-supplied GraphQL
// argument -- see schema.resolvers.go's WorkGraphEdges/Flow/Artifacts for the
// "authorized org always wins" wiring).
package workgraph

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graph/model"
)

// QueryClient is the read-only ClickHouse query boundary this package needs
// -- same shape as hotspots.QueryClient / reviewedges.QueryClient / etc.,
// declared locally per those packages' own doc comments.
type QueryClient interface {
	Query(ctx context.Context, statement string, bindings []clickhouse.Binding) (clickhouse.RowScanner, error)
}

// MembershipNotMaterialized mirrors Python's MEMBERSHIP_NOT_MATERIALIZED
// (work_graph.py:728) -- the wire value the web client uses to distinguish a
// transient rollout state (membership table not yet populated) from a
// genuine empty theme-filter result.
const MembershipNotMaterialized = "MEMBERSHIP_NOT_MATERIALIZED"

// legacyRunID mirrors _membership_run_scope.py's LEGACY_RUN_ID.
const legacyRunID = "__legacy__"

// latestCompleteRunSubquery mirrors
// _membership_run_scope.py's LATEST_COMPLETE_RUN_SUBQUERY verbatim (pyformat
// %(org_id)s translated to the native {org_id:String} placeholder style this
// Go codebase's clickhouse.Binding uses -- same translation reviewedges.go /
// hotspots.go apply to every hand-copied query).
const latestCompleteRunSubquery = `
        SELECT argMax(run_id, completed_at) AS latest_run_id
        FROM work_unit_membership_runs
        WHERE org_id = {org_id:String}
`

// legacyNodeMaxJoin mirrors _membership_run_scope.py's LEGACY_NODE_MAX_JOIN
// verbatim. Aliased `lnm`, joined on (org_id, node_type, node_id) against
// `m` (work_unit_membership) -- the caller's alias must be exactly `m`.
const legacyNodeMaxJoin = `
            LEFT JOIN (
                SELECT
                    org_id,
                    node_type,
                    node_id,
                    max(computed_at) AS legacy_max_computed_at
                FROM work_unit_membership
                WHERE org_id = {org_id:String} AND run_id = ''
                GROUP BY org_id, node_type, node_id
            ) AS lnm
                ON lnm.org_id = m.org_id
                AND lnm.node_type = m.node_type
                AND lnm.node_id = m.node_id
`

// runScopePredicate mirrors _membership_run_scope.py's RUN_SCOPE_PREDICATE
// verbatim (string-substituted LEGACY_RUN_ID, exactly as the Python module
// does at import time via the f-string).
var runScopePredicate = "(latest_run.latest_run_id != '" + legacyRunID + "' " +
	"AND m.run_id = latest_run.latest_run_id) " +
	"OR (latest_run.latest_run_id = '" + legacyRunID + "' AND m.run_id = '' " +
	"AND m.computed_at = lnm.legacy_max_computed_at)"

// dependencyRelationshipTypeMap mirrors work_graph.py:71-87's
// _DEPENDENCY_RELATIONSHIP_TYPE_MAP verbatim (15 entries, including the
// "is_parent_of" -> "parent_of" entry that is easy to miss on a skim).
var dependencyRelationshipTypeMap = map[string]string{
	"blocks":          "blocks",
	"blocked_by":      "is_blocked_by",
	"is_blocked_by":   "is_blocked_by",
	"relates":         "relates",
	"relates_to":      "relates",
	"is_related_to":   "is_related_to",
	"duplicates":      "duplicates",
	"duplicate":       "duplicates",
	"is_duplicate_of": "is_duplicate_of",
	"parent":          "parent_of",
	"parent_of":       "parent_of",
	"is_parent_of":    "parent_of",
	"child":           "child_of",
	"child_of":        "child_of",
	"is_child_of":     "child_of",
}

// dependencyEdgeTypes mirrors work_graph.py:58-69's _DEPENDENCY_EDGE_TYPES
// frozenset verbatim -- the edge types resolve_work_graph_edges will splice
// in from work_item_dependencies.
var dependencyEdgeTypes = map[string]struct{}{
	"blocks":          {},
	"is_blocked_by":   {},
	"relates":         {},
	"is_related_to":   {},
	"duplicates":      {},
	"is_duplicate_of": {},
	"parent_of":       {},
	"child_of":        {},
}

// incidentStatusLabels mirrors work_graph.py:47-55's _INCIDENT_STATUS_LABELS
// verbatim.
var incidentStatusLabels = map[string]string{
	"open":          "Open",
	"triggered":     "Triggered",
	"acknowledged":  "Acknowledged",
	"investigating": "Investigating",
	"resolved":      "Resolved",
	"closed":        "Closed",
}

// opaqueHexIDRe mirrors work_graph.py:38's _OPAQUE_HEX_ID_RE.
var opaqueHexIDRe = regexp.MustCompile(`(?i)^[0-9a-f]{24,}$`)

// prEdgeIDRe mirrors work_graph.py:41-44's _PR_EDGE_ID_RE.
var prEdgeIDRe = regexp.MustCompile(`(?i)^([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})#pr(\d+)$`)

// bareUUIDRe mirrors api/services/identity.py:32-35's _BARE_UUID_RE (the
// implementation behind looks_like_uuid).
var bareUUIDRe = regexp.MustCompile(`(?i)^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

// looksLikeUUID mirrors api/services/identity.py:38-42's looks_like_uuid.
func looksLikeUUID(value string) bool {
	if value == "" {
		return false
	}
	return bareUUIDRe.MatchString(strings.TrimSpace(value))
}

// mapNodeType mirrors work_graph.py:158-159's _map_node_type. raw is
// expected already-lowercase (the table's actual column value); the schema
// enum wire value is the uppercase form, so this is a pure case transform,
// not a lookup table -- confirmed by reading
// api/graphql/models/outputs.py:471-492's WorkGraphNodeType enum, whose
// every `.value` is the lowercase snake_case of its member name.
//
// Returns an error for an unrecognized raw value instead of returning the
// invalid enum silently -- found by codex (2026-08-29, delta round, luna):
// Python's `WorkGraphNodeType(value.lower())` RAISES on an unknown value
// (a plain Enum constructor call), which Strawberry turns into a resolver
// error; gqlgen's generated MarshalGQL for these enum types only quotes
// whatever string it is given and does NOT call IsValid() itself, so an
// earlier version of this port that skipped validation would have emitted
// a SCHEMA-INVALID GraphQL response (an enum value not in the schema's
// enum set) for a row with unexpected data -- a real state, reachable by
// a newer edge_type added to the DB before this Go binary's enum set is
// updated, or by data corruption, not merely a theoretical row.
func mapNodeType(raw string) (model.WorkGraphNodeType, error) {
	v := model.WorkGraphNodeType(strings.ToUpper(raw))
	if !v.IsValid() {
		return "", fmt.Errorf("workgraph: %q is not a valid WorkGraphNodeType", raw)
	}
	return v, nil
}

// mapEdgeType mirrors work_graph.py:162-163's _map_edge_type -- same pure
// case-transform property as mapNodeType, confirmed against
// outputs.py:496-543's WorkGraphEdgeType enum, same validate-before-return
// contract (see mapNodeType's doc comment).
func mapEdgeType(raw string) (model.WorkGraphEdgeType, error) {
	v := model.WorkGraphEdgeType(strings.ToUpper(raw))
	if !v.IsValid() {
		return "", fmt.Errorf("workgraph: %q is not a valid WorkGraphEdgeType", raw)
	}
	return v, nil
}

// mapProvenance mirrors work_graph.py:166-167's _map_provenance -- same
// pure case-transform property, confirmed against outputs.py:545-550's
// WorkGraphProvenance enum, same validate-before-return contract (see
// mapNodeType's doc comment).
func mapProvenance(raw string) (model.WorkGraphProvenance, error) {
	v := model.WorkGraphProvenance(strings.ToUpper(raw))
	if !v.IsValid() {
		return "", fmt.Errorf("workgraph: %q is not a valid WorkGraphProvenance", raw)
	}
	return v, nil
}

// lowerNodeTypeInput mirrors reading a WorkGraphNodeTypeInput's `.value` on
// the Python side (api/graphql/models/inputs.py's mirrored input enum, same
// lowercase-value property as the output enum) -- the raw string bound into
// SQL WHERE clauses that compare against the table's lowercase column
// values.
func lowerNodeTypeInput(t model.WorkGraphNodeTypeInput) string {
	return strings.ToLower(string(t))
}

// lowerEdgeTypeInput is lowerNodeTypeInput's WorkGraphEdgeTypeInput
// counterpart.
func lowerEdgeTypeInput(t model.WorkGraphEdgeTypeInput) string {
	return strings.ToLower(string(t))
}

// incidentLabel mirrors work_graph.py:153-155's _incident_label.
func incidentLabel(status string) string {
	if label, ok := incidentStatusLabels[strings.ToLower(status)]; ok {
		return label
	}
	return "Incident"
}

// displayNameFor mirrors work_graph.py:231-258's _display_name_for exactly
// -- resolution priority: (1) lookup-resolved name, (2) human-readable
// pass-through for a non-UUID/non-hex/non-PR-format id, (3) nil for bare
// UUIDs, opaque hex strings, and unresolved UUID-based PR ids (A7/A8: never
// leak a raw UUID to the client, which renders a controlled "Unresolved"
// badge instead).
func displayNameFor(entityID string, resolved map[string]string) *string {
	raw := strings.TrimSpace(entityID)
	if raw == "" {
		return nil
	}
	if resolved != nil {
		if name, ok := resolved[raw]; ok {
			return &name
		}
	}
	if prEdgeIDRe.MatchString(raw) {
		return nil
	}
	if looksLikeUUID(raw) || opaqueHexIDRe.MatchString(raw) {
		return nil
	}
	out := raw
	return &out
}

// subcategoryParentTheme mirrors work_graph.py:667-680's
// _subcategory_parent_theme. Python's canonical SUBCATEGORY_TO_THEME map
// (investment_taxonomy.py:38-40) is DERIVED, not hand-authored:
// `{subcategory: subcategory.split(".", 1)[0] for subcategory in
// SUBCATEGORIES}` -- i.e. for every canonical subcategory key the "looked
// up" value is identical to the function's own fallback-prefix computation.
// So the lookup-then-fallback structure is behaviorally a no-op: BOTH
// branches compute `subcategory.split(".", 1)[0]` for any input, canonical
// or not. This port skips embedding the (large, taxonomy-owned) map and
// computes the split directly -- verified equivalent by reading the map's
// definition, not assumed.
func subcategoryParentTheme(subcategory string) *string {
	prefix, _, _ := strings.Cut(subcategory, ".")
	if prefix == "" {
		return nil
	}
	return &prefix
}

// themeSubcategoryConflict mirrors work_graph.py:683-695's
// _theme_subcategory_conflict.
func themeSubcategoryConflict(theme, subcategory *string) bool {
	if theme == nil || subcategory == nil || *theme == "" || *subcategory == "" {
		return false
	}
	parent := subcategoryParentTheme(*subcategory)
	return parent == nil || *parent != *theme
}
