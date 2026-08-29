package workgraph

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"

	chproto "github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graph/model"
)

// unknownTableIdentifierRe mirrors work_graph.py:835-838's
// _UNKNOWN_TABLE_IDENTIFIER_RE.
var unknownTableIdentifierRe = regexp.MustCompile(`(?i)Unknown table(?: expression identifier)?\s+'([^']+)'`)

// membershipTables mirrors work_graph.py:822-828's _MEMBERSHIP_TABLES.
var membershipTables = map[string]struct{}{
	"work_unit_membership":             {},
	"work_unit_membership_runs":        {},
	"work_unit_membership_scoped_runs": {},
}

// unknownTableNames mirrors work_graph.py:841-851's _unknown_table_names.
func unknownTableNames(text string) map[string]struct{} {
	names := map[string]struct{}{}
	for _, m := range unknownTableIdentifierRe.FindAllStringSubmatch(text, -1) {
		ident := m[1]
		if idx := strings.LastIndex(ident, "."); idx >= 0 {
			ident = ident[idx+1:]
		}
		names[strings.Trim(ident, "`")] = struct{}{}
	}
	return names
}

// unknownTableExceptionCode is ClickHouse's UNKNOWN_TABLE error code.
const unknownTableExceptionCode = 60

// isMissingMembershipTableError mirrors work_graph.py:854-877's
// _is_missing_membership_table_error: true ONLY when a ClickHouse
// missing-table (code 60) error names work_unit_membership OR
// work_unit_membership_runs (or the scoped-runs table) as the unknown
// table -- any other code-60 error (a different missing table, e.g.
// work_graph_edges itself) is NOT swallowed.
//
// Keys on the driver's own TYPED exception via errors.As, not a
// string/substring match. dev-health-go/clickhouse's Client.Query wraps
// every driver error as &operationError{operation, cause} whose OWN
// Error() method returns only the fixed string "ClickHouse query
// failed" -- the real error is clickhouse-go/v2/lib/proto.Exception
// (Code int32, Message string), reachable through the chain because
// operationError implements Unwrap() (returns cause) and errors.As
// walks Unwrap() at every level automatically -- this port's own
// fmt.Errorf("workgraph: ...: %w", err) wrapping on top is walked the
// same way, no extra plumbing needed.
//
// An EARLIER version of this function read err.Error() (later:
// errorChainText(err), a manual multi-level Unwrap+concat) and matched
// the SUBSTRINGS "UNKNOWN_TABLE"/"code: 60" against that text --
// text that exists only because proto.Exception.Error() happens to
// render `fmt.Sprintf("code: %d, message: %s", e.Code, e.Message)`
// today. That matched a FORMAT STRING, not a field: if the driver ever
// reformats Error()'s output (a plausible, non-breaking change on
// their side), the classifier would silently stop matching and this
// exact degraded path goes dead again -- with every existing test still
// green, because a text-shaped test double mirrors the string, not the
// type, and keeps agreeing with itself regardless. Found by codex
// (2026-08-29, gpt-5.6-terra xhigh round) as the original bug (the
// single-level err.Error() call never saw the real client's wrapped
// text at all, only this package's own test fakes' flat errors);
// tightened to a structured check per chris/orchestrator's follow-up
// ruling the same day, once the driver's typed Exception (already an
// indirect dependency via go.mod's direct clickhouse-go/v2 v2.47.0) was
// confirmed to carry Code as a real field, not just a rendered string.
func isMissingMembershipTableError(err error) bool {
	if err == nil {
		return false
	}
	var exc *chproto.Exception
	if !errors.As(err, &exc) || exc.Code != unknownTableExceptionCode {
		return false
	}
	names := unknownTableNames(exc.Message)
	for name := range names {
		if _, ok := membershipTables[name]; ok {
			return true
		}
	}
	return false
}

// membershipKey is (node_type, node_id) -- work_graph.py's
// dict[tuple[str, str], dict[str, str]] key.
type membershipKey struct {
	nodeType, nodeID string
}

type membershipEntry struct {
	dominantTheme       string
	dominantSubcategory string
}

// batchResolveMembership mirrors work_graph.py:535-631's
// _batch_resolve_membership: ONE query for the dominant theme/subcategory
// of every (node_type, node_id) endpoint touched by rows, scoped to the
// latest COMPLETE membership run (CHAOS-2433 protocol -- see workgraph.go's
// run-scope constants). Returns ({}, nil) when work_unit_membership does
// not exist yet (rolling deploy / pre-migration, same narrow code-60 check
// as isMissingMembershipTableError) -- every OTHER error propagates so real
// failures surface loudly instead of a silent empty annotation.
func batchResolveMembership(ctx context.Context, client QueryClient, orgID string, rows []edgeEndpoint, scope *filterScope) (map[membershipKey]membershipEntry, error) {
	endpointSeen := map[membershipKey]struct{}{}
	typeSeen := map[string]struct{}{}
	idSeen := map[string]struct{}{}
	var nodeTypes, nodeIDs []string
	add := func(nodeType, nodeID string) {
		nodeID = strings.TrimSpace(nodeID)
		nodeType = strings.TrimSpace(nodeType)
		if nodeID == "" || nodeType == "" {
			return
		}
		k := membershipKey{nodeType, nodeID}
		if _, ok := endpointSeen[k]; ok {
			return
		}
		endpointSeen[k] = struct{}{}
		if _, ok := typeSeen[nodeType]; !ok {
			typeSeen[nodeType] = struct{}{}
			nodeTypes = append(nodeTypes, nodeType)
		}
		if _, ok := idSeen[nodeID]; !ok {
			idSeen[nodeID] = struct{}{}
			nodeIDs = append(nodeIDs, nodeID)
		}
	}
	for _, row := range rows {
		add(row.sourceType, row.sourceID)
		add(row.targetType, row.targetID)
	}
	if len(endpointSeen) == 0 {
		return map[membershipKey]membershipEntry{}, nil
	}
	sort.Strings(nodeTypes)
	sort.Strings(nodeIDs)

	bindings := []clickhouse.Binding{
		{Name: "org_id", Value: orgID},
		{Name: "node_types", Value: nodeTypes},
		{Name: "node_ids", Value: nodeIDs},
	}
	bindings = addMembershipScopeBindings(bindings, scope)

	// INNER JOIN (%s) AS latest_run, NOT a leading `WITH latest_run AS
	// (...)` CTE: this codebase's clickhouse.Client rejects any statement
	// whose FIRST TOKEN is not literally "SELECT"
	// (dev-health-go/clickhouse/client.go's validateReadOnlyStatement) as
	// an unsafe statement -- a `WITH ...` prefix trips that check even
	// though the statement is pure SELECT. Found LIVE by this port's own
	// dual-run proof, not by inspection: a fake-based unit test cannot
	// catch a client-side statement-shape guard, only a real client call
	// can. Same inlined-subquery shape scope.go's
	// themeMembershipExistsClause/dependencyThemeMembershipExistsClause
	// already use for the identical latest-run subquery, for the same
	// reason.
	query := fmt.Sprintf(`
            SELECT
                m.node_type AS node_type,
                m.node_id AS node_id,
                m.category_kind AS category_kind,
                m.category AS category
            FROM work_unit_membership AS m
            INNER JOIN (%s) AS latest_run ON 1 = 1
            %s
            WHERE m.org_id = {org_id:String}
              AND m.node_type IN {node_types:Array(String)}
              AND m.node_id IN {node_ids:Array(String)}
              AND m.is_dominant = 1
              AND latest_run.latest_run_id != ''
              AND (%s)
        `, membershipRunSubquery(scope), legacyNodeMaxJoin, runScopePredicate)

	rowsResult, err := client.Query(ctx, query, bindings)
	if err != nil {
		if isMissingMembershipTableError(err) {
			return map[membershipKey]membershipEntry{}, nil
		}
		return nil, fmt.Errorf("workgraph: batch resolve membership: %w", err)
	}
	defer rowsResult.Close()

	result := map[membershipKey]membershipEntry{}
	for rowsResult.Next() {
		var nodeType, nodeID, kind, category string
		if scanErr := rowsResult.Scan(&nodeType, &nodeID, &kind, &category); scanErr != nil {
			return nil, fmt.Errorf("workgraph: batch resolve membership scan: %w", scanErr)
		}
		if nodeType == "" || nodeID == "" {
			continue
		}
		k := membershipKey{nodeType, nodeID}
		entry := result[k]
		switch kind {
		case "theme":
			entry.dominantTheme = category
		case "subcategory":
			entry.dominantSubcategory = category
		}
		result[k] = entry
	}
	if err := rowsResult.Err(); err != nil {
		return nil, fmt.Errorf("workgraph: batch resolve membership rows: %w", err)
	}
	return result, nil
}

// nodeTypeRank mirrors work_graph.py:446's _type_rank -- issue before pr
// before every other endpoint type, used only to pick which endpoint's
// membership annotation an edge reports when both endpoints have one.
func nodeTypeRank(nodeType string) int {
	switch strings.ToLower(nodeType) {
	case "issue":
		return 0
	case "pr":
		return 1
	default:
		return 2
	}
}

// rowToEdge mirrors work_graph.py:419-471's _row_to_edge. membership may be
// nil (unfiltered path never calls batchResolveMembership -- no, actually
// Python always calls it; see edges.go's ResolveEdges, which always builds
// this map, possibly empty).
func rowToEdge(row edgeRow, resolved map[string]string, membership map[membershipKey]membershipEntry) (model.WorkGraphEdgeResult, error) {
	sourceID := row.sourceID
	targetID := row.targetID
	sourceTypeRaw := row.sourceType
	if sourceTypeRaw == "" {
		sourceTypeRaw = "issue"
	}
	targetTypeRaw := row.targetType
	if targetTypeRaw == "" {
		targetTypeRaw = "issue"
	}

	var theme, subcategory *string
	if membership != nil {
		type endpoint struct {
			nodeType, nodeID string
		}
		endpoints := []endpoint{
			{sourceTypeRaw, sourceID},
			{targetTypeRaw, targetID},
		}
		sort.SliceStable(endpoints, func(i, j int) bool {
			return nodeTypeRank(endpoints[i].nodeType) < nodeTypeRank(endpoints[j].nodeType)
		})
		for _, ep := range endpoints {
			if m, ok := membership[membershipKey{strings.ToLower(ep.nodeType), ep.nodeID}]; ok {
				if m.dominantTheme != "" {
					t := m.dominantTheme
					theme = &t
				}
				if m.dominantSubcategory != "" {
					s := m.dominantSubcategory
					subcategory = &s
				}
				break
			}
		}
	}

	edgeTypeRaw := row.edgeType
	if edgeTypeRaw == "" {
		edgeTypeRaw = "relates"
	}
	provenanceRaw := row.provenance
	if provenanceRaw == "" {
		provenanceRaw = "heuristic"
	}

	sourceType, err := mapNodeType(sourceTypeRaw)
	if err != nil {
		return model.WorkGraphEdgeResult{}, fmt.Errorf("workgraph: row %q source_type: %w", row.edgeID, err)
	}
	targetType, err := mapNodeType(targetTypeRaw)
	if err != nil {
		return model.WorkGraphEdgeResult{}, fmt.Errorf("workgraph: row %q target_type: %w", row.edgeID, err)
	}
	edgeType, err := mapEdgeType(edgeTypeRaw)
	if err != nil {
		return model.WorkGraphEdgeResult{}, fmt.Errorf("workgraph: row %q edge_type: %w", row.edgeID, err)
	}
	provenance, err := mapProvenance(provenanceRaw)
	if err != nil {
		return model.WorkGraphEdgeResult{}, fmt.Errorf("workgraph: row %q provenance: %w", row.edgeID, err)
	}

	result := model.WorkGraphEdgeResult{
		EdgeID:            row.edgeID,
		SourceType:        sourceType,
		SourceID:          sourceID,
		SourceDisplayName: displayNameFor(sourceID, resolved),
		TargetType:        targetType,
		TargetID:          targetID,
		TargetDisplayName: displayNameFor(targetID, resolved),
		EdgeType:          edgeType,
		Provenance:        provenance,
		Confidence:        row.confidence,
		Evidence:          row.evidence,
		Theme:             theme,
		Subcategory:       subcategory,
	}
	if row.repoID != "" {
		id := row.repoID
		result.RepoID = &id
	}
	if row.provider != "" {
		p := row.provider
		result.Provider = &p
	}
	return result, nil
}
