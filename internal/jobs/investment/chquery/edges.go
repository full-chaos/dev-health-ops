package chquery

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/units"
)

// ErrUnavailable is returned when the reader is constructed without a
// connection.
var ErrUnavailable = errors.New("chquery: clickhouse connection unavailable")

// heuristicProvenance is the provenance value excluded from work-unit grouping.
// Heuristic edges are rule-inferred (same repo + time window) and percolate
// thousands of unrelated nodes into one component (CHAOS-2775). They REMAIN in
// work_graph_edges for display and every other consumer -- only unit grouping
// excludes them.
const heuristicProvenance = "heuristic"

// EdgeQueryOptions scopes a work-graph edge fetch.
type EdgeQueryOptions struct {
	OrganizationID string
	// RepoIDs, when non-empty, restricts to those repositories.
	RepoIDs []string
	// IncludeHeuristic is the INVERSE of Python's `exclude_heuristic: bool = True`
	// (queries.py:18-23), and the inversion is the point.
	//
	// An earlier revision of this file spelled it `ExcludeHeuristic` and claimed
	// in this very comment that an explicit field stopped a caller getting the
	// dangerous default by forgetting a parameter. That was wrong, and codex
	// round 1 on CHAOS-4441 PR2 constructed it: Go's zero value for a bool is
	// FALSE, so `EdgeQueryOptions{OrganizationID: org}` -- the obvious way to
	// write it -- silently DISABLED the exclusion, while Python's omitted
	// argument ENABLES it. The safe case has to be the zero value, not a field
	// the caller must remember to set.
	//
	// Heuristic edges are rule-inferred (same repo + time window) and percolate
	// thousands of unrelated issues/PRs/commits into one component (CHAOS-2775),
	// so letting them into unit grouping is the pathology the cap exists to
	// contain. Set this true ONLY for a caller that wants the full edge set for
	// display and is not grouping work units.
	IncludeHeuristic bool
}

// DefaultEdgeQueryOptions returns options matching Python's defaults for a
// work-unit grouping read. Retained for callers that prefer it, but it is no
// longer load-bearing: the zero value is now the safe one.
func DefaultEdgeQueryOptions(organizationID string) EdgeQueryOptions {
	return EdgeQueryOptions{OrganizationID: organizationID}
}

// EdgeRow is one deduplicated work_graph_edges row, carrying both the fields
// units.BuildComponents groups on and the fields the materializer needs later
// (repo_id for repo attribution, provenance/evidence for structural evidence).
type EdgeRow struct {
	Edge       units.Edge
	EdgeType   string
	RepoID     string
	Provider   string
	Provenance string
	Evidence   string
}

// FetchWorkGraphEdges ports queries.py:18-92 fetch_work_graph_edges.
//
// The returned slice is in the query's ORDER BY sequence and MUST NOT be
// re-sorted before units.BuildComponents: component discovery walks edges in
// input order, and component order is addressed by numeric index elsewhere.
//
// CHAOS-4804: THIS FUNCTION HAS NO ORG GUARD, AND THAT IS PYTHON'S SHAPE.
// An empty OrganizationID omits the org predicate entirely (queries.py:59-62,
// `if org_id:`), and with no RepoIDs the WHERE clause is empty — the query then
// reads every tenant's edges. The GROUP BY carries org_id so the ROWS stay
// per-org, which makes it look contained; it is not. units.NodeKey is
// (type, id) with NO org, so a provider-scoped id present in two tenants
// becomes ONE node and the two graphs fuse into a single component, minting a
// work_unit_id over a node set drawn from multiple organisations.
//
// Reproduced deliberately: a one-plane fix would be an unflagged divergence
// between two planes that must group identically. CHAOS-4804 carries the fix
// for both. Callers that can supply an org MUST supply one — passing "" is not
// a way to say "everything", it is how this becomes reachable.
func (reader *Reader) FetchWorkGraphEdges(ctx context.Context, opts EdgeQueryOptions) ([]EdgeRow, error) {
	if reader == nil || reader.conn == nil {
		return nil, ErrUnavailable
	}

	conditions := make([]string, 0, 2)
	arguments := make([]any, 0, 3)
	if len(opts.RepoIDs) > 0 {
		conditions = append(conditions, "repo_id IN {repo_ids:Array(String)}")
		arguments = append(arguments, clickhouse.Named("repo_ids", dedupeStrings(opts.RepoIDs)))
	}
	if opts.OrganizationID != "" {
		conditions = append(conditions, "org_id = {org_id:String}")
		arguments = append(arguments, clickhouse.Named("org_id", opts.OrganizationID))
	}
	whereSQL := ""
	if len(conditions) > 0 {
		whereSQL = "WHERE " + strings.Join(conditions, " AND ")
	}

	havingSQL := ""
	if !opts.IncludeHeuristic {
		// HAVING must reference the SELECT ALIAS, not repeat
		// argMax(provenance, ...): the alias shadows the raw column and
		// re-aggregating it raises ILLEGAL_AGGREGATION (184). Same ClickHouse
		// trap documented on LATEST_WORK_UNIT_INVESTMENTS_CTE.
		havingSQL = "HAVING provenance != {heuristic_provenance:String}"
		arguments = append(arguments, clickhouse.Named("heuristic_provenance", heuristicProvenance))
	}

	query := fmt.Sprintf(`
        SELECT
            any(edge_id) AS edge_id,
            source_type,
            source_id,
            target_type,
            target_id,
            edge_type,
            toString(argMax(repo_id, last_synced)) AS repo_id,
            argMax(provider, last_synced) AS provider,
            argMax(provenance, last_synced) AS provenance,
            argMax(confidence, last_synced) AS confidence,
            argMax(evidence, last_synced) AS evidence
        FROM work_graph_edges
        %s
        GROUP BY org_id, source_type, source_id, edge_type, target_type, target_id
        %s
        ORDER BY org_id, source_type, source_id, edge_type, target_type, target_id
    `, whereSQL, havingSQL)

	rows, err := reader.conn.Query(ctx, query, arguments...)
	if err != nil {
		return nil, fmt.Errorf("query work_graph_edges: %w", err)
	}
	defer func() { _ = rows.Close() }()

	edges := make([]EdgeRow, 0)
	for rows.Next() {
		var (
			edgeID     string
			sourceType string
			sourceID   string
			targetType string
			targetID   string
			edgeType   string
			repoID     string
			provider   *string
			provenance string
			// Float32 is the column's real type. Scanning into float32 and
			// widening is what Python's driver effectively does; scanning into
			// float64 directly would ask the driver to convert and is not the
			// same value for every input. The split partitions on
			// `confidence >= max` and orders on (confidence, edge_id), so this
			// is not a cosmetic choice.
			confidence float32
			evidence   string
		)
		if err := rows.Scan(
			&edgeID, &sourceType, &sourceID, &targetType, &targetID, &edgeType,
			&repoID, &provider, &provenance, &confidence, &evidence,
		); err != nil {
			return nil, fmt.Errorf("scan work_graph_edges row: %w", err)
		}

		providerValue := ""
		if provider != nil {
			providerValue = *provider
		}

		edges = append(edges, EdgeRow{
			Edge: units.Edge{
				EdgeID:     edgeID,
				SourceType: sourceType,
				SourceID:   sourceID,
				TargetType: targetType,
				TargetID:   targetID,
				Confidence: float64(confidence),
			},
			EdgeType:   edgeType,
			RepoID:     repoID,
			Provider:   providerValue,
			Provenance: provenance,
			Evidence:   evidence,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate work_graph_edges rows: %w", err)
	}
	return edges, nil
}

// ComponentEdges projects the rows into the shape units.BuildComponents takes,
// preserving order.
func ComponentEdges(rows []EdgeRow) []units.Edge {
	edges := make([]units.Edge, len(rows))
	for index, row := range rows {
		edges[index] = row.Edge
	}
	return edges
}
