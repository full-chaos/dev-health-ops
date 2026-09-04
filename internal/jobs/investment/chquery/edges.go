package chquery

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/units"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
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

	outerFilterSQL := ""
	if !opts.IncludeHeuristic {
		// References the OUTER SELECT's `provenance` alias (extracted from
		// the tupled winner below), not a raw column or another aggregate --
		// the outer query no longer aggregates at all (the tupled argMax
		// moved the GROUP BY into the subquery, mirroring
		// cmd/query-api/internal/workgraph/edges.go's own CHAOS-4985 fix),
		// so this is a plain WHERE, not HAVING. Re-aggregating the raw
		// column here would still raise ILLEGAL_AGGREGATION (184) for the
		// same reason the old HAVING-on-alias form avoided it -- same
		// ClickHouse trap documented on LATEST_WORK_UNIT_INVESTMENTS_CTE,
		// just enforced one query level further out now.
		outerFilterSQL = "WHERE provenance != {heuristic_provenance:String}"
		arguments = append(arguments, clickhouse.Named("heuristic_provenance", heuristicProvenance))
	}

	// One tupled argMax(tuple(repo_id, provider, provenance, confidence,
	// evidence), last_synced), computed ONCE in a subquery, parts extracted
	// in the outer SELECT (CHAOS-4985 follow-up, codex round 2 on #2186, P3
	// -- the same hybrid-row-on-a-last_synced-tie defect the sibling
	// cmd/query-api/internal/workgraph/edges.go reader already had, and was
	// already fixed for: two unmerged rows sharing the same last_synced can
	// tie under argMax, which ClickHouse documents as implementation-defined
	// for the tie-break; five INDEPENDENT argMax calls could each break that
	// tie differently and assemble a row that never existed in any single
	// physical insert. Latent here (FetchWorkGraphEdges has no non-test call
	// site at this tip), hence P3 not P2 -- fixed anyway, matching the
	// established remediation rather than leaving a known-defective query on
	// the books. `org_id` is carried through as a passthrough column purely
	// for the ORDER BY (this function deliberately has no org filter by
	// default, see the doc comment above -- a multi-tenant read needs org_id
	// in the sort key), scanned and discarded below since EdgeRow never
	// carried it.
	query := fmt.Sprintf(`
        SELECT
            org_id,
            edge_id,
            source_type,
            source_id,
            target_type,
            target_id,
            edge_type,
            toString(winner.1) AS repo_id,
            winner.2 AS provider,
            winner.3 AS provenance,
            winner.4 AS confidence,
            winner.5 AS evidence
        FROM (
            SELECT
                org_id,
                source_type,
                source_id,
                target_type,
                target_id,
                edge_type,
                any(edge_id) AS edge_id,
                argMax(tuple(repo_id, provider, provenance, confidence, evidence), last_synced) AS winner
            FROM work_graph_edges
            %s
            GROUP BY org_id, source_type, source_id, edge_type, target_type, target_id
        )
        %s
        ORDER BY org_id, source_type, source_id, edge_type, target_type, target_id
    `, whereSQL, outerFilterSQL)

	rows, err := reader.conn.Query(ctx, query, arguments...)
	if err != nil {
		return nil, fmt.Errorf("query work_graph_edges: %w", err)
	}
	defer func() { _ = rows.Close() }()

	edges := make([]EdgeRow, 0)
	for rows.Next() {
		var (
			// orgID is scanned and discarded -- carried through purely for
			// the query's ORDER BY (see the query comment above); EdgeRow
			// never held it before this change either.
			orgID      string
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
			&orgID, &edgeID, &sourceType, &sourceID, &targetType, &targetID, &edgeType,
			&repoID, &provider, &provenance, &confidence, &evidence,
		); err != nil {
			return nil, fmt.Errorf("scan work_graph_edges row: %w", err)
		}

		providerValue := ""
		if provider != nil {
			providerValue = *provider
		}

		// Every String column goes through the Python driver's decode policy
		// (see pythonparity.DecodeClickHouseString). The four node-key columns
		// are the ones that matter most: source_type, source_id, target_type
		// and target_id are hashed into work_unit_id, which addresses rows in
		// BOTH work_unit_investments and work_unit_membership -- written by two
		// different jobs. A byte sequence the two planes spell differently
		// would mint a different work_unit_id in each, which is the silent
		// cross-table divergence this whole port exists to prevent.
		edges = append(edges, EdgeRow{
			Edge: units.Edge{
				EdgeID:     pythonparity.DecodeClickHouseStringValue(edgeID),
				SourceType: pythonparity.DecodeClickHouseStringValue(sourceType),
				SourceID:   pythonparity.DecodeClickHouseStringValue(sourceID),
				TargetType: pythonparity.DecodeClickHouseStringValue(targetType),
				TargetID:   pythonparity.DecodeClickHouseStringValue(targetID),
				Confidence: float64(confidence),
			},
			EdgeType:   pythonparity.DecodeClickHouseStringValue(edgeType),
			RepoID:     pythonparity.DecodeClickHouseStringValue(repoID),
			Provider:   pythonparity.DecodeClickHouseStringValue(providerValue),
			Provenance: pythonparity.DecodeClickHouseStringValue(provenance),
			Evidence:   pythonparity.DecodeClickHouseStringValue(evidence),
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
