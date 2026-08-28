// Package reviewedges is the Go port of
// dev_health_ops.api.graphql.resolvers.review_edges.resolve_review_edges
// (ops/src/dev_health_ops/api/graphql/resolvers/review_edges.py),
// CHAOS-4352 plan §6 Wave 2's canary operation, ported after CHAOS-4368
// Part A (#1980, commit 8d34d8b6e) made the Python resolver's row order
// deterministic -- a prerequisite for the Go/Python parity comparator,
// which cannot validly compare two non-deterministic outputs.
//
// Ported deliberately verbatim: same inner-subquery dedup
// (`argMax(reviews_count, computed_at)` per `(repo_id, reviewer, author,
// day)`, since `review_edges_daily` is append-only plain MergeTree, not
// ReplacingMergeTree -- a recompute/backfill can duplicate a key), same
// `ORDER BY reviews_count DESC, repo_id, reviewer, author, day` (the
// Part-A deterministic tie-break -- the resolver's own GROUP BY key,
// already a total order over the deduplicated row set), same optional
// repo_ids filter (resolved through the org-scoped `repos` catalog by
// slug OR UUID string, exactly as `_fetch_review_edges` does), and the
// same limit clamp (1..2000, `MAX_REVIEW_EDGES_ROWS`).
//
// Side effects: none to replicate. Verified by reading
// `resolve_review_edges`/`_fetch_review_edges` top to bottom: one
// read-only ClickHouse query via `query_dicts` and a dataclass
// construction -- no telemetry/audit hook call inside it or anything it
// calls (same finding class as `featureflags`'s doc comment; unlike
// `home`/investment analytics, which plan §5 calls out by name).
//
// Missing-table behavior deliberately differs from `featureflags`: unlike
// `resolve_feature_flags`, `resolve_review_edges` has NO try/except around
// its ClickHouse call and `ReviewEdgesResult` has no `degradedReason`
// field at all -- a missing `review_edges_daily` table is a real error on
// the Python side, not a degraded empty result. This port does not invent
// a degraded path Python doesn't have; a ClickHouse error propagates as a
// Go error (and, via schema.resolvers.go, a GraphQL error), matching
// Python's actual behavior exactly.
package reviewedges

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graph/model"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graphqldate"
)

// MaxRows mirrors Python's MAX_REVIEW_EDGES_ROWS -- a hard cap on returned
// rows to protect against pathological date ranges.
const MaxRows = 2000

// QueryClient is the read-only ClickHouse query boundary this package
// needs -- same shape as featureflags.QueryClient (declared independently
// per that package's own doc comment: this operation's query shape does
// not fit dev-health-go/readers' id-keyed convention, and adding a reader
// there is a separate, version-bump-owning change owned by the worker
// orchestrator, not this lane). *clickhouse.Client satisfies this
// interface directly.
type QueryClient interface {
	Query(ctx context.Context, statement string, bindings []clickhouse.Binding) (clickhouse.RowScanner, error)
}

// clampLimit bounds a caller-supplied limit to a safe 1..MaxRows range,
// mirroring resolve_review_edges's `effective_limit = max(1, min(raw_limit,
// MAX_REVIEW_EDGES_ROWS))`.
func clampLimit(limit int) int {
	if limit < 1 {
		return 1
	}
	if limit > MaxRows {
		return MaxRows
	}
	return limit
}

// Resolve ports resolve_review_edges/_fetch_review_edges. orgID must
// already be the AUTHORIZED org (the caller's verified envelope claim,
// not necessarily the client-supplied `input.orgId` GraphQL argument --
// see schema.resolvers.go's ReviewEdges for why: Python's resolver
// silently prefers the authorized org over a mismatched GraphQL argument
// rather than erroring, and this port reproduces that same
// "authorized org always wins" behavior by construction, taking only one
// org parameter). limit is clamped internally, same as Python -- callers
// must not pre-clamp and must not trust the GraphQL schema's default
// alone (a client can send any value).
func Resolve(ctx context.Context, client QueryClient, orgID string, sinceDate, untilDate graphqldate.Date, repoIDs []string, limit int) (*model.ReviewEdgesResult, error) {
	if client == nil {
		return nil, errors.New("reviewedges: clickhouse client is required")
	}

	bindings := []clickhouse.Binding{
		{Name: "org_id", Value: orgID},
		{Name: "since_date", Value: sinceDate.String()},
		{Name: "until_date", Value: untilDate.String()},
		{Name: "limit", Value: clampLimit(limit)},
	}

	repoFilter := ""
	if len(repoIDs) > 0 {
		repoFilter = `
              AND repo_id IN (
                  SELECT id FROM repos
                  WHERE org_id = {org_id:String}
                    AND (repo IN {repo_ids:Array(String)} OR toString(id) IN {repo_ids:Array(String)})
              )`
		bindings = append(bindings, clickhouse.Binding{Name: "repo_ids", Value: repoIDs})
	}

	query := `
        SELECT
            reviewer,
            author,
            reviews_count,
            day,
            toString(repo_id) AS repo_id
        FROM (
            SELECT
                repo_id,
                reviewer,
                author,
                day,
                argMax(reviews_count, computed_at) AS reviews_count
            FROM review_edges_daily
            WHERE org_id = {org_id:String}
              AND day >= {since_date:Date}
              AND day <= {until_date:Date}` + repoFilter + `
            GROUP BY repo_id, reviewer, author, day
        )
        ORDER BY reviews_count DESC, repo_id, reviewer, author, day
        LIMIT {limit:UInt64}`

	rows, err := client.Query(ctx, query, bindings)
	if err != nil {
		return nil, fmt.Errorf("reviewedges: query: %w", err)
	}
	defer rows.Close()

	// Non-nil even with zero rows: the schema declares
	// edges: [ReviewEdgeRow!]! (non-null list) -- same "initialize
	// explicitly" convention featureflags.Resolve documents.
	edges := []model.ReviewEdgeRow{}
	for rows.Next() {
		var reviewer, author, repoID string
		// ClickHouse's `reviews_count` column is UInt32 (migration 004);
		// the native Go driver rejects scanning a UInt32 column into a
		// signed destination outright, the same class of mismatch
		// dev-health-go/readers' PullRequestStateRow.Number doc comment
		// documents. Scan into uint32, convert to int (the model field's
		// type) only after the value is safely in Go.
		var reviewsCount uint32
		var day time.Time
		if scanErr := rows.Scan(&reviewer, &author, &reviewsCount, &day, &repoID); scanErr != nil {
			return nil, fmt.Errorf("reviewedges: scan: %w", scanErr)
		}
		edge := model.ReviewEdgeRow{
			Reviewer:     reviewer,
			Author:       author,
			ReviewsCount: int(reviewsCount),
			Day:          graphqldate.New(day),
		}
		// Mirrors Python's `repo_id=str(row["repo_id"]) if
		// row.get("repo_id") else None`: only a non-empty string counts.
		// repo_id is a NOT NULL UUID column, so toString(repo_id) never
		// actually produces "" in practice, but the check is kept for
		// exact behavioral parity rather than assumed.
		if repoID != "" {
			id := repoID
			edge.RepoID = &id
		}
		edges = append(edges, edge)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("reviewedges: rows: %w", err)
	}

	return &model.ReviewEdgesResult{
		Edges:      edges,
		TotalCount: len(edges),
	}, nil
}
