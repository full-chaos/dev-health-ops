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
// day)`: a recompute/backfill writes a newer copy of a key, and
// `review_edges_daily` drops the older copy only at a later merge), same
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

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graphqldate"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/teamscope"
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

// ExcludedIdentitiesFragment is the read-time exclusion of provider bots and self pairs
// (CHAOS-7787, RN-5, ruling 63), ANDed into the inner query's WHERE right after the window
// (before the repo and team filters, the GROUP BY, the LIMIT and the count), so the cut and
// totalCount only ever see the kept rows. Reviewer and author are GROUP BY keys, so filtering the
// rows is the same as filtering the deduplicated pairs.
//
//   - A bot is a login ending in "[bot]" (GitHub reserves the bracketed login for App actors; the
//     same test teamattribution uses). GitHub's `type: Bot` is NOT used: it is not stored.
//   - A self pair is reviewer = author, compared lower-cased and trimmed. Reviewer is the login or
//     display name and author is the email when there is one, so only the same-string case is
//     caught.
//
// The golden test (which pins this statement to the python-recorded text) strips exactly this
// fragment and nothing else.
const ExcludedIdentitiesFragment = `
              AND NOT endsWith(lowerUTF8(trimBoth(reviewer)), '[bot]')
              AND NOT endsWith(lowerUTF8(trimBoth(author)), '[bot]')
              AND lowerUTF8(trimBoth(reviewer)) != lowerUTF8(trimBoth(author))`

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
	return ResolveScoped(ctx, client, orgID, sinceDate, untilDate, Scope{RepoIDs: repoIDs}, limit)
}

// Scope narrows the edges to a set of repositories. Both fields are optional and, when both are
// given, BOTH apply (a pair must be on a listed repository AND on a repository a listed team
// owns): the two are filters of one list. Team = repository OWNERSHIP only
// (teamscope.RepoCondition, from team_repo_ownership), never person membership. This differs
// from the home route, which ORs explicit repositories with a team's.
type Scope struct {
	RepoIDs []string
	// TeamIDs are team ids; blank ids are dropped, and no ids left means no team scope.
	TeamIDs []string
	// AsOf is the one instant the ownership is read at; the zero value means now (UTC).
	AsOf time.Time
}

// ResolveScoped is Resolve with a Scope. With a zero Scope.TeamIDs it issues the same statement
// and bindings as Resolve always did (the frozen golden pins that).
func ResolveScoped(ctx context.Context, client QueryClient, orgID string, sinceDate, untilDate graphqldate.Date, scope Scope, limit int) (*model.ReviewEdgesResult, error) {
	if client == nil {
		return nil, errors.New("reviewedges: clickhouse client is required")
	}

	bindings := []clickhouse.Binding{
		{Name: "org_id", Value: orgID},
		{Name: "since_date", Value: sinceDate.String()},
		{Name: "until_date", Value: untilDate.String()},
		{Name: "limit", Value: clampLimit(limit)},
	}

	repoIDs := scope.RepoIDs
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

	// A team scope is ANDed in beside the repo filter: inside the inner query's WHERE, so it
	// narrows the rows BEFORE the dedup and the LIMIT (a small team is never starved by other
	// teams' bigger rows). repo_id is a UUID column and the condition compares ids as strings.
	asOf := scope.AsOf
	if asOf.IsZero() {
		asOf = time.Now().UTC()
	}
	if teamCondition, teamBindings := teamscope.RepoCondition(orgID, "toString(repo_id)", scope.TeamIDs, asOf); teamCondition != "" {
		repoFilter += "\n              AND " + teamCondition
		bindings = append(bindings, teamBindings...)
	}

	// The deduplicated (pair, day) rows under the org, window, repo and team filters. BOTH the row
	// query and the count query are built from this one text, so the count can never drift from
	// the rows it counts (a test compares them).
	inner := `SELECT
                repo_id,
                reviewer,
                author,
                day,
                argMax(reviews_count, computed_at) AS reviews_count
            FROM review_edges_daily
            WHERE org_id = {org_id:String}
              AND day >= {since_date:Date}
              AND day <= {until_date:Date}` + ExcludedIdentitiesFragment + repoFilter + `
            GROUP BY repo_id, reviewer, author, day`

	query := `
        SELECT
            reviewer,
            author,
            reviews_count,
            day,
            toString(repo_id) AS repo_id
        FROM (
            ` + inner + `
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
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("reviewedges: close rows: %w", err)
	}

	total, err := countRows(ctx, client, inner, bindings)
	if err != nil {
		return nil, err
	}
	// The count and the rows are two reads; never report fewer rows available than were returned.
	if total < len(edges) {
		total = len(edges)
	}

	// CHAOS-8485: the display names and the keys of the people of the returned rows.
	if err := attachPeople(ctx, client, orgID, edges); err != nil {
		return nil, err
	}

	return &model.ReviewEdgesResult{
		Edges:      edges,
		TotalCount: total,
		Truncated:  total > len(edges),
	}, nil
}

// countStatement is the count of the same deduplicated rows the row query cuts from. The
// LIMIT binding is not used here and is not sent.
func countStatement(inner string) string {
	return "SELECT count() FROM (\n            " + inner + "\n        )"
}

func countRows(ctx context.Context, client QueryClient, inner string, bindings []clickhouse.Binding) (int, error) {
	countBindings := make([]clickhouse.Binding, 0, len(bindings))
	for _, b := range bindings {
		if b.Name != "limit" {
			countBindings = append(countBindings, b)
		}
	}
	rows, err := client.Query(ctx, countStatement(inner), countBindings)
	if err != nil {
		return 0, fmt.Errorf("reviewedges: count query: %w", err)
	}
	defer rows.Close()
	var total uint64
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return 0, fmt.Errorf("reviewedges: count rows: %w", err)
		}
		return 0, errors.New("reviewedges: count query returned no row")
	}
	if err := rows.Scan(&total); err != nil {
		return 0, fmt.Errorf("reviewedges: count scan: %w", err)
	}
	return int(total), nil
}
