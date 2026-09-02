// Package chquery is the ClickHouse read side of the native Go
// investment.materialize port (CHAOS-4441 PR2).
//
// It is the Go port of src/dev_health_ops/work_graph/investment/queries.py:
// the work_graph_edges fetch that feeds component grouping, plus the entity
// fetchers the materializer needs to compute time bounds, effort and text
// bundles.
//
// # WHY THE EDGE FETCH IS NOT AN ORDINARY QUERY
//
// fetch_work_graph_edges is the single choke point three consumers share --
// the LLM materializer, the dispatch enumerator, and the no-LLM membership
// backfill (queries.py:25-52). Two of its properties are load-bearing and are
// preserved verbatim rather than "improved":
//
//   - DEDUP BEFORE FILTER. work_graph_edges is a ReplacingMergeTree keyed on
//     (org_id, source_type, source_id, edge_type, target_type, target_id) and a
//     raw read can return stale pre-merge versions. Provenance is NOT part of
//     that identity -- a heuristic link can later be re-emitted as native -- so
//     rows are argMax-collapsed by last_synced per identity and the heuristic
//     filter is applied to the LATEST provenance via HAVING. Filtering before
//     collapsing would let a stale row resurrect an excluded edge, or keep an
//     edge excluded that is now native, depending on merge timing.
//   - DETERMINISTIC ORDER. Partitioned materialization dispatches numeric
//     component_indexes and each chunk worker re-fetches and rebuilds the
//     component list. Component discovery order follows edge row order, so the
//     query ORDERs BY the full identity key. Without it ClickHouse physical row
//     order could differ between dispatcher and worker and index N would name a
//     different component -- silently skipped or double-categorized units.
//
// The heuristic exclusion itself is why the cap in units.BuildComponents is
// survivable at all: heuristic edges percolate thousands of unrelated
// issues/PRs/commits into one component (CHAOS-2775).
package chquery

import (
	"context"
	"sort"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// conn is the narrow ClickHouse capability this package needs. Declared locally
// rather than taking driver.Conn so tests can substitute a fake without a live
// server; driver.Conn satisfies it directly. Same shape as
// internal/jobs/metrics/daily/repouser's.
type conn interface {
	Query(ctx context.Context, query string, args ...any) (driver.Rows, error)
}

// Reader reads the rows the native materializer needs.
type Reader struct {
	conn conn
}

// NewReader returns a Reader over the given ClickHouse connection.
func NewReader(connection conn) (*Reader, error) {
	if connection == nil {
		return nil, ErrUnavailable
	}
	return &Reader{conn: connection}, nil
}

// dateTimeArgument renders a time for a {x:DateTime} named parameter.
//
// NEVER bind a time.Time to a DateTime parameter: clickhouse-go renders it as a
// `toDateTime(...)` EXPRESSION, which the server's literal parser rejects. The
// same trap applies to DateTime64. This is the repouser helper, repeated here
// rather than exported from there because a shared helper across two job
// packages is a coupling neither wants.
func dateTimeArgument(value time.Time) string {
	return value.UTC().Format("2006-01-02 15:04:05")
}

// dedupeStrings preserves first-sighting order while removing duplicates,
// matching Python's `list(dict.fromkeys(ids))`.
//
// Order matters more than it looks: these id lists are bound into `IN` clauses
// whose result feeds maps the materializer reads, and keeping the Python
// ordering keeps any future row-order-sensitive comparison honest.
func dedupeStrings(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	result := make([]string, 0, len(values))
	for _, value := range values {
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	return result
}

// sortedRepoKeys returns the map's keys in a deterministic order.
//
// Python iterates its repo_numbers/repo_commits dicts in insertion order; a Go
// map has none. The per-repo queries are independent and their results are
// merged, so ORDER does not change the returned SET — but it does change the
// order of the returned SLICE, and an unstable slice order would make any
// row-order-sensitive comparison downstream flap between runs for no reason.
// Sorting costs nothing here and removes a source of non-reproducibility from
// the parity harness rather than leaving one to be discovered later.
func sortedRepoKeys[V any](byRepo map[string]V) []string {
	keys := make([]string, 0, len(byRepo))
	for key := range byRepo {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
