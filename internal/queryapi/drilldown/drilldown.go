// Package drilldown is the Go port of the FastAPI "drilldown" REST family
// (api/main.py's GET+POST /api/v1/drilldown/* pair routes, backed by
// api/queries/drilldown.py). It starts with prs.go (fetch_pull_requests,
// /api/v1/drilldown/prs); issues.go (fetch_issues,
// /api/v1/drilldown/issues) is a sibling addition, not yet ported --
// hence the family name rather than a prs-only package name.
//
// CLIENT CONVENTION: a package-local QueryClient interface over
// github.com/full-chaos/dev-health-go/clickhouse's Binding/RowScanner,
// matching quadrant, featureflags and workgraph's own convention in this
// binary (repeat, don't couple, for an interface this narrow) rather than
// importing analytics.QueryClient or investmentexplain's Reader --
// drilldown has no LLM/org-settings dependency and importing either would
// be an unrelated-concern coupling for a two-method interface.
package drilldown

import (
	"context"
	"errors"
	"fmt"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
)

// QueryClient is the narrow ClickHouse read capability this package needs.
type QueryClient interface {
	Query(ctx context.Context, statement string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error)
}

// ErrUnavailable is returned when a Reader is constructed without a client.
var ErrUnavailable = errors.New("drilldown: clickhouse client unavailable")

// queryTimeoutSecs matches investmentexplain/quadrant's own copy of this
// constant: the trailing SETTINGS max_execution_time clause must be a
// LITERAL integer, never a bound {name:UInt64} parameter --
// ClickHouse 26.6.1.1193 (the digest-pinned image
// internal/testsupport/containers.StartClickHouse runs for every Go
// integration test in this repo) fails to PARSE a bound parameter inside a
// SETTINGS clause (Code: 62), even though 26.7.1.1315 (prod)
// parses it fine.
const queryTimeoutSecs = 30

// query_plan_optimize_lazy_materialization = 0 is REQUIRED, not cosmetic,
// for both this package's FINAL reads (prs.go, issues.go): each combines
// a LEFT/INNER JOIN whose preserved side reads FINAL, a WHERE predicate
// on that preserved table, and an ORDER BY <preserved-table column> ...
// LIMIT n whose SELECT list carries a preserved-table column referenced
// ONLY in the select list. That is the exact trigger ClickHouse's own
// query-plan-optimizer team confirmed for a regression shipped in 26.5
// (query_plan_top_k_through_join, ClickHouse#104268) and still open on
// 26.6: the top-K-through-join optimization pushes Sort+Limit onto the
// FINAL-read side, lazy materialization then splits the FilterStep that
// sits on that same island, and the FilterStep's rebuilt header loses the
// WHERE-only column -- ClickHouse/ClickHouse#109211 ("Fix
// NOT_FOUND_COLUMN_IN_BLOCK from topKThroughJoin + lazy materialization
// under FINAL"), which lists this exact setting as one of two sufficient
// workarounds (the other, query_plan_top_k_through_join = 0, works too;
// this one is chosen since it is also the setting EXPLAIN actions=1
// shows engaging here: a JoinLazyColumnsStep/LazilyReadFromMergeTree
// pair appears when it is left on and disappears with it off). Production
// runs ClickHouse 26.7.1.1315, and that engine is what produced the
// NOT_FOUND_COLUMN_IN_BLOCK failures this fix addresses, on this exact
// query shape. Reproduced live end-to-end against a pinned ClickHouse
// build carrying the same defect (26.6.1.1193), with this package's own
// query text, schema and bindings: identical NOT_FOUND_COLUMN_IN_BLOCK
// with the setting on, byte-identical rows with it off. A freshly pulled
// ClickHouse image on a newer patch than 26.7.1.1315 can already carry
// the upstream fix and fail to reproduce this locally -- that is a
// property of which patch a given pull resolves to, not evidence this
// setting is unneeded on the version actually deployed. The guard
// against a regression here is the committed test asserting the
// setting's presence on each composed query text, not a container image.
// Disabling the setting only removes a deferred-read optimization an
// affected engine build cannot use safely with FINAL -- it never changes
// which rows or column values a query returns.
func settingsMaxExecutionTime() string {
	return fmt.Sprintf("SETTINGS max_execution_time = %d, query_plan_optimize_lazy_materialization = 0", queryTimeoutSecs)
}

// Reader reads the ClickHouse data the drilldown family needs.
type Reader struct {
	client QueryClient
}

// NewReader returns a Reader over the given query client.
func NewReader(client QueryClient) (*Reader, error) {
	if client == nil {
		return nil, ErrUnavailable
	}
	return &Reader{client: client}, nil
}
