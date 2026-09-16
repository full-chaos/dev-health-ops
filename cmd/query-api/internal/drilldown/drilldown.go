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
// SETTINGS clause (Code: 62), even though 26.7.5.10 (dev-stack/prod)
// parses it fine.
const queryTimeoutSecs = 30

func settingsMaxExecutionTime() string {
	return fmt.Sprintf("SETTINGS max_execution_time = %d", queryTimeoutSecs)
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
