// Package people is the Go port of the FastAPI "people" REST family's
// search endpoint (api/main.py's GET /api/v1/people,
// people_search, main.py:1048-1065), backed by
// api/services/people.py's search_people_response and
// api/services/people_identity.py / api/utils/identity_aliases.py's
// identity-alias helpers.
//
// Named for the family, not just search: api/services/people.py also
// backs GET /api/v1/people/{person_id}/summary, /metric and the person
// drilldown/prs, drilldown/issues routes -- later CHAOS tickets port
// those into sibling files of this same package rather than one-off
// per-route packages.
//
// CLIENT CONVENTION: a package-local QueryClient interface over
// github.com/full-chaos/dev-health-go/clickhouse's Binding/RowScanner,
// matching drilldown/quadrant/featureflags/workgraph's own convention in
// this binary (repeat, don't couple, for an interface this narrow).
package people

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
var ErrUnavailable = errors.New("people: clickhouse client unavailable")

// queryTimeoutSecs matches drilldown/investmentexplain/quadrant's own copy
// of this constant: the trailing SETTINGS max_execution_time clause must
// be a LITERAL integer, never a bound {name:UInt64} parameter --
// ClickHouse 26.6.1.1193 (the digest-pinned image
// internal/testsupport/containers.StartClickHouse runs for every Go
// integration test in this repo) fails to PARSE a bound parameter inside a
// SETTINGS clause (Code: 62), even though 26.7.5.10 (dev-stack/prod)
// parses it fine.
const queryTimeoutSecs = 30

func settingsMaxExecutionTime() string {
	return fmt.Sprintf("SETTINGS max_execution_time = %d", queryTimeoutSecs)
}

// Reader reads the ClickHouse data the people family needs.
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
