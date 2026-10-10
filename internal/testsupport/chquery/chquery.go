// Package chquery constructs a ClickHouse read-only query client with the
// SAME settings query-api's production route handlers run every read
// through, so a package's seeded integration test exercises the real
// statement guard and result-row/byte ceilings a fake or a raw driver
// connection cannot enforce, against a testcontainers instance.
package chquery

import (
	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/chclient"
)

// NewProductionClient builds a dev-health-go ClickHouse query client for
// dsn through the ONE constructor path of every query-API read client
// (package chclient: unrestricted MaxBytesToRead and the shared result-row
// bound, CHAOS-9126). Every statement this client runs is checked by the same
// validateReadOnlyStatement guard production's client applies
// (clickhouse/client.go), including the "first token must be SELECT" rule a
// leading WITH trips. A test that wants to prove a reader survives
// production's own limits builds its client through this constructor.
func NewProductionClient(dsn string) (*dhclickhouse.Client, error) {
	return dhclickhouse.NewClickHouseQueryClientWithOptions(chclient.Options(dsn))
}

// NewClientWithRowBound is NewProductionClient with an explicit result-row
// bound: for a test that proves a read of more rows than a given ceiling fails
// (the 1,000-row ceiling the REST clients had before CHAOS-9126).
func NewClientWithRowBound(dsn string, rows uint) (*dhclickhouse.Client, error) {
	opts := chclient.Options(dsn)
	opts.MaxResultRows = &rows
	return dhclickhouse.NewClickHouseQueryClientWithOptions(opts)
}
