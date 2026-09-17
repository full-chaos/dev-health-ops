// Package chquery constructs a ClickHouse read-only query client with the
// SAME settings query-api's production route handlers run every read
// through, so a package's seeded integration test exercises the real
// statement guard and result-row/byte ceilings a fake or a raw driver
// connection cannot enforce, against a testcontainers instance.
package chquery

import (
	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
)

// NewProductionClient builds a dev-health-go ClickHouse query client for
// dsn with the same defaults query-api's own route wiring leaves in
// place for the row/byte ceilings that matter here: MaxResultRows is
// left unset, so it inherits dev-health-go's own 1,000-row default
// (clickhouse/options.go's resolveCeilingUint) -- the exact ceiling
// query-api's cmd/query-api/query_route.go's
// newUnrestrictedReadClickHouseOptions ALSO leaves untouched for every
// route but /query itself (that helper only raises MaxBytesToRead).
// Every statement this client runs is checked by the same
// validateReadOnlyStatement guard production's client applies
// (clickhouse/client.go), including the "first token must be SELECT"
// rule a leading WITH trips.
//
// A test that wants to prove a reader survives production's own limits
// -- not just a fixture replay -- should build its client through this
// constructor rather than hand-rolling
// dhclickhouse.NewClickHouseQueryClientWithOptions itself, so every
// package's seeded integration test stays pointed at the same
// production-shaped settings if those defaults ever change.
func NewProductionClient(dsn string) (*dhclickhouse.Client, error) {
	return dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: dsn})
}
