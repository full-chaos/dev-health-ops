//go:build integration

package atlassianteams

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// writeForScopeTest is the write of the two-integration tests: the real Write
// with the scope gate's answer for a run of integrationID in an organization
// that holds the integrations of census.
func writeForScopeTest(ctx context.Context, conn driver.Conn, orgID string, rows Rows, census integrationCensus, integrationID string) (Result, error) {
	return Write(ctx, conn, orgID, rows, everything, scopeOf(census, integrationID))
}
