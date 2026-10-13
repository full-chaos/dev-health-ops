package workerservice

import (
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
)

// NativeTeamCatalogCollectors is the worker's team catalog collector registry
// (newNativeTeamCatalogCollectors) for a caller outside the worker that must
// run the same collectors: the fixture world producer runs them against
// recorded provider answers. The worker itself builds it through
// newProductionTeamCatalogCollectors.
func NativeTeamCatalogCollectors(conn driver.Conn, turn providersync.TeamCatalogSerializer, scopeCensus providersync.OwnershipScopeCensus, atlassianDoer providerfoundation.HTTPDoer) map[string]providersync.TeamCatalogCollector {
	return newNativeTeamCatalogCollectors(conn, turn, scopeCensus, atlassianDoer)
}
