package providersync

import (
	"context"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// CarryFirstTeamCatalogCollector is the one place a team catalog sync
// carries the organization's bare team ids (CarryTeamIDs) to the prefixed
// form: before the collector it wraps reads or writes anything. A collector
// reads sync policies, manual memberships, fallbacks and drift rows by the
// prefixed id in its guards, and closes and opens links by it, before its
// first team write; a carry that ran later would find those rows under the
// bare id and lose them. Every registered collector is wrapped (worker:
// workerservice newNativeTeamCatalogCollectors; CLI: synccli
// buildCatalogCollector), so a new collector cannot run without it.
type CarryFirstTeamCatalogCollector struct {
	Conn      TeamIDCarryConn
	Writer    string
	Collector TeamCatalogCollector
}

func (carry CarryFirstTeamCatalogCollector) CollectTeamCatalog(
	ctx context.Context,
	ref TeamCatalogReference,
	credential providerfoundation.Credential,
	client *providerfoundation.HTTPClient,
	selections TeamCatalogSelections,
	normalizedAt time.Time,
) (TeamCatalogResult, error) {
	if carry.Conn == nil || carry.Collector == nil {
		return TeamCatalogResult{}, ErrInvalidConfiguration
	}
	if err := CarryTeamIDsBeforeWrite(ctx, carry.Conn, ref.OrgID, carry.Writer); err != nil {
		return TeamCatalogResult{}, err
	}
	return carry.Collector.CollectTeamCatalog(ctx, ref, credential, client, selections, normalizedAt)
}

// CarryFirstTeamCatalogCollectors wraps every collector of a registry,
// keyed by provider.
func CarryFirstTeamCatalogCollectors(conn TeamIDCarryConn, collectors map[string]TeamCatalogCollector) map[string]TeamCatalogCollector {
	wrapped := make(map[string]TeamCatalogCollector, len(collectors))
	for provider, collector := range collectors {
		wrapped[provider] = CarryFirstTeamCatalogCollector{Conn: conn, Writer: "team_catalog:" + provider, Collector: collector}
	}
	return wrapped
}
