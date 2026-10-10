package providersync

import (
	"context"
	"errors"
	"fmt"
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
	// Serializer, when set, makes two runs of one (organization, provider)
	// catalog take turns (CHAOS-9140): the read of the open membership rows and
	// the insert of a new fact are not one step, so two overlapping runs each
	// stamped a new fact and left two open rows. A nil Serializer runs without
	// the turn (the operator CLI holds no Postgres).
	Serializer TeamCatalogSerializer
	// Provider is the provider key of the Serializer's turn.
	Provider string
}

// TeamCatalogSerializer gives one run of a (organization, provider) catalog its
// turn. Serialize waits until the turn is free (or ctx ends: the error) and
// returns the release of it.
type TeamCatalogSerializer interface {
	Serialize(ctx context.Context, orgID, provider string) (release func(), err error)
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
	if carry.Serializer != nil {
		release, err := carry.Serializer.Serialize(ctx, ref.OrgID, carry.Provider)
		if err != nil {
			return TeamCatalogResult{}, fmt.Errorf("team catalog turn (%s): %w", carry.Provider, err)
		}
		defer release()
	}
	if err := CarryTeamIDsBeforeWrite(ctx, carry.Conn, ref.OrgID, carry.Writer); err != nil {
		return TeamCatalogResult{}, err
	}
	return carry.Collector.CollectTeamCatalog(ctx, ref, credential, client, selections, normalizedAt)
}

// CarryFirstTeamCatalogCollectors wraps every collector of a registry,
// keyed by provider.
func CarryFirstTeamCatalogCollectors(conn TeamIDCarryConn, collectors map[string]TeamCatalogCollector) map[string]TeamCatalogCollector {
	return CarryFirstSerializedTeamCatalogCollectors(conn, nil, collectors)
}

// CarryFirstSerializedTeamCatalogCollectors is CarryFirstTeamCatalogCollectors
// with the per-(organization, provider) turn of CHAOS-9140.
func CarryFirstSerializedTeamCatalogCollectors(conn TeamIDCarryConn, serializer TeamCatalogSerializer, collectors map[string]TeamCatalogCollector) map[string]TeamCatalogCollector {
	wrapped := make(map[string]TeamCatalogCollector, len(collectors))
	for provider, collector := range collectors {
		wrapped[provider] = CarryFirstTeamCatalogCollector{Conn: conn, Writer: "team_catalog:" + provider, Collector: collector, Serializer: serializer, Provider: provider}
	}
	return wrapped
}

// ErrTeamCatalogCollectorNotCarried marks a collector that reached a dispatch
// site without the carry wrapper.
var ErrTeamCatalogCollectorNotCarried = errors.New("team catalog collector is not wrapped by the team id carry")

// RequireCarried refuses a collector that CarryFirstTeamCatalogCollector does
// not wrap. Both dispatch sites call it before CollectTeamCatalog, so a
// collector type registered outside the wrapped registry fails loud.
func RequireCarried(collector TeamCatalogCollector) error {
	carry, ok := collector.(CarryFirstTeamCatalogCollector)
	if !ok || carry.Conn == nil || carry.Collector == nil {
		return ErrTeamCatalogCollectorNotCarried
	}
	return nil
}
