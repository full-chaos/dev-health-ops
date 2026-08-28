package providersync

import (
	"context"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// LinearTeamCatalogCollector adapts LinearReferenceCatalogRouteHandler
// (the collection walk) and LinearReferenceCatalogClickHouseEffects (the
// write) to the shared, claim-free TeamCatalogCollector seam (CHAOS-4431,
// team-lead ruling 2026-08-28, option (c)). It is the first native
// implementation; CHAOS-4434 (GitHub) and CHAOS-4432 (GitLab) follow the
// same shape against the same interface.
type LinearTeamCatalogCollector struct {
	Handler LinearReferenceCatalogRouteHandler
	Sink    LinearReferenceCatalogClickHouseEffects
}

// CollectTeamCatalog walks Linear's teams/members/projects once and writes
// only the destinations this org has selected (CHAOS-4323). Teams are always
// collected from the wire in one shot with members and projects (Linear's
// GraphQL shape nests members under teams), but a selection still gates the
// WRITE: a deselected surface is never persisted, matching Python's
// per-flag behavior in run_team_autoimport_strict/run_post_sync_team_autoimport.
func (collector LinearTeamCatalogCollector) CollectTeamCatalog(
	ctx context.Context,
	ref TeamCatalogReference,
	credential providerfoundation.Credential,
	client *providerfoundation.HTTPClient,
	selections TeamCatalogSelections,
	normalizedAt time.Time,
) (TeamCatalogResult, error) {
	if ctx == nil || !selections.Any() {
		return TeamCatalogResult{}, nil
	}
	if collector.Sink.Conn == nil || collector.Sink.Lease == nil {
		return TeamCatalogResult{}, ErrInvalidConfiguration
	}
	batch, err := collector.Handler.CollectReferenceCatalog(ctx, ref, credential, client, normalizedAt)
	if err != nil {
		return TeamCatalogResult{}, err
	}
	writeClaim := Claim{Unit: Unit{OrgID: ref.OrgID, Provider: "linear"}}
	result := TeamCatalogResult{}
	if selections.Teams {
		if err := collector.Sink.WriteEffect(ctx, writeClaim, batch.Effects.Teams); err != nil {
			return result, err
		}
		result.TeamsWritten = batch.Result.Teams
		result.TeamKeys = make([]string, 0, len(batch.Rows.Teams))
		for _, team := range batch.Rows.Teams {
			if team.NativeTeamKey != nil && *team.NativeTeamKey != "" {
				result.TeamKeys = append(result.TeamKeys, *team.NativeTeamKey)
			}
		}
	}
	if selections.Members {
		if err := collector.Sink.WriteEffect(ctx, writeClaim, batch.Effects.Members); err != nil {
			return result, err
		}
		if err := collector.Sink.WriteEffect(ctx, writeClaim, batch.Effects.Memberships); err != nil {
			return result, err
		}
		result.MembersWritten = batch.Result.Members
		result.MembershipsWritten = batch.Result.Memberships
	}
	if selections.Projects {
		if err := collector.Sink.WriteEffect(ctx, writeClaim, batch.Effects.Projects); err != nil {
			return result, err
		}
		if err := collector.Sink.WriteEffect(ctx, writeClaim, batch.Effects.Ownership); err != nil {
			return result, err
		}
		result.ProjectsWritten = batch.Result.Projects
		result.OwnershipWritten = batch.Result.Ownership
	}
	return result, nil
}

var _ TeamCatalogCollector = LinearTeamCatalogCollector{}
