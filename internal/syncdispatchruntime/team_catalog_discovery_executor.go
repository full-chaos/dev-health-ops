package syncdispatchruntime

import (
	"context"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
)

// ProviderClientResolver resolves the credential + HTTP client pair a native
// team-catalog collector needs, the same way
// cmd/dev-health-worker/provider_sync.go already builds them for a claimed
// provider-unit -- minus the claim and lease, which this seam never has
// (CHAOS-4431 ruling, team-lead 2026-08-28, option (c)). runID is required
// (not just orgID+provider): an org can have more than one active
// integration for the same provider (observed locally for linear, org
// 70d529e0), so which credential governs is resolved from THIS sync run's
// own integration_id -- the same join resolveAuthoritativeProvider already
// uses (native_reference_discovery.go) -- never "the" active one for the pair.
type ProviderClientResolver interface {
	ResolveClient(ctx context.Context, orgID, runID, provider string) (providerfoundation.Credential, *providerfoundation.HTTPClient, error)
}

// TeamCatalogSelectionsResolver reads CHAOS-4323's three independent
// sync_configurations flags (auto_import_teams/auto_import_projects/
// auto_import_members) for this sync run's own integration, for the same
// multiple-active-integrations reason ProviderClientResolver's doc comment
// explains. Used by the POST-SYNC team-autoimport dispatcher only (mirrors
// Python's non-strict run_team_autoimport, which does gate on these flags) --
// NOT by TeamCatalogDiscoveryExecutor below, which mirrors run_team_autoimport_strict
// instead (src/dev_health_ops/workers/team_autoimport.py:98-103: "This is the
// ONLY call site that threads the resulting selection into the populator
// scope -- run_team_autoimport_strict deliberately does not... reference
// discovery and backfill keep importing everything they always have").
type TeamCatalogSelectionsResolver interface {
	ResolveSelections(ctx context.Context, orgID, runID, provider string) (providersync.TeamCatalogSelections, error)
}

// teamCatalogStrictSelections always selects every surface: the strict
// reference-discovery path (run_team_autoimport_strict) never consults
// sync_options, so the native equivalent must not invent a gate Python never
// had.
var teamCatalogStrictSelections = providersync.TeamCatalogSelections{Teams: true, Projects: true, Members: true}

// TeamCatalogDiscoveryExecutor dispatches reference discovery per provider:
// a provider with a registered native collector runs it directly (importing
// every surface unconditionally, matching run_team_autoimport_strict) and
// skips the Python bridge entirely; every other provider falls through to
// Fallback, the existing BridgeDiscoveryExecutor. It implements the same
// DiscoveryExecutor seam VerifiedDiscoveryExecutor already wraps, so
// ClickHouse readback verification covers native and bridge providers alike.
type TeamCatalogDiscoveryExecutor struct {
	Native   map[string]providersync.TeamCatalogCollector
	Fallback DiscoveryExecutor
	Clients  ProviderClientResolver
	Now      func() time.Time
}

func (executor *TeamCatalogDiscoveryExecutor) now() time.Time {
	if executor.Now != nil {
		return executor.Now().UTC()
	}
	return time.Now().UTC()
}

func (executor *TeamCatalogDiscoveryExecutor) Discover(
	ctx context.Context, orgID, runID, provider string,
) (map[string]any, error) {
	if executor == nil || ctx == nil || orgID == "" || runID == "" || provider == "" {
		return nil, ErrReferenceDiscoveryUnavailable
	}
	normalizedProvider := strings.ToLower(strings.TrimSpace(provider))
	collector, native := executor.Native[normalizedProvider]
	if !native {
		if executor.Fallback == nil {
			return nil, ErrReferenceDiscoveryUnavailable
		}
		return executor.Fallback.Discover(ctx, orgID, runID, provider)
	}
	if executor.Clients == nil {
		return nil, ErrReferenceDiscoveryUnavailable
	}
	credential, client, err := executor.Clients.ResolveClient(ctx, orgID, runID, normalizedProvider)
	if err != nil {
		return nil, err
	}
	result, err := collector.CollectTeamCatalog(ctx, providersync.TeamCatalogReference{
		OrgID: orgID, SyncRunID: runID,
	}, credential, client, teamCatalogStrictSelections, executor.now())
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"provider":            normalizedProvider,
		"outcome":             "native",
		"reference_team_keys": result.TeamKeys,
		"rows_written": map[string]int{
			"teams":                  result.TeamsWritten,
			"members":                result.MembersWritten,
			"team_memberships":       result.MembershipsWritten,
			"projects":               result.ProjectsWritten,
			"team_project_ownership": result.OwnershipWritten,
		},
	}, nil
}

var _ DiscoveryExecutor = &TeamCatalogDiscoveryExecutor{}
