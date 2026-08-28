package providersync

import (
	"context"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// github_team_catalog_adapter.go adapts githubTeamCatalogCollect (the REST
// walk) and GitHubTeamCatalogClickHouseEffects (the write) to the shared,
// claim-free providersync.TeamCatalogCollector seam (CHAOS-4431/CHAOS-4434,
// team-lead ruling 2026-08-28, option (c)) -- the same shape
// LinearTeamCatalogCollector already uses. GitHub has no "Projects" import
// concept at all (auto_import_capabilities("github").projects is always
// False in Python): selections.Projects is read but never produces a
// ProjectsWritten/OwnershipWritten row, matching that permanent capability
// clamp rather than a temporary gap.
// Telemetry is generic, not per-provider: the caller (syncdispatchruntime.
// TeamCatalogDiscoveryExecutor / teamCatalogAutoimportBridge) observes
// dispatch outcome and rows-written-per-table from this method's own return
// value via jobruntime.TeamCatalogObserver (CHAOS-4431) -- no bespoke
// GitHub-specific Observer field needed here.
type GitHubTeamCatalogCollector struct {
	Client GitHubTeamCatalogRouteHandler
	Sink   GitHubTeamCatalogClickHouseEffects
}

// githubOrgNameConfigKeys mirrors team_autoimport_github.py's _github_org
// fallback order (credentials["org"|"organization"|"org_name"|"owner"], then
// scope.sync_options[same]). This seam has no per-sync-run scope to fall
// back to (TeamCatalogReference is claim-free), so only the credential's own
// fields are checked -- both its unencrypted Config (the web form's non-secret
// "Organization / Owner" field, ProviderForms.tsx GitHubForm) and its
// encrypted fields, since it is not yet established which column this
// deployment actually persisted "org" into.
var githubOrgNameConfigKeys = []string{"org", "organization", "org_name", "owner"}

func githubOrgNameFromCredential(credential providerfoundation.Credential) string {
	for _, key := range githubOrgNameConfigKeys {
		if value := strings.TrimSpace(credential.Config[key]); value != "" {
			return value
		}
	}
	for _, key := range githubOrgNameConfigKeys {
		if value, ok := credential.Secret(key); ok && value.Configured() {
			if trimmed := strings.TrimSpace(value.Reveal()); trimmed != "" {
				return trimmed
			}
		}
	}
	return ""
}

func (adapter GitHubTeamCatalogCollector) CollectTeamCatalog(
	ctx context.Context,
	ref TeamCatalogReference,
	credential providerfoundation.Credential,
	client *providerfoundation.HTTPClient,
	selections TeamCatalogSelections,
	normalizedAt time.Time,
) (TeamCatalogResult, error) {
	if ctx == nil || ref.validate() != nil || !selections.Any() ||
		credential.Provider != "github" || client == nil || client.Provider != "github" {
		return TeamCatalogResult{}, ErrInvalidConfiguration
	}
	if adapter.Sink.Conn == nil {
		return TeamCatalogResult{}, ErrInvalidConfiguration
	}
	orgName := githubOrgNameFromCredential(credential)
	if orgName == "" {
		// Matches Python's _populate_async: a missing org is a skip (zero
		// summary), never a hard error -- an org can have GitHub connected
		// with no org name set yet.
		return TeamCatalogResult{}, nil
	}
	if !selections.Teams && !selections.Members {
		return TeamCatalogResult{}, nil
	}

	collector := adapter.Client
	collector.Client = client
	collector.OrgName = orgName
	if collector.Now == nil {
		collector.Now = func() time.Time { return normalizedAt }
	}
	rows, _, err := collector.Collect(ctx, ref.OrgID, selections.Teams, selections.Members)
	if err != nil {
		return TeamCatalogResult{}, err
	}

	// Mirrors _populate_async's roster_write_safe gate: a teams-only run
	// (members not selected) must not erase a previously-imported roster by
	// writing an empty "members" list -- it carries forward whatever is
	// currently persisted, and skips the team-dimension write entirely if
	// that read cannot be confirmed.
	rosterWriteSafe := true
	if selections.Teams && !selections.Members {
		teamIDs := make([]string, 0, len(rows.Teams))
		for _, team := range rows.Teams {
			teamIDs = append(teamIDs, team.ID)
		}
		existing, ok := adapter.Sink.ExistingTeamMembers(ctx, ref.OrgID, teamIDs)
		if !ok {
			rosterWriteSafe = false
		} else {
			for index := range rows.Teams {
				members := existing[rows.Teams[index].ID]
				if members == nil {
					members = []string{}
				}
				rows.Teams[index].Members = members
			}
		}
	}

	result := TeamCatalogResult{}
	if selections.Teams && rosterWriteSafe && len(rows.Teams) > 0 {
		if err := adapter.Sink.WriteTeams(ctx, ref.OrgID, rows.Teams); err != nil {
			return result, err
		}
		result.TeamsWritten = len(rows.Teams)
		result.TeamKeys = make([]string, 0, len(rows.Teams))
		for _, team := range rows.Teams {
			if team.NativeTeamKey != nil && *team.NativeTeamKey != "" {
				result.TeamKeys = append(result.TeamKeys, *team.NativeTeamKey)
			}
		}
	}
	if selections.Members && len(rows.Memberships) > 0 {
		if err := adapter.Sink.WriteMemberships(ctx, ref.OrgID, rows.Memberships); err != nil {
			return result, err
		}
		result.MembershipsWritten = len(rows.Memberships)
		distinctMembers := make(map[string]struct{}, len(rows.Memberships))
		for _, membership := range rows.Memberships {
			distinctMembers[membership.MemberID] = struct{}{}
		}
		result.MembersWritten = len(distinctMembers)
	}
	return result, nil
}

var _ TeamCatalogCollector = GitHubTeamCatalogCollector{}
