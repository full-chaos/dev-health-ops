package providersync

import (
	"context"
	"log/slog"
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
// ProjectsWritten row.
//
// team_repo_ownership rows are reported under TeamCatalogResult.
// RepoOwnershipWritten (CHAOS-4431 added this dedicated field, team-lead
// ruling 2026-08-28) -- a distinct destination table from Linear/GitLab's
// team_project_ownership (OwnershipWritten), so the two never share a
// telemetry label.
//
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
// scope.sync_options[same]) exactly: the credential's own fields (both its
// unencrypted Config -- the web form's non-secret "Organization / Owner"
// field, ProviderForms.tsx GitHubForm -- and its encrypted fields, since it
// is not yet established which column this deployment actually persisted
// "org" into) are checked FIRST, then ref.SyncOptions (this run's own
// sync_configurations.sync_options, CHAOS-4431's SyncOptions addition) as
// the fallback -- matching Python's own credentials-before-scope order.
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

func githubOrgNameFromSyncOptions(syncOptions map[string]any) string {
	for _, key := range githubOrgNameConfigKeys {
		if raw, ok := syncOptions[key]; ok {
			if value, ok := raw.(string); ok {
				if trimmed := strings.TrimSpace(value); trimmed != "" {
					return trimmed
				}
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
	if ctx == nil {
		return TeamCatalogResult{}, nil
	}
	// Mirrors LinearTeamCatalogCollector's gate exactly (CHAOS-4431 codex
	// review round 2): a non-strict caller with nothing selected is a clean
	// skip, never an error -- only strict (reference discovery, which the
	// selections resolver now defaults to "everything on" when no canonical
	// config row exists) is expected to always have something selected.
	if !ref.Strict && !selections.Any() {
		return TeamCatalogResult{}, nil
	}
	if ref.validate() != nil || credential.Provider != "github" || client == nil || client.Provider != "github" {
		return TeamCatalogResult{}, ErrInvalidConfiguration
	}
	if adapter.Sink.Conn == nil {
		return TeamCatalogResult{}, ErrInvalidConfiguration
	}
	orgName := githubOrgNameFromCredential(credential)
	if orgName == "" {
		orgName = githubOrgNameFromSyncOptions(ref.SyncOptions)
	}
	if orgName == "" {
		if ref.Strict {
			// Matches Python's _populate_async under strict_reference_discovery:
			// "raise ValueError(missing GitHub credentials or org for strict
			// reference discovery)" -- a strict caller (reference discovery)
			// must see this as a real failure, never a silent zero result.
			return TeamCatalogResult{}, ErrInvalidConfiguration
		}
		// Non-strict (post-sync): a missing org is a skip (zero summary),
		// never a hard error -- an org can have GitHub connected with no org
		// name set yet.
		return TeamCatalogResult{}, nil
	}
	if !selections.Teams && !selections.Members {
		return TeamCatalogResult{}, nil
	}

	collector := adapter.Client
	collector.Client = client
	collector.OrgName = orgName
	collector.Strict = ref.Strict
	if collector.Now == nil {
		collector.Now = func() time.Time { return normalizedAt }
	}
	rows, _, err := collector.Collect(ctx, ref.OrgID, selections.Teams, selections.Members)
	if err != nil {
		return TeamCatalogResult{}, err
	}

	// CHAOS-4431 codex review finding #6, ROUND 2 correction (P1, team-lead
	// ruling 2026-08-28): the membership-conflict guard must run BEFORE the
	// team roster is rebuilt below, not after -- otherwise a membership this
	// guard rejects could still show up in `teams.members`, a live
	// attribution fallback source, silently reintroducing the exact
	// contradiction the guard exists to prevent. Computed once, up front;
	// both the roster rebuild and the memberships write below read from it.
	var keptMemberships []githubMembershipRow
	var membershipsSkippedManualConflict int
	if selections.Members {
		var err error
		keptMemberships, membershipsSkippedManualConflict, err = applyGitHubTeamMembershipConflictGuard(
			ctx, adapter.Sink.Conn, ref.OrgID, rows.Memberships,
		)
		if err != nil {
			return TeamCatalogResult{}, err
		}
	}

	rosterPreservationFailed := false
	if selections.Teams {
		if selections.Members {
			// Roster-after-filter: rebuild from the CONFLICT-FILTERED
			// memberships, not the raw provider-observed roster Collect
			// baked into the row.
			roster := githubTeamRosterFromMemberships(keptMemberships)
			for index := range rows.Teams {
				members := roster[rows.Teams[index].ID]
				if members == nil {
					members = []string{}
				}
				rows.Teams[index].Members = members
			}

			// CHAOS-4461: a per-team member-fetch failure under non-strict
			// must not let that team's roster stay at the [] the rebuild
			// above just gave it (that team has zero rows in keptMemberships
			// because its fetch never even ran, not because every one of
			// its memberships conflicted) -- confirm and carry forward its
			// currently-persisted roster instead. Python has no equivalent
			// for this per-team case (team-lead ruling 2026-08-28: fix in Go
			// only, tracked as a Python-side gap in CHAOS-4461, dies with
			// CHAOS-4435).
			if len(rows.FailedMemberFetchTeamIDs) > 0 {
				preserved, ok := adapter.Sink.ExistingTeamMembers(ctx, ref.OrgID, rows.FailedMemberFetchTeamIDs)
				failed := make(map[string]struct{}, len(rows.FailedMemberFetchTeamIDs))
				for _, id := range rows.FailedMemberFetchTeamIDs {
					failed[id] = struct{}{}
				}
				if !ok {
					// Cannot confirm these teams' current rosters -- skip
					// writing THEIR rows entirely this cycle rather than
					// risk clobbering an unconfirmed roster. Every other
					// team still writes normally. Same shape as
					// TeamCatalogResult.RosterPreservationFailed's
					// documented case -- report it under the same shared
					// telemetry outcome.
					rosterPreservationFailed = true
					filtered := rows.Teams[:0]
					for _, team := range rows.Teams {
						if _, isFailed := failed[team.ID]; !isFailed {
							filtered = append(filtered, team)
						}
					}
					rows.Teams = filtered
					slog.Default().WarnContext(ctx, "github_team_catalog_roster_preservation_failed",
						"org_id", ref.OrgID, "team_ids", rows.FailedMemberFetchTeamIDs)
				} else {
					for index := range rows.Teams {
						if _, isFailed := failed[rows.Teams[index].ID]; !isFailed {
							continue
						}
						members := preserved[rows.Teams[index].ID]
						if members == nil {
							// No prior row for this team (genuinely new) has
							// nothing to preserve -- [] is the correct,
							// confirmed answer here, not an unconfirmed guess.
							members = []string{}
						}
						rows.Teams[index].Members = members
					}
					slog.Default().InfoContext(ctx, "github_team_catalog_roster_preserved_after_fetch_failure",
						"org_id", ref.OrgID, "team_ids", rows.FailedMemberFetchTeamIDs)
				}
			}
		} else {
			// Mirrors _populate_async's roster_write_safe gate: a teams-only
			// run (members not selected) must not erase a previously-
			// imported roster by writing an empty "members" list -- it
			// carries forward whatever is currently persisted, and skips
			// the team-dimension write entirely if that read cannot be
			// confirmed.
			teamIDs := make([]string, 0, len(rows.Teams))
			for _, team := range rows.Teams {
				teamIDs = append(teamIDs, team.ID)
			}
			existing, ok := adapter.Sink.ExistingTeamMembers(ctx, ref.OrgID, teamIDs)
			if !ok {
				rosterPreservationFailed = true
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
	}

	result := TeamCatalogResult{RosterPreservationFailed: rosterPreservationFailed}
	if selections.Teams && !rosterPreservationFailed && len(rows.Teams) > 0 {
		// CHAOS-4431 codex review findings #3/#6, team-lead ruling
		// 2026-08-28: fail-safe guard ahead of the full CHAOS-2622
		// drift-aware projector -- a team whose sync_policy is not the
		// auto-apply default (0) is left completely untouched, not
		// overwritten with this call's observed values.
		keptTeams, skippedTeamIDs, err := applyGitHubTeamSyncPolicyGuard(ctx, adapter.Sink.Conn, ref.OrgID, rows.Teams)
		if err != nil {
			return result, err
		}
		result.TeamsSkippedPolicy = len(skippedTeamIDs)
		if len(keptTeams) > 0 {
			if err := adapter.Sink.WriteTeams(ctx, ref.OrgID, keptTeams); err != nil {
				return result, err
			}
			result.TeamsWritten = len(keptTeams)
			result.TeamKeys = make([]string, 0, len(keptTeams))
			for _, team := range keptTeams {
				if team.NativeTeamKey != nil && *team.NativeTeamKey != "" {
					result.TeamKeys = append(result.TeamKeys, *team.NativeTeamKey)
				}
			}
		}
	}
	// CHAOS-4434 correction: team_repo_ownership is gated ONLY on
	// selections.Teams, matching _populate_async exactly -- Python writes it
	// even on a roster_write_safe=false run (the roster gate protects only
	// the `teams` row's members field, never this table). Independent of the
	// sync_policy guard above too -- that guard is scoped to the `teams`
	// table only, matching Linear's own applyTeamSyncPolicyGuard doc comment.
	if selections.Teams && len(rows.RepoOwnership) > 0 {
		if err := adapter.Sink.WriteTeamRepoOwnership(ctx, ref.OrgID, rows.RepoOwnership); err != nil {
			return result, err
		}
		result.RepoOwnershipWritten = len(rows.RepoOwnership)
	}
	if selections.Members {
		result.MembershipsSkippedManualConflict = membershipsSkippedManualConflict
		if len(keptMemberships) > 0 {
			if err := adapter.Sink.WriteMemberships(ctx, ref.OrgID, keptMemberships); err != nil {
				return result, err
			}
			result.MembershipsWritten = len(keptMemberships)
			// MembersWritten stays 0 (codex round 2, P2): Linear's collector
			// sets it to its own `members` table row count
			// (linear_team_catalog_collector.go: batch.Result.Members ==
			// len(rows.Members)) -- GitHub has no `members` table writer at
			// all, only `team_memberships`, so reporting a distinct-identity
			// count under this field would claim rows were written to a
			// table this producer never touches.
		}
	}
	return result, nil
}

var _ TeamCatalogCollector = GitHubTeamCatalogCollector{}
