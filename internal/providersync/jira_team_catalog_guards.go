package providersync

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// jira_team_catalog_guards.go is Jira's own thin wrapper over the two
// shared, provider-agnostic fail-safe guards CHAOS-4431 built for Linear
// (team_membership_conflict_guard.go finding #6, team_sync_policy_guard.go
// finding #3) and every native collector since reuses -- same shape,
// Jira-typed rows.

// jiraMembershipConflictsWithManualState mirrors
// membershipConflictsWithManualState exactly (see
// gitlabMembershipConflictsWithManualState's identical doc comment for the
// full rationale): a manual-membership or fallback row to the EXACT SAME
// team as this native row is a CONFIRMATION; a row to ANY OTHER team is the
// conflict.
func jiraMembershipConflictsWithManualState(
	row jiraTeamCatalogMembershipRow,
	manualTeamsByMember map[string]map[string]struct{},
	fallbackTeamsByIdentity map[string]map[string]struct{},
) bool {
	if manualTeams, hasManual := manualTeamsByMember[row.MemberID]; hasManual {
		if anyTeamDiffersFrom(manualTeams, row.TeamID) {
			return true
		}
	}
	if len(fallbackTeamsByIdentity) == 0 {
		return false
	}
	candidates := make([]string, 0, len(row.IdentityFacets)+2)
	if row.RawProviderUserID != nil {
		candidates = append(candidates, *row.RawProviderUserID)
	}
	if row.RawEmail != nil {
		candidates = append(candidates, *row.RawEmail)
	}
	candidates = append(candidates, row.IdentityFacets...)
	for _, candidate := range candidates {
		normalized := normalizeMembershipIdentity(candidate)
		if normalized == "" {
			continue
		}
		if fallbackTeams, hasFallback := fallbackTeamsByIdentity[normalized]; hasFallback {
			if anyTeamDiffersFrom(fallbackTeams, row.TeamID) {
				return true
			}
		}
	}
	return false
}

// applyJiraTeamMembershipConflictGuard filters a batch of native membership
// rows against both active-conflict sources, returning the rows safe to
// write and a count of how many were skipped -- mirrors
// applyTeamMembershipConflictGuard exactly. CHAOS-4444: every skipped
// (conflicting) row is also staged as a team_drift_changes row via the
// shared reviewMembershipsForDrift engine, and a stale pending row for a
// member this run observed but no longer sees conflicting (or sees at all)
// is resolved/superseded. observedTeamIDs is every team (project) whose
// lead lookup succeeded this run -- Jira's lead lookup is all-or-nothing
// (a single failure aborts the whole walk, see jiraTeamCatalogWalkFailure),
// so whenever this guard runs at all, it is every team in the batch.
func applyJiraTeamMembershipConflictGuard(
	ctx context.Context, conn driver.Conn, orgID string, rows []jiraTeamCatalogMembershipRow, observedTeamIDs []string, now time.Time,
) ([]jiraTeamCatalogMembershipRow, int, int, int, error) {
	if len(rows) == 0 && len(observedTeamIDs) == 0 {
		return rows, 0, 0, 0, nil
	}
	manualTeamsByMember, err := resolveActiveManualMembershipTeams(ctx, conn, orgID, jiraTeamCatalogProvider)
	if err != nil {
		return nil, 0, 0, 0, err
	}
	fallbackTeamsByIdentity, err := resolveActiveMemberAttributionFallbackTeams(ctx, conn, orgID, jiraTeamCatalogProvider)
	if err != nil {
		return nil, 0, 0, 0, err
	}
	kept := make([]jiraTeamCatalogMembershipRow, 0, len(rows))
	skipped := 0
	conflictedIdx := make(map[int]struct{})
	views := make([]teamDriftMembershipView, len(rows))
	for index, row := range rows {
		views[index] = jiraMembershipRowToDriftView(row)
		if jiraMembershipConflictsWithManualState(row, manualTeamsByMember, fallbackTeamsByIdentity) {
			skipped++
			conflictedIdx[index] = struct{}{}
			continue
		}
		kept = append(kept, row)
	}
	staged, superseded, err := reviewMembershipsForDrift(ctx, conn, orgID, jiraTeamCatalogProvider, views, conflictedIdx, observedTeamIDs, now)
	if err != nil {
		return nil, 0, 0, 0, err
	}
	return kept, skipped, staged, superseded, nil
}

// applyJiraTeamSyncPolicyGuard is Jira's wrapper over the shared
// reviewTeamRowsForDrift engine (team_drift_review.go, CHAOS-4444): a team
// whose sync_policy is not the auto-apply default (0) is left completely
// untouched by this write, and its diff against the currently-persisted row
// is staged as a team_drift_changes row for review. Mirrors
// applyTeamSyncPolicyGuard exactly, typed to jiraTeamCatalogTeamRow.
func applyJiraTeamSyncPolicyGuard(
	ctx context.Context, conn driver.Conn, orgID string, teams []jiraTeamCatalogTeamRow, now time.Time,
) ([]jiraTeamCatalogTeamRow, []string, int, int, error) {
	if len(teams) == 0 {
		return teams, nil, 0, 0, nil
	}
	views := make([]teamDriftTeamView, len(teams))
	for index, team := range teams {
		views[index] = jiraTeamRowToDriftView(team)
	}
	keptIdx, skipped, staged, superseded, err := reviewTeamRowsForDrift(ctx, conn, orgID, views, now)
	if err != nil {
		return nil, nil, 0, 0, err
	}
	kept := make([]jiraTeamCatalogTeamRow, 0, len(keptIdx))
	for _, index := range keptIdx {
		kept = append(kept, teams[index])
	}
	return kept, skipped, staged, superseded, nil
}

func jiraMembershipRowToDriftView(row jiraTeamCatalogMembershipRow) teamDriftMembershipView {
	return teamDriftMembershipView{
		Provider: row.Provider, TeamID: row.TeamID, MemberID: row.MemberID,
		RawProviderUserID: row.RawProviderUserID, RawEmail: row.RawEmail, IdentityFacets: row.IdentityFacets,
		Source: row.Source, IsPrimary: row.IsPrimary, Specificity: row.Specificity, Priority: row.Priority,
		ValidFrom: row.ValidFrom, ValidTo: row.ValidTo, UpdatedAt: row.UpdatedAt,
	}
}

func jiraTeamRowToDriftView(row jiraTeamCatalogTeamRow) teamDriftTeamView {
	name := row.Name
	return teamDriftTeamView{
		ID: row.ID, Provider: row.Provider, NativeTeamKey: teamDriftNativeKey(row.NativeTeamKey, row.ID),
		Name: &name, Description: row.Description,
		Members: row.Members, ProjectKeys: row.ProjectKeys, RepoPatterns: row.RepoPatterns,
		IsActive: row.IsActive != 0, ParentTeamID: row.ParentTeamID,
	}
}
