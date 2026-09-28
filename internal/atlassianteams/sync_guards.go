package atlassianteams

import (
	"context"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/providersync"
)

// sync_guards.go routes every Write call -- whether triggered by the
// automatic post-sync path or the standalone `dho sync teams --provider
// jira` CLI verb -- through the SAME team_sync_policies (CHAOS-2622) and
// manual-membership-conflict guards every other native team-catalog
// collector (Linear/GitHub/GitLab/Jira project-as-team) already applies
// before writing to the `teams`/`team_memberships` tables (codex review r1,
// CHAOS-7002: Write previously inserted Atlassian Teams rows directly,
// bypassing both -- a team an admin pinned sync_policy=2 (manual) could be
// overwritten, and a native membership contradicting a manual pin was
// written active instead of staged for review).

// filterTeamsBySyncPolicy returns the subset of teams safe to write this
// call: a team whose sync_policy is not the auto-apply default is left
// completely untouched (staged for review instead), same contract every
// other provider's own teams write already gets. Called with the FULL,
// unfiltered incoming team snapshot -- teamsInScope's own missing/
// deactivation computation runs on that same unfiltered list, so a team
// this guard skips is never mistaken for one the snapshot stopped
// reporting (which would deactivate it, the opposite of "left untouched").
func filterTeamsBySyncPolicy(ctx context.Context, conn driver.Conn, orgID string, teams []TeamRow, now time.Time) ([]TeamRow, error) {
	if len(teams) == 0 {
		return teams, nil
	}
	views := make([]providersync.TeamDriftTeamView, len(teams))
	for index, team := range teams {
		name := team.Name
		views[index] = providersync.TeamDriftTeamView{
			ID: team.ID, Provider: team.Provider, NativeTeamKey: team.NativeTeamKey,
			Name: &name, Description: team.Description,
			Members: []string{}, ProjectKeys: team.ProjectKeys, RepoPatterns: []string{},
			IsActive: team.IsActive != 0,
		}
	}
	keptIdx, _, _, _, err := providersync.ReviewTeamRowsForDrift(ctx, conn, orgID, views, now)
	if err != nil {
		return nil, err
	}
	kept := make([]TeamRow, 0, len(keptIdx))
	for _, index := range keptIdx {
		kept = append(kept, teams[index])
	}
	return kept, nil
}

// filterMembershipsByManualConflict returns the subset of memberships safe
// to write this call. Filtering happens BEFORE planMemberships (unlike the
// teams guard above): a membership this guard drops must also retract any
// previously-written active row for it, and planMemberships' own
// current-vs-open diff is exactly the mechanism that does that once the
// dropped row is simply absent from the fresh snapshot it plans against.
// observedTeamIDs is every team whose membership listing this run actually
// attempted (Collect only calls IterTeamUsers for an active team, so a
// team's membership row set is all-or-nothing per team, matching Jira's own
// per-run observed-teams contract).
func filterMembershipsByManualConflict(
	ctx context.Context, conn driver.Conn, orgID string, memberships []MembershipRow, observedTeamIDs []string, now time.Time,
) ([]MembershipRow, error) {
	if len(memberships) == 0 && len(observedTeamIDs) == 0 {
		return memberships, nil
	}
	views := make([]providersync.TeamDriftMembershipView, len(memberships))
	for index, row := range memberships {
		var rawProviderUserID *string
		if trimmed := strings.TrimSpace(row.RawProviderUserID); trimmed != "" {
			rawProviderUserID = &trimmed
		}
		views[index] = providersync.TeamDriftMembershipView{
			Provider: row.Provider, TeamID: row.TeamID, MemberID: row.MemberID,
			RawProviderUserID: rawProviderUserID, IdentityFacets: row.IdentityFacets,
			Source: row.Source, IsPrimary: row.IsPrimary, Specificity: row.Specificity, Priority: row.Priority,
			ValidFrom: row.ValidFrom, UpdatedAt: row.UpdatedAt,
		}
	}
	keptIdx, _, _, _, err := providersync.ApplyTeamMembershipConflictGuard(ctx, conn, orgID, Provider, views, observedTeamIDs, now)
	if err != nil {
		return nil, err
	}
	kept := make([]MembershipRow, 0, len(keptIdx))
	for _, index := range keptIdx {
		kept = append(kept, memberships[index])
	}
	return kept, nil
}

func distinctAtlassianMembers(memberships []MembershipRow) int {
	seen := make(map[string]struct{}, len(memberships))
	for _, row := range memberships {
		seen[row.MemberID] = struct{}{}
	}
	return len(seen)
}
