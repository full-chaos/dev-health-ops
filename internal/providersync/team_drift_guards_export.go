package providersync

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// team_drift_guards_export.go exports the shared team_sync_policies
// (CHAOS-2622) and manual-membership-conflict drift-review engines
// (team_drift_review.go / identity_drift_review.go /
// team_membership_conflict_guard.go) for internal/atlassianteams, the one
// native team-catalog writer that lives outside this package (it already
// imports providersync for PreserveExistingTeamManualMembers). Every
// in-package collector (Linear/GitHub/GitLab/Jira project-as-team) reaches
// the same engine through its own thin, provider-typed wrapper (see
// jira_team_catalog_guards.go); this file is that wrapper for a caller
// that cannot see the package's unexported row/view types directly.

// TeamDriftTeamView is the exported mirror of teamDriftTeamView.
type TeamDriftTeamView struct {
	ID, Provider, NativeTeamKey        string
	Name, Description                  *string
	Members, ProjectKeys, RepoPatterns []string
	IsActive                           bool
	ParentTeamID                       *string
}

func (v TeamDriftTeamView) toInternal() teamDriftTeamView {
	return teamDriftTeamView{
		ID: v.ID, Provider: v.Provider, NativeTeamKey: v.NativeTeamKey,
		Name: v.Name, Description: v.Description,
		Members: v.Members, ProjectKeys: v.ProjectKeys, RepoPatterns: v.RepoPatterns,
		IsActive: v.IsActive, ParentTeamID: v.ParentTeamID,
	}
}

// ReviewTeamRowsForDrift exports reviewTeamRowsForDrift: the shared
// team_sync_policies guard + drift-staging engine every native collector's
// applyXTeamSyncPolicyGuard wrapper delegates to. See its doc comment
// (team_drift_review.go) for the full per-policy behavior.
func ReviewTeamRowsForDrift(
	ctx context.Context, conn driver.Conn, orgID string, teams []TeamDriftTeamView, now time.Time,
) (keptIdx []int, skippedTeamIDs []string, stagedTeams int, superseded int, err error) {
	views := make([]teamDriftTeamView, len(teams))
	for index, team := range teams {
		views[index] = team.toInternal()
	}
	return reviewTeamRowsForDrift(ctx, conn, orgID, views, now)
}

// TeamDriftMembershipView is the exported mirror of teamDriftMembershipView.
type TeamDriftMembershipView struct {
	Provider, TeamID, MemberID  string
	RawProviderUserID, RawEmail *string
	IdentityFacets              []string
	Source                      string
	IsPrimary                   uint8
	Specificity                 uint16
	Priority                    int32
	ValidFrom                   time.Time
	ValidTo                     *time.Time
	UpdatedAt                   time.Time
}

func (v TeamDriftMembershipView) toInternal() teamDriftMembershipView {
	return teamDriftMembershipView{
		Provider: v.Provider, TeamID: v.TeamID, MemberID: v.MemberID,
		RawProviderUserID: v.RawProviderUserID, RawEmail: v.RawEmail, IdentityFacets: v.IdentityFacets,
		Source: v.Source, IsPrimary: v.IsPrimary, Specificity: v.Specificity, Priority: v.Priority,
		ValidFrom: v.ValidFrom, ValidTo: v.ValidTo, UpdatedAt: v.UpdatedAt,
	}
}

// teamDriftMembershipViewConflictsWithManualState is
// membershipConflictsWithManualState (team_membership_conflict_guard.go)
// re-typed to the generic view instead of linearReferenceMembershipRow --
// every provider wrapper (Linear/Jira) duplicates this same check typed to
// its own row; this is the one used by the exported guard below, since an
// external caller has no provider-typed row for this package to accept.
func teamDriftMembershipViewConflictsWithManualState(
	row teamDriftMembershipView,
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

// ApplyTeamMembershipConflictGuard exports the shared manual-membership /
// attribution-fallback conflict guard plus its drift-staging
// (reviewMembershipsForDrift, identity_drift_review.go), mirroring every
// in-package native collector's own applyXTeamMembershipConflictGuard
// wrapper: same guard, same staging, same kept/skipped/staged/superseded
// contract, returned as indices into rows since this package has no
// provider-typed row to hand back.
func ApplyTeamMembershipConflictGuard(
	ctx context.Context, conn driver.Conn, orgID, provider string, rows []TeamDriftMembershipView, observedTeamIDs []string, now time.Time,
) (keptIdx []int, skipped int, staged int, superseded int, err error) {
	if len(rows) == 0 && len(observedTeamIDs) == 0 {
		return nil, 0, 0, 0, nil
	}
	manualTeamsByMember, err := resolveActiveManualMembershipTeams(ctx, conn, orgID, provider)
	if err != nil {
		return nil, 0, 0, 0, err
	}
	fallbackTeamsByIdentity, err := resolveActiveMemberAttributionFallbackTeams(ctx, conn, orgID, provider)
	if err != nil {
		return nil, 0, 0, 0, err
	}
	views := make([]teamDriftMembershipView, len(rows))
	conflictedIdx := make(map[int]struct{})
	for index, row := range rows {
		views[index] = row.toInternal()
		if teamDriftMembershipViewConflictsWithManualState(views[index], manualTeamsByMember, fallbackTeamsByIdentity) {
			conflictedIdx[index] = struct{}{}
			skipped++
			continue
		}
		keptIdx = append(keptIdx, index)
	}
	staged, superseded, err = reviewMembershipsForDrift(ctx, conn, orgID, provider, views, conflictedIdx, observedTeamIDs, now)
	if err != nil {
		return nil, 0, 0, 0, err
	}
	return keptIdx, skipped, staged, superseded, nil
}
