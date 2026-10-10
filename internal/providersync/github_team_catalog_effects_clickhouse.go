package providersync

import (
	"context"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/teamcreated"
)

// github_team_catalog_effects_clickhouse.go is the direct ClickHouse write
// path for githubTeamRow/githubMembershipRow -- the same two tables
// (teams, team_memberships) the already-shipped Linear reference catalog Go
// route writes (internal/providersync/linear_reference_catalog_effects_
// clickhouse.go), reusing its exact column lists for the shared "teams" and
// "team_memberships" tables so both writers stay byte-compatible with every
// other provider's rows in the same ReplacingMergeTree.
const githubTeamCatalogTeamsInsert = `INSERT INTO teams (id, team_uuid, name, description, members, manual_members, project_keys, repo_patterns, is_active, updated_at, org_id, provider, native_team_key, parent_team_id, created_at)`
const githubTeamCatalogMembershipsInsert = `INSERT INTO team_memberships (org_id, provider, team_id, member_id, raw_provider_user_id, raw_email, identity_facets, source, is_primary, specificity, priority, valid_from, valid_to, updated_at)`

// githubTeamCatalogRepoOwnershipInsert matches team_repo_ownership's exact
// column order (storage/clickhouse.py's write_team_repo_ownership).
const githubTeamCatalogRepoOwnershipInsert = `INSERT INTO team_repo_ownership (org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, priority, valid_from, valid_to, updated_at)`

// GitHubTeamCatalogClickHouseEffects writes githubTeamCatalogRows and reads
// the currently-persisted roster for a members-off run.
type GitHubTeamCatalogClickHouseEffects struct {
	Conn driver.Conn
}

func (sink GitHubTeamCatalogClickHouseEffects) validRow(orgID string, teamRow githubTeamRow) bool {
	return teamRow.Provider == githubTeamCatalogProvider && teamRow.OrgID == orgID &&
		strings.TrimSpace(teamRow.ID) == teamRow.ID && teamRow.ID != "" && !teamRow.UpdatedAt.IsZero()
}

func (sink GitHubTeamCatalogClickHouseEffects) validMembership(orgID string, row githubMembershipRow) bool {
	return row.Provider == githubTeamCatalogProvider && row.OrgID == orgID &&
		row.Source == githubTeamCatalogSource && strings.TrimSpace(row.TeamID) != "" &&
		strings.TrimSpace(row.MemberID) != "" && !row.ValidFrom.IsZero() && !row.UpdatedAt.IsZero()
}

// WriteTeams upserts every team row (ReplacingMergeTree on id dedupes by
// updated_at, matching every other teams writer in this codebase).
//
// CHAOS-4321/CHAOS-4446: this producer never populates ManualMembers
// itself, so every row here is first stamped with its CURRENTLY persisted
// manual_members value (PreserveExistingTeamManualMembers, the shared
// helper every native team-catalog collector uses) before the INSERT --
// omitting the column instead would send ClickHouse's [] DEFAULT and, once
// this row's updated_at wins under FINAL, permanently erase an admin's
// override.
func (sink GitHubTeamCatalogClickHouseEffects) WriteTeams(ctx context.Context, orgID string, rows []githubTeamRow) error {
	if sink.Conn == nil || strings.TrimSpace(orgID) == "" {
		return ErrInvalidConfiguration
	}
	if len(rows) == 0 {
		return nil
	}
	teamIDs := make([]string, 0, len(rows))
	for _, row := range rows {
		teamIDs = append(teamIDs, row.ID)
	}
	existingManual, err := PreserveExistingTeamManualMembers(ctx, sink.Conn, orgID, teamIDs)
	if err != nil {
		return ErrEffectRecoveryUnsafe
	}
	createdAt, err := teamcreated.Carry(ctx, sink.Conn, orgID, teamIDs)
	if err != nil {
		return ErrEffectRecoveryUnsafe
	}
	batch, err := sink.Conn.PrepareBatch(ctx, githubTeamCatalogTeamsInsert)
	if err != nil {
		return err
	}
	defer batch.Abort()
	for _, row := range rows {
		if !sink.validRow(orgID, row) {
			return ErrInvalidConfiguration
		}
		teamUUID, err := uuid.Parse(row.TeamUUID)
		if err != nil {
			return ErrInvalidConfiguration
		}
		manualMembers := existingManual[row.ID]
		if manualMembers == nil {
			manualMembers = []string{}
		}
		if err := batch.Append(
			row.ID, teamUUID, row.Name, row.Description, row.Members, manualMembers, row.ProjectKeys,
			row.RepoPatterns, row.IsActive, row.UpdatedAt, row.OrgID, row.Provider,
			row.NativeTeamKey, row.ParentTeamID, teamcreated.For(createdAt, row.ID, row.UpdatedAt),
		); err != nil {
			return err
		}
	}
	return batch.Send()
}

// WriteMemberships upserts every membership row.
func (sink GitHubTeamCatalogClickHouseEffects) WriteMemberships(ctx context.Context, orgID string, rows []githubMembershipRow) error {
	if sink.Conn == nil || strings.TrimSpace(orgID) == "" {
		return ErrInvalidConfiguration
	}
	if len(rows) == 0 {
		return nil
	}
	batch, err := sink.Conn.PrepareBatch(ctx, githubTeamCatalogMembershipsInsert)
	if err != nil {
		return err
	}
	defer batch.Abort()
	for _, row := range rows {
		if !sink.validMembership(orgID, row) {
			return ErrInvalidConfiguration
		}
		if err := batch.Append(
			row.OrgID, row.Provider, row.TeamID, row.MemberID, row.RawProviderUserID,
			row.RawEmail, row.IdentityFacets, row.Source, row.IsPrimary, row.Specificity,
			row.Priority, row.ValidFrom, row.ValidTo, row.UpdatedAt,
		); err != nil {
			return err
		}
	}
	return batch.Send()
}

func (sink GitHubTeamCatalogClickHouseEffects) validRepoOwnership(orgID string, row githubTeamRepoOwnershipRow) bool {
	return row.Provider == githubTeamCatalogProvider && row.OrgID == orgID &&
		row.Source == githubTeamCatalogSource && strings.TrimSpace(row.TeamID) != "" &&
		strings.TrimSpace(row.RepoFullName) != "" && !row.ValidFrom.IsZero() && !row.UpdatedAt.IsZero()
}

// WriteTeamRepoOwnership upserts every team<->repo grant row. CHAOS-4434
// correction: this is the ONLY producer of GitHub's team_repo_ownership rows
// -- see githubTeamRow's doc comment for why team_repo_ownership_derivation
// cannot cover this case.
func (sink GitHubTeamCatalogClickHouseEffects) WriteTeamRepoOwnership(
	ctx context.Context, orgID string, rows []githubTeamRepoOwnershipRow,
) error {
	if sink.Conn == nil || strings.TrimSpace(orgID) == "" {
		return ErrInvalidConfiguration
	}
	if len(rows) == 0 {
		return nil
	}
	batch, err := sink.Conn.PrepareBatch(ctx, githubTeamCatalogRepoOwnershipInsert)
	if err != nil {
		return err
	}
	defer batch.Abort()
	for _, row := range rows {
		if !sink.validRepoOwnership(orgID, row) {
			return ErrInvalidConfiguration
		}
		var repoID *uuid.UUID
		if row.RepoID != nil {
			parsed, err := uuid.Parse(*row.RepoID)
			if err != nil {
				return ErrInvalidConfiguration
			}
			repoID = &parsed
		}
		if err := batch.Append(
			row.OrgID, row.Provider, row.TeamID, repoID, row.RepoFullName, row.MatchType,
			row.Source, row.IsPrimary, row.Specificity, row.Priority, row.ValidFrom, row.ValidTo, row.UpdatedAt,
		); err != nil {
			return err
		}
	}
	return batch.Send()
}

// ExistingTeamMembers is the Go port of team_autoimport_github.py's
// _existing_team_members: the CURRENTLY persisted roster for these team ids,
// read for a members-off ("teams" selected, "members" not) run to carry
// forward instead of overwriting it with []. Deliberately does NOT filter on
// provider in SQL (matching the Python query's own CHAOS-4323 round-3
// finding: `teams` dedupes ONLY on `id` under ReplacingMergeTree(updated_at)
// ORDER BY (id) -- ADD COLUMN org_id/provider never joined the sort key -- so
// filtering on provider too risks missing the row entirely when the latest
// version was written under a different/blank provider tag).
//
// ok=false means the read genuinely could not be confirmed (ClickHouse
// error) -- the caller MUST skip the team-dimension write for this run
// rather than treat a failed read as "these teams have no members" and erase
// an existing roster. An empty, non-nil map with ok=true is a real, confirmed
// answer (no team_ids to look up, or the query found no matching rows).
func (sink GitHubTeamCatalogClickHouseEffects) ExistingTeamMembers(
	ctx context.Context, orgID string, teamIDs []string,
) (map[string][]string, bool) {
	if len(teamIDs) == 0 {
		return map[string][]string{}, true
	}
	if sink.Conn == nil || strings.TrimSpace(orgID) == "" {
		return nil, false
	}
	result, err := sink.Conn.Query(ctx,
		`SELECT id, members FROM teams FINAL WHERE org_id = ? AND id IN ?`,
		orgID, teamIDs,
	)
	if err != nil {
		return nil, false
	}
	defer result.Close()
	roster := make(map[string][]string, len(teamIDs))
	for result.Next() {
		var id string
		var members []string
		if err := result.Scan(&id, &members); err != nil {
			return nil, false
		}
		roster[id] = members
	}
	if err := result.Err(); err != nil {
		return nil, false
	}
	return roster, true
}

// openProviderAccessRepoOwnership reads the open provider_access rows of the
// listed teams under the run's GitHub org (repo full name prefix "<org>/", the
// prefix Collect builds): a team id "gh:<slug>" holds no GitHub org, so two
// GitHub orgs of one tenant can share it.
//
// A failed read is an error, never an empty answer: the caller must not plan a
// close, or re-stamp valid_from, from a read that did not happen.
func (sink GitHubTeamCatalogClickHouseEffects) openProviderAccessRepoOwnership(
	ctx context.Context, orgID, githubOrg string, teamIDs []string,
) ([]githubTeamRepoOwnershipRow, error) {
	result, err := sink.Conn.Query(ctx, `
SELECT team_id, repo_id, repo_full_name, match_type, is_primary, specificity, priority, valid_from
FROM team_repo_ownership FINAL
WHERE org_id = ? AND provider = ? AND source = ? AND team_id IN ?
  AND startsWith(repo_full_name, ?)
  AND (valid_to IS NULL OR valid_to > now64(3, 'UTC'))`,
		orgID, githubTeamCatalogProvider, githubTeamCatalogSource, teamIDs, githubOrg+"/")
	if err != nil {
		return nil, err
	}
	defer result.Close()
	var open []githubTeamRepoOwnershipRow
	for result.Next() {
		row := githubTeamRepoOwnershipRow{OrgID: orgID, Provider: githubTeamCatalogProvider, Source: githubTeamCatalogSource}
		var repoID *uuid.UUID
		if err := result.Scan(&row.TeamID, &repoID, &row.RepoFullName, &row.MatchType,
			&row.IsPrimary, &row.Specificity, &row.Priority, &row.ValidFrom); err != nil {
			return nil, err
		}
		if repoID != nil {
			text := repoID.String()
			row.RepoID = &text
		}
		open = append(open, row)
	}
	if err := result.Err(); err != nil {
		return nil, err
	}
	return open, nil
}

const githubRepoOwnershipTeamsQuery = `SELECT DISTINCT team_id FROM team_repo_ownership FINAL
WHERE org_id = ? AND provider = ? AND source = ? AND startsWith(repo_full_name, ?)
  AND (valid_to IS NULL OR valid_to > now64(3, 'UTC'))`

// OpenRepoOwnershipTeamsNotListed reads the teams that hold an open
// provider_access row under the run's GitHub org (repo full name prefix
// "<org>/") and that the run's team listing does not return: the dropped teams
// of the ownership close (team_absence.go). A failed read is an error, never
// "no team".
func (sink GitHubTeamCatalogClickHouseEffects) OpenRepoOwnershipTeamsNotListed(
	ctx context.Context, orgID, githubOrg string, listed []string,
) ([]string, error) {
	if sink.Conn == nil || strings.TrimSpace(orgID) == "" || strings.TrimSpace(githubOrg) == "" {
		return nil, ErrInvalidConfiguration
	}
	return openTeamIDsNotListed(ctx, sink.Conn, githubRepoOwnershipTeamsQuery, "AND team_id NOT IN ?", listed,
		orgID, githubTeamCatalogProvider, githubTeamCatalogSource, githubOrg+"/")
}

// SnapshotTeamRepoOwnership writes this run's team<->repo grants and closes
// the open provider_access rows of the LISTED teams that GitHub no longer
// returns, through the shared snapshot rule (PlanOwnershipSnapshot, a repo full
// name stands in for the project id). githubOrg is the GitHub org the run
// listed: only rows whose repo full name starts with "<githubOrg>/" are read or
// closed. readTeamIDs is every team whose repo listing returned: their open
// rows give the first-seen valid_from. grants is the grant kind of the teams
// decideOwnershipClose lets close, with the gate's proof; a team outside it,
// and a run with none, closes nothing. An already-open grant keeps its first
// valid_from, so a repeat run replaces the row instead of adding one. Rows of
// another org, provider or source are never read and never closed.
//
// It returns the grants written and the plan.
func (sink GitHubTeamCatalogClickHouseEffects) SnapshotTeamRepoOwnership(
	ctx context.Context, orgID, githubOrg string, fresh []githubTeamRepoOwnershipRow, readTeamIDs []string,
	grants KindSnapshot[OwnershipSnapshotRow], at time.Time,
) (written int, plan SnapshotPlan, err error) {
	if sink.Conn == nil || strings.TrimSpace(orgID) == "" || strings.TrimSpace(githubOrg) == "" || at.IsZero() {
		return 0, SnapshotPlan{}, ErrInvalidConfiguration
	}
	if len(readTeamIDs) == 0 {
		return len(fresh), SnapshotPlan{}, sink.WriteTeamRepoOwnership(ctx, orgID, fresh)
	}
	open, err := sink.openProviderAccessRepoOwnership(ctx, orgID, githubOrg, readTeamIDs)
	if err != nil {
		return 0, SnapshotPlan{}, err
	}
	rows, plan := githubRepoOwnershipSnapshot(fresh, open, at, grants)
	if err := sink.WriteTeamRepoOwnership(ctx, orgID, rows); err != nil {
		return 0, SnapshotPlan{}, err
	}
	return len(fresh), plan, nil
}

// githubRepoOwnershipSnapshot applies the shared snapshot rule
// (PlanOwnershipSnapshot, a repo full name standing in for the project id) to
// one run: the fresh grants on their first-seen valid_from, then the open rows
// the snapshot no longer holds, closed at `at`. grants names the teams whose
// repo listing reached a confirmed end; an open row of a team outside it is of
// no kind and is never closed. It returns the rows to write and the plan.
func githubRepoOwnershipSnapshot(
	fresh, open []githubTeamRepoOwnershipRow, at time.Time, grants KindSnapshot[OwnershipSnapshotRow],
) ([]githubTeamRepoOwnershipRow, SnapshotPlan) {
	facts := func(rows []githubTeamRepoOwnershipRow) []OwnershipSnapshotRow {
		out := make([]OwnershipSnapshotRow, len(rows))
		for index, row := range rows {
			// validRepoOwnership refuses an empty repository name before a row
			// is written, so a zero id here never reaches the table.
			repoID, _ := GitHubRepoProjectID(row.RepoFullName)
			out[index] = OwnershipSnapshotRow{TeamID: row.TeamID, ProjectID: repoID, Source: row.Source, ValidFrom: row.ValidFrom}
		}
		return out
	}
	plan := PlanOwnershipSnapshot(facts(fresh), facts(open), at, grants)
	rows := make([]githubTeamRepoOwnershipRow, 0, len(fresh)+len(plan.Retract))
	for index, row := range fresh {
		row.ValidFrom = plan.ValidFrom[index]
		rows = append(rows, row)
	}
	for _, retraction := range plan.Retract {
		row := open[retraction.Open]
		closedAt := retraction.ClosedAt
		row.ValidTo = &closedAt
		row.UpdatedAt = at
		rows = append(rows, row)
	}
	return rows, plan
}
