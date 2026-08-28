package providersync

import (
	"context"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
)

// github_team_catalog_effects_clickhouse.go is the direct ClickHouse write
// path for githubTeamRow/githubMembershipRow -- the same two tables
// (teams, team_memberships) the already-shipped Linear reference catalog Go
// route writes (internal/providersync/linear_reference_catalog_effects_
// clickhouse.go), reusing its exact column lists for the shared "teams" and
// "team_memberships" tables so both writers stay byte-compatible with every
// other provider's rows in the same ReplacingMergeTree.
const githubTeamCatalogTeamsInsert = `INSERT INTO teams (id, team_uuid, name, description, members, manual_members, project_keys, repo_patterns, is_active, updated_at, org_id, provider, native_team_key, parent_team_id)`
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
// CHAOS-4321: this producer never populates ManualMembers itself, so every
// row here is first stamped with its CURRENTLY persisted manual_members
// value (existingManualMembers, below) before the INSERT -- omitting the
// column instead would send ClickHouse's [] DEFAULT and, once this row's
// updated_at wins under FINAL, permanently erase an admin's override. This
// mirrors storage/clickhouse.py's insert_teams/_preserve_existing_
// manual_members exactly (see githubTeamRow.ManualMembers's doc comment).
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
	existingManual, ok := sink.existingManualMembers(ctx, orgID, teamIDs)
	if !ok {
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
			row.NativeTeamKey, row.ParentTeamID,
		); err != nil {
			return err
		}
	}
	return batch.Send()
}

// existingManualMembers reads the CURRENTLY persisted manual_members for
// these team ids, unconditionally, before every WriteTeams call (unlike
// ExistingTeamMembers/roster preservation below, which is gated on the
// members-off selection). ok=false means the caller must fail the write
// rather than risk sending ClickHouse's [] DEFAULT for a column it cannot
// currently confirm.
func (sink GitHubTeamCatalogClickHouseEffects) existingManualMembers(
	ctx context.Context, orgID string, teamIDs []string,
) (map[string][]string, bool) {
	if len(teamIDs) == 0 {
		return map[string][]string{}, true
	}
	result, err := sink.Conn.Query(ctx,
		`SELECT id, manual_members FROM teams FINAL WHERE org_id = ? AND id IN ?`,
		orgID, teamIDs,
	)
	if err != nil {
		return nil, false
	}
	defer result.Close()
	existing := make(map[string][]string, len(teamIDs))
	for result.Next() {
		var id string
		var manualMembers []string
		if err := result.Scan(&id, &manualMembers); err != nil {
			return nil, false
		}
		existing[id] = manualMembers
	}
	if err := result.Err(); err != nil {
		return nil, false
	}
	return existing, true
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
