package providersync

import (
	"context"
	"reflect"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/google/uuid"
)

// GitLabTeamCatalogClickHouseEffects is the concrete bridge from the
// collector's typed rows to the four durable tables, mirroring
// LinearReferenceCatalogClickHouseEffects's shape against the SAME physical
// tables (`teams`, `team_project_ownership`, `team_memberships`, `projects`).
//
// Two behaviors here are GitLab-specific and deliberately absent from the
// Linear port because Python's team_autoimport_gitlab.py needs them and
// team_autoimport_linear.py's simpler "native" source does not:
//
//   - manual_members carry-forward (CHAOS-4321, chris 2026-08-26: "sync must
//     not clear it"): every `teams` write reads the CURRENTLY persisted
//     manual_members for that row and writes it back unchanged, because this
//     is a ReplacingMergeTree full-row-version INSERT -- omitting the column
//     would silently reset an admin's manual override to [].
//   - roster preservation for a teams-only run (CHAOS-4323 round 2): when
//     MembersAuthoritative is false (want_teams=true, want_members=false),
//     the write carries forward the EXISTING persisted roster instead of
//     overwriting it with an empty one. Python fails closed here (skips the
//     whole team-dimension write when the existing roster cannot be
//     confirmed); this port does the same by returning an error, which
//     fails only the `teams` destination -- team_project_ownership,
//     team_memberships, and projects are independent EffectBatches and are
//     unaffected.
type GitLabTeamCatalogClickHouseEffects struct {
	Conn  driver.Conn
	Lease providerfoundation.LeaseGuard
}

const gitlabTeamCatalogTeamsInsert = `INSERT INTO teams (id, team_uuid, name, description, members, manual_members, project_keys, repo_patterns, is_active, updated_at, org_id, provider, native_team_key, parent_team_id)`
const gitlabTeamCatalogOwnershipInsert = `INSERT INTO team_project_ownership (org_id, provider, team_id, project_id, project_key, source, is_primary, specificity, priority, valid_from, valid_to, updated_at)`
const gitlabTeamCatalogMembershipsInsert = `INSERT INTO team_memberships (org_id, provider, team_id, member_id, raw_provider_user_id, raw_email, identity_facets, source, is_primary, specificity, priority, valid_from, valid_to, updated_at)`
const gitlabTeamCatalogProjectsInsert = `INSERT INTO projects (id, org_id, provider, project_key, name, is_active, state, target_date, url, team_ids, team_keys, lead_id, lead_name, lead_email, updated_at, last_synced)`

func (sink GitLabTeamCatalogClickHouseEffects) WriteEffect(ctx context.Context, claim Claim, effect EffectBatch) error {
	if err := sink.validateRequest(ctx, claim, effect); err != nil {
		return err
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return err
	}
	switch effect.Destination {
	case gitlabTeamCatalogTeamsDestination:
		rows, err := decodeEffectRows[gitlabTeamCatalogTeamRow](effect)
		if err != nil {
			return err
		}
		for _, row := range rows {
			if err := validateGitLabTeamRow(claim, row); err != nil {
				return err
			}
		}
		return sink.writeTeams(ctx, claim, rows)
	case gitlabTeamCatalogOwnershipDestination:
		rows, err := decodeEffectRows[gitlabTeamCatalogOwnershipRow](effect)
		if err != nil {
			return err
		}
		for _, row := range rows {
			if err := validateGitLabOwnershipRow(claim, row); err != nil {
				return err
			}
		}
		return sink.writeOwnership(ctx, rows)
	case gitlabTeamCatalogMembershipsDestination:
		rows, err := decodeEffectRows[gitlabTeamCatalogMembershipRow](effect)
		if err != nil {
			return err
		}
		for _, row := range rows {
			if err := validateGitLabMembershipRow(claim, row); err != nil {
				return err
			}
		}
		return sink.writeMemberships(ctx, rows)
	case gitlabTeamCatalogProjectsDestination:
		rows, err := decodeEffectRows[gitlabTeamCatalogProjectRow](effect)
		if err != nil {
			return err
		}
		for _, row := range rows {
			if err := row.validate(claim); err != nil {
				return err
			}
		}
		return sink.writeProjects(ctx, rows)
	default:
		return ErrInvalidConfiguration
	}
}

func (sink GitLabTeamCatalogClickHouseEffects) InspectEffect(ctx context.Context, claim Claim, effect EffectBatch) (EffectInspection, error) {
	if err := sink.validateRequest(ctx, claim, effect); err != nil {
		return EffectConflict, err
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return EffectConflict, err
	}
	switch effect.Destination {
	case gitlabTeamCatalogTeamsDestination:
		expected, err := decodeEffectRows[gitlabTeamCatalogTeamRow](effect)
		if err != nil {
			return EffectConflict, err
		}
		return inspectGitLabTeamCatalogRows(expected, func(row gitlabTeamCatalogTeamRow) (EffectInspection, error) {
			return sink.inspectTeam(ctx, claim, row)
		})
	case gitlabTeamCatalogOwnershipDestination:
		expected, err := decodeEffectRows[gitlabTeamCatalogOwnershipRow](effect)
		if err != nil {
			return EffectConflict, err
		}
		return inspectGitLabTeamCatalogRows(expected, func(row gitlabTeamCatalogOwnershipRow) (EffectInspection, error) {
			return sink.inspectOwnership(ctx, claim, row)
		})
	case gitlabTeamCatalogMembershipsDestination:
		expected, err := decodeEffectRows[gitlabTeamCatalogMembershipRow](effect)
		if err != nil {
			return EffectConflict, err
		}
		return inspectGitLabTeamCatalogRows(expected, func(row gitlabTeamCatalogMembershipRow) (EffectInspection, error) {
			return sink.inspectMembership(ctx, claim, row)
		})
	case gitlabTeamCatalogProjectsDestination:
		expected, err := decodeEffectRows[gitlabTeamCatalogProjectRow](effect)
		if err != nil {
			return EffectConflict, err
		}
		return inspectGitLabTeamCatalogRows(expected, func(row gitlabTeamCatalogProjectRow) (EffectInspection, error) {
			return sink.inspectProject(ctx, claim, row)
		})
	default:
		return EffectConflict, ErrInvalidConfiguration
	}
}

// validateRequest deliberately does NOT call claim.Validate(): CHAOS-4431
// (team-lead ruling, 2026-08-28, option (c)) made the caller claim-free --
// this sink is written to from a once-per-sync-run reference-catalog walk
// with no lease or claimed provider-unit behind it, not from inside a
// dataset route's Collect(). claim.Validate() requires a live lease and a
// registered (provider, dataset) capability, neither of which exists here;
// the only properties this write path still needs are "this really is
// gitlab" and "this really is this org" (every row below also re-checks
// OrgID itself). Mirrors linear_reference_catalog_effects_clickhouse.go's
// identical adaptation.
func (sink GitLabTeamCatalogClickHouseEffects) validateRequest(ctx context.Context, claim Claim, effect EffectBatch) error {
	if ctx == nil || sink.Lease == nil || sink.Conn == nil ||
		claim.Provider != gitlabTeamCatalogProvider || strings.TrimSpace(claim.OrgID) == "" ||
		effect.Recovery != EffectReadbackRequired ||
		!validDigest(effect.ContentDigest) || effect.PayloadBytes < 0 || !gitlabTeamCatalogDestination(effect.Destination) {
		return ErrInvalidConfiguration
	}
	return nil
}

func gitlabTeamCatalogDestination(destination string) bool {
	switch destination {
	case gitlabTeamCatalogTeamsDestination, gitlabTeamCatalogOwnershipDestination,
		gitlabTeamCatalogMembershipsDestination, gitlabTeamCatalogProjectsDestination:
		return true
	default:
		return false
	}
}

// gitlabExistingTeamRosterQuery batch-reads the currently-persisted
// `members` roster for every team a caller is about to write, mirroring
// PreserveExistingTeamManualMembers's shape (team_manual_members.go) but for
// the GitLab-specific members-off preservation case (CHAOS-4323 round 2):
// unlike manual_members, no other native provider needs this today --
// Linear always collects members alongside teams in one GraphQL walk, so it
// has no "teams selected, members not" partial-collection mode at all. A
// team with no existing row simply has no entry in the returned map; the
// caller defaults that to an empty roster (a genuinely new team), never
// treats a missing entry as an error.
func gitlabExistingTeamRoster(
	ctx context.Context, conn driver.Conn, orgID string, teamIDs []string,
) (map[string][]string, error) {
	if conn == nil || strings.TrimSpace(orgID) == "" {
		return nil, ErrInvalidConfiguration
	}
	if len(teamIDs) == 0 {
		return nil, nil
	}
	rows, err := conn.Query(ctx,
		"SELECT id, members FROM teams FINAL WHERE org_id = {org_id:String} AND id IN {team_ids:Array(String)}",
		clickhouse.Named("org_id", orgID), clickhouse.Named("team_ids", teamIDs),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	existing := make(map[string][]string, len(teamIDs))
	for rows.Next() {
		var id string
		var members []string
		if err := rows.Scan(&id, &members); err != nil {
			return nil, err
		}
		existing[id] = members
	}
	return existing, rows.Err()
}

func (sink GitLabTeamCatalogClickHouseEffects) writeTeams(ctx context.Context, claim Claim, rows []gitlabTeamCatalogTeamRow) error {
	if len(rows) == 0 {
		return nil
	}
	teamIDs := make([]string, 0, len(rows))
	for _, row := range rows {
		teamIDs = append(teamIDs, row.ID)
	}
	// CHAOS-4321/CHAOS-4446: batched, one round trip for every touched team,
	// shared with every native provider's teams writer (Linear included) so
	// the carry-forward logic lives in exactly one place.
	existingManualMembers, err := PreserveExistingTeamManualMembers(ctx, sink.Conn, claim.OrgID, teamIDs)
	if err != nil {
		// Fail closed: a read failure must never fall through to writing an
		// unconfirmed manual_members reset.
		return err
	}
	existingRoster, err := gitlabExistingTeamRoster(ctx, sink.Conn, claim.OrgID, teamIDs)
	if err != nil {
		// Fail closed (team_autoimport_gitlab._existing_team_members): a
		// read failure must never fall through to writing an unconfirmed
		// empty roster either.
		return err
	}
	batch, err := sink.Conn.PrepareBatch(ctx, gitlabTeamCatalogTeamsInsert)
	if err != nil {
		return err
	}
	defer batch.Abort()
	for _, row := range rows {
		teamUUID, err := uuid.Parse(row.TeamUUID)
		if err != nil {
			return ErrInvalidConfiguration
		}
		members := row.Members
		if !row.MembersAuthoritative {
			members = existingRoster[row.ID]
		}
		if members == nil {
			members = []string{}
		}
		manualMembers := existingManualMembers[row.ID]
		if manualMembers == nil {
			manualMembers = []string{}
		}
		if err := batch.Append(
			row.ID, teamUUID, row.Name, row.Description, members, manualMembers, row.ProjectKeys, row.RepoPatterns,
			row.IsActive, row.UpdatedAt, row.OrgID, row.Provider, row.NativeTeamKey, row.ParentTeamID,
		); err != nil {
			return err
		}
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return err
	}
	return batch.Send()
}

func (sink GitLabTeamCatalogClickHouseEffects) writeOwnership(ctx context.Context, rows []gitlabTeamCatalogOwnershipRow) error {
	if len(rows) == 0 {
		return nil
	}
	batch, err := sink.Conn.PrepareBatch(ctx, gitlabTeamCatalogOwnershipInsert)
	if err != nil {
		return err
	}
	defer batch.Abort()
	for _, row := range rows {
		if err := batch.Append(row.OrgID, row.Provider, row.TeamID, row.ProjectID, row.ProjectKey, row.Source, row.IsPrimary, row.Specificity, row.Priority, row.ValidFrom, row.ValidTo, row.UpdatedAt); err != nil {
			return err
		}
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return err
	}
	return batch.Send()
}

func (sink GitLabTeamCatalogClickHouseEffects) writeMemberships(ctx context.Context, rows []gitlabTeamCatalogMembershipRow) error {
	if len(rows) == 0 {
		return nil
	}
	batch, err := sink.Conn.PrepareBatch(ctx, gitlabTeamCatalogMembershipsInsert)
	if err != nil {
		return err
	}
	defer batch.Abort()
	for _, row := range rows {
		if err := batch.Append(row.OrgID, row.Provider, row.TeamID, row.MemberID, row.RawProviderUserID, row.RawEmail, row.IdentityFacets, row.Source, row.IsPrimary, row.Specificity, row.Priority, row.ValidFrom, row.ValidTo, row.UpdatedAt); err != nil {
			return err
		}
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return err
	}
	return batch.Send()
}

func (sink GitLabTeamCatalogClickHouseEffects) writeProjects(ctx context.Context, rows []gitlabTeamCatalogProjectRow) error {
	if len(rows) == 0 {
		return nil
	}
	batch, err := sink.Conn.PrepareBatch(ctx, gitlabTeamCatalogProjectsInsert)
	if err != nil {
		return err
	}
	defer batch.Abort()
	for _, row := range rows {
		if err := batch.Append(row.ID, row.OrgID, row.Provider, row.ProjectKey, row.Name, row.IsActive, row.State, row.TargetDate, row.URL, row.TeamIDs, row.TeamKeys, row.LeadID, row.LeadName, row.LeadEmail, row.UpdatedAt, row.LastSynced); err != nil {
			return err
		}
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return err
	}
	return batch.Send()
}

func (sink GitLabTeamCatalogClickHouseEffects) inspectTeam(ctx context.Context, claim Claim, row gitlabTeamCatalogTeamRow) (EffectInspection, error) {
	result, err := sink.Conn.Query(ctx, `SELECT id, team_uuid, name, description, members, project_keys, repo_patterns, is_active, updated_at, org_id, provider, native_team_key, parent_team_id FROM teams FINAL WHERE org_id = ? AND provider = ? AND id = ?`, claim.OrgID, gitlabTeamCatalogProvider, row.ID)
	if err != nil {
		return EffectConflict, err
	}
	defer result.Close()
	var actual gitlabTeamCatalogTeamRow
	var teamUUID uuid.UUID
	found := 0
	for result.Next() {
		if err := result.Scan(&actual.ID, &teamUUID, &actual.Name, &actual.Description, &actual.Members, &actual.ProjectKeys, &actual.RepoPatterns, &actual.IsActive, &actual.UpdatedAt, &actual.OrgID, &actual.Provider, &actual.NativeTeamKey, &actual.ParentTeamID); err != nil {
			return EffectConflict, err
		}
		actual.TeamUUID = teamUUID.String()
		found++
	}
	if err := result.Err(); err != nil {
		return EffectConflict, err
	}
	if found == 0 {
		return EffectAbsent, nil
	}
	// Members is intentionally excluded from this comparison when the
	// written row was not authoritative for it (a teams-only run carries
	// forward whatever roster happened to be persisted at write time, which
	// this readback cannot re-derive without racing the same read).
	if found != 1 || row.ID != actual.ID || row.TeamUUID != actual.TeamUUID || row.Name != actual.Name ||
		!reflect.DeepEqual(row.Description, actual.Description) || !reflect.DeepEqual(row.ProjectKeys, actual.ProjectKeys) ||
		!reflect.DeepEqual(row.RepoPatterns, actual.RepoPatterns) || row.IsActive != actual.IsActive ||
		row.OrgID != actual.OrgID || row.Provider != actual.Provider ||
		!reflect.DeepEqual(row.NativeTeamKey, actual.NativeTeamKey) || !reflect.DeepEqual(row.ParentTeamID, actual.ParentTeamID) ||
		!row.UpdatedAt.Equal(actual.UpdatedAt) {
		return EffectConflict, nil
	}
	if row.MembersAuthoritative && !reflect.DeepEqual(row.Members, actual.Members) {
		return EffectConflict, nil
	}
	return EffectExact, nil
}

func (sink GitLabTeamCatalogClickHouseEffects) inspectOwnership(ctx context.Context, claim Claim, row gitlabTeamCatalogOwnershipRow) (EffectInspection, error) {
	// valid_from is part of team_project_ownership's ORDER BY key
	// (org_id, provider, project_id, team_id, source, valid_from), so a
	// fresh sync's new valid_from does NOT ReplacingMergeTree-collide with
	// an older row for the same (team, project) pair -- both survive under
	// FINAL as distinct row VERSIONS, exactly like team_autoimport_gitlab.py
	// has always produced (it also stamps valid_from=now() unconditionally
	// on every sync, never reusing a prior open interval). Filtering by the
	// row's own valid_from scopes this readback to the exact version this
	// write produced, not the whole accumulated history.
	// toUnixTimestamp64Milli both sides rather than comparing valid_from
	// directly: a raw time.Time query PARAMETER (as opposed to a typed
	// PrepareBatch column Append) has no reliable declared precision/
	// timezone tag to match DateTime64(3, 'UTC') against, so `valid_from = ?`
	// can silently match zero rows even though the value is stored exactly
	// as written (verified directly against the real local stack, CHAOS-4432
	// live proof). Millisecond-integer equality sidesteps that ambiguity.
	result, err := sink.Conn.Query(ctx, `SELECT org_id, provider, team_id, project_id, project_key, source, is_primary, specificity, priority, valid_from, valid_to, updated_at FROM team_project_ownership FINAL WHERE org_id = ? AND provider = ? AND team_id = ? AND project_id = ? AND source = ? AND toUnixTimestamp64Milli(valid_from) = ?`, claim.OrgID, gitlabTeamCatalogProvider, row.TeamID, row.ProjectID, gitlabTeamCatalogSource, row.ValidFrom.UnixMilli())
	if err != nil {
		return EffectConflict, err
	}
	defer result.Close()
	var actual gitlabTeamCatalogOwnershipRow
	found := 0
	for result.Next() {
		if err := result.Scan(&actual.OrgID, &actual.Provider, &actual.TeamID, &actual.ProjectID, &actual.ProjectKey, &actual.Source, &actual.IsPrimary, &actual.Specificity, &actual.Priority, &actual.ValidFrom, &actual.ValidTo, &actual.UpdatedAt); err != nil {
			return EffectConflict, err
		}
		found++
	}
	if err := result.Err(); err != nil {
		return EffectConflict, err
	}
	if found == 0 {
		return EffectAbsent, nil
	}
	if found != 1 || !equalGitLabOwnership(row, actual) {
		return EffectConflict, nil
	}
	return EffectExact, nil
}

func (sink GitLabTeamCatalogClickHouseEffects) inspectMembership(ctx context.Context, claim Claim, row gitlabTeamCatalogMembershipRow) (EffectInspection, error) {
	// Same valid_from-in-sort-key reasoning as inspectOwnership above:
	// team_memberships' ORDER BY is (org_id, provider, team_id, member_id,
	// source, valid_from).
	result, err := sink.Conn.Query(ctx, `SELECT org_id, provider, team_id, member_id, raw_provider_user_id, raw_email, identity_facets, source, is_primary, specificity, priority, valid_from, valid_to, updated_at FROM team_memberships FINAL WHERE org_id = ? AND provider = ? AND team_id = ? AND member_id = ? AND source = ? AND toUnixTimestamp64Milli(valid_from) = ?`, claim.OrgID, gitlabTeamCatalogProvider, row.TeamID, row.MemberID, gitlabTeamCatalogSource, row.ValidFrom.UnixMilli())
	if err != nil {
		return EffectConflict, err
	}
	defer result.Close()
	var actual gitlabTeamCatalogMembershipRow
	found := 0
	for result.Next() {
		if err := result.Scan(&actual.OrgID, &actual.Provider, &actual.TeamID, &actual.MemberID, &actual.RawProviderUserID, &actual.RawEmail, &actual.IdentityFacets, &actual.Source, &actual.IsPrimary, &actual.Specificity, &actual.Priority, &actual.ValidFrom, &actual.ValidTo, &actual.UpdatedAt); err != nil {
			return EffectConflict, err
		}
		found++
	}
	if err := result.Err(); err != nil {
		return EffectConflict, err
	}
	if found == 0 {
		return EffectAbsent, nil
	}
	if found != 1 || !equalGitLabMembership(row, actual) {
		return EffectConflict, nil
	}
	return EffectExact, nil
}

func (sink GitLabTeamCatalogClickHouseEffects) inspectProject(ctx context.Context, claim Claim, row gitlabTeamCatalogProjectRow) (EffectInspection, error) {
	result, err := sink.Conn.Query(ctx, `SELECT id, org_id, provider, project_key, name, is_active, state, target_date, url, team_ids, team_keys, lead_id, lead_name, lead_email, updated_at, last_synced FROM projects FINAL WHERE org_id = ? AND provider = ? AND id = ?`, claim.OrgID, gitlabTeamCatalogProvider, row.ID)
	if err != nil {
		return EffectConflict, err
	}
	defer result.Close()
	var actual gitlabTeamCatalogProjectRow
	found := 0
	for result.Next() {
		if err := result.Scan(&actual.ID, &actual.OrgID, &actual.Provider, &actual.ProjectKey, &actual.Name, &actual.IsActive, &actual.State, &actual.TargetDate, &actual.URL, &actual.TeamIDs, &actual.TeamKeys, &actual.LeadID, &actual.LeadName, &actual.LeadEmail, &actual.UpdatedAt, &actual.LastSynced); err != nil {
			return EffectConflict, err
		}
		found++
	}
	if err := result.Err(); err != nil {
		return EffectConflict, err
	}
	if found == 0 {
		return EffectAbsent, nil
	}
	if found != 1 || !equalGitLabProject(row, actual) {
		return EffectConflict, nil
	}
	return EffectExact, nil
}

func inspectGitLabTeamCatalogRows[T any](rows []T, inspect func(T) (EffectInspection, error)) (EffectInspection, error) {
	if len(rows) == 0 {
		return EffectAbsent, nil
	}
	exact, absent := 0, 0
	for _, row := range rows {
		inspection, err := inspect(row)
		if err != nil {
			return EffectConflict, err
		}
		switch inspection {
		case EffectExact:
			exact++
		case EffectAbsent:
			absent++
		default:
			return EffectConflict, nil
		}
	}
	if exact == len(rows) {
		return EffectExact, nil
	}
	if absent == len(rows) {
		return EffectAbsent, nil
	}
	return EffectConflict, nil
}

func equalGitLabOwnership(left, right gitlabTeamCatalogOwnershipRow) bool {
	return left.OrgID == right.OrgID && left.Provider == right.Provider && left.TeamID == right.TeamID &&
		left.ProjectID == right.ProjectID && reflect.DeepEqual(left.ProjectKey, right.ProjectKey) && left.Source == right.Source &&
		left.IsPrimary == right.IsPrimary && left.Specificity == right.Specificity && left.Priority == right.Priority &&
		left.ValidFrom.Equal(right.ValidFrom) && reflect.DeepEqual(left.ValidTo, right.ValidTo) && left.UpdatedAt.Equal(right.UpdatedAt)
}

func equalGitLabMembership(left, right gitlabTeamCatalogMembershipRow) bool {
	return left.OrgID == right.OrgID && left.Provider == right.Provider && left.TeamID == right.TeamID &&
		left.MemberID == right.MemberID && reflect.DeepEqual(left.RawProviderUserID, right.RawProviderUserID) &&
		reflect.DeepEqual(left.RawEmail, right.RawEmail) && reflect.DeepEqual(left.IdentityFacets, right.IdentityFacets) &&
		left.Source == right.Source && left.IsPrimary == right.IsPrimary && left.Specificity == right.Specificity &&
		left.Priority == right.Priority && left.ValidFrom.Equal(right.ValidFrom) &&
		reflect.DeepEqual(left.ValidTo, right.ValidTo) && left.UpdatedAt.Equal(right.UpdatedAt)
}

func equalGitLabProject(left, right gitlabTeamCatalogProjectRow) bool {
	return left.ID == right.ID && left.OrgID == right.OrgID && left.Provider == right.Provider &&
		reflect.DeepEqual(left.ProjectKey, right.ProjectKey) && left.Name == right.Name && left.IsActive == right.IsActive &&
		left.State == right.State && gitlabTargetDateEqual(left.TargetDate, right.TargetDate) && left.URL == right.URL &&
		reflect.DeepEqual(left.TeamIDs, right.TeamIDs) && reflect.DeepEqual(left.TeamKeys, right.TeamKeys) &&
		reflect.DeepEqual(left.LeadID, right.LeadID) && reflect.DeepEqual(left.LeadName, right.LeadName) &&
		reflect.DeepEqual(left.LeadEmail, right.LeadEmail) && left.UpdatedAt.Equal(right.UpdatedAt) &&
		left.LastSynced.Equal(right.LastSynced)
}

func gitlabTargetDateEqual(left, right *time.Time) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return left.Equal(*right)
}

var _ EffectSink = GitLabTeamCatalogClickHouseEffects{}
var _ EffectReadback = GitLabTeamCatalogClickHouseEffects{}
