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

// JiraTeamCatalogClickHouseEffects is the concrete bridge from the
// collector's typed rows to the durable tables, mirroring
// GitLabTeamCatalogClickHouseEffects's shape against the SAME physical
// tables (`teams`, `team_project_ownership`, `team_memberships`, `projects`,
// `sprints`). manual_members carry-forward reuses the shared
// PreserveExistingTeamManualMembers helper every native provider's teams
// writer shares; roster preservation for a teams-only run reuses the
// shared PreserveExistingTeamMembersRoster helper (unlike GitLab, which
// still carries its own provider-local duplicate of that read).
type JiraTeamCatalogClickHouseEffects struct {
	Conn  driver.Conn
	Lease providerfoundation.LeaseGuard
}

const jiraTeamCatalogTeamsInsert = `INSERT INTO teams (id, team_uuid, name, description, members, manual_members, project_keys, repo_patterns, is_active, updated_at, org_id, provider, native_team_key, parent_team_id)`
const jiraTeamCatalogOwnershipInsert = `INSERT INTO team_project_ownership (org_id, provider, team_id, project_id, project_key, source, is_primary, specificity, priority, valid_from, valid_to, updated_at)`
const jiraTeamCatalogMembershipsInsert = `INSERT INTO team_memberships (org_id, provider, team_id, member_id, raw_provider_user_id, raw_email, identity_facets, source, is_primary, specificity, priority, valid_from, valid_to, updated_at)`
const jiraTeamCatalogProjectsInsert = `INSERT INTO projects (id, org_id, provider, project_key, name, is_active, state, target_date, url, team_ids, team_keys, lead_id, lead_name, lead_email, updated_at, last_synced)`

func (sink JiraTeamCatalogClickHouseEffects) WriteEffect(ctx context.Context, claim Claim, effect EffectBatch) error {
	if err := sink.validateRequest(ctx, claim, effect); err != nil {
		return err
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return err
	}
	switch effect.Destination {
	case jiraTeamCatalogTeamsDestination:
		rows, err := decodeEffectRows[jiraTeamCatalogTeamRow](effect)
		if err != nil {
			return err
		}
		for _, row := range rows {
			if err := validateJiraTeamRow(claim, row); err != nil {
				return err
			}
		}
		return sink.writeTeams(ctx, claim, rows)
	case jiraTeamCatalogOwnershipDestination:
		rows, err := decodeEffectRows[jiraTeamCatalogOwnershipRow](effect)
		if err != nil {
			return err
		}
		for _, row := range rows {
			if err := validateJiraOwnershipRow(claim, row); err != nil {
				return err
			}
		}
		return sink.writeOwnership(ctx, rows)
	case jiraTeamCatalogMembershipsDestination:
		rows, err := decodeEffectRows[jiraTeamCatalogMembershipRow](effect)
		if err != nil {
			return err
		}
		for _, row := range rows {
			if err := validateJiraMembershipRow(claim, row); err != nil {
				return err
			}
		}
		return sink.writeMemberships(ctx, rows)
	case jiraTeamCatalogProjectsDestination:
		rows, err := decodeEffectRows[jiraTeamCatalogProjectRow](effect)
		if err != nil {
			return err
		}
		for _, row := range rows {
			if err := row.validate(claim); err != nil {
				return err
			}
		}
		return sink.writeProjects(ctx, rows)
	case jiraTeamCatalogSprintsDestination:
		rows, err := decodeEffectRows[jiraSprintRow](effect)
		if err != nil {
			return err
		}
		for _, row := range rows {
			if err := validateJiraSprint(row, claim); err != nil {
				return err
			}
		}
		return sink.writeSprints(ctx, rows)
	default:
		return ErrInvalidConfiguration
	}
}

func (sink JiraTeamCatalogClickHouseEffects) InspectEffect(ctx context.Context, claim Claim, effect EffectBatch) (EffectInspection, error) {
	if err := sink.validateRequest(ctx, claim, effect); err != nil {
		return EffectConflict, err
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return EffectConflict, err
	}
	switch effect.Destination {
	case jiraTeamCatalogTeamsDestination:
		expected, err := decodeEffectRows[jiraTeamCatalogTeamRow](effect)
		if err != nil {
			return EffectConflict, err
		}
		return inspectJiraTeamCatalogRows(expected, func(row jiraTeamCatalogTeamRow) (EffectInspection, error) {
			return sink.inspectTeam(ctx, claim, row)
		})
	case jiraTeamCatalogOwnershipDestination:
		expected, err := decodeEffectRows[jiraTeamCatalogOwnershipRow](effect)
		if err != nil {
			return EffectConflict, err
		}
		return inspectJiraTeamCatalogRows(expected, func(row jiraTeamCatalogOwnershipRow) (EffectInspection, error) {
			return sink.inspectOwnership(ctx, claim, row)
		})
	case jiraTeamCatalogMembershipsDestination:
		expected, err := decodeEffectRows[jiraTeamCatalogMembershipRow](effect)
		if err != nil {
			return EffectConflict, err
		}
		return inspectJiraTeamCatalogRows(expected, func(row jiraTeamCatalogMembershipRow) (EffectInspection, error) {
			return sink.inspectMembership(ctx, claim, row)
		})
	case jiraTeamCatalogProjectsDestination:
		expected, err := decodeEffectRows[jiraTeamCatalogProjectRow](effect)
		if err != nil {
			return EffectConflict, err
		}
		return inspectJiraTeamCatalogRows(expected, func(row jiraTeamCatalogProjectRow) (EffectInspection, error) {
			return sink.inspectProject(ctx, claim, row)
		})
	case jiraTeamCatalogSprintsDestination:
		expected, err := decodeEffectRows[jiraSprintRow](effect)
		if err != nil {
			return EffectConflict, err
		}
		return inspectJiraTeamCatalogRows(expected, func(row jiraSprintRow) (EffectInspection, error) {
			return sink.inspectSprint(ctx, claim, row)
		})
	default:
		return EffectConflict, ErrInvalidConfiguration
	}
}

// validateRequest deliberately does NOT call claim.Validate() -- see
// GitLabTeamCatalogClickHouseEffects.validateRequest's identical doc
// comment: this write path is claim-free (CHAOS-4431 ruling, option (c)),
// with no lease or claimed provider-unit behind it.
func (sink JiraTeamCatalogClickHouseEffects) validateRequest(ctx context.Context, claim Claim, effect EffectBatch) error {
	if ctx == nil || sink.Lease == nil || sink.Conn == nil ||
		claim.Provider != jiraTeamCatalogProvider || strings.TrimSpace(claim.OrgID) == "" ||
		effect.Recovery != EffectReadbackRequired ||
		!validDigest(effect.ContentDigest) || effect.PayloadBytes < 0 || !jiraTeamCatalogDestination(effect.Destination) {
		return ErrInvalidConfiguration
	}
	return nil
}

func jiraTeamCatalogDestination(destination string) bool {
	switch destination {
	case jiraTeamCatalogTeamsDestination, jiraTeamCatalogOwnershipDestination,
		jiraTeamCatalogMembershipsDestination, jiraTeamCatalogProjectsDestination, jiraTeamCatalogSprintsDestination:
		return true
	default:
		return false
	}
}

func (sink JiraTeamCatalogClickHouseEffects) writeTeams(ctx context.Context, claim Claim, rows []jiraTeamCatalogTeamRow) error {
	if len(rows) == 0 {
		return nil
	}
	teamIDs := make([]string, 0, len(rows))
	for _, row := range rows {
		teamIDs = append(teamIDs, row.ID)
	}
	existingManualMembers, err := PreserveExistingTeamManualMembers(ctx, sink.Conn, claim.OrgID, teamIDs)
	if err != nil {
		return err
	}
	batch, err := sink.Conn.PrepareBatch(ctx, jiraTeamCatalogTeamsInsert)
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

func (sink JiraTeamCatalogClickHouseEffects) writeOwnership(ctx context.Context, rows []jiraTeamCatalogOwnershipRow) error {
	if len(rows) == 0 {
		return nil
	}
	batch, err := sink.Conn.PrepareBatch(ctx, jiraTeamCatalogOwnershipInsert)
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

func (sink JiraTeamCatalogClickHouseEffects) writeMemberships(ctx context.Context, rows []jiraTeamCatalogMembershipRow) error {
	if len(rows) == 0 {
		return nil
	}
	batch, err := sink.Conn.PrepareBatch(ctx, jiraTeamCatalogMembershipsInsert)
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

func (sink JiraTeamCatalogClickHouseEffects) writeProjects(ctx context.Context, rows []jiraTeamCatalogProjectRow) error {
	if len(rows) == 0 {
		return nil
	}
	batch, err := sink.Conn.PrepareBatch(ctx, jiraTeamCatalogProjectsInsert)
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

// writeSprints shares the `sprints` table and its INSERT column list with
// GitHubSprintsClickHouseAdapter (gitHubSprintsInsert,
// github_work_items_direct_effects_clickhouse.go) rather than duplicating
// them -- jiraSprintRow is a type alias for githubSprintRow (see
// jira_work_items_rows.go), so the same columns and row shape apply
// unchanged. Mirrors LinearReferenceCatalogClickHouseEffects.writeSprints
// exactly.
func (sink JiraTeamCatalogClickHouseEffects) writeSprints(ctx context.Context, rows []jiraSprintRow) error {
	if len(rows) == 0 {
		return nil
	}
	batch, err := sink.Conn.PrepareBatch(ctx, gitHubSprintsInsert)
	if err != nil {
		return err
	}
	defer batch.Abort()
	for _, row := range rows {
		if err := batch.Append(row.Provider, row.SprintID, row.NativeTeamKey, row.Name, row.State, row.StartedAt, row.EndedAt, row.CompletedAt, row.LastSynced, row.OrgID); err != nil {
			return err
		}
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return err
	}
	return batch.Send()
}

func (sink JiraTeamCatalogClickHouseEffects) inspectTeam(ctx context.Context, claim Claim, row jiraTeamCatalogTeamRow) (EffectInspection, error) {
	result, err := sink.Conn.Query(ctx, `SELECT id, team_uuid, name, description, members, project_keys, repo_patterns, is_active, updated_at, org_id, provider, native_team_key, parent_team_id FROM teams FINAL WHERE org_id = ? AND provider = ? AND id = ?`, claim.OrgID, jiraTeamCatalogProvider, row.ID)
	if err != nil {
		return EffectConflict, err
	}
	defer result.Close()
	var actual jiraTeamCatalogTeamRow
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
	if found != 1 || row.ID != actual.ID || row.TeamUUID != actual.TeamUUID || row.Name != actual.Name ||
		!reflect.DeepEqual(row.Description, actual.Description) || !reflect.DeepEqual(row.ProjectKeys, actual.ProjectKeys) ||
		!reflect.DeepEqual(row.RepoPatterns, actual.RepoPatterns) || row.IsActive != actual.IsActive ||
		row.OrgID != actual.OrgID || row.Provider != actual.Provider ||
		!reflect.DeepEqual(row.NativeTeamKey, actual.NativeTeamKey) || !reflect.DeepEqual(row.ParentTeamID, actual.ParentTeamID) ||
		!row.UpdatedAt.Equal(actual.UpdatedAt) {
		return EffectConflict, nil
	}
	if !reflect.DeepEqual(row.Members, actual.Members) {
		return EffectConflict, nil
	}
	return EffectExact, nil
}

func (sink JiraTeamCatalogClickHouseEffects) inspectOwnership(ctx context.Context, claim Claim, row jiraTeamCatalogOwnershipRow) (EffectInspection, error) {
	result, err := sink.Conn.Query(ctx, `SELECT org_id, provider, team_id, project_id, project_key, source, is_primary, specificity, priority, valid_from, valid_to, updated_at FROM team_project_ownership FINAL WHERE org_id = ? AND provider = ? AND team_id = ? AND project_id = ? AND source = ? AND toUnixTimestamp64Milli(valid_from) = ?`, claim.OrgID, jiraTeamCatalogProvider, row.TeamID, row.ProjectID, row.Source, row.ValidFrom.UnixMilli())
	if err != nil {
		return EffectConflict, err
	}
	defer result.Close()
	var actual jiraTeamCatalogOwnershipRow
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
	if found != 1 || !equalJiraOwnership(row, actual) {
		return EffectConflict, nil
	}
	return EffectExact, nil
}

func (sink JiraTeamCatalogClickHouseEffects) inspectMembership(ctx context.Context, claim Claim, row jiraTeamCatalogMembershipRow) (EffectInspection, error) {
	result, err := sink.Conn.Query(ctx, `SELECT org_id, provider, team_id, member_id, raw_provider_user_id, raw_email, identity_facets, source, is_primary, specificity, priority, valid_from, valid_to, updated_at FROM team_memberships FINAL WHERE org_id = ? AND provider = ? AND team_id = ? AND member_id = ? AND source = ? AND toUnixTimestamp64Milli(valid_from) = ?`, claim.OrgID, jiraTeamCatalogProvider, row.TeamID, row.MemberID, row.Source, row.ValidFrom.UnixMilli())
	if err != nil {
		return EffectConflict, err
	}
	defer result.Close()
	var actual jiraTeamCatalogMembershipRow
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
	if found != 1 || !equalJiraMembership(row, actual) {
		return EffectConflict, nil
	}
	return EffectExact, nil
}

func (sink JiraTeamCatalogClickHouseEffects) inspectProject(ctx context.Context, claim Claim, row jiraTeamCatalogProjectRow) (EffectInspection, error) {
	result, err := sink.Conn.Query(ctx, `SELECT id, org_id, provider, project_key, name, is_active, state, target_date, url, team_ids, team_keys, lead_id, lead_name, lead_email, updated_at, last_synced FROM projects FINAL WHERE org_id = ? AND provider = ? AND id = ?`, claim.OrgID, jiraTeamCatalogProvider, row.ID)
	if err != nil {
		return EffectConflict, err
	}
	defer result.Close()
	var actual jiraTeamCatalogProjectRow
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
	if found != 1 || !equalJiraProject(row, actual) {
		return EffectConflict, nil
	}
	return EffectExact, nil
}

func (sink JiraTeamCatalogClickHouseEffects) inspectSprint(ctx context.Context, claim Claim, row jiraSprintRow) (EffectInspection, error) {
	result, err := sink.Conn.Query(ctx, gitHubSprintsSelect, claim.OrgID, jiraTeamCatalogProvider, row.SprintID)
	if err != nil {
		return EffectConflict, err
	}
	defer result.Close()
	var actual jiraSprintRow
	found := 0
	for result.Next() {
		if err := result.Scan(&actual.Provider, &actual.SprintID, &actual.NativeTeamKey, &actual.Name, &actual.State, &actual.StartedAt, &actual.EndedAt, &actual.CompletedAt, &actual.LastSynced, &actual.OrgID); err != nil {
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
	if found != 1 || !equalJiraSprint(row, actual) {
		return EffectConflict, nil
	}
	return EffectExact, nil
}

func inspectJiraTeamCatalogRows[T any](rows []T, inspect func(T) (EffectInspection, error)) (EffectInspection, error) {
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

func equalJiraOwnership(left, right jiraTeamCatalogOwnershipRow) bool {
	return left.OrgID == right.OrgID && left.Provider == right.Provider && left.TeamID == right.TeamID &&
		left.ProjectID == right.ProjectID && reflect.DeepEqual(left.ProjectKey, right.ProjectKey) && left.Source == right.Source &&
		left.IsPrimary == right.IsPrimary && left.Specificity == right.Specificity && left.Priority == right.Priority &&
		left.ValidFrom.Equal(right.ValidFrom) && reflect.DeepEqual(left.ValidTo, right.ValidTo) && left.UpdatedAt.Equal(right.UpdatedAt)
}

func equalJiraMembership(left, right jiraTeamCatalogMembershipRow) bool {
	return left.OrgID == right.OrgID && left.Provider == right.Provider && left.TeamID == right.TeamID &&
		left.MemberID == right.MemberID && reflect.DeepEqual(left.RawProviderUserID, right.RawProviderUserID) &&
		reflect.DeepEqual(left.RawEmail, right.RawEmail) && reflect.DeepEqual(left.IdentityFacets, right.IdentityFacets) &&
		left.Source == right.Source && left.IsPrimary == right.IsPrimary && left.Specificity == right.Specificity &&
		left.Priority == right.Priority && left.ValidFrom.Equal(right.ValidFrom) &&
		reflect.DeepEqual(left.ValidTo, right.ValidTo) && left.UpdatedAt.Equal(right.UpdatedAt)
}

func equalJiraProject(left, right jiraTeamCatalogProjectRow) bool {
	return left.ID == right.ID && left.OrgID == right.OrgID && left.Provider == right.Provider &&
		reflect.DeepEqual(left.ProjectKey, right.ProjectKey) && left.Name == right.Name && left.IsActive == right.IsActive &&
		left.State == right.State && jiraTargetDateEqual(left.TargetDate, right.TargetDate) && left.URL == right.URL &&
		reflect.DeepEqual(left.TeamIDs, right.TeamIDs) && reflect.DeepEqual(left.TeamKeys, right.TeamKeys) &&
		reflect.DeepEqual(left.LeadID, right.LeadID) && reflect.DeepEqual(left.LeadName, right.LeadName) &&
		reflect.DeepEqual(left.LeadEmail, right.LeadEmail) && left.UpdatedAt.Equal(right.UpdatedAt) &&
		left.LastSynced.Equal(right.LastSynced)
}

func equalJiraSprint(left, right jiraSprintRow) bool {
	return left.Provider == right.Provider && left.SprintID == right.SprintID &&
		reflect.DeepEqual(left.NativeTeamKey, right.NativeTeamKey) && reflect.DeepEqual(left.Name, right.Name) &&
		reflect.DeepEqual(left.State, right.State) && jiraTargetDateEqual(left.StartedAt, right.StartedAt) &&
		jiraTargetDateEqual(left.EndedAt, right.EndedAt) && jiraTargetDateEqual(left.CompletedAt, right.CompletedAt) &&
		left.LastSynced.Equal(right.LastSynced) && left.OrgID == right.OrgID
}

func jiraTargetDateEqual(left, right *time.Time) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return left.Equal(*right)
}

// jiraLegacyProjectOwnershipLinks ports team_autoimport_jira.
// _load_jira_legacy_links + its call site verbatim: a per-org carry-forward
// read of jira_project_ops_team_links (an admin-curated project->ops-team
// mapping that predates native discovery), producing a Projects row ONLY
// for a project_key not already covered by this run's native discovery, and
// an Ownership row (source="jira_legacy") unconditionally for every linked
// pair. Read failures are swallowed exactly like Python's own bare
// `except Exception: return []` -- a missing/broken legacy table must never
// fail an otherwise-healthy native sync.
func jiraLegacyProjectOwnershipLinks(
	ctx context.Context, conn driver.Conn, orgID string, normalizedAt time.Time,
) ([]jiraTeamCatalogProjectRow, []jiraTeamCatalogOwnershipRow, error) {
	if conn == nil || strings.TrimSpace(orgID) == "" {
		return nil, nil, ErrInvalidConfiguration
	}
	rows, err := conn.Query(ctx, `
SELECT project_key, ops_team_id, project_name
FROM jira_project_ops_team_links FINAL
WHERE org_id = {org_id:String}`,
		clickhouse.Named("org_id", orgID),
	)
	if err != nil {
		// Mirrors _load_jira_legacy_links's own swallow-and-return-[] --
		// a missing table (e.g. a fixture/test ClickHouse without this
		// migration applied) must never fail native discovery.
		return nil, nil, nil
	}
	defer rows.Close()
	projects := make([]jiraTeamCatalogProjectRow, 0)
	ownership := make([]jiraTeamCatalogOwnershipRow, 0)
	for rows.Next() {
		var projectKey, opsTeamID, projectName string
		if err := rows.Scan(&projectKey, &opsTeamID, &projectName); err != nil {
			return nil, nil, nil
		}
		projectKey = strings.TrimSpace(projectKey)
		opsTeamID = strings.TrimSpace(opsTeamID)
		if projectKey == "" || opsTeamID == "" {
			continue
		}
		name := strings.TrimSpace(projectName)
		if name == "" {
			name = projectKey
		}
		projects = append(projects, normalizeJiraProjectRow(orgID, projectKey, name, normalizedAt))
		key := projectKey
		ownership = append(ownership, jiraTeamCatalogOwnershipRow{
			OrgID: orgID, Provider: jiraTeamCatalogProvider, TeamID: opsTeamID,
			ProjectID: jiraProjectID(orgID, projectKey), ProjectKey: &key, Source: jiraTeamCatalogLegacySource,
			IsPrimary: 1, Specificity: jiraTeamCatalogLegacySpecificity, Priority: jiraTeamCatalogLegacyPriority,
			ValidFrom: normalizedAt, UpdatedAt: normalizedAt,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, nil, nil
	}
	return projects, ownership, nil
}

var _ EffectSink = JiraTeamCatalogClickHouseEffects{}
var _ EffectReadback = JiraTeamCatalogClickHouseEffects{}
