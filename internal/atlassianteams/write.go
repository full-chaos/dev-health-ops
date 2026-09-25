package atlassianteams

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/providersync"
)

// The column lists are the ones the project-as-team Jira catalog writes
// (providersync/jira_team_catalog_effects_clickhouse.go), so both writers fill
// the same physical tables the same way.
const (
	teamsInsert       = `INSERT INTO teams (id, team_uuid, name, description, members, manual_members, project_keys, repo_patterns, is_active, updated_at, org_id, provider, native_team_key, parent_team_id)`
	membershipsInsert = `INSERT INTO team_memberships (org_id, provider, team_id, member_id, raw_provider_user_id, raw_email, identity_facets, source, is_primary, specificity, priority, valid_from, valid_to, updated_at)`
	ownershipInsert   = `INSERT INTO team_project_ownership (org_id, provider, team_id, project_id, project_key, source, is_primary, specificity, priority, valid_from, valid_to, updated_at)`

	existingProjectKeysQuery = "SELECT id, project_keys FROM teams FINAL WHERE org_id = {org_id:String} AND provider = {provider:String} AND id IN {team_ids:Array(String)}"
)

// Result counts what a write retracted: rows of members or project links that
// an Atlassian team had before and this snapshot no longer has.
type Result struct {
	ExpiredMemberships int
	ExpiredOwnership   int
}

const (
	atlassianTeamARIPrefix = "ari:cloud:identity::team/"

	knownTeamsQuery      = "SELECT id FROM teams FINAL WHERE org_id = {org_id:String} AND provider = {provider:String} AND startsWith(ifNull(native_team_key, ''), {ari_prefix:String})"
	openMembershipsQuery = "SELECT team_id, member_id, raw_provider_user_id, raw_email, identity_facets, toString(source), is_primary, specificity, priority, valid_from " +
		"FROM team_memberships FINAL WHERE org_id = {org_id:String} AND provider = {provider:String} AND source = 'native' AND team_id IN {team_ids:Array(String)} AND valid_to IS NULL"
	openOwnershipQuery = "SELECT team_id, project_id, project_key, toString(source), is_primary, specificity, priority, valid_from " +
		"FROM team_project_ownership FINAL WHERE org_id = {org_id:String} AND provider = {provider:String} AND source = 'native' AND team_id IN {team_ids:Array(String)} AND valid_to IS NULL"
)

// Write stores the rows of one collection and retracts what the snapshot no
// longer has.
//
// The team catalog row is written LAST: ClickHouse has no transaction across
// tables, so a failure part way leaves memberships and project links without
// their catalog row (a re-run repairs it) rather than a listed team without
// its members. The error names the stages already committed.
//
// Retraction: the members and project links an Atlassian team held before and
// the snapshot omits (a person who left, a project it stopped working on, an
// archived or deleted team) are closed with a replacement row (valid_to = now,
// the same sort key), because the attribution loaders read valid_to. Only rows
// of Atlassian teams (their catalog row carries the team ARI) with source
// native are touched; the project-as-team rows never are. Members and links
// that stay keep their original valid_from, so a re-run replaces a row instead
// of adding one. Teams are written only when the structure was selected; an
// existing team's manual members are carried over, and its project keys when
// the project links were not read this run.
func Write(ctx context.Context, conn driver.Conn, orgID string, rows Rows, selections Selections) (Result, error) {
	var result Result
	if conn == nil || orgID == "" {
		return result, ErrConfiguration
	}
	scope, err := teamsInScope(ctx, conn, orgID, rows.Teams)
	if err != nil {
		return result, fmt.Errorf("read known atlassian teams: %w", err)
	}
	var memberships []MembershipRow
	var expiredMemberships []openMembership
	if selections.Members {
		if memberships, expiredMemberships, err = planMemberships(ctx, conn, orgID, scope, rows.Memberships); err != nil {
			return result, fmt.Errorf("read current team memberships: %w", err)
		}
	}
	var ownership []OwnershipRow
	var expiredOwnership []openOwnership
	if selections.Projects {
		if ownership, expiredOwnership, err = planOwnership(ctx, conn, orgID, scope, rows.Ownership); err != nil {
			return result, fmt.Errorf("read current team project ownership: %w", err)
		}
	}
	now := time.Time{}
	for _, team := range rows.Teams {
		now = team.UpdatedAt
		break
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	var done []string
	fail := func(stage string, err error) (Result, error) {
		if len(done) > 0 {
			return result, fmt.Errorf("%s: %w (already written: %s; a re-run completes the rest)", stage, err, strings.Join(done, ", "))
		}
		return result, fmt.Errorf("%s: %w", stage, err)
	}
	if selections.Members && (len(memberships) > 0 || len(expiredMemberships) > 0) {
		if err := writeMemberships(ctx, conn, orgID, memberships, expiredMemberships, now); err != nil {
			return fail("write team memberships", err)
		}
		result.ExpiredMemberships = len(expiredMemberships)
		done = append(done, "team memberships")
	}
	if selections.Projects && (len(ownership) > 0 || len(expiredOwnership) > 0) {
		if err := writeOwnership(ctx, conn, orgID, ownership, expiredOwnership, now); err != nil {
			return fail("write team project ownership", err)
		}
		result.ExpiredOwnership = len(expiredOwnership)
		done = append(done, "team project ownership")
	}
	if selections.Structure && len(rows.Teams) > 0 {
		if err := writeTeams(ctx, conn, orgID, rows.Teams, !selections.Projects); err != nil {
			return fail("write teams", err)
		}
	}
	return result, nil
}

// teamsInScope is every Atlassian team this run answers for: the ones the
// snapshot returned and the ones already in the catalog.
func teamsInScope(ctx context.Context, conn driver.Conn, orgID string, teams []TeamRow) ([]string, error) {
	seen := map[string]bool{}
	var ids []string
	for _, team := range teams {
		if !seen[team.ID] {
			seen[team.ID] = true
			ids = append(ids, team.ID)
		}
	}
	result, err := conn.Query(ctx, knownTeamsQuery,
		clickhouse.Named("org_id", orgID), clickhouse.Named("provider", Provider), clickhouse.Named("ari_prefix", atlassianTeamARIPrefix))
	if err != nil {
		return nil, err
	}
	defer result.Close()
	for result.Next() {
		var id string
		if err := result.Scan(&id); err != nil {
			return nil, err
		}
		if !seen[id] {
			seen[id] = true
			ids = append(ids, id)
		}
	}
	return ids, result.Err()
}

type openMembership struct {
	teamID, memberID  string
	rawProviderUserID *string
	rawEmail          *string
	identityFacets    []string
	source            string
	isPrimary         uint8
	specificity       uint16
	priority          int32
	validFrom         time.Time
}

type openOwnership struct {
	teamID, projectID string
	projectKey        *string
	source            string
	isPrimary         uint8
	specificity       uint16
	priority          int32
	validFrom         time.Time
}

// planMemberships reads the open memberships of the teams in scope, gives each
// fresh row the valid_from of the row it replaces, and returns the open rows
// the snapshot no longer has.
func planMemberships(ctx context.Context, conn driver.Conn, orgID string, scope []string, fresh []MembershipRow) ([]MembershipRow, []openMembership, error) {
	if len(scope) == 0 {
		return fresh, nil, nil
	}
	result, err := conn.Query(ctx, openMembershipsQuery,
		clickhouse.Named("org_id", orgID), clickhouse.Named("provider", Provider), clickhouse.Named("team_ids", scope))
	if err != nil {
		return nil, nil, err
	}
	defer result.Close()
	var open []openMembership
	for result.Next() {
		var row openMembership
		if err := result.Scan(&row.teamID, &row.memberID, &row.rawProviderUserID, &row.rawEmail, &row.identityFacets, &row.source,
			&row.isPrimary, &row.specificity, &row.priority, &row.validFrom); err != nil {
			return nil, nil, err
		}
		open = append(open, row)
	}
	if err := result.Err(); err != nil {
		return nil, nil, err
	}
	firstSeen := map[string]time.Time{}
	for _, row := range open {
		key := row.teamID + "\x00" + row.memberID
		if at, ok := firstSeen[key]; !ok || row.validFrom.Before(at) {
			firstSeen[key] = row.validFrom
		}
	}
	current := map[string]bool{}
	out := make([]MembershipRow, len(fresh))
	for i, row := range fresh {
		key := row.TeamID + "\x00" + row.MemberID
		current[key] = true
		if at, ok := firstSeen[key]; ok && at.Before(row.ValidFrom) {
			row.ValidFrom = at
		}
		out[i] = row
	}
	var expired []openMembership
	for _, row := range open {
		if !current[row.teamID+"\x00"+row.memberID] {
			expired = append(expired, row)
		}
	}
	return out, expired, nil
}

func planOwnership(ctx context.Context, conn driver.Conn, orgID string, scope []string, fresh []OwnershipRow) ([]OwnershipRow, []openOwnership, error) {
	if len(scope) == 0 {
		return fresh, nil, nil
	}
	result, err := conn.Query(ctx, openOwnershipQuery,
		clickhouse.Named("org_id", orgID), clickhouse.Named("provider", Provider), clickhouse.Named("team_ids", scope))
	if err != nil {
		return nil, nil, err
	}
	defer result.Close()
	var open []openOwnership
	for result.Next() {
		var row openOwnership
		if err := result.Scan(&row.teamID, &row.projectID, &row.projectKey, &row.source, &row.isPrimary, &row.specificity, &row.priority, &row.validFrom); err != nil {
			return nil, nil, err
		}
		open = append(open, row)
	}
	if err := result.Err(); err != nil {
		return nil, nil, err
	}
	firstSeen := map[string]time.Time{}
	for _, row := range open {
		key := row.teamID + "\x00" + row.projectID
		if at, ok := firstSeen[key]; !ok || row.validFrom.Before(at) {
			firstSeen[key] = row.validFrom
		}
	}
	current := map[string]bool{}
	out := make([]OwnershipRow, len(fresh))
	for i, row := range fresh {
		key := row.TeamID + "\x00" + row.ProjectID
		current[key] = true
		if at, ok := firstSeen[key]; ok && at.Before(row.ValidFrom) {
			row.ValidFrom = at
		}
		out[i] = row
	}
	var expired []openOwnership
	for _, row := range open {
		if !current[row.teamID+"\x00"+row.projectID] {
			expired = append(expired, row)
		}
	}
	return out, expired, nil
}

func writeTeams(ctx context.Context, conn driver.Conn, orgID string, teams []TeamRow, keepProjectKeys bool) error {
	ids := make([]string, len(teams))
	for i, team := range teams {
		ids[i] = team.ID
	}
	manual, err := providersync.PreserveExistingTeamManualMembers(ctx, conn, orgID, ids)
	if err != nil {
		return err
	}
	var existingKeys map[string][]string
	if keepProjectKeys {
		if existingKeys, err = readProjectKeys(ctx, conn, orgID, ids); err != nil {
			return err
		}
	}
	batch, err := conn.PrepareBatch(ctx, teamsInsert)
	if err != nil {
		return err
	}
	defer func() { _ = batch.Abort() }()
	for _, team := range teams {
		manualMembers := manual[team.ID]
		if manualMembers == nil {
			manualMembers = []string{}
		}
		keys := team.ProjectKeys
		if keepProjectKeys {
			keys = existingKeys[team.ID]
		}
		if keys == nil {
			keys = []string{}
		}
		nativeKey := team.NativeTeamKey
		if err := batch.Append(
			team.ID, team.TeamUUID, team.Name, team.Description, []string{}, manualMembers, keys, []string{},
			team.IsActive, team.UpdatedAt, team.OrgID, team.Provider, &nativeKey, (*string)(nil),
		); err != nil {
			return err
		}
	}
	return batch.Send()
}

func readProjectKeys(ctx context.Context, conn driver.Conn, orgID string, ids []string) (map[string][]string, error) {
	result, err := conn.Query(ctx, existingProjectKeysQuery,
		clickhouse.Named("org_id", orgID), clickhouse.Named("provider", Provider), clickhouse.Named("team_ids", ids))
	if err != nil {
		return nil, err
	}
	defer result.Close()
	keys := map[string][]string{}
	for result.Next() {
		var id string
		var projectKeys []string
		if err := result.Scan(&id, &projectKeys); err != nil {
			return nil, err
		}
		keys[id] = projectKeys
	}
	return keys, result.Err()
}

func writeMemberships(ctx context.Context, conn driver.Conn, orgID string, rows []MembershipRow, expired []openMembership, now time.Time) error {
	batch, err := conn.PrepareBatch(ctx, membershipsInsert)
	if err != nil {
		return err
	}
	defer func() { _ = batch.Abort() }()
	for _, row := range rows {
		raw := row.RawProviderUserID
		if err := batch.Append(
			row.OrgID, row.Provider, row.TeamID, row.MemberID, &raw, (*string)(nil), row.IdentityFacets, row.Source,
			row.IsPrimary, row.Specificity, row.Priority, row.ValidFrom, (*time.Time)(nil), row.UpdatedAt,
		); err != nil {
			return err
		}
	}
	// A closed replacement has the sort key of the open row it retracts.
	for _, row := range expired {
		closedAt := now
		if err := batch.Append(
			orgID, Provider, row.teamID, row.memberID, row.rawProviderUserID, row.rawEmail, row.identityFacets, row.source,
			row.isPrimary, row.specificity, row.priority, row.validFrom, &closedAt, now,
		); err != nil {
			return err
		}
	}
	return batch.Send()
}

func writeOwnership(ctx context.Context, conn driver.Conn, orgID string, rows []OwnershipRow, expired []openOwnership, now time.Time) error {
	batch, err := conn.PrepareBatch(ctx, ownershipInsert)
	if err != nil {
		return err
	}
	defer func() { _ = batch.Abort() }()
	for _, row := range rows {
		key := row.ProjectKey
		if err := batch.Append(
			row.OrgID, row.Provider, row.TeamID, row.ProjectID, &key, row.Source, row.IsPrimary, row.Specificity,
			row.Priority, row.ValidFrom, (*time.Time)(nil), row.UpdatedAt,
		); err != nil {
			return err
		}
	}
	for _, row := range expired {
		closedAt := now
		if err := batch.Append(
			orgID, Provider, row.teamID, row.projectID, row.projectKey, row.source, row.isPrimary, row.specificity,
			row.priority, row.validFrom, &closedAt, now,
		); err != nil {
			return err
		}
	}
	return batch.Send()
}
