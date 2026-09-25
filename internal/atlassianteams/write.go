package atlassianteams

import (
	"context"
	"fmt"
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

// Write stores the rows of one collection. Teams are written only when the
// structure was selected; an existing team's manual members are carried over,
// and its project keys when the project links were not read this run.
func Write(ctx context.Context, conn driver.Conn, orgID string, rows Rows, selections Selections) error {
	if conn == nil || orgID == "" {
		return ErrConfiguration
	}
	if selections.Structure && len(rows.Teams) > 0 {
		if err := writeTeams(ctx, conn, orgID, rows.Teams, !selections.Projects); err != nil {
			return fmt.Errorf("write teams: %w", err)
		}
	}
	if selections.Members && len(rows.Memberships) > 0 {
		if err := writeMemberships(ctx, conn, rows.Memberships); err != nil {
			return fmt.Errorf("write team memberships: %w", err)
		}
	}
	if selections.Projects && len(rows.Ownership) > 0 {
		if err := writeOwnership(ctx, conn, rows.Ownership); err != nil {
			return fmt.Errorf("write team project ownership: %w", err)
		}
	}
	return nil
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

func writeMemberships(ctx context.Context, conn driver.Conn, rows []MembershipRow) error {
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
	return batch.Send()
}

func writeOwnership(ctx context.Context, conn driver.Conn, rows []OwnershipRow) error {
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
	return batch.Send()
}
