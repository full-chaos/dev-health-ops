package teamsidentity

import (
	"context"
	"errors"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/teamid"
)

// atlassianTeamARIPrefix starts the native_team_key of every Atlassian team
// row: internal/atlassianteams writes the team ARI there.
const atlassianTeamARIPrefix = "ari:cloud:identity::team/"

// storedAtlassianTeamsQuery lists the ACTIVE Atlassian teams of an
// organization that the team catalog already holds. A Jira project is not a
// team: a row whose native key is a project key (the retired project-as-team
// class) has no ARI there and is never listed.
const storedAtlassianTeamsQuery = "SELECT id, name, description, members, project_keys FROM teams FINAL " +
	"WHERE org_id = {org_id:String} AND provider = 'jira' AND is_active = 1 " +
	"AND startsWith(ifNull(native_team_key, ''), {ari_prefix:String}) ORDER BY id"

var errDiscoverJiraNoStore = errors.New("jira team discovery needs the team catalog store")

// discoverJira lists the Jira teams of the organization: the active
// Atlassian teams the Jira team catalog sync stored in ClickHouse. It makes
// no provider call. It used to list the Jira projects (GET
// /rest/api/3/project/search) as teams; an import of that list wrote one team
// per project, the project-as-team class the catalog now retires. The
// credential gives only associations.provider_org (the client's resolved base
// URL); the client sends no request.
func discoverJira(ctx context.Context, conn driver.Conn, orgID string, credential providerfoundation.Credential) ([]discoveredTeam, error) {
	if conn == nil {
		return nil, errDiscoverJiraNoStore
	}
	client, err := providerfoundation.NewJiraClient(credential, discoveryHTTPClient, providerfoundation.DefaultRetryPolicy(), alwaysValidLease{})
	if err != nil {
		return nil, err
	}
	providerOrg := ""
	if client.BaseURL != nil {
		providerOrg = client.BaseURL.String()
	}
	rows, err := conn.Query(ctx, storedAtlassianTeamsQuery,
		clickhouse.Named("org_id", orgID), clickhouse.Named("ari_prefix", atlassianTeamARIPrefix))
	if err != nil {
		return nil, fmt.Errorf("query stored atlassian teams: %w", err)
	}
	defer rows.Close()
	teams := []discoveredTeam{}
	for rows.Next() {
		var (
			id, name             string
			description          *string
			members, projectKeys []string
		)
		if err := rows.Scan(&id, &name, &description, &members, &projectKeys); err != nil {
			return nil, fmt.Errorf("scan stored atlassian team: %w", err)
		}
		if name == "" {
			name = teamid.Native("jira", id)
		}
		if projectKeys == nil {
			projectKeys = []string{}
		}
		memberCount := int64(len(members))
		associations := pyjson.NewObject()
		associations.Set("project_keys", projectKeys)
		associations.Set("provider_org", providerOrg)
		teams = append(teams, discoveredTeam{
			ProviderType:   "jira",
			ProviderTeamID: teamid.Native("jira", id),
			Name:           name,
			Description:    description,
			MemberCount:    &memberCount,
			Associations:   associations,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read stored atlassian teams: %w", err)
	}
	return teams, nil
}
