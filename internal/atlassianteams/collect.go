// Package atlassianteams syncs Atlassian Teams (the organization's real teams,
// not Jira projects) into the ClickHouse team dimensions: the team catalog
// (teams), who is on each team (team_memberships) and which Jira projects a
// team actively works on (team_project_ownership). The data comes from the
// full-chaos/atlassian client (vendored under third_party/atlassian): the
// teamSearchV2 search for the teams, and the Teamwork Graph for each team's
// users and active projects.
//
// Nothing in the Python api ever read Atlassian Teams: its `sync teams
// --provider jira` and the worker's Jira auto-import both treat a Jira PROJECT
// as the team. Those project-as-team rows (team id = the project key) are a
// separate option and are never touched here: an Atlassian team's id is the
// uuid of its ARI, so the two id spaces cannot collide.
package atlassianteams

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"atlassian/atlassian"

	"github.com/full-chaos/dev-health-ops/internal/identityalias"
)

// Provider is the provider identity every row is written under: the same as
// the project-as-team rows of the Jira catalog.
const Provider = "jira"

// Source is the `source` of every membership and ownership row. It is the
// existing 'native' enum value (the provider's own team structure): the
// ClickHouse enum has no room for another value without a migration.
const Source = "native"

// Specificity and Priority order an Atlassian team's project ownership against
// the project-as-team owner of the same project (native, 100/10): the real
// team outranks the project standing in for one.
const (
	OwnershipSpecificity  = 110
	OwnershipPriority     = 10
	MembershipSpecificity = 100
	MembershipPriority    = 10
)

const (
	teamARIPrefix = "team/"
	userARIPrefix = "user/"
	defaultPage   = 50
)

// Client is the part of the atlassian graph client the sync reads. The
// package's *graph.Client satisfies it; tests serve a fake gateway to the
// real client.
type Client interface {
	SearchTeams(ctx context.Context, organizationID, siteID, query string, pageSize int) ([]atlassian.AtlassianTeam, error)
	IterTeamUsers(ctx context.Context, teamID string, pageSize int) ([]atlassian.TeamworkUserRelation, error)
	IterTeamActiveProjects(ctx context.Context, teamID string, pageSize int) ([]atlassian.TeamworkProject, error)
}

// Selections says which of the three dimensions a run reads and writes.
type Selections struct {
	Structure bool // the teams themselves
	Members   bool // team_memberships
	Projects  bool // team_project_ownership
}

// Params are the inputs of one collection.
type Params struct {
	OrgID          string
	OrganizationID string // the Atlassian organization id (ATLASSIAN_ORGANIZATION_ID)
	SiteID         string // the Atlassian cloud id (ATLASSIAN_CLOUD_ID)
	Selections     Selections
	PageSize       int
	Now            time.Time
	Resolver       *identityalias.Resolver
}

// TeamRow is one `teams` row.
type TeamRow struct {
	ID            string
	TeamUUID      uuid.UUID
	Name          string
	Description   *string
	ProjectKeys   []string
	IsActive      uint8
	UpdatedAt     time.Time
	OrgID         string
	Provider      string
	NativeTeamKey string
}

// MembershipRow is one `team_memberships` row.
type MembershipRow struct {
	OrgID             string
	Provider          string
	TeamID            string
	MemberID          string
	RawProviderUserID string
	IdentityFacets    []string
	Source            string
	IsPrimary         uint8
	Specificity       uint16
	Priority          int32
	ValidFrom         time.Time
	UpdatedAt         time.Time
}

// OwnershipRow is one `team_project_ownership` row.
type OwnershipRow struct {
	OrgID       string
	Provider    string
	TeamID      string
	ProjectID   string
	ProjectKey  string
	Source      string
	IsPrimary   uint8
	Specificity uint16
	Priority    int32
	ValidFrom   time.Time
	UpdatedAt   time.Time
}

// Rows is what one collection produced.
type Rows struct {
	Teams       []TeamRow
	Memberships []MembershipRow
	Ownership   []OwnershipRow
	// SkippedProjects counts project links dropped because the graph node
	// carried no Jira project key (the project id of the ownership row is
	// built from it).
	SkippedProjects int
}

// ErrConfiguration marks an input the sync cannot run without.
var ErrConfiguration = errors.New("atlassianteams: configuration")

// teamID is the team id of a row: the uuid of the team's ARI
// (ari:cloud:identity::team/<uuid>), lower-cased.
func teamID(ari string) (string, error) {
	ari = strings.TrimSpace(ari)
	i := strings.LastIndex(ari, teamARIPrefix)
	if i < 0 || !strings.HasPrefix(ari, "ari:") {
		return "", fmt.Errorf("team id %q is not a team ARI", ari)
	}
	id := strings.ToLower(strings.TrimSpace(ari[i+len(teamARIPrefix):]))
	if id == "" {
		return "", fmt.Errorf("team id %q has no uuid", ari)
	}
	return id, nil
}

// accountID is the Atlassian account id of a Teamwork Graph user node id
// (ari:cloud:identity::user/<accountId>). The package's mapper returns the
// node ARI, not the bare account id.
func accountID(nodeID string) (string, bool) {
	nodeID = strings.TrimSpace(nodeID)
	if i := strings.LastIndex(nodeID, userARIPrefix); i >= 0 && strings.HasPrefix(nodeID, "ari:") {
		nodeID = nodeID[i+len(userARIPrefix):]
	}
	nodeID = strings.TrimSpace(nodeID)
	return nodeID, nodeID != ""
}

// memberID is the member id the Jira auto-import writes: "jira:" and the
// lower-cased account id, so a person is one member across both.
func memberID(account string) string {
	return "jira:" + strings.ToLower(strings.TrimSpace(account))
}

// Collect reads the selected dimensions of every Atlassian team and returns
// the rows to write. It is all or nothing: any read that fails fails the run
// (a membership list cut short by an error would read as members who left).
func Collect(ctx context.Context, client Client, params Params) (Rows, error) {
	if client == nil || strings.TrimSpace(params.OrgID) == "" || strings.TrimSpace(params.OrganizationID) == "" || strings.TrimSpace(params.SiteID) == "" {
		return Rows{}, ErrConfiguration
	}
	if !params.Selections.Structure && !params.Selections.Members && !params.Selections.Projects {
		return Rows{}, fmt.Errorf("%w: nothing selected", ErrConfiguration)
	}
	page := params.PageSize
	if page <= 0 {
		page = defaultPage
	}
	now := params.Now.UTC()
	resolver := params.Resolver
	if resolver == nil {
		resolver = identityalias.LoadDefault()
	}

	teams, err := client.SearchTeams(ctx, params.OrganizationID, params.SiteID, "", page)
	if err != nil {
		return Rows{}, fmt.Errorf("search atlassian teams: %w", err)
	}
	var rows Rows
	seen := map[string]bool{}
	for _, team := range teams {
		id, err := teamID(team.ID)
		if err != nil {
			return Rows{}, err
		}
		if seen[id] {
			continue
		}
		seen[id] = true
		name := strings.TrimSpace(team.DisplayName)
		if name == "" {
			name = id
		}
		active := strings.EqualFold(strings.TrimSpace(team.State), "ACTIVE")
		row := TeamRow{
			ID: id, TeamUUID: uuid.NewSHA1(uuid.NameSpaceURL, []byte("team:"+id)), Name: name,
			Description: optional(team.Description), ProjectKeys: []string{}, IsActive: boolByte(active),
			UpdatedAt: now, OrgID: params.OrgID, Provider: Provider, NativeTeamKey: strings.TrimSpace(team.ID),
		}
		// An archived team keeps its row (inactive) and has no members or
		// project links to read.
		if active && params.Selections.Members {
			relations, err := client.IterTeamUsers(ctx, team.ID, page)
			if err != nil {
				return Rows{}, fmt.Errorf("read members of team %s: %w", id, err)
			}
			members := map[string]bool{}
			for _, relation := range relations {
				if relation.RelationType != "TEAM_MEMBER" {
					continue
				}
				account, ok := accountID(relation.SubjectUserID)
				if !ok {
					continue
				}
				member := memberID(account)
				if members[member] {
					continue
				}
				facets := resolver.MembershipFacets(Provider, "", account, "")
				if len(facets) == 0 {
					continue
				}
				members[member] = true
				rows.Memberships = append(rows.Memberships, MembershipRow{
					OrgID: params.OrgID, Provider: Provider, TeamID: id, MemberID: member,
					RawProviderUserID: facets[0], IdentityFacets: facets, Source: Source, IsPrimary: 1,
					Specificity: MembershipSpecificity, Priority: MembershipPriority, ValidFrom: now, UpdatedAt: now,
				})
			}
		}
		if active && params.Selections.Projects {
			projects, err := client.IterTeamActiveProjects(ctx, team.ID, page)
			if err != nil {
				return Rows{}, fmt.Errorf("read projects of team %s: %w", id, err)
			}
			keys := map[string]bool{}
			for _, project := range projects {
				key := ""
				if project.ProjectKey != nil {
					key = strings.TrimSpace(*project.ProjectKey)
				}
				if key == "" {
					rows.SkippedProjects++
					continue
				}
				if keys[key] {
					continue
				}
				keys[key] = true
				rows.Ownership = append(rows.Ownership, OwnershipRow{
					OrgID: params.OrgID, Provider: Provider, TeamID: id, ProjectID: params.OrgID + ":" + Provider + ":" + key,
					ProjectKey: key, Source: Source, IsPrimary: 1, Specificity: OwnershipSpecificity,
					Priority: OwnershipPriority, ValidFrom: now, UpdatedAt: now,
				})
				row.ProjectKeys = append(row.ProjectKeys, key)
			}
			sort.Strings(row.ProjectKeys)
		}
		rows.Teams = append(rows.Teams, row)
	}
	return rows, nil
}

func optional(value *string) *string {
	if value == nil {
		return nil
	}
	trimmed := strings.TrimSpace(*value)
	if trimmed == "" {
		return nil
	}
	return &trimmed
}

func boolByte(value bool) uint8 {
	if value {
		return 1
	}
	return 0
}
