// Package atlassianteams syncs Atlassian Teams (the organization's real teams,
// not Jira projects) into the ClickHouse team dimensions: the team catalog
// (teams), who is on each team (team_memberships) and which Jira projects a
// team owns (team_project_ownership). The data comes from the
// full-chaos/atlassian client (vendored under third_party/vendor/atlassian):
// the teamSearchV2 search for the teams, the Teamwork Graph for each team's
// users, and the team-to-container relation
// (graphStore_teamConnectedToContainer) for the Jira projects ("spaces") a
// team is connected to. That relation is the ONE ownership source of this
// package: a team owns a project only when the provider returns a link row
// that carries the team id and the project id. A name is never a link.
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
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"atlassian/atlassian"
	"atlassian/atlassian/graph"

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
	// jiraARIPrefix / jiraProjectARIResource bound a Jira project ARI:
	// "ari:cloud:jira:<site>:project/<native id>".
	jiraARIPrefix          = "ari:cloud:jira:"
	jiraProjectARIResource = "project/"
	defaultPage            = 50
)

// Client is the part of the atlassian graph client the sync reads. The
// package's *graph.Client satisfies it; tests serve a fake gateway to the
// real client.
type Client interface {
	SearchTeams(ctx context.Context, organizationID, siteID, query string, pageSize int) ([]atlassian.AtlassianTeam, error)
	IterTeamUsers(ctx context.Context, teamID string, pageSize int) ([]atlassian.TeamworkUserRelation, error)
	// IterTeamConnectedContainers returns every container connected to a
	// team, or an error when the provider's last page was not reached.
	IterTeamConnectedContainers(ctx context.Context, teamID string, pageSize int) ([]graph.TeamConnectedContainer, error)
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

// The union arms of the team-to-container relation this collector knows.
// A node of any other type is a provider-side change: it is counted, and the
// snapshot is not complete.
const (
	containerJiraProject     = "JiraProject"
	containerConfluenceSpace = "ConfluenceSpace"
	containerLoomSpace       = "LoomSpace"
)

// ProjectLinkCounts counts the team-to-container links of one collection.
// Seen is every link of every team read that reached its end; it equals the
// ownership rows collected plus the five skip counts. FailedTeamReads counts
// the teams whose link read did not reach its end (their links are not in
// Seen).
type ProjectLinkCounts struct {
	Seen int
	// SkippedNonJira: a ConfluenceSpace or LoomSpace link. Valid, never written.
	SkippedNonJira int
	// SkippedUnknownType: a link whose node is of no known type, or absent.
	SkippedUnknownType int
	// SkippedNoNativeID: a JiraProject link with no native project id (no
	// Jira project ARI, or a projectId that disagrees with it).
	SkippedNoNativeID int
	// SkippedNoProjectKey: a JiraProject link with no project key.
	SkippedNoProjectKey int
	// SkippedDuplicate: a second link of the same team to the same project.
	SkippedDuplicate int
	FailedTeamReads  int
}

// Skipped is the links seen and not collected as an ownership row.
func (c ProjectLinkCounts) Skipped() int {
	return c.SkippedNonJira + c.SkippedUnknownType + c.SkippedNoNativeID + c.SkippedNoProjectKey + c.SkippedDuplicate
}

// Rows is what one collection produced.
type Rows struct {
	Teams       []TeamRow
	Memberships []MembershipRow
	Ownership   []OwnershipRow
	// ProjectLinks counts the links behind Ownership.
	ProjectLinks ProjectLinkCounts
	// ProjectLinksComplete says the team search ended and every active team's
	// link read reached the provider's last page with only known link types.
	// Only Collect sets it. Rows built any other way leave it false, and Write
	// then closes no project link.
	ProjectLinksComplete bool
	// ProjectLinkFailure is the first error of a team's link read, nil when
	// every read ended. A failed link read does not fail the collection: the
	// teams and the members are still returned, and so are the links of the
	// teams whose read ended. Nothing is closed (ProjectLinksComplete is
	// false).
	ProjectLinkFailure error
	// FailedProjectLinkTeams holds the id of every team whose link read did
	// not reach its end.
	FailedProjectLinkTeams []string
	// UnreadableProjectLinkTeams holds the id of every team whose read
	// returned at least one JiraProject link and not one of them carried a
	// Jira project ARI this collector can read. An empty answer for such a
	// team is "the links could not be read", not "the team has no project":
	// Write closes none of that team's links. A team with at least one
	// readable link is not here; its other links are counted in ProjectLinks.
	UnreadableProjectLinkTeams []string
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

// jiraNativeProjectID returns the native Jira project id of a Jira project
// ARI ("ari:cloud:jira:<site>:project/<id>"). That id is the one project
// identity on the platform -- the Jira work-items route writes it into
// work_items.project_id and `projects` -- so a team's project link carries it
// and reaches the project's work items by id. Anything else has no such
// identity and gets no link; an id is never built from the project key.
//
// The ARI is read whole, not by its tail: after the product prefix there is
// one site segment and one resource, and the resource is "project/<id>". An
// ARI that only ENDS in ":project/<id>" names something inside another
// resource, not a project. The id is the decimal form Jira REST returns for
// project.id: a positive int64 with no leading zero, so the text equals the
// text the work-items route writes.
func jiraNativeProjectID(ari string) (string, bool) {
	rest, ok := strings.CutPrefix(strings.TrimSpace(ari), jiraARIPrefix)
	if !ok {
		return "", false
	}
	site, resource, _ := strings.Cut(rest, ":")
	if strings.Contains(site, "/") {
		return "", false
	}
	id, ok := strings.CutPrefix(resource, jiraProjectARIResource)
	if !ok || id == "" || id[0] == '0' {
		return "", false
	}
	for _, r := range id {
		if r < '0' || r > '9' {
			return "", false
		}
	}
	if _, err := strconv.ParseInt(id, 10, 64); err != nil {
		return "", false
	}
	return id, true
}

// memberID is the member id the Jira auto-import writes: "jira:" and the
// lower-cased account id, so a person is one member across both.
func memberID(account string) string {
	return "jira:" + strings.ToLower(strings.TrimSpace(account))
}

// siteQueryContext is the X-Query-Context value of a site: its platform site ARI. A site id that is already an
// ARI is passed through unchanged; an empty one sends no header.
func siteQueryContext(siteID string) string {
	siteID = strings.TrimSpace(siteID)
	switch {
	case siteID == "":
		return ""
	case strings.HasPrefix(strings.ToLower(siteID), "ari:"):
		return siteID
	}
	return "ari:cloud:platform::site/" + siteID
}

// organizationARI is the organization id in the form teamSearchV2 accepts. tenantContexts answers a
// bare UUID and the gateway refuses it ("Invalid Organization Ari", CHAOS-7132); an id that already is
// an ARI is passed through unchanged.
func organizationARI(id string) string {
	id = strings.TrimSpace(id)
	if strings.HasPrefix(strings.ToLower(id), "ari:") {
		return id
	}
	return "ari:cloud:platform::org/" + id
}

// connectedProjectNativeID is the native Jira project id of a JiraProject
// link node: the id of its project ARI (jiraNativeProjectID). The node's own
// projectId field must agree with it when the provider sends one; two ids for
// one project are no id.
func connectedProjectNativeID(container graph.TeamConnectedContainer) (string, bool) {
	id, ok := jiraNativeProjectID(container.ID)
	if !ok {
		return "", false
	}
	if projectID := strings.TrimSpace(container.ProjectID); projectID != "" && projectID != id {
		return "", false
	}
	return id, true
}

// Collect reads the selected dimensions of every Atlassian team and returns
// the rows to write. The team search and the member reads are all or nothing:
// either one failing fails the run (a membership list cut short by an error
// would read as members who left). The project-link reads are a separate
// leg: a team's link read that fails is recorded in the rows
// (ProjectLinkFailure, FailedProjectLinkTeams), the snapshot is not complete,
// and the teams and members are still returned.
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

	// The per-team reads (members, connected containers) name the site they query (CHAOS-7132: the live gateway
	// refuses the members read without X-Query-Context; read-only probes on 2026-10-02 had the platform site ARI,
	// the Jira site ARI and the organization ARI each accepted by it). The connected-container read answered
	// with the platform site ARI (a read-only call of 2026-10-07); without the header it was not measured. The
	// search and tenant reads answered without it and are not marked.
	siteCtx := graph.WithQueryContext(ctx, siteQueryContext(params.SiteID))

	teams, err := client.SearchTeams(ctx, organizationARI(params.OrganizationID), params.SiteID, "", page)
	if err != nil {
		return Rows{}, fmt.Errorf("search atlassian teams: %w", err)
	}
	var rows Rows
	activeTeams, projectReads := 0, 0
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
			relations, err := client.IterTeamUsers(siteCtx, team.ID, page)
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
		if active {
			activeTeams++
		}
		if active && params.Selections.Projects {
			containers, err := client.IterTeamConnectedContainers(siteCtx, team.ID, page)
			switch {
			case err != nil && ctx.Err() != nil:
				// The run itself was cancelled: not a provider answer.
				return Rows{}, fmt.Errorf("read connected projects of team %s: %w", id, err)
			case err != nil:
				rows.ProjectLinks.FailedTeamReads++
				rows.FailedProjectLinkTeams = append(rows.FailedProjectLinkTeams, id)
				if rows.ProjectLinkFailure == nil {
					rows.ProjectLinkFailure = fmt.Errorf("read connected projects of team %s: %w", id, err)
				}
			default:
				projectReads++
				linked := map[string]bool{}
				readable, refused := 0, 0
				for _, container := range containers {
					rows.ProjectLinks.Seen++
					switch container.Typename {
					case containerJiraProject:
					case containerConfluenceSpace, containerLoomSpace:
						rows.ProjectLinks.SkippedNonJira++
						continue
					default:
						rows.ProjectLinks.SkippedUnknownType++
						continue
					}
					nativeProjectID, ok := connectedProjectNativeID(container)
					if !ok {
						refused++
						rows.ProjectLinks.SkippedNoNativeID++
						continue
					}
					readable++
					key := strings.TrimSpace(container.Key)
					if key == "" {
						rows.ProjectLinks.SkippedNoProjectKey++
						continue
					}
					if linked[nativeProjectID] {
						rows.ProjectLinks.SkippedDuplicate++
						continue
					}
					linked[nativeProjectID] = true
					rows.Ownership = append(rows.Ownership, OwnershipRow{
						OrgID: params.OrgID, Provider: Provider, TeamID: id, ProjectID: nativeProjectID,
						ProjectKey: key, Source: Source, IsPrimary: 1, Specificity: OwnershipSpecificity,
						Priority: OwnershipPriority, ValidFrom: now, UpdatedAt: now,
					})
					row.ProjectKeys = append(row.ProjectKeys, key)
				}
				sort.Strings(row.ProjectKeys)
				if refused > 0 && readable == 0 {
					rows.UnreadableProjectLinkTeams = append(rows.UnreadableProjectLinkTeams, id)
				}
			}
		}
		rows.Teams = append(rows.Teams, row)
	}
	// The team search followed the provider's cursor to its end or returned
	// its error out of this function. A link read counts in projectReads only
	// when the client reached the provider's last page (it returns an error
	// for a failed page, a GraphQL error, a page with no cursor and its page
	// bound). So the links are complete when one read ended for every active
	// team, and every link was of a known type: a type this collector does not
	// know is a provider-side change, and what it would have been is not known.
	rows.ProjectLinksComplete = params.Selections.Projects && projectReads == activeTeams &&
		rows.ProjectLinks.FailedTeamReads == 0 && rows.ProjectLinks.SkippedUnknownType == 0
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
