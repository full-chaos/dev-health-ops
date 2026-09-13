package providersync

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/google/uuid"
)

// Jira team-catalog collection is a provider-only reference collector, the
// same shape as LinearReferenceCatalogRouteHandler/GitLabTeamCatalogRouteHandler:
// it has no CompleteRouteHandler registration of its own. It ports
// src/dev_health_ops/workers/team_autoimport_jira.py's normalization rules --
// Jira has no "team" concept of its own, so a project IS the team unit
// (team_discovery.discover_jira treats every visible project as one
// DiscoveredTeam): project search -> teams catalog row, one project ->
// team_project_ownership row, the project's lead (Jira's only per-project
// "membership" Python ever discovers) -> team_memberships, plus the legacy
// jira_project_ops_team_links carry-forward ownership rows. Board/sprint
// reference discovery is unconditional, mirroring Linear's cycles.
const (
	jiraTeamCatalogProvider = "jira"

	// jiraTeamCatalogSource/jiraTeamCatalogLegacySource are the two
	// team_project_ownership `source` values team_autoimport_jira.py writes:
	// "native" for a project discovered directly off this run's own Jira
	// project search, "jira_legacy" for a jira_project_ops_team_links
	// carry-forward row. team_memberships only ever uses "native" (Jira has
	// no legacy membership carry-forward).
	jiraTeamCatalogSource       = "native"
	jiraTeamCatalogLegacySource = "jira_legacy"

	// Specificity/priority pairs are Python's literal constants
	// (team_autoimport_jira.py's ProjectRecord/TeamProjectOwnershipRecord/
	// TeamMembershipRecord construction), not a derived formula the way
	// GitLab's depth-based specificity is -- Jira's project/team mapping is
	// always exactly one level deep.
	jiraTeamCatalogNativeSpecificity = 100
	jiraTeamCatalogNativePriority    = 10
	jiraTeamCatalogLegacySpecificity = 90
	jiraTeamCatalogLegacyPriority    = 20
)

// --- Provider payload shapes -------------------------------------------------

// jiraTeamCatalogProjectSearchPayload mirrors team_discovery.discover_jira's
// single GET /rest/api/3/project/search?maxResults=100 call exactly --
// deliberately NOT paginated beyond that first page (Python never follows
// `nextPage`/`isLast` here either), so an org with more than 100 visible
// Jira projects loses the same tail Python already does. Not a Go
// regression: parity with an existing, unfixed Python limitation, not a
// product decision this port makes.
type jiraTeamCatalogProjectSearchPayload struct {
	Values []jiraTeamCatalogProjectSearchEntry `json:"values"`
}

type jiraTeamCatalogProjectSearchEntry struct {
	Key         string  `json:"key"`
	Name        string  `json:"name"`
	Description *string `json:"description"`
}

// jiraTeamCatalogProjectDetailPayload is the single-project GET
// (/rest/api/3/project/{key}) response, used for two DIFFERENT and
// separately-failing purposes mirroring Python's two separate call sites:
// team_membership.discover_members_jira reads Lead (this project's only
// "member"); team_autoimport_jira._jira_project_type_key reads
// ProjectTypeKey to gate Agile board discovery. Kept as one payload shape
// (Go issues one GET where Python's two call sites each issue their own),
// each caller reading only the field it needs -- same response body either
// way, so this does not change what either read observes.
type jiraTeamCatalogProjectDetailPayload struct {
	Key            string                      `json:"key"`
	Name           string                      `json:"name"`
	Description    *string                     `json:"description"`
	ProjectTypeKey string                      `json:"projectTypeKey"`
	Lead           *jiraTeamCatalogUserPayload `json:"lead"`
}

type jiraTeamCatalogUserPayload struct {
	AccountID    string `json:"accountId"`
	EmailAddress string `json:"emailAddress"`
	DisplayName  string `json:"displayName"`
}

type jiraTeamCatalogBoardPayload struct {
	ID   json.Number `json:"id"`
	Type string      `json:"type"`
}

type jiraTeamCatalogBoardsPage struct {
	Values []jiraTeamCatalogBoardPayload `json:"values"`
	IsLast *bool                         `json:"isLast"`
}

type jiraTeamCatalogSprintsPage struct {
	Values []json.RawMessage `json:"values"`
	IsLast *bool             `json:"isLast"`
}

// --- Normalized rows, one per destination table ------------------------------

// jiraTeamCatalogTeamRow mirrors gitlabTeamCatalogTeamRow's shape against the
// SAME physical `teams` table. Jira teams are never hierarchical (a project
// has no parent project), so ParentTeamID is always nil.
type jiraTeamCatalogTeamRow struct {
	ID                   string    `json:"id"`
	TeamUUID             string    `json:"team_uuid"`
	Name                 string    `json:"name"`
	Description          *string   `json:"description"`
	Members              []string  `json:"members"`
	MembersAuthoritative bool      `json:"members_authoritative"`
	ProjectKeys          []string  `json:"project_keys"`
	RepoPatterns         []string  `json:"repo_patterns"`
	IsActive             uint8     `json:"is_active"`
	UpdatedAt            time.Time `json:"updated_at"`
	OrgID                string    `json:"org_id"`
	Provider             string    `json:"provider"`
	NativeTeamKey        *string   `json:"native_team_key"`
	ParentTeamID         *string   `json:"parent_team_id"`
}

type jiraTeamCatalogOwnershipRow struct {
	OrgID       string     `json:"org_id"`
	Provider    string     `json:"provider"`
	TeamID      string     `json:"team_id"`
	ProjectID   string     `json:"project_id"`
	ProjectKey  *string    `json:"project_key"`
	Source      string     `json:"source"`
	IsPrimary   uint8      `json:"is_primary"`
	Specificity uint16     `json:"specificity"`
	Priority    int32      `json:"priority"`
	ValidFrom   time.Time  `json:"valid_from"`
	ValidTo     *time.Time `json:"valid_to"`
	UpdatedAt   time.Time  `json:"updated_at"`
}

type jiraTeamCatalogMembershipRow struct {
	OrgID             string     `json:"org_id"`
	Provider          string     `json:"provider"`
	TeamID            string     `json:"team_id"`
	MemberID          string     `json:"member_id"`
	RawProviderUserID *string    `json:"raw_provider_user_id"`
	RawEmail          *string    `json:"raw_email"`
	IdentityFacets    []string   `json:"identity_facets"`
	Source            string     `json:"source"`
	IsPrimary         uint8      `json:"is_primary"`
	Specificity       uint16     `json:"specificity"`
	Priority          int32      `json:"priority"`
	ValidFrom         time.Time  `json:"valid_from"`
	ValidTo           *time.Time `json:"valid_to"`
	UpdatedAt         time.Time  `json:"updated_at"`
}

// jiraTeamCatalogProjectRow shares the `projects` table with Linear/GitLab's
// native project rows -- Jira has no team/lead enrichment beyond the lead
// captured on team_memberships, so those columns stay empty/nil here,
// matching Python's ProjectRecord defaults exactly.
type jiraTeamCatalogProjectRow struct {
	ID         string     `json:"id"`
	OrgID      string     `json:"org_id"`
	Provider   string     `json:"provider"`
	ProjectKey *string    `json:"project_key"`
	Name       string     `json:"name"`
	IsActive   uint8      `json:"is_active"`
	State      string     `json:"state"`
	TargetDate *time.Time `json:"target_date"`
	URL        string     `json:"url"`
	TeamIDs    []string   `json:"team_ids"`
	TeamKeys   []string   `json:"team_keys"`
	LeadID     *string    `json:"lead_id"`
	LeadName   *string    `json:"lead_name"`
	LeadEmail  *string    `json:"lead_email"`
	UpdatedAt  time.Time  `json:"updated_at"`
	LastSynced time.Time  `json:"last_synced"`
}

type JiraTeamCatalogRows struct {
	Teams       []jiraTeamCatalogTeamRow       `json:"teams"`
	Ownership   []jiraTeamCatalogOwnershipRow  `json:"ownership"`
	Memberships []jiraTeamCatalogMembershipRow `json:"memberships"`
	Projects    []jiraTeamCatalogProjectRow    `json:"projects"`
	Sprints     []jiraSprintRow                `json:"sprints"`
}

const (
	jiraTeamCatalogTeamsDestination       = "jira_team_catalog_teams"
	jiraTeamCatalogOwnershipDestination   = "jira_team_catalog_ownership"
	jiraTeamCatalogMembershipsDestination = "jira_team_catalog_memberships"
	jiraTeamCatalogProjectsDestination    = "jira_team_catalog_projects"
	jiraTeamCatalogSprintsDestination     = "jira_team_catalog_sprints"
)

// --- Normalization -----------------------------------------------------------

// jiraTeamID mirrors team_discovery.discover_jira: the team unit IS the
// project, identified by its raw project key -- no provider prefix, unlike
// GitLab's "gl:" or GitHub's team-slug ids.
func jiraTeamID(projectKey string) string {
	return strings.TrimSpace(projectKey)
}

func jiraProjectID(orgID, projectKey string) string {
	return orgID + ":" + jiraTeamCatalogProvider + ":" + projectKey
}

func normalizeJiraTeamRow(
	orgID string, entry jiraTeamCatalogProjectSearchEntry, normalizedAt time.Time,
) (jiraTeamCatalogTeamRow, bool) {
	key := jiraTeamID(entry.Key)
	name := strings.TrimSpace(entry.Name)
	if key == "" || name == "" {
		return jiraTeamCatalogTeamRow{}, false
	}
	nativeTeamKey := key
	return jiraTeamCatalogTeamRow{
		ID: key, TeamUUID: uuid.NewSHA1(uuid.NameSpaceURL, []byte("team:"+key)).String(),
		Name: name, Description: optionalJiraString(entry.Description),
		Members: []string{}, ProjectKeys: []string{key}, RepoPatterns: []string{},
		IsActive: 1, UpdatedAt: normalizedAt, OrgID: orgID, Provider: jiraTeamCatalogProvider,
		NativeTeamKey: &nativeTeamKey, ParentTeamID: nil,
	}, true
}

func normalizeJiraOwnershipRow(orgID, teamID, projectKey string, normalizedAt time.Time) jiraTeamCatalogOwnershipRow {
	key := projectKey
	return jiraTeamCatalogOwnershipRow{
		OrgID: orgID, Provider: jiraTeamCatalogProvider, TeamID: teamID,
		ProjectID: jiraProjectID(orgID, projectKey), ProjectKey: &key, Source: jiraTeamCatalogSource,
		IsPrimary: 1, Specificity: jiraTeamCatalogNativeSpecificity, Priority: jiraTeamCatalogNativePriority,
		ValidFrom: normalizedAt, UpdatedAt: normalizedAt,
	}
}

func normalizeJiraProjectRow(orgID, projectKey, name string, normalizedAt time.Time) jiraTeamCatalogProjectRow {
	key := projectKey
	return jiraTeamCatalogProjectRow{
		ID: jiraProjectID(orgID, projectKey), OrgID: orgID, Provider: jiraTeamCatalogProvider,
		ProjectKey: &key, Name: name, IsActive: 1,
		TeamIDs: []string{}, TeamKeys: []string{},
		UpdatedAt: normalizedAt, LastSynced: normalizedAt,
	}
}

// jiraTeamCatalogMembershipFacets mirrors team_autoimport_jira.py's
// resolver.membership_facets(provider="jira", account_id=..., email=...)
// call with NO alias configured for this identity (the same simplification
// GitHub/GitLab's native collectors already make -- neither consults the
// real org alias map either): with no alias, IdentityResolver.resolve falls
// back to provider_qualified_identity, "jira:accountid:<id>" (Jira carries
// no username, only an accountId), and membership_facets then appends the
// normalized email. facets[0] is written into raw_provider_user_id exactly
// as Python's does.
func jiraTeamCatalogMembershipFacets(accountID string, email *string) []string {
	accountID = strings.TrimSpace(accountID)
	if accountID == "" {
		return nil
	}
	facets := []string{"jira:accountid:" + accountID}
	if email != nil {
		if normalized := strings.ToLower(strings.TrimSpace(*email)); normalized != "" && !containsString(facets, normalized) {
			facets = append(facets, normalized)
		}
	}
	return facets
}

// jiraMemberID mirrors team_autoimport_jira._member_id: "jira:" + the
// LOWERCASED account id -- deliberately not the qualified "accountid:" form
// facets[0] uses; the two are computed independently in Python too.
func jiraMemberID(accountID string) string {
	return "jira:" + strings.ToLower(strings.TrimSpace(accountID))
}

// normalizeJiraMembershipRow mirrors team_autoimport_jira.py's per-team
// member loop: Jira's only discovered "member" is the project's lead
// (is_primary=1 always, since role is always "lead" -- Python's ternary
// never observes any other role for Jira).
func normalizeJiraMembershipRow(
	orgID, teamID string, lead jiraTeamCatalogUserPayload, normalizedAt time.Time,
) (jiraTeamCatalogMembershipRow, bool) {
	accountID := strings.TrimSpace(lead.AccountID)
	if accountID == "" {
		accountID = strings.TrimSpace(lead.EmailAddress)
	}
	if accountID == "" {
		accountID = strings.TrimSpace(lead.DisplayName)
	}
	if accountID == "" {
		return jiraTeamCatalogMembershipRow{}, false
	}
	email := optionalJiraString(&lead.EmailAddress)
	facets := jiraTeamCatalogMembershipFacets(accountID, email)
	if len(facets) == 0 {
		return jiraTeamCatalogMembershipRow{}, false
	}
	rawProviderUserID := facets[0]
	return jiraTeamCatalogMembershipRow{
		OrgID: orgID, Provider: jiraTeamCatalogProvider, TeamID: teamID, MemberID: jiraMemberID(accountID),
		RawProviderUserID: &rawProviderUserID, RawEmail: email, IdentityFacets: facets,
		Source: jiraTeamCatalogSource, IsPrimary: 1,
		Specificity: jiraTeamCatalogNativeSpecificity, Priority: jiraTeamCatalogNativePriority,
		ValidFrom: normalizedAt, UpdatedAt: normalizedAt,
	}, true
}

func optionalJiraString(value *string) *string {
	if value == nil {
		return nil
	}
	trimmed := strings.TrimSpace(*value)
	if trimmed == "" {
		return nil
	}
	return &trimmed
}

func dedupeJiraProjectCatalogRows(rows []jiraTeamCatalogProjectRow) []jiraTeamCatalogProjectRow {
	seen := make(map[string]bool, len(rows))
	result := make([]jiraTeamCatalogProjectRow, 0, len(rows))
	for _, row := range rows {
		key := row.OrgID + "\x00" + row.Provider + "\x00" + row.ID
		if seen[key] {
			continue
		}
		seen[key] = true
		result = append(result, row)
	}
	return result
}

func dedupeJiraOwnershipRows(rows []jiraTeamCatalogOwnershipRow) []jiraTeamCatalogOwnershipRow {
	seen := make(map[string]bool, len(rows))
	result := make([]jiraTeamCatalogOwnershipRow, 0, len(rows))
	for _, row := range rows {
		key := row.OrgID + "\x00" + row.Provider + "\x00" + row.TeamID + "\x00" + row.ProjectID + "\x00" + row.Source
		if seen[key] {
			continue
		}
		seen[key] = true
		result = append(result, row)
	}
	return result
}

func distinctJiraMembershipMembers(rows []jiraTeamCatalogMembershipRow) map[string]struct{} {
	seen := make(map[string]struct{}, len(rows))
	for _, row := range rows {
		seen[row.MemberID] = struct{}{}
	}
	return seen
}

// --- Validation --------------------------------------------------------------

func validateJiraTeamRow(claim Claim, row jiraTeamCatalogTeamRow) error {
	if claim.Provider != jiraTeamCatalogProvider || row.Provider != jiraTeamCatalogProvider ||
		row.OrgID != claim.OrgID || strings.TrimSpace(row.ID) == "" || strings.TrimSpace(row.TeamUUID) == "" ||
		row.UpdatedAt.IsZero() || row.IsActive > 1 {
		return ErrInvalidConfiguration
	}
	if _, err := uuid.Parse(row.TeamUUID); err != nil {
		return ErrInvalidConfiguration
	}
	return nil
}

func validateJiraOwnershipRow(claim Claim, row jiraTeamCatalogOwnershipRow) error {
	if claim.Provider != jiraTeamCatalogProvider || row.Provider != jiraTeamCatalogProvider ||
		row.OrgID != claim.OrgID || strings.TrimSpace(row.TeamID) == "" || strings.TrimSpace(row.ProjectID) == "" ||
		row.IsPrimary > 1 || row.ValidFrom.IsZero() || row.UpdatedAt.IsZero() {
		return ErrInvalidConfiguration
	}
	switch row.Source {
	case jiraTeamCatalogSource:
		if row.Specificity != jiraTeamCatalogNativeSpecificity || row.Priority != jiraTeamCatalogNativePriority {
			return ErrInvalidConfiguration
		}
	case jiraTeamCatalogLegacySource:
		if row.Specificity != jiraTeamCatalogLegacySpecificity || row.Priority != jiraTeamCatalogLegacyPriority {
			return ErrInvalidConfiguration
		}
	default:
		return ErrInvalidConfiguration
	}
	return nil
}

func validateJiraMembershipRow(claim Claim, row jiraTeamCatalogMembershipRow) error {
	if claim.Provider != jiraTeamCatalogProvider || row.Provider != jiraTeamCatalogProvider ||
		row.OrgID != claim.OrgID || strings.TrimSpace(row.TeamID) == "" || strings.TrimSpace(row.MemberID) == "" ||
		row.Source != jiraTeamCatalogSource || row.IsPrimary > 1 || row.Priority != jiraTeamCatalogNativePriority ||
		row.ValidFrom.IsZero() || row.UpdatedAt.IsZero() {
		return ErrInvalidConfiguration
	}
	return nil
}

func (row jiraTeamCatalogProjectRow) validate(claim Claim) error {
	if claim.Provider != jiraTeamCatalogProvider || row.Provider != jiraTeamCatalogProvider ||
		row.OrgID != claim.OrgID || strings.TrimSpace(row.ID) == "" || row.UpdatedAt.IsZero() || row.LastSynced.IsZero() {
		return ErrInvalidConfiguration
	}
	return nil
}
