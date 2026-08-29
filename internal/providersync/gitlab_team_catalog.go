package providersync

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/google/uuid"
)

// GitLab team-catalog collection is a provider-only reference collector, the
// same shape as LinearReferenceCatalogRouteHandler (linear_reference_catalog.go):
// it has no CompleteRouteHandler registration of its own. It ports
// src/dev_health_ops/workers/team_autoimport_gitlab.py's normalization rules
// verbatim -- provider group/subgroup -> teams catalog row, per-group project
// listing -> team_project_ownership, group members -> team_memberships, and
// the flat include_subgroups project listing -> the native projects catalog
// (CHAOS-3380 parity). The caller (whatever cutover lane wires production
// dispatch, CHAOS-4198) decides when this is constructed; this file only
// proves the row shape and provider semantics are byte-parity with Python.
const (
	gitlabTeamCatalogProvider               = "gitlab"
	gitlabTeamCatalogProviderAccessPriority = 300
	gitlabTeamCatalogBaseSpecificity        = 100
	gitlabTeamCatalogChildSpecificityStep   = 10
	gitlabTeamCatalogSource                 = "provider_access"
)

// --- Provider payload shapes -------------------------------------------------

type gitlabTeamCatalogGroupPayload struct {
	ID          json.Number `json:"id"`
	FullPath    string      `json:"full_path"`
	Name        string      `json:"name"`
	Description *string     `json:"description"`
}

type gitlabTeamCatalogProjectPayload struct {
	ID                json.Number `json:"id"`
	PathWithNamespace string      `json:"path_with_namespace"`
	Name              string      `json:"name"`
	Archived          bool        `json:"archived"`
	WebURL            string      `json:"web_url"`
}

type gitlabTeamCatalogMemberPayload struct {
	Username string  `json:"username"`
	Name     string  `json:"name"`
	Email    *string `json:"email"`
}

// --- Normalized rows, one per destination table ------------------------------

// gitlabTeamCatalogTeamRow mirrors linearReferenceTeamRow's shape against the
// SAME physical `teams` table. MembersAuthoritative distinguishes a
// want_members=true run (Members is the complete, authoritative roster this
// run observed, even if empty) from a want_members=false, want_teams=true run
// (Members carries no signal; the effects sink must preserve whatever roster
// is currently persisted rather than overwrite it with an empty one --
// team_autoimport_gitlab.py's CHAOS-4323 round-2 roster-preservation rule).
type gitlabTeamCatalogTeamRow struct {
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

type gitlabTeamCatalogOwnershipRow struct {
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

type gitlabTeamCatalogMembershipRow struct {
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

// gitlabTeamCatalogProjectRow is the native project catalog row (CHAOS-3380
// parity for GitLab: an Ask Dev subject catalog entry, not team-ownership).
// It shares the `projects` table with Linear's native project rows, so it
// carries the same 16 columns; GitLab has no team/lead enrichment, so those
// stay empty/nil, matching Python's ProjectRecord defaults.
type gitlabTeamCatalogProjectRow struct {
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

type GitLabTeamCatalogRows struct {
	Teams       []gitlabTeamCatalogTeamRow       `json:"teams"`
	Ownership   []gitlabTeamCatalogOwnershipRow  `json:"ownership"`
	Memberships []gitlabTeamCatalogMembershipRow `json:"memberships"`
	Projects    []gitlabTeamCatalogProjectRow    `json:"projects"`
	// FailedMemberFetchTeamIDs (CHAOS-4461, ruling extended from GitHub to
	// GitLab by team-lead, 2026-08-28) lists the teams (by their "gl:" id,
	// matching gitlabTeamCatalogTeamRow.ID) whose /members fetch failed
	// under a non-strict Collect (TeamCatalogReference.Strict == false)
	// while members were globally selected. These teams are deliberately
	// left with MembersAuthoritative == false in the post-loop roster
	// rebuild below, so GitLabTeamCatalogClickHouseEffects.writeTeams's
	// existing roster-preservation path (CHAOS-4323 round 2) confirms and
	// carries forward their currently-persisted roster instead of writing
	// an unconfirmed empty one. Under strict (reference discovery), the
	// fetch error is returned immediately instead -- this field stays
	// empty in that path.
	FailedMemberFetchTeamIDs []string `json:"-"`
}

const (
	gitlabTeamCatalogTeamsDestination       = "gitlab_team_catalog_teams"
	gitlabTeamCatalogOwnershipDestination   = "gitlab_team_catalog_ownership"
	gitlabTeamCatalogMembershipsDestination = "gitlab_team_catalog_memberships"
	gitlabTeamCatalogProjectsDestination    = "gitlab_team_catalog_projects"
)

type GitLabTeamCatalogEffects struct {
	Teams       *EffectBatch
	Ownership   *EffectBatch
	Memberships *EffectBatch
	Projects    *EffectBatch
}

// Batches returns only the destinations this run actually produced -- a
// dimension the CHAOS-4323 selection turned off is not written at all,
// mirroring team_autoimport_gitlab.populate()'s want_teams/want_projects/
// want_members gates (each gate skips the write call entirely, not just the
// row content).
func (effects GitLabTeamCatalogEffects) Batches() []EffectBatch {
	batches := make([]EffectBatch, 0, 4)
	for _, batch := range []*EffectBatch{effects.Teams, effects.Ownership, effects.Memberships, effects.Projects} {
		if batch != nil {
			batches = append(batches, *batch)
		}
	}
	return batches
}

func BuildGitLabTeamCatalogEffects(rows GitLabTeamCatalogRows, wantTeams, wantProjects, wantMembers bool) (GitLabTeamCatalogEffects, error) {
	var effects GitLabTeamCatalogEffects
	if wantTeams {
		batch, err := effectBatchFromValues(gitlabTeamCatalogTeamsDestination, EffectReadbackRequired, rows.Teams)
		if err != nil {
			return GitLabTeamCatalogEffects{}, err
		}
		effects.Teams = &batch
	}
	if wantProjects {
		ownership, err := effectBatchFromValues(gitlabTeamCatalogOwnershipDestination, EffectReadbackRequired, rows.Ownership)
		if err != nil {
			return GitLabTeamCatalogEffects{}, err
		}
		effects.Ownership = &ownership
		projects, err := effectBatchFromValues(gitlabTeamCatalogProjectsDestination, EffectReadbackRequired, rows.Projects)
		if err != nil {
			return GitLabTeamCatalogEffects{}, err
		}
		effects.Projects = &projects
	}
	if wantMembers {
		memberships, err := effectBatchFromValues(gitlabTeamCatalogMembershipsDestination, EffectReadbackRequired, rows.Memberships)
		if err != nil {
			return GitLabTeamCatalogEffects{}, err
		}
		effects.Memberships = &memberships
	}
	return effects, nil
}

// --- Normalization -----------------------------------------------------------

// gitlabTeamID mirrors team_autoimport_gitlab._team_id: "gl:" + the group's
// full_path, idempotent against an already-prefixed input.
func gitlabTeamID(fullPath string) string {
	return "gl:" + strings.TrimPrefix(strings.TrimSpace(fullPath), "gl:")
}

// gitlabParentTeamID mirrors _parent_team_id's path-derived branch (GitLab
// associations never carry an explicit parent_team_id/parent_provider_team_id
// key, so that branch of the Python function never fires for this provider).
func gitlabParentTeamID(fullPath string) *string {
	fullPath = strings.TrimSpace(fullPath)
	idx := strings.LastIndex(fullPath, "/")
	if idx < 0 {
		return nil
	}
	parent := gitlabTeamID(fullPath[:idx])
	return &parent
}

// gitlabTeamDepth mirrors _depth/_parent_by_team: a parent only counts if it
// is itself one of the discovered team ids (guards against a path-derived
// parent that discovery never returned).
func gitlabTeamDepth(teamID string, parentByTeam map[string]string) int {
	depth := 0
	current := teamID
	visited := map[string]bool{}
	for {
		parent, ok := parentByTeam[current]
		if !ok || visited[current] {
			return depth
		}
		visited[current] = true
		current = parent
		depth++
	}
}

func gitlabTeamCatalogParentByTeam(fullPaths []string) map[string]string {
	teamIDs := make(map[string]bool, len(fullPaths))
	for _, path := range fullPaths {
		teamIDs[gitlabTeamID(path)] = true
	}
	parents := make(map[string]string, len(fullPaths))
	for _, path := range fullPaths {
		teamID := gitlabTeamID(path)
		parent := gitlabParentTeamID(path)
		if parent != nil && teamIDs[*parent] {
			parents[teamID] = *parent
		}
	}
	return parents
}

func normalizeGitLabTeamRow(
	orgID string,
	group gitlabTeamCatalogGroupPayload,
	projectKeys []string,
	normalizedAt time.Time,
) gitlabTeamCatalogTeamRow {
	fullPath := strings.TrimSpace(group.FullPath)
	teamID := gitlabTeamID(fullPath)
	nativeTeamKey := fullPath
	return gitlabTeamCatalogTeamRow{
		ID: teamID, TeamUUID: uuid.NewSHA1(uuid.NameSpaceURL, []byte("team:"+teamID)).String(),
		Name: gitlabFirstNonEmpty(group.Name, teamID), Description: group.Description,
		Members: []string{}, ProjectKeys: append([]string(nil), projectKeys...), RepoPatterns: []string{},
		IsActive: 1, UpdatedAt: normalizedAt, OrgID: orgID, Provider: gitlabTeamCatalogProvider,
		NativeTeamKey: &nativeTeamKey, ParentTeamID: gitlabParentTeamID(fullPath),
	}
}

func normalizeGitLabOwnershipRow(
	orgID, teamID, projectPath string,
	specificity uint16,
	normalizedAt time.Time,
) gitlabTeamCatalogOwnershipRow {
	projectKey := projectPath
	return gitlabTeamCatalogOwnershipRow{
		OrgID: orgID, Provider: gitlabTeamCatalogProvider, TeamID: teamID,
		ProjectID: projectPath, ProjectKey: &projectKey, Source: gitlabTeamCatalogSource,
		IsPrimary: 0, Specificity: specificity, Priority: gitlabTeamCatalogProviderAccessPriority,
		ValidFrom: normalizedAt, UpdatedAt: normalizedAt,
	}
}

// gitlabTeamCatalogMembershipFacets mirrors IdentityResolver.membership_facets
// under the DEFAULT (production) identity_mapping.yaml, which ships
// `identities: []` -- an empty alias map (verified: src/dev_health_ops/config/
// identity_mapping.yaml, no org currently configures aliases). With no
// aliases configured, resolve() for a username-only member degrades to the
// provider-qualified id, so the full facet ladder collapses to exactly:
// "gitlab:<username>" first, then the normalized email if present. This is
// the identical simplification internal/jobs/metrics/daily/repouser/identity.go
// documents for the SAME default config (see that package's own doc comment
// for "the alias-resolution gap this does not close" as an accepted, tracked
// limitation), and the one linearReferenceMembershipFacets already relies on
// for Linear's own ALREADY-MERGED reference-catalog port -- codex review
// flagged this as a P1 for this PR; it is a pre-existing, org-configurable-
// alias gap shared by every native Go reference-catalog collector today; not
// a regression this port introduces. Wiring the real IdentityResolver
// (parsing identity_mapping.yaml, alias matching) into Go is out of scope
// here and belongs to CHAOS-4453 (filed by lane-4434, shared child of
// CHAOS-4198), which closes this gap for all three native providers at
// once, not one provider's port.
func gitlabTeamCatalogMembershipFacets(username string, email *string) []string {
	username = strings.TrimSpace(username)
	if username == "" {
		return nil
	}
	facets := []string{"gitlab:" + username}
	if email != nil {
		if normalized := strings.ToLower(strings.TrimSpace(*email)); normalized != "" && !containsString(facets, normalized) {
			facets = append(facets, normalized)
		}
	}
	return facets
}

func normalizeGitLabMembershipRow(
	orgID, teamID string,
	payload gitlabTeamCatalogMemberPayload,
	normalizedAt time.Time,
) (gitlabTeamCatalogMembershipRow, string, bool) {
	username := strings.TrimSpace(payload.Username)
	if username == "" {
		return gitlabTeamCatalogMembershipRow{}, "", false
	}
	memberID := "gl:" + username
	facets := gitlabTeamCatalogMembershipFacets(username, payload.Email)
	if len(facets) == 0 {
		return gitlabTeamCatalogMembershipRow{}, "", false
	}
	rawProviderUserID := facets[0]
	row := gitlabTeamCatalogMembershipRow{
		OrgID: orgID, Provider: gitlabTeamCatalogProvider, TeamID: teamID, MemberID: memberID,
		RawProviderUserID: &rawProviderUserID, RawEmail: optionalGitLabString(payload.Email),
		IdentityFacets: facets, Source: gitlabTeamCatalogSource, IsPrimary: 0,
		Specificity: gitlabTeamCatalogBaseSpecificity, Priority: gitlabTeamCatalogProviderAccessPriority,
		ValidFrom: normalizedAt, UpdatedAt: normalizedAt,
	}
	return row, memberID, true
}

func gitlabProjectCatalogID(orgID, nativeID string) string {
	return orgID + ":gitlab:" + nativeID
}

func normalizeGitLabProjectCatalogRow(
	orgID string,
	payload gitlabTeamCatalogProjectPayload,
	normalizedAt time.Time,
) (gitlabTeamCatalogProjectRow, bool) {
	nativeID := strings.TrimSpace(payload.ID.String())
	path := strings.TrimSpace(payload.PathWithNamespace)
	if nativeID == "" || path == "" {
		return gitlabTeamCatalogProjectRow{}, false
	}
	isActive := uint8(1)
	if payload.Archived {
		isActive = 0
	}
	projectKey := path
	return gitlabTeamCatalogProjectRow{
		ID: gitlabProjectCatalogID(orgID, nativeID), OrgID: orgID, Provider: gitlabTeamCatalogProvider,
		ProjectKey: &projectKey, Name: gitlabFirstNonEmpty(payload.Name, path), IsActive: isActive,
		TeamIDs: []string{}, TeamKeys: []string{}, URL: strings.TrimSpace(payload.WebURL),
		UpdatedAt: normalizedAt, LastSynced: normalizedAt,
	}, true
}

func gitlabRosterFromMemberships(rows []gitlabTeamCatalogMembershipRow) map[string][]string {
	roster := map[string][]string{}
	for _, row := range rows {
		bucket := roster[row.TeamID]
		values := row.IdentityFacets
		if len(values) == 0 {
			for _, value := range []*string{row.RawProviderUserID, row.RawEmail} {
				if value != nil && strings.TrimSpace(*value) != "" {
					values = append(values, *value)
				}
			}
		}
		for _, value := range values {
			if value != "" && !containsString(bucket, value) {
				bucket = append(bucket, value)
			}
		}
		roster[row.TeamID] = bucket
	}
	return roster
}

func gitlabFirstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func optionalGitLabString(value *string) *string {
	if value == nil {
		return nil
	}
	trimmed := strings.TrimSpace(*value)
	if trimmed == "" {
		return nil
	}
	return &trimmed
}

func dedupeGitLabProjectCatalogRows(rows []gitlabTeamCatalogProjectRow) []gitlabTeamCatalogProjectRow {
	seen := make(map[string]bool, len(rows))
	result := make([]gitlabTeamCatalogProjectRow, 0, len(rows))
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

func validateGitLabTeamRow(claim Claim, row gitlabTeamCatalogTeamRow) error {
	if claim.Provider != gitlabTeamCatalogProvider || row.Provider != gitlabTeamCatalogProvider ||
		row.OrgID != claim.OrgID || strings.TrimSpace(row.ID) == "" || strings.TrimSpace(row.TeamUUID) == "" ||
		row.UpdatedAt.IsZero() || row.IsActive > 1 {
		return ErrInvalidConfiguration
	}
	if _, err := uuid.Parse(row.TeamUUID); err != nil {
		return ErrInvalidConfiguration
	}
	return nil
}

func validateGitLabOwnershipRow(claim Claim, row gitlabTeamCatalogOwnershipRow) error {
	if claim.Provider != gitlabTeamCatalogProvider || row.Provider != gitlabTeamCatalogProvider ||
		row.OrgID != claim.OrgID || strings.TrimSpace(row.TeamID) == "" || strings.TrimSpace(row.ProjectID) == "" ||
		row.Source != gitlabTeamCatalogSource || row.IsPrimary != 0 || row.Priority != gitlabTeamCatalogProviderAccessPriority ||
		row.ValidFrom.IsZero() || row.UpdatedAt.IsZero() {
		return ErrInvalidConfiguration
	}
	return nil
}

func validateGitLabMembershipRow(claim Claim, row gitlabTeamCatalogMembershipRow) error {
	if claim.Provider != gitlabTeamCatalogProvider || row.Provider != gitlabTeamCatalogProvider ||
		row.OrgID != claim.OrgID || strings.TrimSpace(row.TeamID) == "" || strings.TrimSpace(row.MemberID) == "" ||
		row.Source != gitlabTeamCatalogSource || row.IsPrimary != 0 || row.Priority != gitlabTeamCatalogProviderAccessPriority ||
		row.ValidFrom.IsZero() || row.UpdatedAt.IsZero() {
		return ErrInvalidConfiguration
	}
	return nil
}

func (row gitlabTeamCatalogProjectRow) validate(claim Claim) error {
	if claim.Provider != gitlabTeamCatalogProvider || row.Provider != gitlabTeamCatalogProvider ||
		row.OrgID != claim.OrgID || strings.TrimSpace(row.ID) == "" || row.UpdatedAt.IsZero() || row.LastSynced.IsZero() {
		return ErrInvalidConfiguration
	}
	return nil
}
