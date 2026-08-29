package providersync

import (
	"strings"
	"time"

	"github.com/google/uuid"
)

// github_team_catalog.go ports src/dev_health_ops/workers/team_autoimport_github.py
// (CHAOS-4434) to Go: GitHub org teams -> teams (source=provider_access),
// team members -> team_memberships, and team<->repo grants -> team_repo_
// ownership (source=provider_access). GitHub has no "Projects" auto-import
// concept (auto_import_capabilities("github").projects is always False in
// Python), so this producer never writes team_project_ownership.
//
// CORRECTION (team-lead ruling, 2026-08-28, after codex round 1 flagged it):
// the ticket's original premise -- "repo ownership goes through the
// Go-native team_repo_ownership_derivation, leave it alone" -- was WRONG.
// team_repo_ownership_derivation (CHAOS-4365 item 1b) derives solely from
// team_project_ownership (internal/providersync/team_repo_ownership_
// derivation_clickhouse.go:185's FROM clause), which GitHub NEVER writes at
// all (want_projects is permanently False). So that derivation has zero
// GitHub input and cannot reproduce GitHub's direct team<->repo grants --
// this producer's own write (githubTeamRepoOwnershipRow / WriteTeamRepoOwnership)
// is the only source for them, ported here rather than left a follow-up: a
// native provider that stops refreshing a live table on cutover is a
// blocker, not a nice-to-have.
//
// Scope boundary (documented, not silently dropped): the Python module also
// layers two review/conflict subsystems on top of the raw writes --
// clickhouse_team_drift_projector (team-level field changes: name/description/
// members/project_keys/repo_patterns, gated by a per-org policy) and
// clickhouse_identity_drift.split_memberships_for_review (holds back a
// membership row that would fight a `manual` override or a member fallback,
// staging it in team_drift_changes instead of writing it). Neither layer is
// ported here. This matches the ALREADY-SHIPPED CHAOS-3716 Linear reference
// catalog Go route (internal/providersync/linear_reference_catalog*.go), which
// performs the exact same kind of direct upsert into teams/team_memberships
// with no drift-review layer. Roster preservation for a members-off run IS
// ported (GitHubTeamCatalogClickHouseEffects.ExistingTeamMembers) since it is a
// simple, self-contained safe-fail read with no policy dependency. The two
// skipped layers are tracked as a follow-up under CHAOS-4198 (see PR
// RISK-NOTES) rather than partially/silently reimplemented.

const (
	githubTeamCatalogProvider               = "github"
	githubTeamCatalogSource                 = "provider_access"
	githubTeamCatalogBaseSpecificity        = 100
	githubTeamCatalogProviderAccessPriority = 300
)

// githubTeamPayload is the REST shape of one element from
// GET /orgs/{org}/teams (and the per-team detail fetch is never needed: the
// org-teams list endpoint already carries slug/name/description).
type githubTeamPayload struct {
	Slug        string  `json:"slug"`
	Name        string  `json:"name"`
	Description *string `json:"description"`
}

// githubTeamRepoPayload is one element from
// GET /orgs/{org}/teams/{slug}/repos.
type githubTeamRepoPayload struct {
	Name string `json:"name"`
}

// githubTeamMemberPayload is one element from
// GET /orgs/{org}/teams/{slug}/members. GitHub's team-members endpoint never
// returns an email (a "simple user" object); email is resolved separately per
// login the same way PyGithub's lazy NamedUser.email completion does
// (GET /users/{login}), see githubTeamCatalogMemberEmail in the route file.
type githubTeamMemberPayload struct {
	Login string `json:"login"`
}

// githubTeamRow mirrors the exact `teams` row shape Python's insert_teams
// writes for a GitHub-sourced team (storage/clickhouse.py ClickHouseStore.
// insert_teams + team_autoimport_github.py's _team_rows). ProjectKeys is
// always empty: GitHub teams carry no project association at all.
type githubTeamRow struct {
	ID          string   `json:"id"`
	TeamUUID    string   `json:"team_uuid"`
	Name        string   `json:"name"`
	Description *string  `json:"description"`
	Members     []string `json:"members"`
	// ManualMembers carries forward the CURRENTLY persisted teams.
	// manual_members provenance column (CHAOS-4321, migration 079): this
	// producer never sets it itself (matching Python's _team_rows, which has
	// no such key either) -- it is always populated by the write layer
	// (GitHubTeamCatalogClickHouseEffects.WriteTeams) from a pre-write
	// ClickHouse read, exactly mirroring storage/clickhouse.py's
	// insert_teams/_preserve_existing_manual_members. Left unset here (nil)
	// is the correct default for a normalizer that has not yet consulted
	// ClickHouse; WriteTeams must never send a bare nil straight to the
	// INSERT without first resolving it, or a brand-new admin override gets
	// silently wiped on this team's very next sync.
	ManualMembers []string  `json:"manual_members"`
	ProjectKeys   []string  `json:"project_keys"`
	RepoPatterns  []string  `json:"repo_patterns"`
	IsActive      uint8     `json:"is_active"`
	UpdatedAt     time.Time `json:"updated_at"`
	OrgID         string    `json:"org_id"`
	Provider      string    `json:"provider"`
	NativeTeamKey *string   `json:"native_team_key"`
	// ParentTeamID is always nil: team_discovery.py's discover_github never
	// populates associations["parent_team_id"], so the Python producer's own
	// _parent_team_id always returns None for GitHub today. Matching CURRENT
	// behavior, not GitHub's API-native team hierarchy, is the byte-parity
	// contract for this port.
	ParentTeamID *string `json:"parent_team_id"`
}

// githubMembershipRow mirrors TeamMembershipRecord exactly as
// team_autoimport_github.py's _membership_rows constructs it.
type githubMembershipRow struct {
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

// githubTeamRepoOwnershipRow mirrors TeamRepoOwnershipRecord exactly as
// team_autoimport_github.py's _repo_ownership_rows constructs it (CHAOS-4434
// correction: this table has NO Go-native replacement elsewhere --
// team_repo_ownership_derivation (CHAOS-4365) derives solely from
// team_project_ownership, which GitHub never writes at all, so this
// producer's direct write is the only source for GitHub's team<->repo
// grants and must ship in this same PR, not a follow-up).
type githubTeamRepoOwnershipRow struct {
	OrgID        string     `json:"org_id"`
	Provider     string     `json:"provider"`
	TeamID       string     `json:"team_id"`
	RepoID       *string    `json:"repo_id"`
	RepoFullName string     `json:"repo_full_name"`
	MatchType    string     `json:"match_type"`
	Source       string     `json:"source"`
	IsPrimary    uint8      `json:"is_primary"`
	Specificity  uint16     `json:"specificity"`
	Priority     int32      `json:"priority"`
	ValidFrom    time.Time  `json:"valid_from"`
	ValidTo      *time.Time `json:"valid_to"`
	UpdatedAt    time.Time  `json:"updated_at"`
}

// githubTeamCatalogRows is the complete, deduplicated output of one
// collection pass, ready for ClickHouse write.
type githubTeamCatalogRows struct {
	Teams         []githubTeamRow
	Memberships   []githubMembershipRow
	RepoOwnership []githubTeamRepoOwnershipRow
	// FailedMemberFetchTeamIDs (CHAOS-4461) lists the teams (by their "gh:"
	// id, matching githubTeamRow.ID) whose member fetch failed under a
	// non-strict Collect while members were globally selected. The roster
	// rebuild below can only ever produce [] for these teams (no
	// memberships were ever added for them); the caller (GitHubTeamCatalog
	// Collector.CollectTeamCatalog) MUST NOT write that empty roster --
	// it must confirm and carry forward the currently-persisted one instead,
	// the same roster_write_safe discipline the members-globally-off path
	// already uses, applied per-team.
	FailedMemberFetchTeamIDs []string
	// ObservedMembershipTeamIDs (CHAOS-4444) lists every team (by "gh:" id)
	// whose member fetch was ATTEMPTED and SUCCEEDED this call, independent
	// of wantTeams -- Teams' own row (and therefore rows.Teams) is only
	// ever populated `if wantTeams`, but member fetches happen whenever
	// wantMembers is true regardless, so deriving "observed" scopes from
	// rows.Teams undercounts (silently empty) whenever a run selects
	// Members without Teams. This is the identity-drift review engine's
	// observed_team_ids equivalent -- see reviewMembershipsForDrift's doc
	// comment for why an unobserved scope must never have its stale pending
	// changes resolved.
	ObservedMembershipTeamIDs []string
}

// githubTeamID mirrors team_autoimport_github.py's _team_id: "gh:" + the
// GitHub team slug, idempotent against an already-prefixed input.
func githubTeamID(slug string) string {
	return "gh:" + strings.TrimPrefix(strings.TrimSpace(slug), "gh:")
}

// githubTeamUUID mirrors ClickHouseStore.insert_teams's deterministic
// fallback: uuid.uuid5(uuid.NAMESPACE_URL, f"team:{team_id}").
func githubTeamUUID(teamID string) string {
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte("team:"+teamID)).String()
}

// githubQualifiedIdentity mirrors providers/identity.py's
// provider_qualified_identity("github", username=login): the stable,
// no-email facet team auto-import and the work-item assignee ladder both
// converge on (CHAOS-2609).
func githubQualifiedIdentity(login string) string {
	return "github:" + strings.TrimSpace(login)
}

// githubMembershipFacets mirrors IdentityResolver.membership_facets for a
// GitHub member. It assumes an empty alias_to_canonical map: the same
// simplification the already-shipped Linear route makes
// (linearReferenceMembershipFacets), consistent with this deployment's
// checked-in src/dev_health_ops/config/identity_mapping.yaml shipping
// `identities: []` (see internal/jobs/metrics/daily/repouser/identity.go's
// doc comment) -- with an empty alias map, resolve()'s alias lookup always
// misses, so the no-email identity and the qualified identity coincide at
// "github:<login>", exactly as this function returns.
func githubMembershipFacets(login, email string) []string {
	login = strings.TrimSpace(login)
	if login == "" {
		return nil
	}
	facets := []string{githubQualifiedIdentity(login)}
	if normalized := strings.ToLower(strings.TrimSpace(email)); normalized != "" && !containsString(facets, normalized) {
		facets = append(facets, normalized)
	}
	return facets
}

func normalizeGitHubTeam(
	orgID string,
	payload githubTeamPayload,
	repoPatterns []string,
	normalizedAt time.Time,
) (githubTeamRow, error) {
	slug := strings.TrimSpace(payload.Slug)
	if strings.TrimSpace(orgID) == "" || slug == "" || normalizedAt.IsZero() {
		return githubTeamRow{}, ErrInvalidConfiguration
	}
	normalizedAt = normalizedAt.UTC().Truncate(time.Microsecond)
	teamID := githubTeamID(slug)
	name := strings.TrimSpace(payload.Name)
	if name == "" {
		name = teamID
	}
	nativeTeamKey := slug
	patterns := repoPatterns
	if patterns == nil {
		patterns = []string{}
	}
	return githubTeamRow{
		ID: teamID, TeamUUID: githubTeamUUID(teamID), Name: name, Description: payload.Description,
		Members: []string{}, ProjectKeys: []string{}, RepoPatterns: patterns, IsActive: 1,
		UpdatedAt: normalizedAt, OrgID: orgID, Provider: githubTeamCatalogProvider,
		NativeTeamKey: &nativeTeamKey, ParentTeamID: nil,
	}, nil
}

func normalizeGitHubMembership(
	orgID, teamSlug, login, email string,
	normalizedAt time.Time,
) (githubMembershipRow, error) {
	login = strings.TrimSpace(login)
	if strings.TrimSpace(orgID) == "" || strings.TrimSpace(teamSlug) == "" || login == "" || normalizedAt.IsZero() {
		return githubMembershipRow{}, ErrInvalidConfiguration
	}
	normalizedAt = normalizedAt.UTC().Truncate(time.Microsecond)
	facets := githubMembershipFacets(login, email)
	if len(facets) == 0 {
		return githubMembershipRow{}, ErrInvalidConfiguration
	}
	rawProviderUserID := facets[0]
	var rawEmail *string
	if trimmed := strings.TrimSpace(email); trimmed != "" {
		rawEmail = &trimmed
	}
	return githubMembershipRow{
		OrgID: orgID, Provider: githubTeamCatalogProvider, TeamID: githubTeamID(teamSlug),
		MemberID: "gh:" + login, RawProviderUserID: &rawProviderUserID, RawEmail: rawEmail,
		IdentityFacets: facets, Source: githubTeamCatalogSource, IsPrimary: 0,
		Specificity: githubTeamCatalogBaseSpecificity, Priority: githubTeamCatalogProviderAccessPriority,
		ValidFrom: normalizedAt, ValidTo: nil, UpdatedAt: normalizedAt,
	}, nil
}

// githubTeamRosterFromMemberships mirrors _roster_from_memberships: the
// union (first-seen order, deduplicated) of every membership row's identity
// facets, grouped by team_id.
func githubTeamRosterFromMemberships(rows []githubMembershipRow) map[string][]string {
	roster := make(map[string][]string)
	for _, row := range rows {
		bucket := roster[row.TeamID]
		for _, facet := range row.IdentityFacets {
			if facet != "" && !containsString(bucket, facet) {
				bucket = append(bucket, facet)
			}
		}
		roster[row.TeamID] = bucket
	}
	return roster
}

func dedupeGitHubTeams(rows []githubTeamRow) []githubTeamRow {
	result := make([]githubTeamRow, 0, len(rows))
	for _, row := range rows {
		found := false
		for index := range result {
			if result[index].OrgID == row.OrgID && result[index].ID == row.ID {
				result[index] = row
				found = true
				break
			}
		}
		if !found {
			result = append(result, row)
		}
	}
	return result
}

// normalizeGitHubTeamRepoOwnership mirrors team_autoimport_github.py's
// _repo_ownership_rows per (team, repo) pair. specificity always collapses
// to githubTeamCatalogBaseSpecificity: Python's BASE_SPECIFICITY +
// depth*CHILD_SPECIFICITY_STEP, where depth is the team's position in
// parent_by_team -- always 0 for GitHub today, since ParentTeamID is always
// nil (see githubTeamRow.ParentTeamID's doc comment). repo_id is always nil,
// matching TeamRepoOwnershipRecord's own default (_repo_ownership_rows never
// passes it).
func normalizeGitHubTeamRepoOwnership(
	orgID, teamSlug, repoFullName string, normalizedAt time.Time,
) (githubTeamRepoOwnershipRow, error) {
	teamSlug = strings.TrimSpace(teamSlug)
	repoFullName = strings.TrimSpace(repoFullName)
	if strings.TrimSpace(orgID) == "" || teamSlug == "" || repoFullName == "" || normalizedAt.IsZero() {
		return githubTeamRepoOwnershipRow{}, ErrInvalidConfiguration
	}
	normalizedAt = normalizedAt.UTC().Truncate(time.Microsecond)
	return githubTeamRepoOwnershipRow{
		OrgID: orgID, Provider: githubTeamCatalogProvider, TeamID: githubTeamID(teamSlug),
		RepoID: nil, RepoFullName: repoFullName, MatchType: "exact", Source: githubTeamCatalogSource,
		IsPrimary: 0, Specificity: githubTeamCatalogBaseSpecificity, Priority: githubTeamCatalogProviderAccessPriority,
		ValidFrom: normalizedAt, ValidTo: nil, UpdatedAt: normalizedAt,
	}, nil
}

func dedupeGitHubTeamRepoOwnership(rows []githubTeamRepoOwnershipRow) []githubTeamRepoOwnershipRow {
	result := make([]githubTeamRepoOwnershipRow, 0, len(rows))
	for _, row := range rows {
		found := false
		for index := range result {
			if result[index].OrgID == row.OrgID && result[index].TeamID == row.TeamID &&
				result[index].RepoFullName == row.RepoFullName {
				result[index] = row
				found = true
				break
			}
		}
		if !found {
			result = append(result, row)
		}
	}
	return result
}

func dedupeGitHubMemberships(rows []githubMembershipRow) []githubMembershipRow {
	result := make([]githubMembershipRow, 0, len(rows))
	for _, row := range rows {
		found := false
		for index := range result {
			if result[index].OrgID == row.OrgID && result[index].TeamID == row.TeamID && result[index].MemberID == row.MemberID {
				result[index] = row
				found = true
				break
			}
		}
		if !found {
			result = append(result, row)
		}
	}
	return result
}
