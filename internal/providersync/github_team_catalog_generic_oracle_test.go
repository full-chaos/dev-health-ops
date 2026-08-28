package providersync

import (
	"encoding/json"
	"testing"
	"time"
)

// github_team_catalog_generic_oracle_test.go proves the Go port (CHAOS-4434)
// matches the LIVE, checked-in Python producer (team_autoimport_github.py's
// _github_team_row / _github_membership_row, extracted from _team_rows /
// _membership_rows without behavior change specifically to make this
// comparison possible -- see those functions' doc comments) via the shared
// live-python-oracle harness (ci/check_go.sh live-python-oracles).

type githubTeamCatalogTeamProducerRow struct {
	ID            string    `json:"id"`
	Name          string    `json:"name"`
	Description   *string   `json:"description"`
	Members       []string  `json:"members"`
	ProjectKeys   []string  `json:"project_keys"`
	RepoPatterns  []string  `json:"repo_patterns"`
	IsActive      bool      `json:"is_active"`
	UpdatedAt     time.Time `json:"updated_at"`
	OrgID         string    `json:"org_id"`
	Provider      string    `json:"provider"`
	NativeTeamKey string    `json:"native_team_key"`
	ParentTeamID  *string   `json:"parent_team_id"`
}

func buildGitHubTeamCatalogTeamOracleRow(t *testing.T, input map[string]any) githubTeamCatalogTeamProducerRow {
	t.Helper()
	rawPatterns, err := json.Marshal(input["repo_patterns"])
	if err != nil {
		t.Fatal(err)
	}
	var repoPatterns []string
	if err := json.Unmarshal(rawPatterns, &repoPatterns); err != nil {
		t.Fatal(err)
	}
	normalizedAt, err := time.Parse(time.RFC3339Nano, input["normalized_at"].(string))
	if err != nil {
		t.Fatal(err)
	}
	var description *string
	if value, ok := input["description"].(string); ok {
		description = &value
	}
	payload := githubTeamPayload{Slug: input["team_slug"].(string), Name: input["name"].(string), Description: description}
	team, err := normalizeGitHubTeam(input["org_id"].(string), payload, repoPatterns, normalizedAt)
	if err != nil {
		t.Fatal(err)
	}
	return githubTeamCatalogTeamProducerRow{
		ID: team.ID, Name: team.Name, Description: team.Description, Members: team.Members,
		ProjectKeys: team.ProjectKeys, RepoPatterns: team.RepoPatterns, IsActive: team.IsActive == 1,
		UpdatedAt: team.UpdatedAt, OrgID: team.OrgID, Provider: team.Provider,
		NativeTeamKey: *team.NativeTeamKey, ParentTeamID: team.ParentTeamID,
	}
}

func TestGitHubTeamCatalogTeamRowMatchesLivePythonProducer(t *testing.T) {
	compareRowsAgainstPythonOracle(
		t, "github/team-catalog/team",
		[]oracleCase{
			{ID: "team_with_repos", Input: map[string]any{
				"org_id": "org-acme", "team_slug": "platform", "name": "Platform",
				"description": "Platform team", "repo_patterns": []any{"acme/api", "acme/web"},
				"normalized_at": "2026-08-10T12:34:56.789Z",
			}},
			{ID: "team_without_description_or_repos", Input: map[string]any{
				"org_id": "org-acme", "team_slug": "ops", "name": "Operations",
				"description": nil, "repo_patterns": []any{},
				"normalized_at": "2026-08-10T12:34:56.789Z",
			}},
		},
		buildGitHubTeamCatalogTeamOracleRow,
		nil,
	)
}

// githubTeamCatalogMembershipProducerRow mirrors dataclasses.asdict(
// TeamMembershipRecord(...)) field-for-field (schemas.py).
type githubTeamCatalogMembershipProducerRow struct {
	OrgID             string     `json:"org_id"`
	Provider          string     `json:"provider"`
	TeamID            string     `json:"team_id"`
	MemberID          string     `json:"member_id"`
	RawProviderUserID *string    `json:"raw_provider_user_id"`
	RawEmail          *string    `json:"raw_email"`
	IdentityFacets    []string   `json:"identity_facets"`
	Source            string     `json:"source"`
	IsPrimary         int        `json:"is_primary"`
	Specificity       int        `json:"specificity"`
	Priority          int        `json:"priority"`
	ValidFrom         time.Time  `json:"valid_from"`
	ValidTo           *time.Time `json:"valid_to"`
	UpdatedAt         time.Time  `json:"updated_at"`
}

func buildGitHubTeamCatalogMembershipOracleRow(t *testing.T, input map[string]any) githubTeamCatalogMembershipProducerRow {
	t.Helper()
	normalizedAt, err := time.Parse(time.RFC3339Nano, input["normalized_at"].(string))
	if err != nil {
		t.Fatal(err)
	}
	email := ""
	if value, ok := input["email"].(string); ok {
		email = value
	}
	membership, err := normalizeGitHubMembership(
		input["org_id"].(string), input["team_slug"].(string), input["login"].(string), email, normalizedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	return githubTeamCatalogMembershipProducerRow{
		OrgID: membership.OrgID, Provider: membership.Provider, TeamID: membership.TeamID,
		MemberID: membership.MemberID, RawProviderUserID: membership.RawProviderUserID,
		RawEmail: membership.RawEmail, IdentityFacets: membership.IdentityFacets, Source: membership.Source,
		IsPrimary: int(membership.IsPrimary), Specificity: int(membership.Specificity),
		Priority: int(membership.Priority), ValidFrom: membership.ValidFrom, ValidTo: membership.ValidTo,
		UpdatedAt: membership.UpdatedAt,
	}
}

func TestGitHubTeamCatalogMembershipRowMatchesLivePythonProducer(t *testing.T) {
	compareRowsAgainstPythonOracle(
		t, "github/team-catalog/membership",
		[]oracleCase{
			{ID: "member_with_email", Input: map[string]any{
				"org_id": "org-acme", "team_slug": "platform", "login": "octocat",
				"email": "Octocat@Example.com", "normalized_at": "2026-08-10T12:34:56.789Z",
			}},
			{ID: "member_without_email", Input: map[string]any{
				"org_id": "org-acme", "team_slug": "ops", "login": "monalisa",
				"normalized_at": "2026-08-10T12:34:56.789Z",
			}},
		},
		buildGitHubTeamCatalogMembershipOracleRow,
		nil,
	)
}

type githubTeamCatalogFacetsProducerRow struct {
	Facets []string `json:"facets"`
}

func TestGitHubTeamCatalogFacetsMatchLivePythonResolver(t *testing.T) {
	compareRowsAgainstPythonOracle(
		t, "github/team-catalog/facets",
		[]oracleCase{
			{ID: "login_with_email", Input: map[string]any{"login": "octocat", "email": "Octocat@Example.com"}},
			{ID: "login_without_email", Input: map[string]any{"login": "monalisa"}},
		},
		func(t *testing.T, input map[string]any) githubTeamCatalogFacetsProducerRow {
			t.Helper()
			email := ""
			if value, ok := input["email"].(string); ok {
				email = value
			}
			return githubTeamCatalogFacetsProducerRow{Facets: githubMembershipFacets(input["login"].(string), email)}
		},
		nil,
	)
}

// githubTeamCatalogRepoOwnershipProducerRow mirrors dataclasses.asdict(
// TeamRepoOwnershipRecord(...)) field-for-field (schemas.py). repo_id is
// always nil: _repo_ownership_rows never sets it.
type githubTeamCatalogRepoOwnershipProducerRow struct {
	OrgID        string     `json:"org_id"`
	Provider     string     `json:"provider"`
	TeamID       string     `json:"team_id"`
	RepoFullName string     `json:"repo_full_name"`
	MatchType    string     `json:"match_type"`
	Source       string     `json:"source"`
	IsPrimary    int        `json:"is_primary"`
	Specificity  int        `json:"specificity"`
	Priority     int        `json:"priority"`
	ValidFrom    time.Time  `json:"valid_from"`
	ValidTo      *time.Time `json:"valid_to"`
	UpdatedAt    time.Time  `json:"updated_at"`
	RepoID       *string    `json:"repo_id"`
}

func TestGitHubTeamCatalogRepoOwnershipRowMatchesLivePythonProducer(t *testing.T) {
	compareRowsAgainstPythonOracle(
		t, "github/team-catalog/repo-ownership",
		[]oracleCase{
			{ID: "single_repo_grant", Input: map[string]any{
				"org_id": "org-acme", "team_slug": "platform", "repo_full_name": "acme/api",
				"normalized_at": "2026-08-10T12:34:56.789Z",
			}},
			{ID: "different_team_and_repo", Input: map[string]any{
				"org_id": "org-acme", "team_slug": "ops", "repo_full_name": "acme/infra",
				"normalized_at": "2026-08-10T12:34:56.789Z",
			}},
		},
		func(t *testing.T, input map[string]any) githubTeamCatalogRepoOwnershipProducerRow {
			t.Helper()
			normalizedAt, err := time.Parse(time.RFC3339Nano, input["normalized_at"].(string))
			if err != nil {
				t.Fatal(err)
			}
			row, err := normalizeGitHubTeamRepoOwnership(
				input["org_id"].(string), input["team_slug"].(string), input["repo_full_name"].(string), normalizedAt,
			)
			if err != nil {
				t.Fatal(err)
			}
			return githubTeamCatalogRepoOwnershipProducerRow{
				OrgID: row.OrgID, Provider: row.Provider, TeamID: row.TeamID, RepoFullName: row.RepoFullName,
				MatchType: row.MatchType, Source: row.Source, IsPrimary: int(row.IsPrimary),
				Specificity: int(row.Specificity), Priority: int(row.Priority), ValidFrom: row.ValidFrom,
				ValidTo: row.ValidTo, UpdatedAt: row.UpdatedAt, RepoID: row.RepoID,
			}
		},
		nil,
	)
}
