package providersync

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

func TestGitLabTeamID(t *testing.T) {
	if got := gitlabTeamID("org/team-a"); got != "gl:org/team-a" {
		t.Fatalf("got %q", got)
	}
	// Idempotent against an already-prefixed input, matching Python's
	// removeprefix("gl:") before re-prefixing.
	if got := gitlabTeamID("gl:org/team-a"); got != "gl:org/team-a" {
		t.Fatalf("got %q", got)
	}
}

func TestGitLabParentTeamID(t *testing.T) {
	if got := gitlabParentTeamID("org"); got != nil {
		t.Fatalf("root should have no parent, got %v", *got)
	}
	got := gitlabParentTeamID("org/team-a")
	if got == nil || *got != "gl:org" {
		t.Fatalf("got %v", got)
	}
}

func TestGitLabTeamDepthOnlyCountsDiscoveredParents(t *testing.T) {
	parentByTeam := gitlabTeamCatalogParentByTeam([]string{"org", "org/team-a"})
	if depth := gitlabTeamDepth("gl:org", parentByTeam); depth != 0 {
		t.Fatalf("root depth = %d", depth)
	}
	if depth := gitlabTeamDepth("gl:org/team-a", parentByTeam); depth != 1 {
		t.Fatalf("subgroup depth = %d", depth)
	}
	// A path-derived parent that was never discovered must not count --
	// mirrors _parent_by_team's existence filter.
	orphanParents := gitlabTeamCatalogParentByTeam([]string{"org/team-a"})
	if depth := gitlabTeamDepth("gl:org/team-a", orphanParents); depth != 0 {
		t.Fatalf("orphan depth = %d, want 0 (parent never discovered)", depth)
	}
}

func TestGitLabTeamCatalogMembershipFacets(t *testing.T) {
	facets := gitlabTeamCatalogMembershipFacets("octocat", nil)
	if len(facets) != 1 || facets[0] != "gitlab:octocat" {
		t.Fatalf("got %v", facets)
	}
	email := "Octo.Cat@Example.com"
	facets = gitlabTeamCatalogMembershipFacets("octocat", &email)
	if len(facets) != 2 || facets[0] != "gitlab:octocat" || facets[1] != "octo.cat@example.com" {
		t.Fatalf("got %v", facets)
	}
	if got := gitlabTeamCatalogMembershipFacets("", nil); got != nil {
		t.Fatalf("empty username should yield no facets, got %v", got)
	}
}

func TestNormalizeGitLabProjectCatalogRow(t *testing.T) {
	now := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)
	row, ok := normalizeGitLabProjectCatalogRow("org-1", gitlabTeamCatalogProjectPayload{
		ID: "42", PathWithNamespace: "org/team-a/svc", Name: "svc", Archived: true, WebURL: "https://gitlab.example.com/org/team-a/svc",
	}, now)
	if !ok {
		t.Fatal("expected ok")
	}
	if row.ID != "org-1:gitlab:42" {
		t.Fatalf("id = %q", row.ID)
	}
	if row.IsActive != 0 {
		t.Fatalf("archived project should be inactive, got is_active=%d", row.IsActive)
	}
	if row.ProjectKey == nil || *row.ProjectKey != "org/team-a/svc" {
		t.Fatalf("project_key = %v", row.ProjectKey)
	}

	if _, ok := normalizeGitLabProjectCatalogRow("org-1", gitlabTeamCatalogProjectPayload{ID: "", PathWithNamespace: "x"}, now); ok {
		t.Fatal("missing numeric id must be rejected")
	}
}

func TestGitlabRosterFromMemberships(t *testing.T) {
	rows := []gitlabTeamCatalogMembershipRow{
		{TeamID: "gl:org", IdentityFacets: []string{"gitlab:alice", "alice@example.com"}},
		{TeamID: "gl:org", IdentityFacets: []string{"gitlab:bob"}},
		{TeamID: "gl:org/team-a", IdentityFacets: []string{"gitlab:carol"}},
	}
	roster := gitlabRosterFromMemberships(rows)
	if got := roster["gl:org"]; len(got) != 3 {
		t.Fatalf("root roster = %v", got)
	}
	if got := roster["gl:org/team-a"]; len(got) != 1 || got[0] != "gitlab:carol" {
		t.Fatalf("subgroup roster = %v", got)
	}
}

// --- End-to-end route collection against a fake GitLab API -------------------

type gitlabTeamCatalogFakeServer struct {
	*httptest.Server
	requests []string
}

// newGitLabTeamCatalogFakeServer routes by hand on r.URL.EscapedPath() rather
// than http.ServeMux: GitLab's own API accepts a group's full_path with "/"
// percent-encoded as %2F in the :id segment (exactly what providerRelativePath
// produces for a multi-segment full_path), but Go's http.ServeMux will not
// pattern-match a request whose raw path contains an escaped slash against a
// literal-slash pattern -- a stdlib routing limitation of the TEST DOUBLE,
// not of the production HTTP client/pagination code being exercised here.
func newGitLabTeamCatalogFakeServer(t *testing.T) *gitlabTeamCatalogFakeServer {
	t.Helper()
	fake := &gitlabTeamCatalogFakeServer{}

	group := func(id int, fullPath, name string) map[string]any {
		return map[string]any{"id": id, "full_path": fullPath, "name": name, "description": nil}
	}
	projectPayload := func(id int, path, name string, archived bool) map[string]any {
		return map[string]any{
			"id": id, "path_with_namespace": path, "name": name, "archived": archived,
			"web_url": "https://gitlab.example.com/" + path,
		}
	}
	memberPayload := func(username, email string) map[string]any {
		payload := map[string]any{"username": username, "name": username}
		if email != "" {
			payload["email"] = email
		}
		return payload
	}

	fake.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fake.requests = append(fake.requests, r.URL.String())
		switch r.URL.EscapedPath() {
		case "/api/v4/groups/org":
			writeGitLabTeamCatalogJSON(t, w, group(1, "org", "Org"))
		case "/api/v4/groups/org/subgroups":
			writeGitLabTeamCatalogJSON(t, w, []map[string]any{group(2, "org/team-a", "Team A")})
		case "/api/v4/groups/org/projects":
			if r.URL.Query().Get("include_subgroups") == "true" {
				writeGitLabTeamCatalogJSON(t, w, []map[string]any{
					projectPayload(100, "org/root-svc", "root-svc", false),
					projectPayload(101, "org/team-a/svc", "svc", false),
					projectPayload(102, "org/team-a/archived-svc", "archived-svc", true),
				})
				return
			}
			writeGitLabTeamCatalogJSON(t, w, []map[string]any{projectPayload(100, "org/root-svc", "root-svc", false)})
		case "/api/v4/groups/org%2Fteam-a/projects":
			writeGitLabTeamCatalogJSON(t, w, []map[string]any{projectPayload(101, "org/team-a/svc", "svc", false)})
		case "/api/v4/groups/org/members":
			writeGitLabTeamCatalogJSON(t, w, []map[string]any{memberPayload("root-owner", "")})
		case "/api/v4/groups/org%2Fteam-a/members":
			writeGitLabTeamCatalogJSON(t, w, []map[string]any{memberPayload("alice", "alice@example.com")})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(fake.Close)
	return fake
}

func writeGitLabTeamCatalogJSON(t *testing.T, w http.ResponseWriter, payload any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		t.Fatalf("encode: %v", err)
	}
}

func gitlabTeamCatalogTestClient(t *testing.T, baseURL string) *providerfoundation.HTTPClient {
	t.Helper()
	client, err := providerfoundation.NewHTTPClient(
		"gitlab", baseURL, http.DefaultClient,
		func(*http.Request) error { return nil },
		providerfoundation.RetryPolicy{MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond},
		providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
	)
	if err != nil {
		t.Fatalf("new http client: %v", err)
	}
	return client
}

func TestGitLabTeamCatalogCollectAllSelections(t *testing.T) {
	fake := newGitLabTeamCatalogFakeServer(t)
	client := gitlabTeamCatalogTestClient(t, fake.URL)
	ref := TeamCatalogReference{OrgID: "org-1", SyncRunID: "run-1"}
	selections := TeamCatalogSelections{Teams: true, Projects: true, Members: true}
	credential := providerfoundation.Credential{Provider: "gitlab", Config: map[string]string{"group_path": "org"}}
	now := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)

	batch, err := (GitLabTeamCatalogRouteHandler{}).CollectTeamCatalog(context.Background(), ref, credential, client, selections, now)
	if err != nil {
		t.Fatalf("collect: %v requests=%v", err, fake.requests)
	}
	if len(batch.Rows.Teams) != 2 {
		t.Fatalf("teams = %d, want 2 (root + team-a)", len(batch.Rows.Teams))
	}
	var root, teamA *gitlabTeamCatalogTeamRow
	for i := range batch.Rows.Teams {
		switch batch.Rows.Teams[i].ID {
		case "gl:org":
			root = &batch.Rows.Teams[i]
		case "gl:org/team-a":
			teamA = &batch.Rows.Teams[i]
		}
	}
	if root == nil || teamA == nil {
		t.Fatalf("missing expected teams: %+v", batch.Rows.Teams)
	}
	if teamA.ParentTeamID == nil || *teamA.ParentTeamID != "gl:org" {
		t.Fatalf("team-a parent = %v", teamA.ParentTeamID)
	}
	if root.ParentTeamID != nil {
		t.Fatalf("root parent should be nil, got %v", *root.ParentTeamID)
	}
	if len(root.ProjectKeys) != 1 || root.ProjectKeys[0] != "org/root-svc" {
		t.Fatalf("root project_keys = %v", root.ProjectKeys)
	}
	if len(teamA.ProjectKeys) != 1 || teamA.ProjectKeys[0] != "org/team-a/svc" {
		t.Fatalf("team-a project_keys = %v", teamA.ProjectKeys)
	}
	if !root.MembersAuthoritative || len(root.Members) != 1 || root.Members[0] != "gitlab:root-owner" {
		t.Fatalf("root members = %v (authoritative=%v)", root.Members, root.MembersAuthoritative)
	}
	if !teamA.MembersAuthoritative || len(teamA.Members) != 2 {
		t.Fatalf("team-a members = %v", teamA.Members)
	}

	// Ownership: root depth 0 -> specificity 100; team-a depth 1 -> 110.
	if len(batch.Rows.Ownership) != 2 {
		t.Fatalf("ownership rows = %d", len(batch.Rows.Ownership))
	}
	for _, row := range batch.Rows.Ownership {
		if row.Source != "provider_access" || row.Priority != gitlabTeamCatalogProviderAccessPriority || row.IsPrimary != 0 {
			t.Fatalf("ownership row mis-shaped: %+v", row)
		}
		switch row.TeamID {
		case "gl:org":
			if row.Specificity != 100 {
				t.Fatalf("root specificity = %d", row.Specificity)
			}
		case "gl:org/team-a":
			if row.Specificity != 110 {
				t.Fatalf("team-a specificity = %d", row.Specificity)
			}
		}
	}

	// Memberships: 2 rows (root-owner, alice), each provider_access/specificity 100.
	if len(batch.Rows.Memberships) != 2 {
		t.Fatalf("membership rows = %d", len(batch.Rows.Memberships))
	}
	for _, row := range batch.Rows.Memberships {
		if row.Specificity != gitlabTeamCatalogBaseSpecificity {
			t.Fatalf("membership specificity = %d, want %d", row.Specificity, gitlabTeamCatalogBaseSpecificity)
		}
	}

	// Native project catalog: 3 rows from the flat include_subgroups listing,
	// one archived -> inactive.
	if len(batch.Rows.Projects) != 3 {
		t.Fatalf("native project rows = %d", len(batch.Rows.Projects))
	}
	activeCount := 0
	for _, row := range batch.Rows.Projects {
		if row.IsActive == 1 {
			activeCount++
		}
	}
	if activeCount != 2 {
		t.Fatalf("active native projects = %d, want 2", activeCount)
	}

	if batch.Effects.Teams == nil || batch.Effects.Ownership == nil || batch.Effects.Memberships == nil || batch.Effects.Projects == nil {
		t.Fatalf("expected all four effect batches when all selections are on")
	}
	if got := len(batch.Effects.Batches()); got != 4 {
		t.Fatalf("Batches() = %d, want 4", got)
	}
}

// TestGitLabTeamCatalogNativeProjectCatalogIsUnscopedByDesignToday documents
// a known, open gap (codex review finding, CHAOS-4432 RISK-NOTES): the
// native project catalog is NOT filtered by selected IntegrationSource ids.
// Python's source_external_ids filter is populated by a live DB join
// (sync_run_units -> integration_sources) reference_discovery.py's
// _load_discovery_context does, a separate computation from
// TeamCatalogReference.SyncOptions -- team-lead's "no per-provider
// injection seams" ruling means this collector does not add its own DB
// dependency to recover it. This test pins the CURRENT behavior so a
// silent regression in either direction (accidentally scoping, or the gap
// growing) is visible, not a guess about what SHOULD happen once
// TeamCatalogReference grows a field for it.
func TestGitLabTeamCatalogNativeProjectCatalogIsUnscopedByDesignToday(t *testing.T) {
	fake := newGitLabTeamCatalogFakeServer(t)
	client := gitlabTeamCatalogTestClient(t, fake.URL)
	ref := TeamCatalogReference{OrgID: "org-1", SyncRunID: "run-1"}
	selections := TeamCatalogSelections{Projects: true}
	credential := providerfoundation.Credential{Provider: "gitlab", Config: map[string]string{"group_path": "org"}}
	now := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)

	batch, err := (GitLabTeamCatalogRouteHandler{}).CollectTeamCatalog(context.Background(), ref, credential, client, selections, now)
	if err != nil {
		t.Fatalf("collect: %v", err)
	}
	if len(batch.Rows.Projects) != 3 {
		t.Fatalf("native projects = %d, want 3 (every discovered project, unscoped)", len(batch.Rows.Projects))
	}
}

func TestGitLabTeamCatalogTeamsOnlySkipsMembersAndProjects(t *testing.T) {
	fake := newGitLabTeamCatalogFakeServer(t)
	client := gitlabTeamCatalogTestClient(t, fake.URL)
	ref := TeamCatalogReference{OrgID: "org-1", SyncRunID: "run-1"}
	selections := TeamCatalogSelections{Teams: true}
	credential := providerfoundation.Credential{Provider: "gitlab", Config: map[string]string{"group_path": "org"}}
	now := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)

	batch, err := (GitLabTeamCatalogRouteHandler{}).CollectTeamCatalog(context.Background(), ref, credential, client, selections, now)
	if err != nil {
		t.Fatalf("collect: %v requests=%v", err, fake.requests)
	}
	if len(batch.Rows.Teams) != 2 {
		t.Fatalf("teams = %d", len(batch.Rows.Teams))
	}
	for _, row := range batch.Rows.Teams {
		if row.MembersAuthoritative {
			t.Fatalf("teams-only run must not claim roster authority for %s", row.ID)
		}
	}
	if len(batch.Rows.Ownership) != 0 || len(batch.Rows.Memberships) != 0 || len(batch.Rows.Projects) != 0 {
		t.Fatalf("expected only team rows, got ownership=%d memberships=%d projects=%d",
			len(batch.Rows.Ownership), len(batch.Rows.Memberships), len(batch.Rows.Projects))
	}
	if batch.Effects.Teams == nil || batch.Effects.Ownership != nil || batch.Effects.Memberships != nil || batch.Effects.Projects != nil {
		t.Fatalf("expected only the teams effect batch")
	}
	// The route must never hit the members endpoint when members are off.
	for _, request := range fake.requests {
		if strings.Contains(request, "/members") {
			t.Fatalf("members endpoint hit despite auto_import_members=false: %s", request)
		}
	}
}

func TestGitLabTeamCatalogNoSelectionsIsZeroSummary(t *testing.T) {
	ref := TeamCatalogReference{OrgID: "org-1", SyncRunID: "run-1"}
	credential := providerfoundation.Credential{Provider: "gitlab", Config: map[string]string{"group_path": "org"}}
	client, err := providerfoundation.NewHTTPClient("gitlab", "https://gitlab.example.com", http.DefaultClient,
		func(*http.Request) error { return nil }, providerfoundation.RetryPolicy{MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond},
		providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }))
	if err != nil {
		t.Fatal(err)
	}
	batch, err := (GitLabTeamCatalogRouteHandler{}).CollectTeamCatalog(context.Background(), ref, credential, client, TeamCatalogSelections{}, time.Now())
	if err != nil {
		t.Fatalf("collect: %v", err)
	}
	if len(batch.Rows.Teams) != 0 || len(batch.Effects.Batches()) != 0 {
		t.Fatalf("expected a no-op zero summary, got %+v", batch)
	}
}

// TestGitLabTeamCatalogGroupPathPrecedence proves gitlabTeamCatalogGroupPath
// mirrors team_autoimport_gitlab._gitlab_group's exact key precedence
// (group_path outranks group outranks owner, credential.Config outranks
// ref.SyncOptions) -- team-lead ruling, 2026-08-28: group_path resolves
// from ref.SyncOptions directly, no injectable resolver.
func TestGitLabTeamCatalogGroupPathPrecedence(t *testing.T) {
	cases := []struct {
		name        string
		credential  providerfoundation.Credential
		syncOptions map[string]any
		want        string
	}{
		{
			name:        "sync_options owner only (org 70d529e0's real shape)",
			credential:  providerfoundation.Credential{Provider: "gitlab"},
			syncOptions: map[string]any{"owner": "full.chaos", "auto_import_teams": false},
			want:        "full.chaos",
		},
		{
			name:        "sync_options group_path outranks group and owner",
			credential:  providerfoundation.Credential{Provider: "gitlab"},
			syncOptions: map[string]any{"group_path": "org/team-a", "group": "org", "owner": "org-owner"},
			want:        "org/team-a",
		},
		{
			name:        "credential.Config outranks ref.SyncOptions",
			credential:  providerfoundation.Credential{Provider: "gitlab", Config: map[string]string{"owner": "cred-owner"}},
			syncOptions: map[string]any{"owner": "sync-options-owner"},
			want:        "cred-owner",
		},
		{
			name:        "nothing configured",
			credential:  providerfoundation.Credential{Provider: "gitlab"},
			syncOptions: nil,
			want:        "",
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			ref := TeamCatalogReference{OrgID: "org-1", SyncRunID: "run-1", SyncOptions: testCase.syncOptions}
			if got := gitlabTeamCatalogGroupPath(testCase.credential, ref); got != testCase.want {
				t.Fatalf("got %q, want %q", got, testCase.want)
			}
		})
	}
}

func TestGitLabTeamCatalogRejectsInvalidInputs(t *testing.T) {
	ref := TeamCatalogReference{OrgID: "org-1", SyncRunID: "run-1"}
	selections := TeamCatalogSelections{Teams: true}
	client, err := providerfoundation.NewHTTPClient("gitlab", "https://gitlab.example.com", http.DefaultClient,
		func(*http.Request) error { return nil }, providerfoundation.RetryPolicy{MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond},
		providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }))
	if err != nil {
		t.Fatal(err)
	}
	// No group_path anywhere (empty Config, no GroupPathResolver).
	credential := providerfoundation.Credential{Provider: "gitlab"}
	if _, err := (GitLabTeamCatalogRouteHandler{}).CollectTeamCatalog(context.Background(), ref, credential, client, selections, time.Now()); err == nil {
		t.Fatal("expected error for missing group_path")
	}
	// Wrong credential provider.
	credential = providerfoundation.Credential{Provider: "github", Config: map[string]string{"group_path": "org"}}
	if _, err := (GitLabTeamCatalogRouteHandler{}).CollectTeamCatalog(context.Background(), ref, credential, client, selections, time.Now()); err == nil {
		t.Fatal("expected error for mismatched credential provider")
	}
	// Invalid reference (no OrgID/SyncRunID).
	credential = providerfoundation.Credential{Provider: "gitlab", Config: map[string]string{"group_path": "org"}}
	if _, err := (GitLabTeamCatalogRouteHandler{}).CollectTeamCatalog(context.Background(), TeamCatalogReference{}, credential, client, selections, time.Now()); err == nil {
		t.Fatal("expected error for invalid reference")
	}
}
