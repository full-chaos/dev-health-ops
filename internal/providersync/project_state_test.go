package providersync

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/projectmembership"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// CHAOS-9438: one cell per provider x projects. Each asserts the state the
// `projects` row carries (category + deciding provider fact), which is what the
// acr project status filter reads.

var projectStateClosedSet = map[string]bool{
	"backlog": true, "planned": true, "started": true, "paused": true, "completed": true, "canceled": true,
}

func requireProjectState(t *testing.T, label, gotState, gotSource, wantState, wantSource string) {
	t.Helper()
	if !projectStateClosedSet[gotState] {
		t.Fatalf("%s: state %q is outside the closed set", label, gotState)
	}
	if gotState != wantState || gotSource != wantSource {
		t.Fatalf("%s: state=%q source=%q, want %q / %q", label, gotState, gotSource, wantState, wantSource)
	}
}

func TestGitLabProjectStateByArchivedFlag(t *testing.T) {
	now := time.Date(2026, 10, 13, 0, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		archived      bool
		state, source string
	}{
		{false, "started", "gitlab.project.archived=false"},
		{true, "completed", "gitlab.project.archived=true"},
	} {
		row, ok := normalizeGitLabProjectCatalogRow("org-1", gitlabTeamCatalogProjectPayload{
			ID: "42", PathWithNamespace: "g/p", Name: "p", Archived: tc.archived,
		}, now)
		if !ok {
			t.Fatal("row rejected")
		}
		requireProjectState(t, "gitlab", row.State, row.StateSource, tc.state, tc.source)
	}
}

func TestLinearProjectStateKeepsTheClosedSetAndNeverEmpty(t *testing.T) {
	for _, in := range []string{"backlog", "planned", "started", "paused", "completed", "canceled", "Completed", "", "weird"} {
		got := projectmembership.LinearProjectState(strings.ToLower(strings.TrimSpace(in)))
		want := strings.ToLower(in)
		wantSource := "linear.project.status.type=" + want
		if !projectStateClosedSet[want] {
			want, wantSource = "started", "linear.project.status.type=unread"
		}
		requireProjectState(t, "linear "+in, got.State, got.Source, want, wantSource)
	}
}

func TestJiraCatalogProjectRowIsStartedBecauseTheSearchReturnsLiveProjects(t *testing.T) {
	row := normalizeJiraProjectRow("org-1", testPID("10001"), "OPS", "Ops", time.Now())
	requireProjectState(t, "jira catalog", row.State, row.StateSource, "started", "jira.project.status=live")
}

func jiraProjectGetClient(t *testing.T, status int, body string) *providerfoundation.HTTPClient {
	t.Helper()
	doer := jiraWorkItemsDoerFunc(func(request *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: status, Header: http.Header{"Content-Type": []string{"application/json"}},
			Body: io.NopCloser(strings.NewReader(body)), Request: request,
		}, nil
	})
	client, err := providerfoundation.NewHTTPClient(
		"jira", "https://acme.atlassian.net", fakehttp.Client(doer),
		func(request *http.Request) error { return nil },
		providerfoundation.RetryPolicy{MaxAttempts: 1, InitialWait: time.Millisecond, MaxWait: time.Millisecond},
		providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
	)
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func TestJiraEnsureProjectRowStateFollowsTheProjectLifecycle(t *testing.T) {
	now := time.Date(2026, 10, 13, 0, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name          string
		http          int
		body          string
		state, source string
	}{
		{"live", 200, `{"key":"OPS","name":"Ops"}`, "started", "jira.project.status=live"},
		{"archived", 200, `{"key":"OPS","name":"Ops","archived":true}`, "completed", "jira.project.status=archived"},
		{"deleted", 200, `{"key":"OPS","name":"Ops","deleted":true}`, "canceled", "jira.project.status=deleted"},
		{"lookup failed", 500, `{}`, "started", "jira.project.status=unread"},
	} {
		client := jiraProjectGetClient(t, tc.http, tc.body)
		entry, ok := resolveJiraProjectCatalog(context.Background(), client, map[string]jiraProjectCatalogEntry{}, "10001", "Ops", "10001", "OPS")
		if !ok {
			t.Fatalf("%s: unresolved", tc.name)
		}
		row := jiraEnsureProjectsRow("org-1", "10001", entry.Key, entry.Name, entry.Status, now)
		requireProjectState(t, "jira "+tc.name, row.State, row.StateSource, tc.state, tc.source)
	}
}

func TestGitHubProjectV2BoardRowStateFollowsTheClosedFlag(t *testing.T) {
	for _, tc := range []struct {
		name, project string
		state, source string
	}{
		{"open", `"closed":false,`, "started", "github.projectv2.closed=false"},
		{"closed", `"closed":true,`, "completed", "github.projectv2.closed=true"},
		{"flag absent", ``, "started", "github.projectv2.closed=unread"},
	} {
		doer := &gitHubProjectV2Doer{t: t, replies: []string{
			`{"data":{"organization":{"projectV2":{` + tc.project + `"items":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}}}`,
		}}
		client := githubProjectV2TestClient(t, fakehttp.Client(doer))
		claim := githubWorkItemOracleClaim()
		claim.IntegrationConfig = map[string]any{"github_projects_v2": []any{map[string]any{"org_login": "acme", "project_number": 3}}}
		credential := providerfoundation.Credential{Provider: "github", ID: claim.CredentialID}
		result, err := (GitHubProjectV2Fetcher{}).Fetch(context.Background(), claim, credential, client,
			time.Date(2026, 10, 13, 0, 0, 0, 0, time.UTC), nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(result.Rows.Projects) != 1 {
			t.Fatalf("%s: projects=%+v", tc.name, result.Rows.Projects)
		}
		row := result.Rows.Projects[0]
		requireProjectState(t, "github "+tc.name, row.State, row.StateSource, tc.state, tc.source)
	}
}

func TestProjectsInsertCarriesStateColumnsInValueOrder(t *testing.T) {
	row := projectmembership.EnsureProjectsRow("o", "github", "id", "", "n", time.Unix(0, 0)).
		WithState(projectmembership.ProjectState{State: "completed", Source: "x"})
	values := row.Values()
	if !strings.Contains(projectmembership.ProjectsInsert, "is_active,state,state_source,updated_at") ||
		values[5] != uint8(1) || values[6] != "completed" || values[7] != "x" {
		t.Fatalf("insert=%s values=%v", projectmembership.ProjectsInsert, values)
	}
}

// A Linear project first seen through a work item (the ensure row) is never stateless: it reads
// started with source "unread", and stays epoch-anchored so the real catalog row wins.
func TestLinearEnsureRowCarriesAState(t *testing.T) {
	row := linearEnsureProjectsRow("org", "proj-1", "Name", time.Now())
	if row.State != "started" || row.StateSource != "linear.project.status.type=unread" {
		t.Fatalf("ensure row state = %q source = %q, want started / linear.project.status.type=unread", row.State, row.StateSource)
	}
	if !row.UpdatedAt.Equal(linearProjectsEpoch) {
		t.Fatalf("ensure row updated_at = %v, want the epoch anchor", row.UpdatedAt)
	}
}
