package providersync

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// A work-items unit holds exactly the half-open window [since, before) it was given. The GitLab API bound
// (updated_before) is inclusive and the Jira date bounds are wider than the window by up to 50 hours, so
// the fetch can return items outside it; those belong to the adjacent window and must not be written twice.
// nativeTestClaim's window is [2026-07-01T00:00:00Z, 2026-07-31T23:59:59Z).
var workItemsWindowCases = []struct {
	number  int
	updated string
	keep    bool
	reason  string
}{
	{1, "2026-06-30T23:00:00Z", false, "an hour before the start"},
	{2, "2026-07-01T00:00:00Z", true, "exactly at the start"},
	{3, "2026-07-15T12:00:00Z", true, "inside"},
	{4, "2026-07-31T23:59:59Z", false, "exactly at the end belongs to the next window"},
	{5, "2026-08-01T01:00:00Z", false, "after the end"},
}

func TestGitLabWorkItemsRouteKeepsOnlyTheHalfOpenWindow(t *testing.T) {
	root := "/api/v4/projects/123"
	issues := make([]map[string]any, 0, len(workItemsWindowCases))
	for _, c := range workItemsWindowCases {
		issues = append(issues, map[string]any{
			"iid": c.number, "title": fmt.Sprintf("issue %d", c.number), "description": "", "state": "opened",
			"created_at": "2026-06-01T09:00:00Z", "updated_at": c.updated, "closed_at": nil, "labels": []string{},
		})
	}
	body, _ := json.Marshal(issues)
	doer := &gitLabWorkItemsDoer{responses: map[string][]string{
		root:                    {`{"id":123,"path":"api","path_with_namespace":"acme/api","name":"api"}`},
		root + "/issues?page=1": {string(body), `[]`},
	}}
	off := false
	claim := nativeTestClaim("gitlab", "work-items")
	batch, err := (GitLabWorkItemsRouteHandler{
		StatusMapping: loadRealStatusMapping(t), PerPage: 10, MaxPages: 10, NestedMaxPages: 10,
		FetchComments: &off, FetchHistory: &off, FetchLabels: &off, FetchLinks: &off, FetchMilestones: &off,
		IncludeMRs: &off, FetchEpics: &off,
	}).Collect(context.Background(), claim, providerfoundation.Credential{Provider: "gitlab", ID: claim.CredentialID},
		gitLabWorkItemsClient(t, fakehttp.Client(doer)), time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	assertKeptWorkItems(t, batch, "gitlab:acme/api#")
}

func TestJiraWorkItemsRouteKeepsOnlyTheHalfOpenWindow(t *testing.T) {
	issues := make([]map[string]any, 0, len(workItemsWindowCases))
	for _, c := range workItemsWindowCases {
		issues = append(issues, map[string]any{
			"key": fmt.Sprintf("OPS-%d", c.number), "self": "https://acme.atlassian.net/rest/api/3/issue/1",
			"fields": map[string]any{
				"project": map[string]any{"key": "OPS", "id": "10001", "name": "Operations"}, "summary": "s", "description": "",
				"status":    map[string]any{"name": "To Do", "statusCategory": map[string]any{"key": "new"}},
				"issuetype": map[string]any{"name": "Task"}, "labels": []string{},
				"created": "2026-06-01T09:00:00Z", "updated": c.updated,
			},
		})
	}
	search, _ := json.Marshal(map[string]any{"issues": issues, "isLast": true})
	doer := jiraWorkItemsDoerFunc(func(request *http.Request) (*http.Response, error) {
		body := ""
		switch request.URL.Path {
		case "/rest/api/3/search/jql":
			body = string(search)
		case "/rest/api/3/field":
			body = `[{"id":"summary","name":"Summary","custom":false,"schema":{"type":"string"}}]`
		default:
			t.Fatalf("unexpected Jira request %s", request.URL.String())
		}
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"application/json"}},
			Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
	})
	claim := nativeTestClaim("jira", "work-items")
	claim.SourceExternalID = "OPS"
	claim.DatasetOptions = map[string]any{"fetch_comments": false}
	client := jiraWorkItemsTestClient(t, fakehttp.Client(doer), providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }))
	batch, err := (JiraWorkItemsRouteHandler{StatusMapping: loadRealStatusMapping(t), Identity: jiraRouteIdentity}).Collect(
		context.Background(), claim, providerfoundation.Credential{}, client, time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	assertKeptWorkItems(t, batch, "jira:OPS-")
}

// assertKeptWorkItems checks the work_items rows of a batch against workItemsWindowCases: the id of each row
// ends in the case number.
func assertKeptWorkItems(t *testing.T, batch CompleteRouteBatch, idPrefix string) {
	t.Helper()
	rows := []json.RawMessage{}
	for _, effect := range batch.Effects {
		if effect.Destination == "work_items" {
			rows = effect.Rows
		}
	}
	got := map[int]bool{}
	for _, raw := range rows {
		for _, c := range workItemsWindowCases {
			if strings.Contains(string(raw), fmt.Sprintf(`"%s%d"`, idPrefix, c.number)) {
				got[c.number] = true
			}
		}
	}
	for _, c := range workItemsWindowCases {
		if got[c.number] != c.keep {
			t.Errorf("item %d (updated %s, %s): written=%v, want %v", c.number, c.updated, c.reason, got[c.number], c.keep)
		}
	}
}

// The Atlassian route builds the same JQL; an issue the search returns outside the claim's window is not
// written by it either.
func TestJiraAtlassianRouteDoesNotWriteAnIssueOutsideTheWindow(t *testing.T) {
	claim := jiraAtlassianClaim()
	since := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	before := time.Date(2020, 2, 1, 0, 0, 0, 0, time.UTC)
	claim.SinceAt, claim.BeforeAt = &since, &before
	doer := &jiraAtlassianDoer{t: t}
	client := jiraWorkItemsTestClient(t, fakehttp.Client(doer), providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }))
	batch, err := jiraAtlassianCompleteHandler(t).Collect(
		context.Background(), claim, providerfoundation.Credential{}, client, time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	for _, effect := range batch.Effects {
		if effect.Destination == "work_items" && len(effect.Rows) != 0 {
			t.Fatalf("work_items rows = %d for an issue updated outside the window, want 0", len(effect.Rows))
		}
	}
}
