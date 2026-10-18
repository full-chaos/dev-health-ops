package providersync

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
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

// jiraFixtureWindow widens the end of nativeTestClaim's July window to the end of August 2026, which holds
// the updated times of the Jira route fixtures: a work-items unit now keeps only the items updated inside
// its window, and these tests are about what the route does with the items it keeps.
func jiraFixtureWindow(claim Claim) Claim {
	before := time.Date(2026, 8, 31, 23, 59, 59, 0, time.UTC)
	claim.BeforeAt = &before
	return claim
}

// Every dropped item is counted by provider; one Info line per call names the provider, the window and the
// count (and only when something was dropped); each item is a Debug line, never a Warn. A kept item and an
// item with no updated time are neither dropped nor counted.
func TestDropOutsideWorkItemWindowCountsEachDroppedItemAndLogsOneInfoLine(t *testing.T) {
	var logged lockedLogBuffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logged, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previous) })

	claim := nativeTestClaim("gitlab", "work-items")
	metrics := providerfoundation.NewMetrics()
	type item struct {
		id      string
		updated string
	}
	view := func(i item) workItemWindowView {
		at, err := time.Parse(time.RFC3339, i.updated)
		return workItemWindowView{ID: i.id, UpdatedAt: at, Known: err == nil}
	}
	items := []item{{"1", "2026-06-30T23:59:59Z"}, {"2", "2026-07-01T00:00:00Z"}, {"3", "2026-07-31T23:59:59Z"}, {"4", "2026-08-01T00:00:00Z"}, {"5", ""}}
	kept := dropOutsideWorkItemWindow(context.Background(), metrics, claim, items, view)
	if len(kept) != 2 || kept[0].id != "2" || kept[1].id != "5" {
		t.Fatalf("kept = %+v, want items 2 (at the start) and 5 (no updated time)", kept)
	}
	var rendered strings.Builder
	if err := metrics.WritePrometheus(&rendered); err != nil {
		t.Fatal(err)
	}
	if want := `dev_health_work_items_outside_window_dropped_total{provider="gitlab"} 3`; !strings.Contains(rendered.String(), want) {
		t.Fatalf("metrics lack %q", want)
	}
	output := logged.String()
	if got := strings.Count(output, "msg=providersync.work_items.dropped_outside_window "); got != 1 {
		t.Fatalf("%d summary lines, want exactly 1: %s", got, output)
	}
	for _, want := range []string{"level=INFO msg=providersync.work_items.dropped_outside_window ", "provider=gitlab", "dropped=3", "kept=2", "reason=outside_window"} {
		if !strings.Contains(output, want) {
			t.Fatalf("summary line lacks %q: %s", want, output)
		}
	}
	if got := strings.Count(output, "level=DEBUG msg=providersync.work_items.dropped_outside_window_item"); got != 3 {
		t.Fatalf("%d Debug item lines, want 3: %s", got, output)
	}
	if strings.Contains(output, "level=WARN") {
		t.Fatalf("a dropped item must not warn: %s", output)
	}

	var quiet lockedLogBuffer
	slog.SetDefault(slog.New(slog.NewTextHandler(&quiet, &slog.HandlerOptions{Level: slog.LevelDebug})))
	dropOutsideWorkItemWindow(context.Background(), metrics, claim, items[1:2], view)
	if strings.Contains(quiet.String(), "dropped_outside_window") {
		t.Fatalf("a call that dropped nothing logged: %s", quiet.String())
	}
}

// The planner and the Jira route share one predicate for a query the window does not bound.
func TestWorkItemsFetchIsUnbounded(t *testing.T) {
	for name, tc := range map[string]struct {
		provider string
		options  map[string]any
		want     bool
	}{
		"jira fetch_all":                  {"jira", map[string]any{"fetch_all": true}, true},
		"jira fetch_all as text":          {"jira", map[string]any{"fetch_all": "true"}, true},
		"jira fetch_all false":            {"jira", map[string]any{"fetch_all": false}, false},
		"jira jql override":               {"jira", map[string]any{"jql_override": "project = X"}, true},
		"jira empty jql override":         {"jira", map[string]any{"jql_override": ""}, false},
		"jira no options":                 {"jira", nil, false},
		"jira unrelated option":           {"jira", map[string]any{"fetch_comments": true}, false},
		"github ignores the jira options": {"github", map[string]any{"fetch_all": true}, false},
		"gitlab":                          {"gitlab", map[string]any{"jql_override": "x"}, false},
	} {
		if got := WorkItemsFetchIsUnbounded(tc.provider, tc.options); got != tc.want {
			t.Errorf("%s: WorkItemsFetchIsUnbounded = %v, want %v", name, got, tc.want)
		}
	}
}
