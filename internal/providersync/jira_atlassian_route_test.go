package providersync

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

type jiraAtlassianDoer struct {
	t      *testing.T
	mu     sync.Mutex
	paths  []string
	boards int
}

func (doer *jiraAtlassianDoer) Do(request *http.Request) (*http.Response, error) {
	doer.t.Helper()
	doer.mu.Lock()
	doer.paths = append(doer.paths, request.URL.String())
	doer.mu.Unlock()

	var body string
	switch {
	case request.URL.Path == "/rest/api/3/search":
		body = `{"issues":[{"key":"OPS-201","self":"https://acme.atlassian.net/rest/api/3/issue/OPS-201","fields":{"project":{"key":"OPS","id":"10001","name":"Operations"},"summary":"Atlassian path","status":{"name":"Done","statusCategory":{"key":"done"}},"issuetype":{"name":"Task"},"labels":["support"],"priority":{"name":"Highest"},"assignee":{"accountId":"assignee-201","displayName":"Assignee"},"reporter":{"emailAddress":"reporter@example.com","accountId":"reporter-201","displayName":"Reporter"},"created":"2026-08-01T08:00:00Z","updated":"2026-08-02T09:00:00Z","resolutiondate":"2026-08-02T08:30:00Z","customfield_10020":[{"id":9001,"name":"August"}],"issuelinks":[{"type":{"outward":"blocks","inward":"is blocked by"},"outwardIssue":{"key":"OPS-202"}}]}}],"startAt":0,"total":1}`
	case strings.HasPrefix(request.URL.Path, "/rest/api/3/issue/OPS-201/changelog"):
		body = `{"values":[{"created":"2026-08-01T09:00:00Z","author":{"accountId":"account-1"},"items":[{"field":"status","fromString":"To Do","toString":"Done"}]}],"total":1,"isLast":true}`
	case strings.HasPrefix(request.URL.Path, "/rest/api/3/issue/OPS-201/comment"):
		body = `{"comments":[{"created":"2026-08-02T10:00:00Z","author":{"accountId":"commenter"},"body":"verified"}],"isLast":true}`
	case strings.HasPrefix(request.URL.Path, "/rest/api/3/issue/OPS-201/worklog"):
		body = `{"startAt":0,"maxResults":100,"total":1,"worklogs":[{"id":"wl-201","author":{"accountId":"worker","displayName":"Worker"},"started":"2026-08-01T10:00:00.123456Z","timeSpentSeconds":2700,"created":"2026-08-01T10:01:00.123456Z","updated":"2026-08-01T10:02:00.123456Z"}]}`
	case request.URL.Path == "/rest/agile/1.0/board":
		doer.boards++
		body = `{"values":[{"id":77,"name":"Operations"}],"isLast":true}`
	case request.URL.Path == "/rest/agile/1.0/board/77/sprint":
		body = `{"values":[{"id":9001,"name":"August","state":"active","startDate":"2026-08-01T00:00:00Z","endDate":"2026-08-31T00:00:00Z"}],"isLast":true}`
	default:
		doer.t.Fatalf("unexpected Atlassian request %s", request.URL.String())
	}
	return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
}

func jiraAtlassianClaim() Claim {
	claim := nativeTestClaim("jira", "work-items")
	claim.SourceExternalID = "OPS"
	claim.DatasetOptions = map[string]any{
		"fetch_worklogs":      true,
		"fetch_board_sprints": true,
		"fetch_comments":      true,
		"comments_limit":      10,
		"sprint_field":        "customfield_10020",
	}
	return claim
}

func TestJiraAtlassianRouteCollectsWorklogsBoardsAndCanonicalEdges(t *testing.T) {
	claim := jiraAtlassianClaim()
	doer := &jiraAtlassianDoer{t: t}
	client := jiraWorkItemsTestClient(t, doer, providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }))
	batch, err := (JiraAtlassianRouteHandler{StatusMapping: loadRealStatusMapping(t), Identity: jiraRouteIdentity}).Collect(
		context.Background(), claim, providerfoundation.Credential{}, client,
		time.Date(2026, 8, 10, 12, 0, 0, 123456000, time.UTC),
	)
	if err != nil {
		t.Fatal(err)
	}
	if batch.Watermark == nil || !batch.Watermark.Equal(*claim.BeforeAt) {
		t.Fatalf("watermark=%v want=%v", batch.Watermark, claim.BeforeAt)
	}
	if len(batch.Effects) != 7 {
		t.Fatalf("effects=%d want=7", len(batch.Effects))
	}
	if batch.Result["worklogs_synced"] != 1 || batch.Result["sprints_synced"] != 1 || batch.Result["dependencies_synced"] != 1 || batch.Result["interactions_synced"] != 1 {
		t.Fatalf("result=%#v", batch.Result)
	}
	if batch.Evidence.Requests != 6 || batch.Evidence.Pages != 1 {
		t.Fatalf("evidence=%+v", batch.Evidence)
	}
	var worklog jiraWorklogRow
	var workItem jiraWorkItemRow
	for _, effect := range batch.Effects {
		if effect.Destination == "work_items" && len(effect.Rows) == 1 {
			if err := json.Unmarshal(effect.Rows[0], &workItem); err != nil {
				t.Fatal(err)
			}
		}
		if effect.Destination == "worklogs" && len(effect.Rows) == 1 {
			if err := json.Unmarshal(effect.Rows[0], &worklog); err != nil {
				t.Fatal(err)
			}
		}
	}
	if worklog.WorklogID != "wl-201" || worklog.TimeSpentSeconds != 2700 || worklog.Author == nil || *worklog.Author != "jira:accountid:worker" {
		t.Fatalf("worklog=%+v", worklog)
	}
	if len(workItem.Assignees) != 1 || workItem.Assignees[0] != "jira:accountid:assignee-201" || workItem.Reporter == nil || *workItem.Reporter != "reporter@example.com" {
		t.Fatalf("Atlassian identity breadth lost: work_item=%+v", workItem)
	}
	for _, raw := range doer.paths {
		parsed, err := url.Parse(raw)
		if err != nil {
			t.Fatal(err)
		}
		if parsed.Path == "/rest/api/3/search" && parsed.Query().Get("startAt") != "0" {
			t.Fatalf("search pagination=%s", raw)
		}
	}
}

func TestJiraAtlassianRouteReferenceCacheSkipsBoardEnumeration(t *testing.T) {
	claim := jiraAtlassianClaim()
	claim.DatasetOptions["fetch_worklogs"] = false
	doer := &jiraAtlassianDoer{t: t}
	normalizedAt := time.Date(2026, 8, 10, 12, 0, 0, 123456000, time.UTC)
	refName, refState := "August", "active"
	refs := []jiraSprintRow{{Provider: "jira", SprintID: "9001", Name: &refName, State: &refState, LastSynced: normalizedAt, OrgID: claim.OrgID}}
	client := jiraWorkItemsTestClient(t, doer, providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }))
	batch, err := (JiraAtlassianRouteHandler{StatusMapping: loadRealStatusMapping(t), ReferenceSprints: refs}).Collect(
		context.Background(), claim, providerfoundation.Credential{}, client, normalizedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	if doer.boards != 0 {
		t.Fatalf("board enumeration ignored reference cache: %d", doer.boards)
	}
	if batch.Watermark == nil || batch.Result["sprints_synced"] != 1 {
		t.Fatalf("batch=%+v", batch)
	}
}

func TestJiraAtlassianRouteWorklogFailureIsTypedAndWithholdsWatermark(t *testing.T) {
	claim := jiraAtlassianClaim()
	doer := jiraAtlassianDoerFunc(func(request *http.Request) (*http.Response, error) {
		if strings.HasPrefix(request.URL.Path, "/rest/api/3/issue/OPS-201/worklog") {
			return nil, context.DeadlineExceeded
		}
		return (&jiraAtlassianDoer{t: t}).Do(request)
	})
	client := jiraWorkItemsTestClient(t, doer, providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }))
	batch, err := (JiraAtlassianRouteHandler{StatusMapping: loadRealStatusMapping(t), Identity: jiraRouteIdentity}).Collect(
		context.Background(), claim, providerfoundation.Credential{}, client, time.Date(2026, 8, 10, 12, 0, 0, 123456000, time.UTC),
	)
	if err != nil {
		t.Fatal(err)
	}
	if batch.Watermark != nil {
		t.Fatalf("optional worklog failure advanced watermark: %v", batch.Watermark)
	}
	incomplete, ok := batch.Result["incomplete"].([]string)
	if !ok || len(incomplete) != 1 || incomplete[0] != "worklogs:jira:OPS-201" {
		t.Fatalf("incomplete=%#v", batch.Result["incomplete"])
	}
}

type jiraAtlassianDoerFunc func(*http.Request) (*http.Response, error)

func (doer jiraAtlassianDoerFunc) Do(request *http.Request) (*http.Response, error) {
	return doer(request)
}
