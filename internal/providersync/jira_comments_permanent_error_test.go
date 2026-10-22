package providersync

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

type jiraCommentReadCase struct {
	name      string
	status    int
	body      string
	transport error
}

// jiraPermanentCommentCases is pinned: one row per read failure shape the
// classifier names. Permanent = no hold; the others keep the hold.
var jiraPermanentCommentCases = []jiraCommentReadCase{
	{name: "404", status: 404, body: `{"errorMessages":["Issue does not exist"]}`},
	{name: "403 per issue", status: 403, body: `{"errorMessages":["no permission"]}`},
	{name: "400", status: 400, body: `{}`},
	{name: "410", status: 410, body: `{}`},
	{name: "409", status: 409, body: `{}`},
	{name: "malformed body", status: 200, body: `{"comments":[`},
	{name: "missing comments array", status: 200, body: `{"isLast":true}`},
}

var jiraHeldCommentCases = []jiraCommentReadCase{
	{name: "401", status: 401, body: `{}`},
	{name: "408", status: 408, body: `{}`},
	{name: "429", status: 429, body: `{}`},
	{name: "500", status: 500, body: `{}`},
	{name: "503", status: 503, body: `{}`},
	{name: "transport", transport: errors.New("connection reset")},
	{name: "deadline", transport: context.DeadlineExceeded},
}

func TestJiraCommentReadCaseTablesPinned(t *testing.T) {
	if len(jiraPermanentCommentCases) != 7 || len(jiraHeldCommentCases) != 7 {
		t.Fatalf("rows permanent=%d held=%d want 7/7", len(jiraPermanentCommentCases), len(jiraHeldCommentCases))
	}
}

func (c jiraCommentReadCase) respond(request *http.Request) (*http.Response, error) {
	if c.transport != nil {
		return nil, c.transport
	}
	return &http.Response{StatusCode: c.status, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(c.body)), Request: request}, nil
}

// failOneIssueComments makes the comment read of OPS-1 fail as the case says;
// every other request goes to the budget doer (OPS-2 reads 2 comments).
func failOneIssueComments(t *testing.T, c jiraCommentReadCase) jiraAtlassianDoerFunc {
	inner := &jiraBudgetDoer{t: t, issues: 2, perIssue: 2}
	return func(request *http.Request) (*http.Response, error) {
		if request.URL.Path == "/rest/api/3/issue/OPS-1/comment" {
			return c.respond(request)
		}
		return inner.Do(request)
	}
}

func TestJiraAtlassianPermanentCommentErrorDoesNotHoldWatermarkTwoRuns(t *testing.T) {
	for _, c := range jiraPermanentCommentCases {
		t.Run(c.name, func(t *testing.T) {
			for run := 1; run <= 2; run++ {
				batch := collectJiraComments(t, jiraCommentsClaim(nil), failOneIssueComments(t, c))
				if batch.Watermark == nil {
					t.Fatalf("run %d: watermark held by a permanent comment error", run)
				}
				if _, present := batch.Result["incomplete"]; present {
					t.Fatalf("run %d: incomplete=%#v", run, batch.Result["incomplete"])
				}
				if got := fmt.Sprint(batch.Result["incomplete_nonholding"]); got != "[comments:permanent:1]" {
					t.Fatalf("run %d: markers=%s", run, got)
				}
				if batch.Result["comments_permanent_skipped_issues"] != 1 {
					t.Fatalf("run %d: count key=%#v", run, batch.Result["comments_permanent_skipped_issues"])
				}
				if rows := jiraInteractionRows(t, batch); len(rows) != 2 {
					t.Fatalf("run %d: rows=%d want the 2 rows of OPS-2", run, len(rows))
				}
			}
		})
	}
}

func TestJiraAtlassianTransientCommentErrorStillHoldsWatermark(t *testing.T) {
	for _, c := range jiraHeldCommentCases {
		t.Run(c.name, func(t *testing.T) {
			batch := collectJiraComments(t, jiraCommentsClaim(nil), failOneIssueComments(t, c))
			if batch.Watermark != nil {
				t.Fatal("watermark advanced on a hold-class comment error")
			}
			incomplete, _ := batch.Result["incomplete"].([]string)
			if len(incomplete) != 1 || incomplete[0] != "comments:jira:OPS-1" {
				t.Fatalf("incomplete=%#v", batch.Result["incomplete"])
			}
			if _, present := batch.Result["incomplete_nonholding"]; present {
				t.Fatalf("markers=%#v", batch.Result["incomplete_nonholding"])
			}
		})
	}
}

func TestJiraAtlassianSearchLevel403Fails(t *testing.T) {
	doer := jiraAtlassianDoerFunc(func(request *http.Request) (*http.Response, error) {
		if request.URL.Path == "/rest/api/3/search/jql" {
			return jiraCommentReadCase{status: 403, body: `{}`}.respond(request)
		}
		return (&jiraAtlassianDoer{t: t}).Do(request)
	})
	client := jiraWorkItemsTestClient(t, fakehttp.Client(doer), providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }))
	batch, err := jiraAtlassianCompleteHandler(t).Collect(
		context.Background(), jiraCommentsClaim(nil), providerfoundation.Credential{}, client,
		time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC),
	)
	if err == nil || batch.Watermark != nil {
		t.Fatalf("a search-level 403 must fail the unit, err=%v watermark=%v", err, batch.Watermark)
	}
}

func jiraWorkItemsCommentCaseDoer(c jiraCommentReadCase) jiraWorkItemsDoerFunc {
	issue := func(key string) string {
		return fmt.Sprintf(`{"key":"%[1]s","self":"https://acme.atlassian.net/rest/api/3/issue/%[1]s","fields":{"project":{"key":"OPS"},"summary":"x","status":{"name":"Open","statusCategory":{"key":"new"}},"issuetype":{"name":"Task"},"labels":[],"created":"2026-07-20T00:00:00Z","updated":"2026-07-21T00:00:00Z"},"changelog":{"histories":[]}}`, key)
	}
	ok := func(request *http.Request, body string) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
	}
	return func(request *http.Request) (*http.Response, error) {
		switch {
		case request.URL.Path == "/rest/api/3/search/jql":
			return ok(request, `{"issues":[`+issue("OPS-1")+`,`+issue("OPS-2")+`],"isLast":true}`)
		case request.URL.Path == "/rest/api/3/field":
			return ok(request, `[{"id":"summary","name":"Summary","custom":false,"schema":{"type":"string"}}]`)
		case request.URL.Path == "/rest/api/3/issue/OPS-1/comment":
			return c.respond(request)
		case request.URL.Path == "/rest/api/3/issue/OPS-2/comment":
			return ok(request, `{"comments":[{"id":"c1","created":"2026-07-21T11:00:00Z","author":{"accountId":"a"},"body":"x"}],"isLast":true}`)
		}
		return nil, errors.New("unexpected " + request.URL.String())
	}
}

func collectJiraWorkItemsCommentCase(t *testing.T, c jiraCommentReadCase) CompleteRouteBatch {
	t.Helper()
	claim := nativeTestClaim("jira", "work-items")
	claim.SourceExternalID = "OPS"
	claim.DatasetOptions = map[string]any{"fetch_comments": true}
	client := jiraWorkItemsTestClient(t, fakehttp.Client(jiraWorkItemsCommentCaseDoer(c)), providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }))
	batch, err := (JiraWorkItemsRouteHandler{StatusMapping: loadRealStatusMapping(t), Identity: jiraRouteIdentity}).Collect(
		context.Background(), claim, providerfoundation.Credential{}, client, time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatal(err)
	}
	return batch
}

func TestJiraWorkItemsRoutePermanentCommentErrorDoesNotHoldWatermarkTwoRuns(t *testing.T) {
	for _, c := range jiraPermanentCommentCases {
		t.Run(c.name, func(t *testing.T) {
			for run := 1; run <= 2; run++ {
				batch := collectJiraWorkItemsCommentCase(t, c)
				if batch.Watermark == nil {
					t.Fatalf("run %d: watermark held by a permanent comment error", run)
				}
				if _, present := batch.Result["incomplete"]; present {
					t.Fatalf("run %d: incomplete=%#v", run, batch.Result["incomplete"])
				}
				if got := fmt.Sprint(batch.Result["incomplete_nonholding"]); got != "[comments:permanent:1]" {
					t.Fatalf("run %d: markers=%s", run, got)
				}
				if batch.Result["comments_permanent_skipped_issues"] != 1 || batch.Result["interactions_synced"] != 1 {
					t.Fatalf("run %d: result=%#v", run, batch.Result)
				}
			}
		})
	}
}

func TestJiraWorkItemsRouteTransientCommentErrorStillHoldsWatermark(t *testing.T) {
	for _, c := range jiraHeldCommentCases {
		t.Run(c.name, func(t *testing.T) {
			batch := collectJiraWorkItemsCommentCase(t, c)
			incomplete, _ := batch.Result["incomplete"].([]string)
			if batch.Watermark != nil || len(incomplete) != 1 || incomplete[0] != "comments:jira:OPS-1" {
				t.Fatalf("watermark=%v incomplete=%#v", batch.Watermark, batch.Result["incomplete"])
			}
			if _, present := batch.Result["incomplete_nonholding"]; present {
				t.Fatalf("markers=%#v", batch.Result["incomplete_nonholding"])
			}
		})
	}
}
