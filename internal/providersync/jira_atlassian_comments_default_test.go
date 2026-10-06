package providersync

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// CHAOS-8806: Jira issue comments are collected when the dataset options carry
// no fetch_comments (new and existing configurations alike).

func jiraCommentsClaim(options map[string]any) Claim {
	claim := nativeTestClaim("jira", "work-items")
	claim.SourceExternalID = "OPS"
	claim.DatasetOptions = options
	return claim
}

// jiraManyCommentsDoer serves `total` comments for OPS-201 in pages of
// maxResults, and every other path through the stock Atlassian test doer.
func jiraManyCommentsDoer(t *testing.T, total int, commentRequests *int) jiraAtlassianDoerFunc {
	return func(request *http.Request) (*http.Response, error) {
		if !strings.HasPrefix(request.URL.Path, "/rest/api/3/issue/OPS-201/comment") {
			return (&jiraAtlassianDoer{t: t}).Do(request)
		}
		*commentRequests++
		query := request.URL.Query()
		start, _ := strconv.Atoi(query.Get("startAt"))
		size, _ := strconv.Atoi(query.Get("maxResults"))
		items := make([]string, 0, size)
		for index := start; index < start+size && index < total; index++ {
			items = append(items, fmt.Sprintf(`{"id":"c%d","created":"2026-08-02T10:00:00Z","author":{"accountId":"commenter"},"body":"x"}`, index))
		}
		body := `{"comments":[` + strings.Join(items, ",") + `],"isLast":` + strconv.FormatBool(start+size >= total) + `}`
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
	}
}

func collectJiraComments(t *testing.T, claim Claim, doer jiraAtlassianDoerFunc) CompleteRouteBatch {
	t.Helper()
	client := jiraWorkItemsTestClient(t, fakehttp.Client(doer), providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }))
	batch, err := jiraAtlassianCompleteHandler(t).Collect(
		context.Background(), claim, providerfoundation.Credential{}, client,
		time.Date(2026, 8, 10, 12, 0, 0, 123456000, time.UTC),
	)
	if err != nil {
		t.Fatal(err)
	}
	return batch
}

func jiraInteractionRows(t *testing.T, batch CompleteRouteBatch) []jiraWorkItemInteractionRow {
	t.Helper()
	var rows []jiraWorkItemInteractionRow
	for _, effect := range batch.Effects {
		if effect.Destination != "work_item_interactions" {
			continue
		}
		for _, raw := range effect.Rows {
			var row jiraWorkItemInteractionRow
			if err := json.Unmarshal(raw, &row); err != nil {
				t.Fatal(err)
			}
			rows = append(rows, row)
		}
	}
	return rows
}

func TestJiraAtlassianCommentsOnWhenOptionAbsent(t *testing.T) {
	requests := 0
	batch := collectJiraComments(t, jiraCommentsClaim(nil), jiraManyCommentsDoer(t, 2, &requests))
	if requests != 1 {
		t.Fatalf("comment requests=%d want=1", requests)
	}
	rows := jiraInteractionRows(t, batch)
	if len(rows) != 2 || rows[0].InteractionID != "c0" || rows[1].InteractionID != "c1" ||
		rows[0].Provider != "jira" || rows[0].WorkItemID != "jira:OPS-201" {
		t.Fatalf("stored interaction rows=%+v", rows)
	}
	if batch.Watermark == nil {
		t.Fatal("watermark held on a clean comment fetch")
	}
}

func TestJiraAtlassianCommentsExplicitFalseStaysOff(t *testing.T) {
	requests := 0
	batch := collectJiraComments(t, jiraCommentsClaim(map[string]any{"fetch_comments": false}), jiraManyCommentsDoer(t, 2, &requests))
	if requests != 0 || len(jiraInteractionRows(t, batch)) != 0 {
		t.Fatalf("comments fetched with fetch_comments=false: requests=%d", requests)
	}
}

func TestJiraAtlassianCommentsDefaultCapIs500AndExplicitLimitWins(t *testing.T) {
	requests := 0
	batch := collectJiraComments(t, jiraCommentsClaim(nil), jiraManyCommentsDoer(t, 1200, &requests))
	if rows := jiraInteractionRows(t, batch); len(rows) != 500 {
		t.Fatalf("default cap: rows=%d want=500", len(rows))
	}
	if requests != 10 {
		t.Fatalf("default cap: comment requests=%d want=10 (500 at 50 per page)", requests)
	}
	requests = 0
	batch = collectJiraComments(t, jiraCommentsClaim(map[string]any{"comments_limit": 3}), jiraManyCommentsDoer(t, 1200, &requests))
	if rows := jiraInteractionRows(t, batch); len(rows) != 3 {
		t.Fatalf("explicit limit: rows=%d want=3", len(rows))
	}
}

func TestJiraAtlassianCommentsFetchErrorHoldsWatermarkAndKeepsUnit(t *testing.T) {
	doer := jiraAtlassianDoerFunc(func(request *http.Request) (*http.Response, error) {
		if strings.HasPrefix(request.URL.Path, "/rest/api/3/issue/OPS-201/comment") {
			return nil, context.DeadlineExceeded
		}
		return (&jiraAtlassianDoer{t: t}).Do(request)
	})
	batch := collectJiraComments(t, jiraCommentsClaim(nil), doer)
	if batch.Watermark != nil {
		t.Fatalf("comment failure advanced watermark: %v", batch.Watermark)
	}
	incomplete, ok := batch.Result["incomplete"].([]string)
	if !ok || len(incomplete) != 1 || incomplete[0] != "comments:jira:OPS-201" {
		t.Fatalf("incomplete=%#v", batch.Result["incomplete"])
	}
	if len(jiraInteractionRows(t, batch)) != 0 {
		t.Fatal("interaction rows written despite comment fetch failure")
	}
	if len(batch.Effects) == 0 {
		t.Fatal("work item effects dropped by a comment failure")
	}
}

// jiraBudgetDoer serves `issues` issues (OPS-1..OPS-n) from the search and
// `perIssue` comments for each one. It counts comment and worklog reads.
type jiraBudgetDoer struct {
	t         *testing.T
	issues    int
	perIssue  int
	comments  int
	worklogs  int
	failIssue string
}

func (doer *jiraBudgetDoer) Do(request *http.Request) (*http.Response, error) {
	respond := func(body string) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
	}
	path := request.URL.Path
	switch {
	case path == "/rest/api/3/search/jql":
		items := make([]string, 0, doer.issues)
		for index := 1; index <= doer.issues; index++ {
			items = append(items, fmt.Sprintf(`{"id":"%d","key":"OPS-%d","self":"https://acme.atlassian.net/rest/api/3/issue/OPS-%d","fields":{"project":{"key":"OPS","id":"10001","name":"Operations"},"summary":"Issue %d","status":{"name":"Done","statusCategory":{"key":"done"}},"issuetype":{"name":"Task"},"labels":[],"created":"2026-08-01T08:00:00Z","updated":"2026-08-02T09:00:00Z","resolutiondate":"2026-08-02T08:30:00Z"}}`, 20000+index, index, index, index))
		}
		return respond(`{"issues":[` + strings.Join(items, ",") + `],"isLast":true}`)
	case strings.HasSuffix(path, "/changelog"):
		return respond(`{"values":[],"total":0,"isLast":true}`)
	case strings.HasSuffix(path, "/worklog"):
		doer.worklogs++
		return respond(`{"startAt":0,"maxResults":100,"total":0,"worklogs":[]}`)
	case strings.HasSuffix(path, "/comment"):
		doer.comments++
		key := strings.TrimSuffix(strings.TrimPrefix(path, "/rest/api/3/issue/"), "/comment")
		if key == doer.failIssue {
			return nil, context.DeadlineExceeded
		}
		query := request.URL.Query()
		start, _ := strconv.Atoi(query.Get("startAt"))
		size, _ := strconv.Atoi(query.Get("maxResults"))
		items := make([]string, 0, size)
		for index := start; index < start+size && index < doer.perIssue; index++ {
			items = append(items, fmt.Sprintf(`{"id":"%s-c%d","created":"2026-08-02T10:00:00Z","author":{"accountId":"commenter"},"body":"x"}`, key, index))
		}
		return respond(`{"comments":[` + strings.Join(items, ",") + `],"isLast":` + strconv.FormatBool(start+size >= doer.perIssue) + `}`)
	}
	doer.t.Fatalf("unexpected request %s", request.URL.String())
	return nil, nil
}

func jiraWorkItemEffectRows(t *testing.T, batch CompleteRouteBatch) int {
	t.Helper()
	count := 0
	for _, effect := range batch.Effects {
		if effect.Destination == "work_items" {
			count += len(effect.Rows)
		}
	}
	return count
}

// The vet's repro (CHAOS-8806): 250 issues x 600 comments with the default cap
// of 500 would be 125,000 interaction rows, above the 100,000 rows per table
// the effect ledger accepts, which failed the whole unit and lost every
// work-item effect. The row budget (50,000) keeps the unit whole.
func TestJiraAtlassianCommentsRowBudgetKeepsLargeProjectUnitWhole(t *testing.T) {
	doer := &jiraBudgetDoer{t: t, issues: 250, perIssue: 600}
	batch := collectJiraComments(t, jiraCommentsClaim(nil), doer.Do)
	if got := jiraWorkItemEffectRows(t, batch); got != 250 {
		t.Fatalf("work item rows=%d want=250", got)
	}
	if rows := jiraInteractionRows(t, batch); len(rows) != jiraAtlassianDefaultCommentsRowBudget {
		t.Fatalf("interaction rows=%d want=%d", len(rows), jiraAtlassianDefaultCommentsRowBudget)
	}
	if got := batch.Result["incomplete_nonholding"]; fmt.Sprint(got) != "[comments:budget:150]" {
		t.Fatalf("budget marker=%#v", got)
	}
	if _, held := batch.Result["incomplete"]; held || batch.Watermark == nil {
		t.Fatalf("budget case held the watermark: incomplete=%#v watermark=%v", batch.Result["incomplete"], batch.Watermark)
	}
	if doer.comments != 100*10 {
		t.Fatalf("comment requests=%d want=1000 (100 issues x 10 pages; skipped issues are never read)", doer.comments)
	}
}

func TestJiraAtlassianCommentsRowBudgetBoundary(t *testing.T) {
	for _, tc := range []struct {
		name         string
		budget       int
		wantRows     int
		wantSkipped  int
		wantMarker   bool
		wantComments int
	}{
		{"exactly at the budget", 10, 10, 3, true, 2},
		{"one over the budget", 11, 11, 2, true, 3},
		{"budget above the data", 100, 25, 0, false, 5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			doer := &jiraBudgetDoer{t: t, issues: 5, perIssue: 5}
			batch := collectJiraComments(t, jiraCommentsClaim(map[string]any{"comments_row_budget": tc.budget, "fetch_worklogs": true}), doer.Do)
			if rows := jiraInteractionRows(t, batch); len(rows) != tc.wantRows {
				t.Fatalf("rows=%d want=%d", len(rows), tc.wantRows)
			}
			marker, present := batch.Result["incomplete_nonholding"]
			if present != tc.wantMarker || (present && fmt.Sprint(marker) != fmt.Sprintf("[comments:budget:%d]", tc.wantSkipped)) {
				t.Fatalf("marker=%#v present=%v want skipped=%d", marker, present, tc.wantSkipped)
			}
			if doer.comments != tc.wantComments {
				t.Fatalf("comment reads=%d want=%d", doer.comments, tc.wantComments)
			}
			// A skipped comment read must not skip the rest of the issue's work.
			if doer.worklogs != 5 || jiraWorkItemEffectRows(t, batch) != 5 {
				t.Fatalf("worklog reads=%d work items=%d want 5 and 5", doer.worklogs, jiraWorkItemEffectRows(t, batch))
			}
			if batch.Watermark == nil {
				t.Fatal("watermark held")
			}
		})
	}
}

func TestJiraAtlassianCommentsRowBudgetOptionCanOnlyLowerIt(t *testing.T) {
	for option, want := range map[any]int{
		5: 5, 0: jiraAtlassianDefaultCommentsRowBudget, -1: jiraAtlassianDefaultCommentsRowBudget,
		1_000_000: jiraAtlassianDefaultCommentsRowBudget, "abc": jiraAtlassianDefaultCommentsRowBudget,
	} {
		if got := jiraAtlassianCommentsRowBudget(jiraCommentsClaim(map[string]any{"comments_row_budget": option})); got != want {
			t.Fatalf("option %v: budget=%d want=%d", option, got, want)
		}
	}
	if got := jiraAtlassianCommentsRowBudget(jiraCommentsClaim(nil)); got != jiraAtlassianDefaultCommentsRowBudget {
		t.Fatalf("absent option: budget=%d", got)
	}
	if jiraAtlassianDefaultCommentsRowBudget*2 > 100_000 {
		t.Fatal("default budget must stay well under the effect ledger bound")
	}
}

func TestJiraAtlassianCommentsFetchErrorStillHoldsWatermarkNextToBudget(t *testing.T) {
	doer := &jiraBudgetDoer{t: t, issues: 4, perIssue: 3, failIssue: "OPS-1"}
	batch := collectJiraComments(t, jiraCommentsClaim(map[string]any{"comments_row_budget": 3}), doer.Do)
	incomplete, _ := batch.Result["incomplete"].([]string)
	if len(incomplete) != 1 || incomplete[0] != "comments:jira:OPS-1" || batch.Watermark != nil {
		t.Fatalf("incomplete=%#v watermark=%v", batch.Result["incomplete"], batch.Watermark)
	}
	if rows := jiraInteractionRows(t, batch); len(rows) != 3 {
		t.Fatalf("rows=%d want=3", len(rows))
	}
}
