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
