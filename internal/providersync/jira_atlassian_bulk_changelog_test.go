package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

type bulkChangelogRequest struct {
	IssueIDsOrKeys []string `json:"issueIdsOrKeys"`
	MaxResults     int      `json:"maxResults"`
	NextPageToken  string   `json:"nextPageToken"`
}

type bulkChangelogServer struct {
	t        *testing.T
	requests []bulkChangelogRequest
	reply    func(index int, request bulkChangelogRequest) (int, string)
}

func (server *bulkChangelogServer) Do(request *http.Request) (*http.Response, error) {
	if request.Method != http.MethodPost || request.URL.Path != "/rest/api/3/changelog/bulkfetch" {
		server.t.Fatalf("unexpected request %s %s", request.Method, request.URL.String())
	}
	raw, err := io.ReadAll(request.Body)
	if err != nil {
		return nil, err
	}
	var parsed bulkChangelogRequest
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&parsed); err != nil {
		server.t.Fatalf("bulk body %s: %v (the schema forbids extra properties)", raw, err)
	}
	index := len(server.requests)
	server.requests = append(server.requests, parsed)
	status, body := server.reply(index, parsed)
	return &http.Response{StatusCode: status, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
}

func bulkIssues(count int) []map[string]any {
	issues := make([]map[string]any, 0, count)
	for index := 1; index <= count; index++ {
		issues = append(issues, map[string]any{"id": strconv.Itoa(10000 + index), "key": fmt.Sprintf("OPS-%d", index)})
	}
	return issues
}

func bulkHistory(id string) string {
	return `{"id":"` + id + `","created":"2026-08-01T09:00:00Z","items":[{"field":"status"}]}`
}

func historyIDs(t *testing.T, values []any) []string {
	t.Helper()
	ids := make([]string, 0, len(values))
	for _, value := range values {
		ids = append(ids, value.(map[string]any)["id"].(string))
	}
	return ids
}

func TestCollectJiraAtlassianChangelogsOneRequestForManyIssues(t *testing.T) {
	t.Parallel()
	server := &bulkChangelogServer{t: t, reply: func(int, bulkChangelogRequest) (int, string) {
		return http.StatusOK, `{"issueChangeLogs":[{"issueId":"10002","changeHistories":[` + bulkHistory("a") + `]}],"nextPageToken":null}`
	}}
	client := jiraDevStatusTestClient(t, server)
	result, pages, err := collectJiraAtlassianChangelogs(context.Background(), client, bulkIssues(150), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(server.requests) != 1 || pages != 1 {
		t.Fatalf("requests=%d pages=%d, want 1 request for 150 issues (the per-issue route made 150)", len(server.requests), pages)
	}
	if got := len(server.requests[0].IssueIDsOrKeys); got != 150 || server.requests[0].MaxResults != jiraAtlassianBulkChangelogPerPage {
		t.Fatalf("request ids=%d maxResults=%d", got, server.requests[0].MaxResults)
	}
	if got := historyIDs(t, result["10002"]); !reflect.DeepEqual(got, []string{"a"}) {
		t.Fatalf("histories %v", got)
	}
	if len(result["10001"]) != 0 || result["10001"] == nil {
		t.Fatalf("an issue absent after the last page must have an empty, non-nil history, got %#v", result["10001"])
	}
}

func TestCollectJiraAtlassianChangelogsChunksAt1000Issues(t *testing.T) {
	t.Parallel()
	server := &bulkChangelogServer{t: t, reply: func(int, bulkChangelogRequest) (int, string) {
		return http.StatusOK, `{"issueChangeLogs":[],"nextPageToken":null}`
	}}
	issues := append(bulkIssues(1001), bulkIssues(5)...)
	if _, _, err := collectJiraAtlassianChangelogs(context.Background(), jiraDevStatusTestClient(t, server), issues, 10); err != nil {
		t.Fatal(err)
	}
	if len(server.requests) != 2 || len(server.requests[0].IssueIDsOrKeys) != 1000 || len(server.requests[1].IssueIDsOrKeys) != 1 {
		sizes := make([]int, 0)
		for _, request := range server.requests {
			sizes = append(sizes, len(request.IssueIDsOrKeys))
		}
		t.Fatalf("chunk sizes %v, want [1000 1] (duplicate ids sent once)", sizes)
	}
}

func TestCollectJiraAtlassianChangelogsMergesAnIssueSplitAcrossPages(t *testing.T) {
	t.Parallel()
	server := &bulkChangelogServer{t: t, reply: func(index int, request bulkChangelogRequest) (int, string) {
		switch index {
		case 0:
			if request.NextPageToken != "" {
				t.Errorf("first page carried token %q", request.NextPageToken)
			}
			return http.StatusOK, `{"issueChangeLogs":[{"issueId":"10001","changeHistories":[` + bulkHistory("h1") + `,` + bulkHistory("h2") + `]}],"nextPageToken":"t1"}`
		case 1:
			if request.NextPageToken != "t1" || len(request.IssueIDsOrKeys) != 2 {
				t.Errorf("second page request %+v", request)
			}
			return http.StatusOK, `{"issueChangeLogs":[{"issueId":"10001","changeHistories":[` + bulkHistory("h3") + `]},{"issueId":"10002","changeHistories":[` + bulkHistory("g1") + `]}],"nextPageToken":"t2"}`
		default:
			return http.StatusOK, `{"issueChangeLogs":[{"issueId":"10002","changeHistories":[` + bulkHistory("g2") + `]}],"nextPageToken":null}`
		}
	}}
	result, pages, err := collectJiraAtlassianChangelogs(context.Background(), jiraDevStatusTestClient(t, server), bulkIssues(2), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(server.requests) != 3 || pages != 3 {
		t.Fatalf("requests=%d pages=%d", len(server.requests), pages)
	}
	if got := historyIDs(t, result["10001"]); !reflect.DeepEqual(got, []string{"h1", "h2", "h3"}) {
		t.Fatalf("10001 histories %v, want arrival order h1 h2 h3", got)
	}
	if got := historyIDs(t, result["10002"]); !reflect.DeepEqual(got, []string{"g1", "g2"}) {
		t.Fatalf("10002 histories %v", got)
	}
}

func TestCollectJiraAtlassianChangelogsRefusals(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		issues   []map[string]any
		maxPages int
		reply    func(index int, request bulkChangelogRequest) (int, string)
		want     error
	}{
		{name: "issue without an id", issues: []map[string]any{{"key": "OPS-1"}}, maxPages: 5, want: providerfoundation.ErrNormalizationInvalid},
		{name: "http error", issues: bulkIssues(1), maxPages: 5, want: nil,
			reply: func(int, bulkChangelogRequest) (int, string) {
				return http.StatusBadRequest, `{"errorMessages":["bad"]}`
			}},
		{name: "page without issueChangeLogs", issues: bulkIssues(1), maxPages: 5, want: providerfoundation.ErrNormalizationInvalid,
			reply: func(int, bulkChangelogRequest) (int, string) { return http.StatusOK, `{"nextPageToken":null}` }},
		{name: "entry for an issue never asked for", issues: bulkIssues(1), maxPages: 5, want: providerfoundation.ErrNormalizationInvalid,
			reply: func(int, bulkChangelogRequest) (int, string) {
				return http.StatusOK, `{"issueChangeLogs":[{"issueId":"99999","changeHistories":[]}],"nextPageToken":null}`
			}},
		{name: "entry without changeHistories", issues: bulkIssues(1), maxPages: 5, want: providerfoundation.ErrNormalizationInvalid,
			reply: func(int, bulkChangelogRequest) (int, string) {
				return http.StatusOK, `{"issueChangeLogs":[{"issueId":"10001"}],"nextPageToken":null}`
			}},
		{name: "repeated token", issues: bulkIssues(1), maxPages: 5, want: ErrPaginationCapExceeded,
			reply: func(int, bulkChangelogRequest) (int, string) {
				return http.StatusOK, `{"issueChangeLogs":[],"nextPageToken":"same"}`
			}},
		{name: "page cap", issues: bulkIssues(1), maxPages: 2, want: ErrPaginationCapExceeded,
			reply: func(index int, _ bulkChangelogRequest) (int, string) {
				return http.StatusOK, `{"issueChangeLogs":[],"nextPageToken":"t` + strconv.Itoa(index) + `"}`
			}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			server := &bulkChangelogServer{t: t, reply: test.reply}
			result, _, err := collectJiraAtlassianChangelogs(context.Background(), jiraDevStatusTestClient(t, server), test.issues, test.maxPages)
			if err == nil || result != nil {
				t.Fatalf("result=%v err=%v, want a refusal and no partial result", result, err)
			}
			if test.want != nil && !errors.Is(err, test.want) {
				t.Fatalf("err=%v want %v", err, test.want)
			}
		})
	}
}

func TestJiraAtlassianRouteFetchesAllChangelogsInOneBulkRequest(t *testing.T) {
	issue := func(id, key string) string {
		return `{"id":"` + id + `","key":"` + key + `","self":"https://acme.atlassian.net/rest/api/3/issue/` + id + `","fields":{"project":{"key":"OPS","id":"10001","name":"Operations"},"summary":"s","status":{"name":"Open","statusCategory":{"key":"new"}},"issuetype":{"name":"Task"},"labels":[],"created":"2026-08-01T08:00:00Z","updated":"2026-08-02T09:00:00Z"}}`
	}
	doer := &jiraAtlassianDoer{t: t, searchBody: `{"issues":[` + issue("20001", "OPS-1") + `,` + issue("20002", "OPS-2") + `,` + issue("20003", "OPS-3") + `],"isLast":true}`}
	claim := jiraAtlassianClaim()
	claim.DatasetOptions = map[string]any{"fetch_comments": false}
	client := jiraWorkItemsTestClient(t, fakehttp.Client(doer), providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }))
	batch, err := jiraAtlassianCompleteHandler(t).Collect(
		context.Background(), claim, providerfoundation.Credential{}, client, time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	bulk, perIssue := 0, 0
	for _, path := range doer.paths {
		if strings.Contains(path, "/changelog/bulkfetch") {
			bulk++
		}
		if strings.Contains(path, "/issue/") && strings.HasSuffix(path, "/changelog") {
			perIssue++
		}
	}
	if bulk != 1 || perIssue != 0 {
		t.Fatalf("bulk=%d perIssue=%d for 3 issues, want 1 and 0 (was 3 per-issue requests)", bulk, perIssue)
	}
	if batch.Evidence.Records != 3 {
		t.Fatalf("evidence=%+v", batch.Evidence)
	}
}
