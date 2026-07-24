package providerfoundation

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestGitHubLinkPaginationFollowsOpaqueNextAndAppliesQueryOnce(t *testing.T) {
	t.Parallel()
	doer := &paginationDoer{responses: []paginationResponse{
		{
			body: `[{"id":1}]`,
			headers: http.Header{
				"Link": {`<https://api.github.com/repos/acme/api/issues?page=2&per_page=100>; rel="next", <https://api.github.com/repos/acme/api/issues?page=2&per_page=100>; rel="last"`},
			},
		},
		{body: `[{"id":2}]`},
	}}
	client := paginationClient(t, "github", "https://api.github.com", doer)
	result, err := CollectGitHubLinkPages(context.Background(), client, GitHubPageOptions{
		Path: "/repos/acme/api/issues",
		Query: url.Values{
			"state":    {"all"},
			"per_page": {"100"},
		},
		MaxPages: 100,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Pages != 2 || result.CapReached || len(result.Items) != 2 {
		t.Fatalf("result=%+v", result)
	}
	if got := doer.requests[0].URL.Query().Get("state"); got != "all" {
		t.Fatalf("first request state=%q", got)
	}
	if got := doer.requests[1].URL.Query().Get("state"); got != "" {
		t.Fatalf("opaque next URL was mutated with state=%q", got)
	}
}

func TestGitHubLinkPaginationReportsHardCapWithoutExtraCall(t *testing.T) {
	t.Parallel()
	doer := &paginationDoer{responses: []paginationResponse{{
		body:    `[{"id":1}]`,
		headers: http.Header{"Link": {`<https://api.github.com/items?page=2>; rel="next"`}},
	}}}
	client := paginationClient(t, "github", "https://api.github.com", doer)
	result, err := CollectGitHubLinkPages(context.Background(), client, GitHubPageOptions{
		Path: "/items", MaxPages: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !result.CapReached || result.Pages != 1 || len(doer.requests) != 1 {
		t.Fatalf("result=%+v calls=%d", result, len(doer.requests))
	}
}

func TestGitHubLinkPaginationRejectsSameHostSchemeDowngradeBeforeRequest(t *testing.T) {
	t.Parallel()
	doer := &paginationDoer{responses: []paginationResponse{{
		body:    `[{"id":1}]`,
		headers: http.Header{"Link": {`<http://api.github.com/items?page=2>; rel="next"`}},
	}}}
	client := paginationClient(t, "github", "https://api.github.com", doer)
	_, err := CollectGitHubLinkPages(context.Background(), client, GitHubPageOptions{
		Path: "/items", MaxPages: 2,
	})
	if !errors.Is(err, ErrCredentialInvalid) {
		t.Fatalf("scheme downgrade error=%v", err)
	}
	if len(doer.requests) != 1 {
		t.Fatalf("downgraded request reached doer: calls=%d", len(doer.requests))
	}
}

func TestPaginationRejectsExcessiveBoundsWithoutRequests(t *testing.T) {
	t.Parallel()
	doer := &paginationDoer{}
	client := paginationClient(t, "github", "https://api.github.com", doer)
	if _, err := CollectGitHubLinkPages(context.Background(), client, GitHubPageOptions{
		Path: "/items", MaxPages: maximumProviderPages + 1,
	}); !errors.Is(err, ErrPaginationInvalid) {
		t.Fatalf("GitHub excessive cap error=%v", err)
	}
	if _, err := CollectGitLabPageParamPages(context.Background(), client, GitLabPageOptions{
		Path: "/items", PerPage: maximumGitLabPerPage + 1, MaxPages: 1,
	}); !errors.Is(err, ErrPaginationInvalid) {
		t.Fatalf("GitLab excessive per_page error=%v", err)
	}
	if _, err := CollectGitLabPageParamPages(context.Background(), client, GitLabPageOptions{
		Path: "/items", PerPage: 1, MaxPages: maximumProviderPages + 1,
	}); !errors.Is(err, ErrPaginationInvalid) {
		t.Fatalf("GitLab excessive cap error=%v", err)
	}
	if len(doer.requests) != 0 {
		t.Fatalf("invalid pagination reached doer: calls=%d", len(doer.requests))
	}
}

func TestGitLabPaginationUsesHeaderThenItemCountFallback(t *testing.T) {
	t.Parallel()
	doer := &paginationDoer{responses: []paginationResponse{
		{body: `[{"id":1},{"id":2}]`, headers: http.Header{"X-Next-Page": {"4"}}},
		{body: `[{"id":3},{"id":4}]`},
		{body: `[{"id":5}]`},
	}}
	client := paginationClient(t, "gitlab", "https://gitlab.example", doer)
	result, err := CollectGitLabPageParamPages(context.Background(), client, GitLabPageOptions{
		Path: "/api/v4/projects/1/merge_requests",
		Query: url.Values{
			"order_by": {"updated_at"},
			"sort":     {"desc"},
		},
		PerPage: 2, MaxPages: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Pages != 3 || result.CapReached || len(result.Items) != 5 {
		t.Fatalf("result=%+v", result)
	}
	wantPages := []string{"1", "4", "5"}
	for index, request := range doer.requests {
		if got := request.URL.Query().Get("page"); got != wantPages[index] {
			t.Fatalf("request %d page=%q want=%q", index, got, wantPages[index])
		}
		if got := request.URL.Query().Get("per_page"); got != "2" {
			t.Fatalf("request %d per_page=%q", index, got)
		}
	}
}

func TestGitLabMalformedNextPageStopsWithoutSpeculation(t *testing.T) {
	t.Parallel()
	doer := &paginationDoer{responses: []paginationResponse{{
		body:    `[{"id":1}]`,
		headers: http.Header{"X-Next-Page": {"not-a-page"}},
	}}}
	client := paginationClient(t, "gitlab", "https://gitlab.example", doer)
	result, err := CollectGitLabPageParamPages(context.Background(), client, GitLabPageOptions{
		Path: "/api/v4/projects", PerPage: 1, MaxPages: 100,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Pages != 1 || len(result.Items) != 1 || len(doer.requests) != 1 {
		t.Fatalf("result=%+v calls=%d", result, len(doer.requests))
	}
}

func TestLinearGraphQLPaginationPostsFirstAfterAndRejectsErrors(t *testing.T) {
	t.Parallel()
	doer := &paginationDoer{responses: []paginationResponse{
		{body: `{"data":{"issues":{"nodes":[{"id":"one"}],"pageInfo":{"hasNextPage":true,"endCursor":"cursor-2"}}}}`},
		{body: `{"data":{"issues":{"nodes":[{"id":"two"}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}`},
	}}
	client := paginationClient(t, "linear", "https://api.linear.app", doer)
	result, err := CollectLinearGraphQLPages(context.Background(), client, LinearPageOptions{
		Query: "query Issues($first: Int!, $after: String) { issues { nodes { id } } }",
		Variables: map[string]any{
			"filter": map[string]any{"archivedAt": map[string]any{"null": true}},
		},
		ConnectionPath: []string{"issues"},
		PerPage:        50,
		MaxPages:       10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Pages != 2 || result.CapReached || len(result.Items) != 2 {
		t.Fatalf("result=%+v", result)
	}
	var first, second struct {
		Variables map[string]any `json:"variables"`
	}
	if json.Unmarshal([]byte(doer.bodies[0]), &first) != nil ||
		json.Unmarshal([]byte(doer.bodies[1]), &second) != nil {
		t.Fatalf("request bodies=%q", doer.bodies)
	}
	if first.Variables["first"] != float64(50) || first.Variables["after"] != nil {
		t.Fatalf("first variables=%v", first.Variables)
	}
	if second.Variables["after"] != "cursor-2" {
		t.Fatalf("second variables=%v", second.Variables)
	}

	errorDoer := &paginationDoer{responses: []paginationResponse{{
		body: `{"errors":[{"message":"Query is too complex","extensions":{"code":"COMPLEXITY_LIMIT"}}]}`,
	}}}
	errorClient := paginationClient(t, "linear", "https://api.linear.app", errorDoer)
	_, err = CollectLinearGraphQLPages(context.Background(), errorClient, LinearPageOptions{
		Query:          "query Issues { issues { nodes { id } } }",
		ConnectionPath: []string{"issues"},
		PerPage:        50, MaxPages: 1,
	})
	if !errors.Is(err, ErrGraphQLComplexity) || len(errorDoer.requests) != 1 {
		t.Fatalf("complexity error=%v calls=%d", err, len(errorDoer.requests))
	}
}

func TestJiraPaginationPreservesTokenThenOffsetAndIsLast(t *testing.T) {
	t.Parallel()
	doer := &paginationDoer{responses: []paginationResponse{
		{body: `{"issues":[{"id":"1"}],"nextPageToken":"opaque"}`},
		{body: `{"issues":[{"id":"2"},{"id":"3"}]}`},
		{body: `{"issues":[{"id":"4"}],"isLast":true}`},
	}}
	client := paginationClient(t, "jira", "https://acme.atlassian.net", doer)
	result, err := CollectJiraTokenOffsetPages(context.Background(), client, JiraPageOptions{
		Path: "/rest/api/3/search/jql",
		Query: url.Values{
			"jql":    {`project = "OPS"`},
			"fields": {"*all"},
		},
		DataKey: "issues", MaxResults: 100, MaxPages: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Pages != 3 || result.CapReached || len(result.Items) != 4 {
		t.Fatalf("result=%+v", result)
	}
	if got := doer.requests[0].URL.Query().Get("startAt"); got != "0" {
		t.Fatalf("first startAt=%q", got)
	}
	if got := doer.requests[1].URL.Query().Get("nextPageToken"); got != "opaque" ||
		doer.requests[1].URL.Query().Has("startAt") {
		t.Fatalf("token request=%s", doer.requests[1].URL.RawQuery)
	}
	if got := doer.requests[2].URL.Query().Get("startAt"); got != "2" ||
		doer.requests[2].URL.Query().Has("nextPageToken") {
		t.Fatalf("offset request=%s", doer.requests[2].URL.RawQuery)
	}
}

func TestLaunchDarklyFlagAndAuditPagination(t *testing.T) {
	t.Parallel()
	firstFlags := make([]map[string]any, launchDarklyFlagPageSize)
	for index := range firstFlags {
		firstFlags[index] = map[string]any{"key": strconv.Itoa(index)}
	}
	firstBody, err := json.Marshal(map[string]any{"items": firstFlags, "totalCount": 51})
	if err != nil {
		t.Fatal(err)
	}
	flagDoer := &paginationDoer{responses: []paginationResponse{
		{body: string(firstBody)},
		{body: `{"items":[{"key":"last"}],"totalCount":51}`},
	}}
	flagClient := paginationClient(t, "launchdarkly", "https://app.launchdarkly.com", flagDoer)
	flags, err := CollectLaunchDarklyOffsetPages(context.Background(), flagClient, LaunchDarklyOffsetOptions{
		Path: "/api/v2/flags/project", MaxPages: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if flags.Pages != 2 || flags.CapReached || len(flags.Items) != 51 ||
		flagDoer.requests[1].URL.Query().Get("offset") != "50" {
		t.Fatalf("flags=%+v second=%s", flags, flagDoer.requests[1].URL.RawQuery)
	}

	since := time.UnixMilli(1_725_000_000_123).UTC()
	auditDoer := &paginationDoer{responses: []paginationResponse{
		{body: `{"items":[{"_id":"1"},{"_id":"2"}],"_links":{"next":{"href":"/api/v2/auditlog?limit=20&after=opaque"}}}`},
		{body: `{"items":[{"_id":"3"}]}`},
	}}
	auditClient := paginationClient(t, "launchdarkly", "https://app.launchdarkly.com", auditDoer)
	audit, err := CollectLaunchDarklyAuditPages(context.Background(), auditClient, LaunchDarklyAuditOptions{
		Since: &since, MaxItems: 25,
	})
	if err != nil {
		t.Fatal(err)
	}
	if audit.Pages != 2 || audit.CapReached || len(audit.Items) != 3 {
		t.Fatalf("audit=%+v", audit)
	}
	if got := auditDoer.requests[0].URL.Query().Get("after"); got != "1725000000123" {
		t.Fatalf("after=%q", got)
	}
	if got := auditDoer.requests[1].URL.Path; got != "/api/v2/auditlog" {
		t.Fatalf("next path=%q", got)
	}
}

func paginationClient(t *testing.T, provider, base string, doer HTTPDoer) *HTTPClient {
	t.Helper()
	client, err := NewHTTPClient(
		provider,
		base,
		doer,
		func(*http.Request) error { return nil },
		RetryPolicy{MaxAttempts: 1, InitialWait: time.Millisecond, MaxWait: time.Millisecond},
		LeaseGuardFunc(func(context.Context) error { return nil }),
	)
	if err != nil {
		t.Fatal(err)
	}
	return client
}

type paginationResponse struct {
	body    string
	headers http.Header
}

type paginationDoer struct {
	responses []paginationResponse
	requests  []*http.Request
	bodies    []string
}

func (d *paginationDoer) Do(request *http.Request) (*http.Response, error) {
	if request.Body != nil {
		body, _ := io.ReadAll(request.Body)
		d.bodies = append(d.bodies, string(body))
		request.Body = io.NopCloser(strings.NewReader(string(body)))
	} else {
		d.bodies = append(d.bodies, "")
	}
	d.requests = append(d.requests, request.Clone(request.Context()))
	response := d.responses[len(d.requests)-1]
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     response.headers.Clone(),
		Body:       io.NopCloser(strings.NewReader(response.body)),
		Request:    request,
	}, nil
}
