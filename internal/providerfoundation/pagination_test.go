package providerfoundation

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
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
}

func (d *paginationDoer) Do(request *http.Request) (*http.Response, error) {
	d.requests = append(d.requests, request.Clone(request.Context()))
	response := d.responses[len(d.requests)-1]
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     response.headers.Clone(),
		Body:       io.NopCloser(strings.NewReader(response.body)),
		Request:    request,
	}, nil
}
