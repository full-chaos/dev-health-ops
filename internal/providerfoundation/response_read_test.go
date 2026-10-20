package providerfoundation

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"syscall"
	"testing"
	"testing/iotest"
)

type bodyFaultDoer struct{ mode string }

func (d bodyFaultDoer) Do(request *http.Request) (*http.Response, error) {
	var body io.Reader = strings.NewReader("not json{")
	if d.mode == "read" {
		body = io.MultiReader(strings.NewReader(`[{"id":`), iotest.ErrReader(syscall.ECONNRESET))
	}
	return &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: io.NopCloser(body), Request: request}, nil
}

func bodyFaultClient(t *testing.T, mode string) *HTTPClient {
	t.Helper()
	return paginationClient(t, "github", "https://api.example.test", bodyFaultDoer{mode: mode})
}

func assertRouteFailure(t *testing.T, err error, route, reason string, sentinel error) {
	t.Helper()
	if err == nil {
		t.Fatalf("error=nil, want a route-carrying failure")
	}
	var providerErr *ProviderError
	if !errors.As(err, &providerErr) || providerErr.Route != route || providerErr.Reason != reason {
		t.Fatalf("error=%v, want ProviderError route=%q reason=%q", err, route, reason)
	}
	if !strings.Contains(err.Error(), " route="+route) || !strings.Contains(err.Error(), "reason="+reason) {
		t.Fatalf("error text %q lacks route=%s reason=%s", err.Error(), route, reason)
	}
	if strings.Contains(err.Error(), "acme-secret") {
		t.Fatalf("error text %q carries the raw path", err.Error())
	}
	if !errors.Is(err, sentinel) {
		t.Fatalf("error=%v does not wrap %v", err, sentinel)
	}
	if providerErr.Class != ErrorPermanent || providerErr.Retryable() {
		t.Fatalf("class=%s retryable=%v, a 2xx body failure stays permanent", providerErr.Class, providerErr.Retryable())
	}
}

func TestPageWalkersCarryRouteOnBodyReadAndDecodeFailure(t *testing.T) {
	ctx := context.Background()
	walkers := map[string]struct {
		route string
		run   func(*HTTPClient) error
	}{
		"github link visit": {"/repos/{owner}/{repo}/issues", func(c *HTTPClient) error {
			_, err := VisitGitHubLinkPages(ctx, c, GitHubPageOptions{Path: "/repos/acme-secret/api/issues", Route: "/repos/{owner}/{repo}/issues", MaxPages: 1}, func(PageVisit) error { return nil })
			return err
		}},
		"gitlab page visit": {"/projects/{id}/issues", func(c *HTTPClient) error {
			_, err := VisitGitLabPageParamPages(ctx, c, GitLabPageOptions{Path: "/projects/acme-secret/issues", Route: "/projects/{id}/issues", PerPage: 10, MaxPages: 1}, func(PageVisit) error { return nil })
			return err
		}},
		"github link collect": {"/repos/{owner}/{repo}/pulls", func(c *HTTPClient) error {
			_, err := CollectGitHubLinkPages(ctx, c, GitHubPageOptions{Path: "/repos/acme-secret/api/pulls", Route: "/repos/{owner}/{repo}/pulls", MaxPages: 1})
			return err
		}},
		"gitlab page collect": {"/projects/{id}/merge_requests", func(c *HTTPClient) error {
			_, err := CollectGitLabPageParamPages(ctx, c, GitLabPageOptions{Path: "/projects/acme-secret/merge_requests", Route: "/projects/{id}/merge_requests", PerPage: 10, MaxPages: 1})
			return err
		}},
		"linear": {"/graphql", func(c *HTTPClient) error {
			_, err := CollectLinearGraphQLPages(ctx, c, LinearPageOptions{Query: "query", ConnectionPath: []string{"issues"}, PerPage: 10, MaxPages: 1})
			return err
		}},
		"jira": {"/rest/api/3/search/jql", func(c *HTTPClient) error {
			_, err := CollectJiraTokenOffsetPages(ctx, c, JiraPageOptions{Path: "/rest/api/3/search/jql", Route: "/rest/api/3/search/jql", DataKey: "issues", MaxResults: 10, MaxPages: 1})
			return err
		}},
		"launchdarkly offset": {"/api/v2/flags/{project}", func(c *HTTPClient) error {
			_, err := CollectLaunchDarklyOffsetPages(ctx, c, LaunchDarklyOffsetOptions{Path: "/api/v2/flags/acme-secret", Route: "/api/v2/flags/{project}", MaxPages: 1})
			return err
		}},
		"launchdarkly audit": {"/api/v2/auditlog", func(c *HTTPClient) error {
			_, err := CollectLaunchDarklyAuditPages(ctx, c, LaunchDarklyAuditOptions{MaxItems: 10})
			return err
		}},
		"pagerduty": {"/incidents", func(c *HTTPClient) error {
			_, err := CollectPagerDutyOffsetPages(ctx, c, PagerDutyOffsetOptions{Path: "/incidents", Route: "/incidents", DataKey: "incidents", PerPage: 10, MaxPages: 1})
			return err
		}},
	}
	for name, walker := range walkers {
		for mode, reason := range map[string]string{"read": ReasonBodyRead, "decode": ReasonBodyDecode} {
			t.Run(name+"/"+mode, func(t *testing.T) {
				assertRouteFailure(t, walker.run(bodyFaultClient(t, mode)), walker.route, reason, ErrPaginationInvalid)
			})
		}
	}
}

func TestResponseReadHelpersNameTheRouteTheResponseWasRequestedWith(t *testing.T) {
	ctx := context.Background()
	for mode, reason := range map[string]string{"read": ReasonBodyRead, "decode": ReasonBodyDecode} {
		client := bodyFaultClient(t, mode)
		response, err := client.DoRoute(ctx, http.MethodGet, "/repos/{owner}/{repo}", "/repos/acme-secret/api", nil)
		if err != nil {
			t.Fatal(err)
		}
		if got := ResponseRoute(response); got != "/repos/{owner}/{repo}" {
			t.Fatalf("ResponseRoute=%q", got)
		}
		body, readErr := ReadResponseBody(response, 1<<20)
		if mode == "read" {
			assertRouteFailure(t, readErr, "/repos/{owner}/{repo}", reason, ErrNormalizationInvalid)
			continue
		}
		if readErr != nil {
			t.Fatal(readErr)
		}
		var target any
		assertRouteFailure(t, DecodeResponseJSON(response, body, &target, DecodeStrict), "/repos/{owner}/{repo}", reason, ErrNormalizationInvalid)
	}
}

func TestResponseReadTransportCauseKeepsClassTextOnly(t *testing.T) {
	response, err := bodyFaultClient(t, "read").DoRoute(context.Background(), http.MethodGet, "/x/{id}", "/x/acme-secret", nil)
	if err != nil {
		t.Fatal(err)
	}
	_, readErr := ReadResponseBodyTransport(response, 1<<20)
	if !errors.Is(readErr, syscall.ECONNRESET) || !strings.Contains(readErr.Error(), "exchange request failed: reset") ||
		!strings.Contains(readErr.Error(), "route=/x/{id}") {
		t.Fatalf("error=%v", readErr)
	}
}

func TestResponseRouteIsEmptyWithoutARoute(t *testing.T) {
	response, err := bodyFaultClient(t, "decode").Do(context.Background(), http.MethodGet, "/x", nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := ResponseRoute(response); got != "" {
		t.Fatalf("ResponseRoute=%q, want empty", got)
	}
	if ResponseRoute(nil) != "" {
		t.Fatal("nil response names a route")
	}
}

type oversizeDoer struct{}

func (oversizeDoer) Do(request *http.Request) (*http.Response, error) {
	return &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Request: request,
		Body: io.NopCloser(io.MultiReader(strings.NewReader(`[`), &repeatReader{b: ' ', n: maxProviderPageBody + 2}))}, nil
}

type repeatReader struct {
	b byte
	n int
}

func (r *repeatReader) Read(p []byte) (int, error) {
	if r.n <= 0 {
		return 0, io.EOF
	}
	n := min(len(p), r.n)
	for i := 0; i < n; i++ {
		p[i] = r.b
	}
	r.n -= n
	return n, nil
}

func TestPageWalkersCarryRouteOnOversizedBody(t *testing.T) {
	ctx := context.Background()
	client := paginationClient(t, "github", "https://api.example.test", oversizeDoer{})
	_, err := CollectGitHubLinkPages(ctx, client, GitHubPageOptions{Path: "/repos/acme-secret/api/pulls", Route: "/repos/{owner}/{repo}/pulls", MaxPages: 1})
	assertRouteFailure(t, err, "/repos/{owner}/{repo}/pulls", ReasonBodyTooLarge, ErrPaginationInvalid)
	_, err = CollectJiraTokenOffsetPages(ctx, client, JiraPageOptions{Path: "/rest/api/3/search/jql", Route: "/rest/api/3/search/jql", DataKey: "issues", MaxResults: 10, MaxPages: 1})
	assertRouteFailure(t, err, "/rest/api/3/search/jql", ReasonBodyTooLarge, ErrPaginationInvalid)
}
