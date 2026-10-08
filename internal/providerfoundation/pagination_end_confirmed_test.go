package providerfoundation

import (
	"context"
	"net/http"
	"net/url"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

func TestGitLabPaginationEndUnconfirmedUnlessXNextPageIsSentEmpty(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name      string
		responses []paginationResponse
		perPage   int
		want      bool
	}{
		{"an empty X-Next-Page on a short page is the end", []paginationResponse{
			{body: `[{"id":1}]`, headers: http.Header{"X-Next-Page": {""}}},
		}, 2, false},
		{"an empty X-Next-Page after a followed page is the end", []paginationResponse{
			{body: `[{"id":1},{"id":2}]`, headers: http.Header{"X-Next-Page": {"2"}}},
			{body: `[{"id":3}]`, headers: http.Header{"X-Next-Page": {""}}},
		}, 2, false},
		{"an empty first page with an empty X-Next-Page is the end", []paginationResponse{
			{body: `[]`, headers: http.Header{"X-Next-Page": {""}}},
		}, 2, false},
		{"a malformed X-Next-Page is not the end", []paginationResponse{
			{body: `[{"id":1},{"id":2}]`, headers: http.Header{"X-Next-Page": {"not-a-page"}}},
		}, 2, true},
		{"a non-positive X-Next-Page is not the end", []paginationResponse{
			{body: `[{"id":1}]`, headers: http.Header{"X-Next-Page": {"0"}}},
		}, 2, true},
		{"a short page without X-Next-Page is not the end", []paginationResponse{
			{body: `[{"id":1}]`},
		}, 2, true},
		{"an empty page without X-Next-Page is not the end", []paginationResponse{
			{body: `[{"id":1},{"id":2}]`},
			{body: `[]`},
		}, 2, true},
		{"a confirmed first page does not confirm a later unconfirmed one", []paginationResponse{
			{body: `[{"id":1},{"id":2}]`, headers: http.Header{"X-Next-Page": {"2"}}},
			{body: `[{"id":3}]`, headers: http.Header{"X-Next-Page": {"x"}}},
		}, 2, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			doer := &paginationDoer{responses: test.responses}
			client := paginationClient(t, "gitlab", "https://gitlab.example", fakehttp.Client(doer))
			result, err := CollectGitLabPageParamPages(context.Background(), client, GitLabPageOptions{
				Path: "/api/v4/groups/org/projects", PerPage: test.perPage, MaxPages: 10,
			})
			if err != nil {
				t.Fatal(err)
			}
			if len(doer.requests) != len(test.responses) {
				t.Fatalf("requests=%d want %d", len(doer.requests), len(test.responses))
			}
			if result.EndUnconfirmed != test.want {
				t.Fatalf("EndUnconfirmed=%v want %v (result %+v)", result.EndUnconfirmed, test.want, result)
			}
		})
	}
}

func TestGitHubLinkPaginationEndUnconfirmedOnMalformedLinkOrFullPageWithoutLink(t *testing.T) {
	t.Parallel()
	const next = `<https://api.github.com/orgs/acme/teams/x/repos?page=2&per_page=2>; rel="next"`
	for _, test := range []struct {
		name      string
		responses []paginationResponse
		want      bool
	}{
		{"no Link on a short single page is the end", []paginationResponse{
			{body: `[{"id":1}]`},
		}, false},
		{"a last page with only prev and first links is the end", []paginationResponse{
			{body: `[{"id":1},{"id":2}]`, headers: http.Header{"Link": {next}}},
			{body: `[{"id":3},{"id":4}]`, headers: http.Header{"Link": {
				`<https://api.github.com/orgs/acme/teams/x/repos?page=1&per_page=2>; rel="prev", <https://api.github.com/orgs/acme/teams/x/repos?page=1&per_page=2>; rel="first"`,
			}}},
		}, false},
		{"a Link without angle brackets is not the end", []paginationResponse{
			{body: `[{"id":1}]`, headers: http.Header{"Link": {`https://api.github.com/orgs/acme/teams/x/repos?page=2; rel="next"`}}},
		}, true},
		{"a Link entry with text before the URL is not the end", []paginationResponse{
			{body: `[{"id":1}]`, headers: http.Header{"Link": {`junk <https://api.github.com/orgs/acme/teams/x/repos?page=1>; rel="prev"`}}},
		}, true},
		{"a Link entry without rel is not the end", []paginationResponse{
			{body: `[{"id":1}]`, headers: http.Header{"Link": {`<https://api.github.com/orgs/acme/teams/x/repos?page=2>`}}},
		}, true},
		{"a rel=next without a URL is not the end", []paginationResponse{
			{body: `[{"id":1}]`, headers: http.Header{"Link": {`<>; rel="next"`}}},
		}, true},
		{"a rel=prev without a URL is not the end", []paginationResponse{
			{body: `[{"id":1}]`, headers: http.Header{"Link": {`< >; rel="prev"`}}},
		}, true},
		{"a full page without any Link is not the end", []paginationResponse{
			{body: `[{"id":1},{"id":2}]`},
		}, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			doer := &paginationDoer{responses: test.responses}
			client := paginationClient(t, "github", "https://api.github.com", fakehttp.Client(doer))
			result, err := CollectGitHubLinkPages(context.Background(), client, GitHubPageOptions{
				Path: "/orgs/acme/teams/x/repos", Query: url.Values{"per_page": {"2"}}, MaxPages: 10,
			})
			if err != nil {
				t.Fatal(err)
			}
			if len(doer.requests) != len(test.responses) {
				t.Fatalf("requests=%d want %d", len(doer.requests), len(test.responses))
			}
			if result.EndUnconfirmed != test.want {
				t.Fatalf("EndUnconfirmed=%v want %v (result %+v)", result.EndUnconfirmed, test.want, result)
			}
		})
	}
}
