package providerfoundation

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

type endProvenCase struct {
	name      string
	responses []paginationResponse
	want      bool
	wantErr   bool
}

func checkEndProven(t *testing.T, test endProvenCase, doer *paginationDoer, result PageCollection, err error) {
	t.Helper()
	if test.wantErr {
		if !errors.Is(err, ErrPaginationInvalid) {
			t.Fatalf("err=%v want ErrPaginationInvalid (result %+v)", err, result)
		}
		return
	}
	if err != nil {
		t.Fatal(err)
	}
	if len(doer.requests) != len(test.responses) {
		t.Fatalf("requests=%d want %d", len(doer.requests), len(test.responses))
	}
	if result.EndProven != test.want {
		t.Fatalf("EndProven=%v want %v (result %+v)", result.EndProven, test.want, result)
	}
}

func TestGitLabPaginationEndProvenOnlyWhenXNextPageIsSentEmpty(t *testing.T) {
	t.Parallel()
	const keysetNext = `<https://gitlab.example/api/v4/groups/org/projects?cursor=x>; rel="next"`
	for _, test := range []endProvenCase{
		{name: "an empty X-Next-Page on a short page is the end", responses: []paginationResponse{
			{body: `[{"id":1}]`, headers: http.Header{"X-Next-Page": {""}}},
		}, want: true},
		{name: "an empty X-Next-Page after a followed page is the end", responses: []paginationResponse{
			{body: `[{"id":1},{"id":2}]`, headers: http.Header{"X-Next-Page": {"2"}}},
			{body: `[{"id":3}]`, headers: http.Header{"X-Next-Page": {""}}},
		}, want: true},
		{name: "an empty first page with an empty X-Next-Page is the end", responses: []paginationResponse{
			{body: `[]`, headers: http.Header{"X-Next-Page": {""}}},
		}, want: true},
		{name: "a full page with an empty X-Next-Page is followed, and the empty page after it is the end", responses: []paginationResponse{
			{body: `[{"id":1},{"id":2}]`, headers: http.Header{"X-Next-Page": {""}}},
			{body: `[]`, headers: http.Header{"X-Next-Page": {""}}},
		}, want: true},
		{name: "a malformed X-Next-Page is not the end", responses: []paginationResponse{
			{body: `[{"id":1},{"id":2}]`, headers: http.Header{"X-Next-Page": {"not-a-page"}}},
		}},
		{name: "a non-positive X-Next-Page is not the end", responses: []paginationResponse{
			{body: `[{"id":1}]`, headers: http.Header{"X-Next-Page": {"0"}}},
		}},
		{name: "a short page without X-Next-Page is not the end", responses: []paginationResponse{
			{body: `[{"id":1}]`},
		}},
		{name: "an empty page without X-Next-Page is not the end", responses: []paginationResponse{
			{body: `[{"id":1},{"id":2}]`},
			{body: `[]`},
		}},
		{name: "a confirmed first page does not confirm a later unconfirmed one", responses: []paginationResponse{
			{body: `[{"id":1},{"id":2}]`, headers: http.Header{"X-Next-Page": {"2"}}},
			{body: `[{"id":3}]`, headers: http.Header{"X-Next-Page": {"x"}}},
		}},
		{name: "an empty X-Next-Page with a Link rel=next is not the end", responses: []paginationResponse{
			{body: `[{"id":1}]`, headers: http.Header{"X-Next-Page": {""}, "Link": {keysetNext}}},
		}},
		{name: "keyset Link rel=next without X-Next-Page is not the end", responses: []paginationResponse{
			{body: `[{"id":1}]`, headers: http.Header{"Link": {keysetNext}}},
		}},
		{name: "an empty X-Next-Page with a Link rel=next in a second field line is not the end", responses: []paginationResponse{
			{body: `[{"id":1}]`, headers: http.Header{"X-Next-Page": {""}, "Link": {
				`<https://gitlab.example/api/v4/groups/org/projects?page=1>; rel="first"`, keysetNext}}},
		}},
		{name: "an empty X-Next-Page with an unreadable Link is not the end", responses: []paginationResponse{
			{body: `[{"id":1}]`, headers: http.Header{"X-Next-Page": {""}, "Link": {`https://gitlab.example/x; rel="last"`}}},
		}},
		{name: "a 200 null body is an error, not an empty page", responses: []paginationResponse{
			{body: `null`, headers: http.Header{"X-Next-Page": {""}}},
		}, wantErr: true},
		{name: "a 200 object body is an error", responses: []paginationResponse{
			{body: `{"message":"x"}`, headers: http.Header{"X-Next-Page": {""}}},
		}, wantErr: true},
		{name: "a null body after a followed page is an error", responses: []paginationResponse{
			{body: `[{"id":1},{"id":2}]`, headers: http.Header{"X-Next-Page": {"2"}}},
			{body: ` null `, headers: http.Header{"X-Next-Page": {""}}},
		}, wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			doer := &paginationDoer{responses: test.responses}
			client := paginationClient(t, "gitlab", "https://gitlab.example", fakehttp.Client(doer))
			result, err := CollectGitLabPageParamPages(context.Background(), client, GitLabPageOptions{
				Path: "/api/v4/groups/org/projects", PerPage: 2, MaxPages: 10,
			})
			checkEndProven(t, test, doer, result, err)
		})
	}
}

func TestGitHubLinkPaginationEndProvenOnlyWhenTheWalkersLinkReadingFindsNoNext(t *testing.T) {
	t.Parallel()
	const (
		page1 = "https://api.github.com/orgs/acme/teams/x/repos?page=1&per_page=2"
		page2 = "https://api.github.com/orgs/acme/teams/x/repos?page=2&per_page=2"
		last  = `<` + page1 + `>; rel="prev", <` + page1 + `>; rel="first"`
	)
	for _, test := range []endProvenCase{
		{name: "no Link on a short single page is the end", responses: []paginationResponse{
			{body: `[{"id":1}]`},
		}, want: true},
		{name: "a last page with only prev and first links is the end", responses: []paginationResponse{
			{body: `[{"id":1},{"id":2}]`, headers: http.Header{"Link": {`<` + page2 + `>; rel="next"`}}},
			{body: `[{"id":3},{"id":4}]`, headers: http.Header{"Link": {last}}},
		}, want: true},
		{name: "a last page whose prev and first links come in two field lines is the end", responses: []paginationResponse{
			{body: `[{"id":1}]`, headers: http.Header{"Link": {`<` + page1 + `>; rel="prev"`, `<` + page1 + `>; rel="first"`}}},
		}, want: true},
		{name: "rel=next in a second Link field line is followed", responses: []paginationResponse{
			{body: `[{"id":1}]`, headers: http.Header{"Link": {`<` + page1 + `>; rel="first"`, `<` + page2 + `>; rel="next"`}}},
			{body: `[{"id":2}]`, headers: http.Header{"Link": {last}}},
		}, want: true},
		{name: "a rel list with next is followed", responses: []paginationResponse{
			{body: `[{"id":1}]`, headers: http.Header{"Link": {`<` + page2 + `>; rel="next last"`}}},
			{body: `[{"id":2}]`, headers: http.Header{"Link": {last}}},
		}, want: true},
		{name: "rel=NEXT in upper case is followed", responses: []paginationResponse{
			{body: `[{"id":1}]`, headers: http.Header{"Link": {`<` + page2 + `>; REL=NEXT`}}},
			{body: `[{"id":2}]`, headers: http.Header{"Link": {last}}},
		}, want: true},
		{name: "a Link without angle brackets is not the end", responses: []paginationResponse{
			{body: `[{"id":1}]`, headers: http.Header{"Link": {`https://api.github.com/orgs/acme/teams/x/repos?page=2; rel="next"`}}},
		}},
		{name: "a Link entry with text before the URL is not the end", responses: []paginationResponse{
			{body: `[{"id":1}]`, headers: http.Header{"Link": {`junk <` + page1 + `>; rel="prev"`}}},
		}},
		{name: "a Link entry without rel is not the end", responses: []paginationResponse{
			{body: `[{"id":1}]`, headers: http.Header{"Link": {`<` + page2 + `>`}}},
		}},
		{name: "an unreadable second Link field line is not the end", responses: []paginationResponse{
			{body: `[{"id":1}]`, headers: http.Header{"Link": {`<` + page1 + `>; rel="prev"`, `garbage`}}},
		}},
		{name: "a rel=next without a URL is not the end", responses: []paginationResponse{
			{body: `[{"id":1}]`, headers: http.Header{"Link": {`<>; rel="next"`}}},
		}},
		{name: "a rel=prev without a URL is not the end", responses: []paginationResponse{
			{body: `[{"id":1}]`, headers: http.Header{"Link": {`< >; rel="prev"`}}},
		}},
		{name: "a full page without any Link is not the end", responses: []paginationResponse{
			{body: `[{"id":1},{"id":2}]`},
		}},
		{name: "a 200 null body is an error, not an empty page", responses: []paginationResponse{
			{body: `null`},
		}, wantErr: true},
		{name: "a 200 object body is an error", responses: []paginationResponse{
			{body: `{"message":"x"}`},
		}, wantErr: true},
		{name: "a null body after a followed page is an error", responses: []paginationResponse{
			{body: `[{"id":1}]`, headers: http.Header{"Link": {`<` + page2 + `>; rel="next"`}}},
			{body: `null`},
		}, wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			doer := &paginationDoer{responses: test.responses}
			client := paginationClient(t, "github", "https://api.github.com", fakehttp.Client(doer))
			result, err := CollectGitHubLinkPages(context.Background(), client, GitHubPageOptions{
				Path: "/orgs/acme/teams/x/repos", Query: url.Values{"per_page": {"2"}}, MaxPages: 10,
			})
			checkEndProven(t, test, doer, result, err)
		})
	}
}

func TestGitHubLinkPaginationEndNotProvenWhenACallerBoundStopsTheWalk(t *testing.T) {
	t.Parallel()
	const last = `<https://api.github.com/orgs/acme/teams/x/repos?page=1>; rel="first"`
	stopAll := func(json.RawMessage) bool { return true }
	for name, options := range map[string]GitHubPageOptions{
		"StopAt":    {StopAt: stopAll},
		"StopAfter": {StopAfter: stopAll},
		"MaxItems":  {MaxItems: 1},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			doer := &paginationDoer{responses: []paginationResponse{{body: `[{"id":1}]`, headers: http.Header{"Link": {last}}}}}
			client := paginationClient(t, "github", "https://api.github.com", fakehttp.Client(doer))
			options.Path, options.Query, options.MaxPages = "/orgs/acme/teams/x/repos", url.Values{"per_page": {"2"}}, 10
			result, err := CollectGitHubLinkPages(context.Background(), client, options)
			if err != nil {
				t.Fatal(err)
			}
			if result.EndProven {
				t.Fatalf("a walk stopped by %s reads as the provider's end (result %+v)", name, result)
			}
		})
	}
}
