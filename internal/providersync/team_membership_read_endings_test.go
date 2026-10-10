package providersync

// Every way a team's member read can
// end, per provider, and whether the team is closable after it.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

type endingsDoer func(*http.Request) (*http.Response, error)

func (doer endingsDoer) Do(request *http.Request) (*http.Response, error) { return doer(request) }

func endingsResponse(request *http.Request, status int, body string, header http.Header) *http.Response {
	if header == nil {
		header = http.Header{}
	}
	header.Set("Content-Type", "application/json")
	return &http.Response{StatusCode: status, Status: fmt.Sprint(status), Header: header,
		Body: io.NopCloser(strings.NewReader(body)), Request: request}
}

func endingsClosable(listed, unproven []string) []string {
	skip := map[string]bool{}
	for _, id := range unproven {
		skip[id] = true
	}
	var out []string
	for _, id := range listed {
		if !skip[id] {
			out = append(out, id)
		}
	}
	sort.Strings(out)
	return out
}

func TestGitHubMemberReadEndings(t *testing.T) {
	type answer struct {
		status int
		body   string
		header http.Header
		err    error
	}
	link := func(value string) http.Header { return http.Header{"Link": []string{value}} }
	next := `<https://api.github.com/orgs/acme/teams/platform/members?page=2>; rel="next"`
	cases := []struct {
		name          string
		page1, page2  answer
		perPage, max  int
		resolveEmail  bool
		userStatus    int
		cancel        bool
		wantClosable  bool
		wantObserved  bool
		wantMemberIDs int
	}{
		{name: "200 short page, no Link", page1: answer{status: 200, body: `[{"login":"a"}]`}, wantClosable: true, wantObserved: true, wantMemberIDs: 1},
		{name: "200 with Link next, page 2 ends", page1: answer{status: 200, body: `[{"login":"a"}]`, header: link(next)}, page2: answer{status: 200, body: `[{"login":"b"}]`}, wantClosable: true, wantObserved: true, wantMemberIDs: 2},
		{name: "403", page1: answer{status: 403, body: `{"message":"Forbidden"}`}},
		{name: "404", page1: answer{status: 404, body: `{"message":"Not Found"}`}},
		{name: "401", page1: answer{status: 401, body: `{"message":"Bad credentials"}`}},
		{name: "500", page1: answer{status: 500, body: `{}`}},
		{name: "429", page1: answer{status: 429, body: `{}`}},
		{name: "403 rate limit", page1: answer{status: 403, body: `{"message":"API rate limit exceeded"}`, header: http.Header{"X-Ratelimit-Remaining": []string{"0"}}}},
		{name: "transport error", page1: answer{err: io.ErrUnexpectedEOF}},
		{name: "page budget exhausted", page1: answer{status: 200, body: `[{"login":"a"}]`, header: link(next)}, max: 1},
		{name: "200 object body", page1: answer{status: 200, body: `{"message":"x"}`}},
		{name: "200 null body", page1: answer{status: 200, body: `null`}},
		{name: "200 empty body", page1: answer{status: 200, body: ``}},
		{name: "200 cut JSON", page1: answer{status: 200, body: `[{"login":"a"},`}},
		{name: "200 [] no Link", page1: answer{status: 200, body: `[]`}, wantClosable: true, wantObserved: true},
		{name: "malformed Link", page1: answer{status: 200, body: `[{"login":"a"}]`, header: link(`garbage`)}, wantObserved: true, wantMemberIDs: 1},
		{name: "Link without next (last page)", page1: answer{status: 200, body: `[{"login":"a"}]`, header: link(`<https://api.github.com/x?page=1>; rel="first"`)}, wantClosable: true, wantObserved: true, wantMemberIDs: 1},
		{name: "full page, no Link", page1: answer{status: 200, body: `[{"login":"a"},{"login":"b"}]`}, perPage: 2, wantObserved: true, wantMemberIDs: 2},
		{name: "page 2 fails 500", page1: answer{status: 200, body: `[{"login":"a"}]`, header: link(next)}, page2: answer{status: 500, body: `{}`}},
		{name: "page 2 empty, no Link", page1: answer{status: 200, body: `[{"login":"a"}]`, header: link(next)}, page2: answer{status: 200, body: `[]`}, wantClosable: true, wantObserved: true, wantMemberIDs: 1},
		{name: "page 2 empty with Link next again", page1: answer{status: 200, body: `[{"login":"a"}]`, header: link(next)}, page2: answer{status: 200, body: `[]`, header: link(next)}, max: 3},
		{name: "node without login", page1: answer{status: 200, body: `[{"login":"a"},{"id":7}]`}, wantObserved: true, wantMemberIDs: 1},
		{name: "node null", page1: answer{status: 200, body: `[{"login":"a"},null]`}, wantObserved: true, wantMemberIDs: 1},
		{name: "node is a string", page1: answer{status: 200, body: `[{"login":"a"},"b"]`}},
		{name: "email lookup 404", page1: answer{status: 200, body: `[{"login":"a"}]`}, resolveEmail: true, userStatus: 404},
		{name: "email lookup 403", page1: answer{status: 200, body: `[{"login":"a"}]`}, resolveEmail: true, userStatus: 403},
		{name: "context cancelled", page1: answer{status: 200, body: `[{"login":"a"}]`}, cancel: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			doer := endingsDoer(func(request *http.Request) (*http.Response, error) {
				switch {
				case request.URL.Path == "/orgs/acme/teams":
					return endingsResponse(request, 200, `[{"slug":"platform","name":"Platform"}]`, nil), nil
				case strings.HasPrefix(request.URL.Path, "/users/"):
					return endingsResponse(request, tc.userStatus, `{"email":null}`, nil), nil
				case request.URL.Path == "/orgs/acme/teams/platform/members":
					if tc.cancel {
						cancel()
						return nil, context.Canceled
					}
					a := tc.page1
					if request.URL.Query().Get("page") == "2" {
						a = tc.page2
					}
					if a.err != nil {
						return nil, a.err
					}
					return endingsResponse(request, a.status, a.body, a.header.Clone()), nil
				}
				t.Fatalf("unexpected request %s", request.URL)
				return nil, nil
			})
			handler := GitHubTeamCatalogRouteHandler{Client: githubTeamCatalogTestClient(t, fakehttp.Client(doer)), OrgName: "acme",
				PerPage: tc.perPage, MaxPages: tc.max, ResolveEmail: tc.resolveEmail}
			rows, _, err := handler.Collect(ctx, "org-1", false, true)
			closable := endingsClosable(rows.ObservedMembershipTeamIDs, rows.UnprovenMembershipTeamIDs)
			observed := len(rows.ObservedMembershipTeamIDs) == 1
			t.Logf("GH | %s | err=%v | observed=%v | unproven=%v | failed=%v | closable=%v | memberships=%d",
				tc.name, err, rows.ObservedMembershipTeamIDs, rows.UnprovenMembershipTeamIDs, rows.FailedMemberFetchTeamIDs, closable, len(rows.Memberships))
			if (len(closable) == 1) != tc.wantClosable || observed != tc.wantObserved || len(rows.Memberships) != tc.wantMemberIDs {
				t.Errorf("closable=%v observed=%v memberships=%d, want closable=%v observed=%v memberships=%d",
					closable, observed, len(rows.Memberships), tc.wantClosable, tc.wantObserved, tc.wantMemberIDs)
			}
		})
	}
}

func TestGitLabMemberReadEndings(t *testing.T) {
	type answer struct {
		status int
		body   string
		header map[string][]string
		hang   bool
	}
	end := map[string][]string{"X-Next-Page": {""}}
	cases := []struct {
		name         string
		pages        map[string]answer
		strict       bool
		wantErr      bool
		wantClosable bool
		wantObserved bool
		wantRows     int
	}{
		{name: "200 with empty X-Next-Page", pages: map[string]answer{"1": {200, `[{"username":"a"}]`, end, false}}, wantClosable: true, wantObserved: true, wantRows: 1},
		{name: "200 short page without X-Next-Page", pages: map[string]answer{"1": {200, `[{"username":"a"}]`, nil, false}}, wantObserved: true, wantRows: 1},
		{name: "X-Next-Page 2 then end", pages: map[string]answer{"1": {200, `[{"username":"a"}]`, map[string][]string{"X-Next-Page": {"2"}}, false}, "2": {200, `[{"username":"b"}]`, end, false}}, wantClosable: true, wantObserved: true, wantRows: 2},
		{name: "403", pages: map[string]answer{"1": {403, `{"message":"403 Forbidden"}`, nil, false}}},
		{name: "404", pages: map[string]answer{"1": {404, `{"message":"404 Group Not Found"}`, nil, false}}},
		{name: "401", pages: map[string]answer{"1": {401, `{"message":"401 Unauthorized"}`, nil, false}}},
		{name: "500", pages: map[string]answer{"1": {500, `{}`, nil, false}}},
		{name: "429", pages: map[string]answer{"1": {429, `{}`, nil, false}}},
		{name: "X-Next-Page garbage", pages: map[string]answer{"1": {200, `[{"username":"a"}]`, map[string][]string{"X-Next-Page": {"abc"}}, false}}, wantObserved: true, wantRows: 1},
		{name: "empty X-Next-Page but Link next (keyset)", pages: map[string]answer{"1": {200, `[{"username":"a"}]`, map[string][]string{"X-Next-Page": {""}, "Link": {`<http://x/next>; rel="next"`}}, false}}, wantObserved: true, wantRows: 1},
		{name: "two X-Next-Page headers", pages: map[string]answer{"1": {200, `[{"username":"a"}]`, map[string][]string{"X-Next-Page": {"", ""}}, false}}, wantObserved: true, wantRows: 1},
		{name: "200 object body", pages: map[string]answer{"1": {200, `{"message":"x"}`, end, false}}},
		{name: "200 null body", pages: map[string]answer{"1": {200, `null`, end, false}}},
		{name: "200 cut JSON", pages: map[string]answer{"1": {200, `[{"username":"a"},`, end, false}}},
		{name: "page 2 fails", pages: map[string]answer{"1": {200, `[{"username":"a"}]`, map[string][]string{"X-Next-Page": {"2"}}, false}, "2": {500, `{}`, nil, false}}},
		{name: "page 2 empty with end signal", pages: map[string]answer{"1": {200, `[{"username":"a"}]`, map[string][]string{"X-Next-Page": {"2"}}, false}, "2": {200, `[]`, end, false}}, wantClosable: true, wantObserved: true, wantRows: 1},
		{name: "page 2 empty without end signal", pages: map[string]answer{"1": {200, `[{"username":"a"}]`, map[string][]string{"X-Next-Page": {"2"}}, false}, "2": {200, `[]`, nil, false}}, wantObserved: true, wantRows: 1},
		{name: "node without username", pages: map[string]answer{"1": {200, `[{"username":"a"},{"name":"ghost"}]`, end, false}}, wantObserved: true, wantRows: 1},
		{name: "[] with end signal", pages: map[string]answer{"1": {200, `[]`, end, false}}, wantClosable: true, wantObserved: true},
		{name: "[] without end signal", pages: map[string]answer{"1": {200, `[]`, nil, false}}, wantObserved: true},
		{name: "endless next page (budget)", pages: map[string]answer{"*": {200, `[{"username":"a"}]`, map[string][]string{"X-Next-Page": {"NEXT"}}, false}}, wantObserved: true, wantRows: 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.EscapedPath() {
				case "/api/v4/groups/org":
					writeGitLabTeamCatalogJSON(t, w, map[string]any{"id": 1, "full_path": "org", "name": "Org"})
				case "/api/v4/groups/org/subgroups":
					w.Header()["X-Next-Page"] = []string{""}
					writeGitLabTeamCatalogJSON(t, w, []map[string]any{})
				case "/api/v4/groups/org/members":
					page := r.URL.Query().Get("page")
					a, ok := tc.pages[page]
					if !ok {
						a = tc.pages["*"]
					}
					for key, values := range a.header {
						out := append([]string(nil), values...)
						for i, value := range out {
							if value == "NEXT" {
								var n int
								fmt.Sscan(page, &n)
								out[i] = fmt.Sprint(n + 1)
							}
						}
						w.Header()[key] = out
					}
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(a.status)
					_, _ = io.WriteString(w, a.body)
				default:
					http.NotFound(w, r)
				}
			}))
			defer server.Close()
			ref := TeamCatalogReference{OrgID: "org-1", SyncRunID: "run-1", Strict: tc.strict}
			credential := providerfoundation.Credential{Provider: "gitlab", Config: map[string]string{"group_path": "org"}}
			batch, err := (GitLabTeamCatalogRouteHandler{}).CollectTeamCatalog(context.Background(), ref, credential,
				gitlabTeamCatalogTestClient(t, server.URL), TeamCatalogSelections{Members: true}, time.Date(2026, 10, 2, 8, 0, 0, 0, time.UTC))
			rows := batch.Rows
			closable := endingsClosable(rows.ObservedMembershipTeamIDs, rows.UnprovenMembershipTeamIDs)
			observed := len(rows.ObservedMembershipTeamIDs) == 1
			t.Logf("GL | %s | err=%v | observed=%v | unproven=%v | failed=%v | closable=%v | memberships=%d | complete=%v truncated=%v skipped=%v",
				tc.name, err, rows.ObservedMembershipTeamIDs, rows.UnprovenMembershipTeamIDs, rows.FailedMemberFetchTeamIDs, closable, len(rows.Memberships),
				batch.Result.Complete, batch.Evidence.Truncated, batch.Result.WalkSkipped)
			if (err != nil) != tc.wantErr || (len(closable) == 1) != tc.wantClosable || observed != tc.wantObserved || len(rows.Memberships) != tc.wantRows {
				t.Errorf("err=%v closable=%v observed=%v memberships=%d, want err=%v closable=%v observed=%v memberships=%d",
					err, closable, observed, len(rows.Memberships), tc.wantErr, tc.wantClosable, tc.wantObserved, tc.wantRows)
			}
		})
	}
}

func TestLinearMemberReadEndings(t *testing.T) {
	team := func(members string) string {
		return `{"data":{"teams":{"nodes":[{"id":"team-raw-eng","key":"ENG","name":"Eng","members":` + members + `}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}`
	}
	alice := `{"id":"user-1","name":"Alice","email":"alice@example.com","active":true}`
	bob := `{"id":"user-2","name":"Bob","email":"bob@example.com","active":true}`
	cycles := `{"data":{"cycles":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}`
	cases := []struct {
		name         string
		responses    []string
		wantErr      bool
		wantUnusable int
		wantRows     int
		wantComplete bool
	}{
		{name: "hasNextPage false", responses: []string{team(`{"nodes":[` + alice + `],"pageInfo":{"hasNextPage":false,"endCursor":null}}`), cycles}, wantRows: 1, wantComplete: true},
		{name: "pageInfo absent", responses: []string{team(`{"nodes":[` + alice + `]}`), cycles}, wantErr: true},
		{name: "pageInfo without hasNextPage", responses: []string{team(`{"nodes":[` + alice + `],"pageInfo":{"endCursor":"c"}}`), cycles}, wantErr: true},
		{name: "members null", responses: []string{team(`null`), cycles}, wantErr: true},
		{name: "hasNextPage true, no cursor", responses: []string{team(`{"nodes":[` + alice + `],"pageInfo":{"hasNextPage":true,"endCursor":null}}`), cycles}, wantErr: true},
		{name: "hasNextPage true, follow-up transport error", responses: []string{team(`{"nodes":[` + alice + `],"pageInfo":{"hasNextPage":true,"endCursor":"c1"}}`)}, wantErr: true},
		{name: "hasNextPage true, follow-up ends", responses: []string{team(`{"nodes":[` + alice + `],"pageInfo":{"hasNextPage":true,"endCursor":"c1"}}`),
			`{"data":{"team":{"members":{"nodes":[` + bob + `],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}}`, cycles}, wantRows: 2, wantComplete: true},
		{name: "follow-up without hasNextPage", responses: []string{team(`{"nodes":[` + alice + `],"pageInfo":{"hasNextPage":true,"endCursor":"c1"}}`),
			`{"data":{"team":{"members":{"nodes":[` + bob + `],"pageInfo":{}}}}}`, cycles}, wantErr: true},
		{name: "follow-up GraphQL errors with data", responses: []string{team(`{"nodes":[` + alice + `],"pageInfo":{"hasNextPage":true,"endCursor":"c1"}}`),
			`{"data":{"team":{"members":{"nodes":[` + bob + `],"pageInfo":{"hasNextPage":false,"endCursor":null}}}},"errors":[{"message":"partial"}]}`, cycles}, wantErr: true},
		{name: "follow-up repeats its cursor", responses: []string{team(`{"nodes":[` + alice + `],"pageInfo":{"hasNextPage":true,"endCursor":"c1"}}`),
			`{"data":{"team":{"members":{"nodes":[` + bob + `],"pageInfo":{"hasNextPage":true,"endCursor":"c1"}}}}}`, cycles}, wantErr: true},
		{name: "teams query GraphQL errors with data", responses: []string{
			`{"data":{"teams":{"nodes":[{"id":"team-raw-eng","key":"ENG","name":"Eng","members":{"nodes":[` + alice + `],"pageInfo":{"hasNextPage":false,"endCursor":null}}}],"pageInfo":{"hasNextPage":false,"endCursor":null}}},"errors":[{"message":"partial"}]}`, cycles}, wantErr: true},
		{name: "node with neither id nor email", responses: []string{team(`{"nodes":[` + alice + `,{"name":"Ghost","active":true}],"pageInfo":{"hasNextPage":false,"endCursor":null}}`), cycles}, wantRows: 1, wantUnusable: 1, wantComplete: true},
		{name: "inactive member", responses: []string{team(`{"nodes":[` + alice + `,{"id":"user-2","email":"bob@example.com","active":false}],"pageInfo":{"hasNextPage":false,"endCursor":null}}`), cycles}, wantRows: 1, wantComplete: true},
		{name: "empty list with end", responses: []string{team(`{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}}`), cycles}, wantComplete: true},
		{name: "nodes null with end", responses: []string{team(`{"nodes":null,"pageInfo":{"hasNextPage":false,"endCursor":null}}`), cycles}, wantUnusable: 1, wantComplete: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			claim := nativeTestClaim("linear", "work-items")
			ref := teamCatalogRefFromClaim(claim)
			ref.Strict = true
			batch, err := (LinearReferenceCatalogRouteHandler{PerPage: 50, MaxPages: 10}).CollectReferenceCatalog(context.Background(), ref,
				providerfoundation.Credential{Provider: "linear", ID: claim.CredentialID},
				linearWorkItemsClient(t, fakehttp.Client(&linearWorkItemsDoer{responses: append([]string(nil), tc.responses...)})),
				TeamCatalogSelections{Teams: true, Members: true}, time.Date(2026, 10, 2, 8, 0, 0, 0, time.UTC))
			t.Logf("linear member read | %s | err=%v | teams=%d | memberships=%d | unusable=%v | membersComplete=%v",
				tc.name, err, len(batch.Rows.Teams), len(batch.Rows.Memberships), batch.Rows.UnusableMemberTeamIDs, batch.Evidence.MembersComplete)
			if (err != nil) != tc.wantErr || len(batch.Rows.UnusableMemberTeamIDs) != tc.wantUnusable || len(batch.Rows.Memberships) != tc.wantRows ||
				batch.Evidence.MembersComplete != tc.wantComplete {
				t.Errorf("err=%v unusable=%d memberships=%d complete=%v, want err=%v unusable=%d memberships=%d complete=%v",
					err, len(batch.Rows.UnusableMemberTeamIDs), len(batch.Rows.Memberships), batch.Evidence.MembersComplete,
					tc.wantErr, tc.wantUnusable, tc.wantRows, tc.wantComplete)
			}
			if err != nil && errors.Is(err, context.Canceled) {
				t.Log("cancelled")
			}
		})
	}
}
