package teamsidentity

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// funcDoer answers every request with the function's response and counts the requests.
type funcDoer struct {
	requests atomic.Int64
	answer   func(request *http.Request, number int) (header http.Header, body string)
}

func (doer *funcDoer) Do(request *http.Request) (*http.Response, error) {
	number := int(doer.requests.Add(1))
	header, body := doer.answer(request, number)
	if header == nil {
		header = http.Header{}
	}
	return &http.Response{StatusCode: http.StatusOK, Header: header, Request: request, Body: io.NopCloser(strings.NewReader(body))}, nil
}

func baseFor(provider string) string {
	if provider == "github" {
		return "https://api.github.com"
	}
	return "https://api.example.test"
}

func hostileClient(t *testing.T, provider string, doer *funcDoer) *providerfoundation.HTTPClient {
	t.Helper()
	client, err := providerfoundation.NewHTTPClient(provider, baseFor(provider), fakehttp.Client(doer),
		func(*http.Request) error { return nil },
		providerfoundation.RetryPolicy{MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond},
		providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }))
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func captureWarns(t *testing.T) *bytes.Buffer {
	t.Helper()
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })
	return &logs
}

func header(pairs ...string) http.Header {
	out := http.Header{}
	for index := 0; index+1 < len(pairs); index += 2 {
		out.Set(pairs[index], pairs[index+1])
	}
	return out
}

// hostileCase is one hostile answer a provider can give and the cause the walk must fail closed with.
type hostileCase struct {
	name   string
	answer func(request *http.Request, number int) (http.Header, string)
	cause  string
	// maxRequests caps the requests sent before the walk gave up
	maxRequests int
}

func linkNext(url string) http.Header { return header("Link", fmt.Sprintf(`<%s>; rel="next"`, url)) }

func githubHostile() []hostileCase {
	return []hostileCase{
		{"repeated rel=next", func(r *http.Request, n int) (http.Header, string) {
			return linkNext("https://api.github.com" + r.URL.Path + "?per_page=100&page=2"), fmt.Sprintf(`[{"slug":"s%d","name":"n%d","login":"u%d"}]`, n, n, n)
		}, walkCauseRepeated, 3},
		{"empty page advertising next", func(r *http.Request, n int) (http.Header, string) {
			return linkNext(fmt.Sprintf("https://api.github.com%s?per_page=100&page=%d", r.URL.Path, n+1)), `[]`
		}, walkCauseEmptyPageToNext, 1},
		{"unbounded fresh rel=next", func(r *http.Request, n int) (http.Header, string) {
			return linkNext(fmt.Sprintf("https://api.github.com%s?per_page=100&page=%d", r.URL.Path, n+1)), fmt.Sprintf(`[{"slug":"s%d","name":"n%d","login":"u%d"}]`, n, n, n)
		}, walkCauseMaxPages, pageWalkMaxPages},
	}
}

func gitlabHostile() []hostileCase {
	body := func(n int) string {
		return fmt.Sprintf(`[{"id":%d,"full_path":"g%d","username":"u%d","path_with_namespace":"p%d"}]`, n, n, n, n)
	}
	return []hostileCase{
		{"X-Next-Page equal to the current page", func(r *http.Request, n int) (http.Header, string) {
			return header("X-Next-Page", "1"), body(n)
		}, walkCauseRepeated, 1},
		{"X-Next-Page repeated after progress", func(r *http.Request, n int) (http.Header, string) {
			return header("X-Next-Page", "2"), body(n)
		}, walkCauseRepeated, 2},
		{"empty page advertising next", func(r *http.Request, n int) (http.Header, string) {
			return header("X-Next-Page", fmt.Sprint(n+1)), `[]`
		}, walkCauseEmptyPageToNext, 1},
		{"unbounded fresh X-Next-Page", func(r *http.Request, n int) (http.Header, string) {
			return header("X-Next-Page", fmt.Sprint(n+1)), body(n)
		}, walkCauseMaxPages, pageWalkMaxPages},
		{"X-Next-Page not a number", func(r *http.Request, n int) (http.Header, string) {
			return header("X-Next-Page", "soon"), body(n)
		}, walkCauseBadContinuation, 1},
	}
}

func assertFailedClosed(t *testing.T, err error, doer *funcDoer, logs *bytes.Buffer, provider string, tc hostileCase) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s: walk returned success on a hostile answer", tc.name)
	}
	if got := int(doer.requests.Load()); got > tc.maxRequests {
		t.Fatalf("%s: %d requests sent, want at most %d", tc.name, got, tc.maxRequests)
	}
	if !strings.Contains(err.Error(), provider) || !strings.Contains(err.Error(), tc.cause) {
		t.Fatalf("%s: error %q does not name the provider %q and cause %q", tc.name, err, provider, tc.cause)
	}
	if !strings.Contains(logs.String(), `"msg":"teams_identity_page_walk_failed"`) || !strings.Contains(logs.String(), `"cause":"`+tc.cause+`"`) {
		t.Fatalf("%s: no Warn line with the cause:\n%s", tc.name, logs.String())
	}
}

func TestGitHubWalksFailClosedOnAHostileContinuation(t *testing.T) {
	for _, tc := range githubHostile() {
		t.Run(tc.name, func(t *testing.T) {
			logs := captureWarns(t)
			doer := &funcDoer{answer: tc.answer}
			client := hostileClient(t, "github", doer)
			_, err := githubListAllPages(context.Background(), client, "org teams", "o", "/orgs/{org}/teams", "/orgs/o/teams?per_page=100",
				func(entry githubTeamListEntry) string { return entry.Slug })
			assertFailedClosed(t, err, doer, logs, "github", tc)
		})
	}
}

func TestGitLabListWalksFailClosedOnAHostileContinuation(t *testing.T) {
	for _, tc := range gitlabHostile() {
		t.Run("subgroups/"+tc.name, func(t *testing.T) {
			logs := captureWarns(t)
			doer := &funcDoer{answer: tc.answer}
			_, _, err := gitlabListGroupPages(context.Background(), hostileClient(t, "gitlab", doer), "g", "/groups/{group}/subgroups", "/api/v4/groups/1/subgroups", 100000)
			assertFailedClosed(t, err, doer, logs, "gitlab", tc)
		})
		t.Run("projects/"+tc.name, func(t *testing.T) {
			logs := captureWarns(t)
			doer := &funcDoer{answer: tc.answer}
			_, _, err := gitlabListProjectPages(context.Background(), hostileClient(t, "gitlab", doer), "g", "/groups/{group}/projects", "/api/v4/groups/1/projects", 100000, false)
			assertFailedClosed(t, err, doer, logs, "gitlab", tc)
		})
	}
}

func withDiscoveryDoer(t *testing.T, doer *funcDoer) {
	t.Helper()
	old := fakehttp.Client(discoveryHTTPClient)
	discoveryHTTPClient = fakehttp.Client(doer)
	t.Cleanup(func() { discoveryHTTPClient = fakehttp.Client(old) })
}

func TestMemberWalksFailClosedOnAHostileContinuation(t *testing.T) {
	token := secrets.NewValue("token")
	for _, tc := range githubHostile() {
		t.Run("github members/"+tc.name, func(t *testing.T) {
			logs := captureWarns(t)
			doer := &funcDoer{answer: func(r *http.Request, n int) (http.Header, string) {
				if strings.HasSuffix(r.URL.Path, "/members") {
					return tc.answer(r, n)
				}
				return nil, `{"url":"https://api.example.test/orgs/o/teams/t"}`
			}}
			withDiscoveryDoer(t, doer)
			_, err := discoverMembersGitHub(context.Background(), token, "o", "t")
			// the detail request is request 1, so the member walk gets one request fewer than its cap plus one
			capped := tc
			capped.maxRequests++
			assertFailedClosed(t, err, doer, logs, "github", capped)
		})
	}
	for _, tc := range gitlabHostile() {
		t.Run("gitlab members/"+tc.name, func(t *testing.T) {
			logs := captureWarns(t)
			doer := &funcDoer{answer: func(r *http.Request, n int) (http.Header, string) {
				if strings.HasSuffix(r.URL.Path, "/members") {
					return tc.answer(r, n)
				}
				return nil, `{"id":7,"full_path":"g","name":"G"}`
			}}
			withDiscoveryDoer(t, doer)
			_, err := discoverMembersGitLab(context.Background(), token, "", "g")
			capped := tc
			capped.maxRequests++
			assertFailedClosed(t, err, doer, logs, "gitlab", capped)
		})
	}
}

func TestJiraIssueSearchFailsClosedOnAHostileContinuation(t *testing.T) {
	issues := func(n int) string {
		return fmt.Sprintf(`{"issues":[{"key":"P-%d","fields":{}}],`, n)
	}
	cases := []hostileCase{
		{"repeated nextPageToken", func(r *http.Request, n int) (http.Header, string) {
			return nil, issues(n) + `"nextPageToken":"same"}`
		}, walkCauseRepeated, 2},
		{"unbounded fresh nextPageToken", func(r *http.Request, n int) (http.Header, string) {
			return nil, issues(n) + fmt.Sprintf(`"nextPageToken":"t%d"}`, n)
		}, walkCauseMaxPages, pageWalkMaxPages},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			logs := captureWarns(t)
			doer := &funcDoer{answer: tc.answer}
			// 500 issues would end the walk first at 1 issue per page only after 500 pages: the page bound must come first
			_, err := jiraSearchIssues(context.Background(), hostileClient(t, "jira", doer), "P", "project = 'P'")
			assertFailedClosed(t, err, doer, logs, "jira", tc)
		})
	}
}

func TestAnItemOnTwoPagesIsListedOnce(t *testing.T) {
	doer := &funcDoer{answer: func(r *http.Request, n int) (http.Header, string) {
		if n == 1 {
			return linkNext("https://api.github.com" + r.URL.Path + "?per_page=100&page=2"), `[{"slug":"a","name":"A"},{"slug":"b","name":"B"}]`
		}
		return nil, `[{"slug":"b","name":"B"},{"slug":"c","name":"C"}]`
	}}
	got, err := githubListAllPages(context.Background(), hostileClient(t, "github", doer), "org teams", "o", "/orgs/{org}/teams", "/orgs/o/teams?per_page=100",
		func(entry githubTeamListEntry) string { return entry.Slug })
	if err != nil || len(got) != 3 || got[0].Slug != "a" || got[1].Slug != "b" || got[2].Slug != "c" {
		t.Fatalf("teams = %+v, err = %v, want a,b,c once each", got, err)
	}
}
