package teamsidentity

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
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

// walkReader reads the failure counter of the test that called captureWarns last.
var walkReader *sdkmetric.ManualReader

func captureWarns(t *testing.T) *bytes.Buffer {
	t.Helper()
	walkReader = sdkmetric.NewManualReader()
	counter, err := sdkmetric.NewMeterProvider(sdkmetric.WithReader(walkReader)).Meter("test").Int64Counter("teams_identity_page_walk_failed_total")
	if err != nil {
		t.Fatal(err)
	}
	previousCounter := pageWalkFailures
	pageWalkFailures = counter
	t.Cleanup(func() { pageWalkFailures = previousCounter })
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
			return linkNext(r.URL.Scheme + "://" + r.URL.Host + r.URL.Path + "?per_page=100&page=2"), fmt.Sprintf(`[{"slug":"s%d","name":"SECRETMARK-n%d","login":"u%d"}]`, n, n, n)
		}, walkCauseRepeated, 3},
		{"empty page advertising next", func(r *http.Request, n int) (http.Header, string) {
			return linkNext(fmt.Sprintf("%s://%s%s?per_page=100&page=%d", r.URL.Scheme, r.URL.Host, r.URL.Path, n+1)), `[]`
		}, walkCauseEmptyPageToNext, 1},
		{"unbounded fresh rel=next", func(r *http.Request, n int) (http.Header, string) {
			return linkNext(fmt.Sprintf("%s://%s%s?per_page=100&page=%d", r.URL.Scheme, r.URL.Host, r.URL.Path, n+1)), fmt.Sprintf(`[{"slug":"s%d","name":"SECRETMARK-n%d","login":"u%d"}]`, n, n, n)
		}, walkCauseMaxPages, pageWalkMaxPages},
	}
}

func gitlabHostile() []hostileCase {
	body := func(n int) string {
		return fmt.Sprintf(`[{"id":%d,"full_path":"SECRETMARK-g%d","username":"u%d","path_with_namespace":"p%d"}]`, n, n, n, n)
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

// assertNoPartial fails when a walk that errored still handed back data.
func assertNoPartial(t *testing.T, name string, got any) {
	t.Helper()
	if value := reflect.ValueOf(got); value.Kind() == reflect.Slice && value.Len() != 0 {
		t.Fatalf("%s: the walk failed but returned %d items", name, value.Len())
	}
}

func assertFailedClosed(t *testing.T, err error, doer *funcDoer, logs *bytes.Buffer, provider string, tc hostileCase) {
	t.Helper()
	providerName := provider
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
	if got := strings.Count(logs.String(), `"msg":"teams_identity_page_walk_failed"`); got != 1 {
		t.Fatalf("%s: %d Warn lines, want 1:\n%s", tc.name, got, logs.String())
	}
	if strings.Contains(logs.String(), "SECRETMARK") || strings.Contains(err.Error(), "SECRETMARK") {
		t.Fatalf("%s: payload text reached the Warn line or the error:\n%s\n%v", tc.name, logs.String(), err)
	}
	var collected metricdata.ResourceMetrics
	if err := walkReader.Collect(context.Background(), &collected); err != nil {
		t.Fatal(err)
	}
	total, matched := int64(0), false
	for _, scope := range collected.ScopeMetrics {
		for _, found := range scope.Metrics {
			sum, ok := found.Data.(metricdata.Sum[int64])
			if !ok || found.Name != "teams_identity_page_walk_failed_total" {
				continue
			}
			for _, point := range sum.DataPoints {
				total += point.Value
				if value, present := point.Attributes.Value("cause"); present && value.AsString() == tc.cause {
					if provider, _ := point.Attributes.Value("provider"); provider.AsString() == providerName {
						matched = true
					}
				}
			}
		}
	}
	if total != 1 || !matched {
		t.Fatalf("%s: failure counter total=%d matched(provider=%s, cause=%s)=%v, want 1 and true", tc.name, total, providerName, tc.cause, matched)
	}
}

func TestGitHubWalksFailClosedOnAHostileContinuation(t *testing.T) {
	for _, tc := range githubHostile() {
		t.Run(tc.name, func(t *testing.T) {
			logs := captureWarns(t)
			doer := &funcDoer{answer: tc.answer}
			client := hostileClient(t, "github", doer)
			got, err := githubListAllPages(context.Background(), client, "org teams", "o", "/orgs/{org}/teams", "/orgs/o/teams?per_page=100",
				func(entry githubTeamListEntry) string { return entry.Slug })
			assertNoPartial(t, tc.name, got)
			assertFailedClosed(t, err, doer, logs, "github", tc)
		})
	}
}

func TestGitLabListWalksFailClosedOnAHostileContinuation(t *testing.T) {
	for _, tc := range gitlabHostile() {
		t.Run("subgroups/"+tc.name, func(t *testing.T) {
			logs := captureWarns(t)
			doer := &funcDoer{answer: tc.answer}
			got, _, err := gitlabListGroupPages(context.Background(), hostileClient(t, "gitlab", doer), "g", "/groups/{group}/subgroups", "/api/v4/groups/1/subgroups", 100000)
			assertNoPartial(t, tc.name, got)
			assertFailedClosed(t, err, doer, logs, "gitlab", tc)
		})
		t.Run("projects/"+tc.name, func(t *testing.T) {
			logs := captureWarns(t)
			doer := &funcDoer{answer: tc.answer}
			got, _, err := gitlabListProjectPages(context.Background(), hostileClient(t, "gitlab", doer), "g", "/groups/{group}/projects", "/api/v4/groups/1/projects", 100000, false)
			assertNoPartial(t, tc.name, got)
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
			got, err := discoverMembersGitHub(context.Background(), token, "o", "t")
			// the detail request is request 1, so the member walk gets one request fewer than its cap plus one
			capped := tc
			capped.maxRequests++
			assertNoPartial(t, tc.name, got)
			assertFailedClosed(t, err, doer, logs, "github", capped)
		})
	}
	for _, tc := range githubHostile() {
		t.Run("gitlab members/"+tc.name, func(t *testing.T) {
			logs := captureWarns(t)
			doer := &funcDoer{answer: func(r *http.Request, n int) (http.Header, string) {
				if strings.HasSuffix(r.URL.Path, "/members") {
					return tc.answer(r, n)
				}
				return nil, `{"id":7,"full_path":"g","name":"G"}`
			}}
			withDiscoveryDoer(t, doer)
			got, err := discoverMembersGitLab(context.Background(), token, "", "g")
			capped := tc
			capped.maxRequests++
			assertNoPartial(t, tc.name, got)
			assertFailedClosed(t, err, doer, logs, "gitlab", capped)
		})
	}
}

func TestJiraIssueSearchFailsClosedOnAHostileContinuation(t *testing.T) {
	issues := func(n int) string {
		return fmt.Sprintf(`{"issues":[{"key":"P-%d","fields":{"summary":"SECRETMARK"}}],`, n)
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
			got, err := jiraSearchIssues(context.Background(), hostileClient(t, "jira", doer), "P", "project = 'P'")
			assertNoPartial(t, tc.name, got)
			assertFailedClosed(t, err, doer, logs, "jira", tc)
		})
	}
}

func TestAnItemOnTwoPagesIsListedOnce(t *testing.T) {
	doer := &funcDoer{answer: func(r *http.Request, n int) (http.Header, string) {
		if n == 1 {
			return linkNext(r.URL.Scheme + "://" + r.URL.Host + r.URL.Path + "?per_page=100&page=2"), `[{"slug":"a","name":"A"},{"slug":"b","name":"B"}]`
		}
		return nil, `[{"slug":"b","name":"B"},{"slug":"c","name":"C"}]`
	}}
	got, err := githubListAllPages(context.Background(), hostileClient(t, "github", doer), "org teams", "o", "/orgs/{org}/teams", "/orgs/o/teams?per_page=100",
		func(entry githubTeamListEntry) string { return entry.Slug })
	if err != nil || len(got) != 3 || got[0].Slug != "a" || got[1].Slug != "b" || got[2].Slug != "c" {
		t.Fatalf("teams = %+v, err = %v, want a,b,c once each", got, err)
	}
}

func TestAnEmptyNextLinkAndAnEmptyJiraPageWithATokenFailClosed(t *testing.T) {
	t.Run("github empty rel=next URL", func(t *testing.T) {
		logs := captureWarns(t)
		doer := &funcDoer{answer: func(r *http.Request, n int) (http.Header, string) {
			return header("Link", `<>; rel="next"`), `[{"slug":"a","name":"A"}]`
		}}
		_, err := githubListAllPages(context.Background(), hostileClient(t, "github", doer), "org teams", "o", "/orgs/{org}/teams", "/orgs/o/teams?per_page=100",
			func(entry githubTeamListEntry) string { return entry.Slug })
		assertFailedClosed(t, err, doer, logs, "github", hostileCase{name: "empty rel=next URL", cause: walkCauseNoContinuation, maxRequests: 1})
	})
	t.Run("jira empty page with a nextPageToken", func(t *testing.T) {
		logs := captureWarns(t)
		doer := &funcDoer{answer: func(r *http.Request, n int) (http.Header, string) {
			if n == 1 {
				return nil, `{"issues":[{"key":"P-1","fields":{}}],"nextPageToken":"t1"}`
			}
			return nil, `{"issues":[],"nextPageToken":"t2"}`
		}}
		_, err := jiraSearchIssues(context.Background(), hostileClient(t, "jira", doer), "P", "project = 'P'")
		assertFailedClosed(t, err, doer, logs, "jira", hostileCase{name: "empty page with token", cause: walkCauseEmptyPageToNext, maxRequests: 2})
	})
}

func TestReproducedRound2WalkHoles(t *testing.T) {
	t.Run("jira empty page with isLast false", func(t *testing.T) {
		logs := captureWarns(t)
		doer := &funcDoer{answer: func(r *http.Request, n int) (http.Header, string) {
			if n == 1 {
				return nil, `{"issues":[{"key":"P-1","fields":{}}],"isLast":false}`
			}
			return nil, `{"issues":[],"isLast":false}`
		}}
		got, err := jiraSearchIssues(context.Background(), hostileClient(t, "jira", doer), "P", "project = 'P'")
		assertNoPartial(t, "jira isLast false", got)
		assertFailedClosed(t, err, doer, logs, "jira", hostileCase{name: "empty page isLast false", cause: walkCauseEmptyPageToNext, maxRequests: 2})
	})
	t.Run("gitlab malformed X-Next-Page after the item limit", func(t *testing.T) {
		logs := captureWarns(t)
		doer := &funcDoer{answer: func(r *http.Request, n int) (http.Header, string) {
			return header("X-Next-Page", "soon"), `[{"id":1,"path_with_namespace":"g/a"},{"id":2,"path_with_namespace":"g/b"}]`
		}}
		got, truncated, err := gitlabListProjectPages(context.Background(), hostileClient(t, "gitlab", doer), "g", "/groups/{group}/projects", "/api/v4/groups/1/projects", 1, false)
		assertNoPartial(t, "gitlab limit", got)
		if truncated {
			t.Fatal("a walk that failed reported a truncated success")
		}
		assertFailedClosed(t, err, doer, logs, "gitlab", hostileCase{name: "malformed after limit", cause: walkCauseBadContinuation, maxRequests: 1})
	})
	t.Run("gitlab leading zero X-Next-Page repeats the page", func(t *testing.T) {
		logs := captureWarns(t)
		doer := &funcDoer{answer: func(r *http.Request, n int) (http.Header, string) {
			return header("X-Next-Page", "01"), fmt.Sprintf(`[{"id":%d,"path_with_namespace":"g/%d"}]`, n, n)
		}}
		got, _, err := gitlabListProjectPages(context.Background(), hostileClient(t, "gitlab", doer), "g", "/groups/{group}/projects", "/api/v4/groups/1/projects", 100000, false)
		assertNoPartial(t, "gitlab 01", got)
		assertFailedClosed(t, err, doer, logs, "gitlab", hostileCase{name: "leading zero", cause: walkCauseBadContinuation, maxRequests: 1})
	})
}

// A malformed or inconsistent continuation signal is never a success, whichever early return the walk would have taken.
func TestAMalformedOrInconsistentContinuationSignalIsNeverASuccess(t *testing.T) {
	issues := func(count, from int) string {
		parts := make([]string, count)
		for index := range parts {
			parts[index] = fmt.Sprintf(`{"key":"P-%d","fields":{}}`, from+index)
		}
		return strings.Join(parts, ",")
	}
	jiraCases := []struct {
		name   string
		answer func(r *http.Request, n int) (http.Header, string)
		cause  string
		max    int
	}{
		{"isLast true with a token", func(r *http.Request, n int) (http.Header, string) {
			return nil, `{"issues":[{"key":"P-1","fields":{}}],"nextPageToken":"t","isLast":true}`
		}, walkCauseInconsistent, 1},
		{"isLast not a boolean", func(r *http.Request, n int) (http.Header, string) {
			return nil, `{"issues":[{"key":"P-1","fields":{}}],"isLast":"false"}`
		}, walkCauseBadContinuation, 1},
		{"token not a string", func(r *http.Request, n int) (http.Header, string) {
			return nil, `{"issues":[{"key":"P-1","fields":{}}],"nextPageToken":5}`
		}, walkCauseBadContinuation, 1},
		{"repeated token on the page that reaches the item limit", func(r *http.Request, n int) (http.Header, string) {
			if n == 1 {
				return nil, `{"issues":[` + issues(100, 0) + `],"nextPageToken":"t1"}`
			}
			return nil, `{"issues":[` + issues(400, 1000) + `],"nextPageToken":"t1"}`
		}, walkCauseRepeated, 2},
	}
	for _, tc := range jiraCases {
		t.Run("jira/"+tc.name, func(t *testing.T) {
			logs := captureWarns(t)
			doer := &funcDoer{answer: tc.answer}
			got, err := jiraSearchIssues(context.Background(), hostileClient(t, "jira", doer), "P", "project = 'P'")
			assertNoPartial(t, tc.name, got)
			assertFailedClosed(t, err, doer, logs, "jira", hostileCase{name: tc.name, cause: tc.cause, maxRequests: tc.max})
		})
	}
	for _, value := range []string{"0", "-1", "1.5", "2 ", " 2", "two"} {
		for _, limit := range []int{1, 100000} {
			t.Run(fmt.Sprintf("gitlab/X-Next-Page %q limit %d", value, limit), func(t *testing.T) {
				logs := captureWarns(t)
				doer := &funcDoer{answer: func(r *http.Request, n int) (http.Header, string) {
					return header("X-Next-Page", value), `[{"id":1,"path_with_namespace":"g/a"},{"id":2,"path_with_namespace":"g/b"}]`
				}}
				got, truncated, err := gitlabListProjectPages(context.Background(), hostileClient(t, "gitlab", doer), "g", "/groups/{group}/projects", "/api/v4/groups/1/projects", limit, false)
				assertNoPartial(t, value, got)
				if truncated {
					t.Fatal("a walk that failed reported a truncated success")
				}
				assertFailedClosed(t, err, doer, logs, "gitlab", hostileCase{name: value, cause: walkCauseBadContinuation, maxRequests: 1})
			})
		}
	}
	t.Run("github rel=next to another host", func(t *testing.T) {
		logs := captureWarns(t)
		doer := &funcDoer{answer: func(r *http.Request, n int) (http.Header, string) {
			return linkNext("https://evil.example.test/orgs/o/teams?page=2"), `[{"slug":"a","name":"A"}]`
		}}
		got, err := githubListAllPages(context.Background(), hostileClient(t, "github", doer), "org teams", "o", "/orgs/{org}/teams", "/orgs/o/teams?per_page=100",
			func(entry githubTeamListEntry) string { return entry.Slug })
		assertNoPartial(t, "other host", got)
		assertFailedClosed(t, err, doer, logs, "github", hostileCase{name: "other host", cause: walkCausePageRequest, maxRequests: 1})
	})
	t.Run("github bare rel=next", func(t *testing.T) {
		logs := captureWarns(t)
		doer := &funcDoer{answer: func(r *http.Request, n int) (http.Header, string) {
			return header("Link", `rel="next"`), `[{"slug":"a","name":"A"}]`
		}}
		got, err := githubListAllPages(context.Background(), hostileClient(t, "github", doer), "org teams", "o", "/orgs/{org}/teams", "/orgs/o/teams?per_page=100",
			func(entry githubTeamListEntry) string { return entry.Slug })
		assertNoPartial(t, "bare rel", got)
		assertFailedClosed(t, err, doer, logs, "github", hostileCase{name: "bare rel=next", cause: walkCauseNoContinuation, maxRequests: 1})
	})
	for _, token := range []string{"0", "false", "[]", `""`} {
		t.Run("jira falsy non-string or empty token "+token, func(t *testing.T) {
			logs := captureWarns(t)
			doer := &funcDoer{answer: func(r *http.Request, n int) (http.Header, string) {
				return nil, `{"issues":[{"key":"P-1","fields":{}}],"nextPageToken":` + token + `}`
			}}
			got, err := jiraSearchIssues(context.Background(), hostileClient(t, "jira", doer), "P", "project = 'P'")
			assertNoPartial(t, token, got)
			assertFailedClosed(t, err, doer, logs, "jira", hostileCase{name: token, cause: walkCauseBadContinuation, maxRequests: 1})
		})
	}
	for _, value := range []string{"02", "+2", "007"} {
		t.Run("gitlab zero-padded or signed forward cursor "+value, func(t *testing.T) {
			logs := captureWarns(t)
			doer := &funcDoer{answer: func(r *http.Request, n int) (http.Header, string) {
				return header("X-Next-Page", value), `[{"id":1,"path_with_namespace":"g/a"}]`
			}}
			got, _, err := gitlabListProjectPages(context.Background(), hostileClient(t, "gitlab", doer), "g", "/groups/{group}/projects", "/api/v4/groups/1/projects", 100000, false)
			assertNoPartial(t, value, got)
			assertFailedClosed(t, err, doer, logs, "gitlab", hostileCase{name: value, cause: walkCauseBadContinuation, maxRequests: 1})
		})
	}
}

// The GitLab member walk follows python-gitlab: the first request carries per_page only (the member-route venue oracle pins it) and
// every later request is the Link header's own URL.
func TestGitLabMembersFirstRequestIsThePinnedOne(t *testing.T) {
	var first, second atomic.Value
	doer := &funcDoer{answer: func(r *http.Request, n int) (http.Header, string) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/members") && n == 2:
			first.Store(r.URL.RequestURI())
			return linkNext(r.URL.Scheme + "://" + r.URL.Host + r.URL.Path + "?page=2&per_page=100"), `[{"id":1,"username":"a","name":"A"}]`
		case strings.HasSuffix(r.URL.Path, "/members"):
			second.Store(r.URL.RequestURI())
			return nil, `[{"id":2,"username":"b","name":"B"}]`
		}
		return nil, `{"id":7,"full_path":"g","name":"G"}`
	}}
	withDiscoveryDoer(t, doer)
	members, err := discoverMembersGitLab(context.Background(), secrets.NewValue("token"), "", "g")
	if err != nil || len(members) != 2 {
		t.Fatalf("members = %d, err = %v", len(members), err)
	}
	if first.Load() != "/api/v4/groups/7/members?per_page=100" || second.Load() != "/api/v4/groups/7/members?page=2&per_page=100" {
		t.Fatalf("requests = %v then %v", first.Load(), second.Load())
	}
}
