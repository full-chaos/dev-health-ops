package providersync

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

const gitHubPullRequestRepoFixture = `{
  "id": 4567,
  "name": "api",
  "full_name": "Acme/API",
  "html_url": "https://github.com/Acme/API",
  "default_branch": "main",
  "language": "Go"
}`

// gitHubPullRequestListFixture has three PRs: #42 inside the claim window
// (2026-07-01..2026-07-31T23:59:59Z), #41 updated before it, and #43 updated
// after it. Only #42 may reach a detail (.../pulls/{number}) request.
const gitHubPullRequestListFixture = `[
  {"number": 43, "updated_at": "2026-08-01T00:00:00Z"},
  {"number": 42, "updated_at": "2026-07-21T15:30:00Z"},
  {"number": 41, "updated_at": "2026-06-15T00:00:00Z"}
]`

// gitHubPullRequestDetailFixture42 and its expected row below were
// independently produced by running the real Python collector against this
// exact JSON --
//
//	gh_pr = _pull_from_item(fixture)                     # code_client.py
//	state = normalize_pr_state(gh_pr.state, gh_pr.merged_at)  # providers/pr_state.py
//	repo_id = get_repo_uuid_from_repo("Acme/API")         # models/git.py
//	pr = build_git_pull_request(repo_id=repo_id, number=gh_pr.number, ...)
//
// which printed repo_id=c7198fbc-1945-3717-05d8-eb78866b4e79, state="merged",
// author_name="octocat", author_email=None, created_at=2026-07-10T09:00:00Z,
// merged_at=closed_at=2026-07-21T15:30:00Z, head_branch="feature/widgets",
// base_branch="main", additions=120, deletions=30, changed_files=5,
// comments_count=3 -- field-for-field identical to the assertions below. This
// is fixture parity, not live parity: it proves Go and Python agree on this
// input, not that either agrees with a live GitHub API response.
const gitHubPullRequestDetailFixture42 = `{
  "id": 991234,
  "number": 42,
  "title": "Add widget support",
  "body": "This PR adds widget support.",
  "state": "closed",
  "user": {"login": "octocat"},
  "created_at": "2026-07-10T09:00:00Z",
  "updated_at": "2026-07-21T15:30:00Z",
  "merged_at": "2026-07-21T15:30:00Z",
  "closed_at": "2026-07-21T15:30:00Z",
  "head": {"ref": "feature/widgets"},
  "base": {"ref": "main"},
  "additions": 120,
  "deletions": 30,
  "changed_files": 5,
  "comments": 3
}`

type gitHubPullRequestDoer struct {
	t        *testing.T
	bodies   map[string]string
	requests []string
}

func (doer *gitHubPullRequestDoer) Do(request *http.Request) (*http.Response, error) {
	doer.t.Helper()
	doer.requests = append(doer.requests, request.URL.Path)
	body, ok := doer.bodies[request.URL.Path]
	if !ok {
		return &http.Response{
			StatusCode: http.StatusNotFound,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(strings.NewReader(`{}`)),
			Request:    request,
		}, nil
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(body)),
		Request:    request,
	}, nil
}

func gitHubPullRequestClient(
	t *testing.T, doer providerfoundation.HTTPDoer, base string,
) *providerfoundation.HTTPClient {
	t.Helper()
	client, err := providerfoundation.NewHTTPClient(
		"github", base, doer,
		func(*http.Request) error { return nil },
		providerfoundation.RetryPolicy{
			MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond,
		},
		providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
	)
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func defaultGitHubPullRequestFixtures() map[string]string {
	return map[string]string{
		"/repos/acme/api":          gitHubPullRequestRepoFixture,
		"/repos/acme/api/pulls":    gitHubPullRequestListFixture,
		"/repos/acme/api/pulls/42": gitHubPullRequestDetailFixture42,
	}
}

// TestGitHubPullRequestRouteEmitsOneBoundedEffect carries the CHAOS-3122
// REST collector parity evidence consumed by the composed PR-social route:
// see gitHubPullRequestDetailFixture42's doc comment.
func TestGitHubPullRequestRouteEmitsOneBoundedEffect(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC)
	doer := &gitHubPullRequestDoer{t: t, bodies: defaultGitHubPullRequestFixtures()}
	client := gitHubPullRequestClient(t, doer, "https://api.github.com")
	claim := nativeTestClaim("github", "prs")

	batch, err := (GitHubPullRequestRouteHandler{
		Now: func() time.Time { return now },
	}).Collect(context.Background(), claim, providerfoundation.Credential{}, client, now)
	if err != nil {
		t.Fatal(err)
	}

	// Only #42 falls inside the claim window, so exactly one detail request
	// (plus the repo GET and the list GET) must have been issued -- #41 and
	// #43 must never reach GET .../pulls/{number}.
	want := []string{
		"/repos/acme/api", "/repos/acme/api/pulls", "/repos/acme/api/pulls/42",
	}
	if len(doer.requests) != len(want) {
		t.Fatalf("requests=%v want=%v", doer.requests, want)
	}
	for index, path := range want {
		if doer.requests[index] != path {
			t.Fatalf("requests=%v want=%v", doer.requests, want)
		}
	}

	if batch.Watermark == nil || !batch.Watermark.Equal(*claim.BeforeAt) {
		t.Fatalf("watermark=%v want claim.BeforeAt=%v", batch.Watermark, claim.BeforeAt)
	}
	if batch.Evidence.Records != 1 || batch.Evidence.CapReached {
		t.Fatalf("evidence=%+v", batch.Evidence)
	}
	if len(batch.Effects) != 1 ||
		batch.Effects[0].Destination != "git_pull_requests" ||
		batch.Effects[0].Recovery != EffectReadbackRequired ||
		len(batch.Effects[0].Rows) != 1 {
		t.Fatalf("effects=%+v", batch.Effects)
	}

	var row pullRequestRow
	if err := json.Unmarshal(batch.Effects[0].Rows[0], &row); err != nil {
		t.Fatal(err)
	}
	wantCreatedAt := time.Date(2026, 7, 10, 9, 0, 0, 0, time.UTC)
	wantMergedAt := time.Date(2026, 7, 21, 15, 30, 0, 0, time.UTC)
	switch {
	case row.RepoID != "c7198fbc-1945-3717-05d8-eb78866b4e79":
	case row.Number != 42:
	case row.Title == nil || *row.Title != "Add widget support":
	case row.Body == nil || *row.Body != "This PR adds widget support.":
	case row.State != "merged":
	case row.AuthorName != "octocat":
	case row.AuthorEmail != nil:
	case !row.CreatedAt.Equal(wantCreatedAt):
	case row.MergedAt == nil || !row.MergedAt.Equal(wantMergedAt):
	case row.ClosedAt == nil || !row.ClosedAt.Equal(wantMergedAt):
	case row.HeadBranch == nil || *row.HeadBranch != "feature/widgets":
	case row.BaseBranch == nil || *row.BaseBranch != "main":
	case row.Additions != 120:
	case row.Deletions != 30:
	case row.ChangedFiles != 5:
	case row.CommentsCount != 3:
	case row.FirstReviewAt != nil:
	case row.FirstCommentAt != nil:
	case row.ChangesRequestedCount != 0:
	case row.ReviewsCount != 0:
	case row.OrgID != claim.OrgID:
	case !row.LastSynced.Equal(now):
	default:
		return
	}
	t.Fatalf("row=%+v", row)
}

// TestGitHubPullRequestRouteAppliesWindowFilter is the same evidence as the
// request-path assertion above, isolated to its own test so a future
// refactor of TestGitHubPullRequestRouteEmitsOneBoundedEffect can't
// accidentally drop window coverage.
func TestGitHubPullRequestRouteAppliesWindowFilter(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC)
	doer := &gitHubPullRequestDoer{t: t, bodies: defaultGitHubPullRequestFixtures()}
	client := gitHubPullRequestClient(t, doer, "https://api.github.com")
	batch, err := (GitHubPullRequestRouteHandler{
		Now: func() time.Time { return now },
	}).Collect(
		context.Background(), nativeTestClaim("github", "prs"),
		providerfoundation.Credential{}, client, now,
	)
	if err != nil {
		t.Fatal(err)
	}
	if batch.Evidence.Records != 1 {
		t.Fatalf("records=%d want 1 (only #42 is inside the window)", batch.Evidence.Records)
	}
	for _, path := range doer.requests {
		if path == "/repos/acme/api/pulls/41" || path == "/repos/acme/api/pulls/43" {
			t.Fatalf("fetched detail for an out-of-window PR: %s", path)
		}
	}
}

// TestGitHubPullRequestRouteFailsClosedOnPaginationCap is codex H2: an
// unbounded Python collector would keep paging past 100 pages, so a capped
// Go fetch must fail the whole unit rather than report success with a
// partial page set and still advance claim.BeforeAt as the watermark --
// which would silently and permanently strand every PR past the cap, since
// no later incremental run revisits a window the watermark already passed.
func TestGitHubPullRequestRouteFailsClosedOnPaginationCap(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC)
	doer := &gitHubPullRequestPagingDoer{t: t, totalPages: 3}
	client := gitHubPullRequestClient(t, doer, "https://api.github.com")
	batch, err := (GitHubPullRequestRouteHandler{
		Now: func() time.Time { return now }, MaxPages: 2,
	}).Collect(
		context.Background(), nativeTestClaim("github", "prs"),
		providerfoundation.Credential{}, client, now,
	)
	if err != ErrPaginationCapExceeded {
		t.Fatalf("error=%v want ErrPaginationCapExceeded", err)
	}
	if batch.Watermark != nil {
		t.Fatalf("watermark=%v want nil: a capped fetch must never advance it", batch.Watermark)
	}
	// Uncapped (MaxPages covers every page) must succeed normally.
	uncappedDoer := &gitHubPullRequestPagingDoer{t: t, totalPages: 3}
	uncappedClient := gitHubPullRequestClient(t, uncappedDoer, "https://api.github.com")
	if _, err := (GitHubPullRequestRouteHandler{
		Now: func() time.Time { return now }, MaxPages: 5,
	}).Collect(
		context.Background(), nativeTestClaim("github", "prs"),
		providerfoundation.Credential{}, uncappedClient, now,
	); err != nil {
		t.Fatalf("uncapped fetch error=%v want nil", err)
	}
}

// TestGitHubPullRequestRouteStopsPaginatingAtTheSinceBoundary is codex H1's
// first half: a repository whose TOTAL history spans more pages than
// MaxPages must still succeed as long as only ONE page falls inside the
// claim's window -- code_client.py's iter_pulls stops the moment a listed
// item's updated_at (sort=updated&direction=desc) is known and older than
// `since`, so Python never pays for, or is limited by, the pages beyond
// that point. Page 1 here holds one in-window PR followed by one
// ancient PR that crosses the since boundary; pages 2 and 3 hold only
// further ancient PRs and must never be requested at all.
func TestGitHubPullRequestRouteStopsPaginatingAtTheSinceBoundary(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC)
	doer := &gitHubPullRequestDeepHistoryDoer{t: t, ancientPages: 2}
	client := gitHubPullRequestClient(t, doer, "https://api.github.com")
	claim := nativeTestClaim("github", "prs") // since=2026-07-01, before=2026-07-31
	batch, err := (GitHubPullRequestRouteHandler{
		Now: func() time.Time { return now }, MaxPages: 2,
	}).Collect(context.Background(), claim, providerfoundation.Credential{}, client, now)
	if err != nil {
		t.Fatalf("Collect error=%v want nil: a deep-history repo with only "+
			"one page inside the window must not cap", err)
	}
	if batch.Evidence.CapReached {
		t.Fatal("CapReached=true: the early stop did not fire before the cap")
	}
	if batch.Evidence.Records != 1 {
		t.Fatalf("records=%d want 1 (only the in-window PR)", batch.Evidence.Records)
	}
	for _, path := range doer.requests {
		if path == "/repos/acme/api/pulls/2" {
			t.Fatalf("fetched detail for the ancient PR: %s", path)
		}
	}
	if doer.listPagesServed != 1 {
		t.Fatalf("list pages served=%d want 1: pages 2/3 must never be requested "+
			"once the since boundary is crossed on page 1", doer.listPagesServed)
	}
}

// gitHubPullRequestDeepHistoryDoer serves one page holding [in-window PR
// #1, ancient PR #2 whose updated_at is older than nativeTestClaim's
// since], followed by `ancientPages` further pages of nothing but more
// ancient PRs and a "next" Link header -- pages a correct implementation
// must never request.
type gitHubPullRequestDeepHistoryDoer struct {
	t               *testing.T
	ancientPages    int
	listPagesServed int
	requests        []string
}

func (doer *gitHubPullRequestDeepHistoryDoer) Do(request *http.Request) (*http.Response, error) {
	doer.t.Helper()
	path := request.URL.Path
	doer.requests = append(doer.requests, path)
	header := http.Header{"Content-Type": []string{"application/json"}}
	switch {
	case path == "/repos/acme/api":
		return &http.Response{
			StatusCode: http.StatusOK, Header: header,
			Body: io.NopCloser(strings.NewReader(gitHubPullRequestRepoFixture)), Request: request,
		}, nil
	case path == "/repos/acme/api/pulls":
		doer.listPagesServed++
		var body string
		if doer.listPagesServed == 1 {
			body = `[{"number": 1, "updated_at": "2026-07-15T00:00:00Z"},` +
				`{"number": 2, "updated_at": "2020-01-01T00:00:00Z"}]`
			// A "next" link is present -- it must never be followed, since
			// the since boundary is crossed by PR #2 on THIS page.
			header.Set("Link", `<https://api.github.com/repos/acme/api/pulls?page=2>; rel="next"`)
		} else {
			body = `[{"number": 999, "updated_at": "2019-01-01T00:00:00Z"}]`
			if doer.listPagesServed-1 < doer.ancientPages {
				header.Set("Link", `<https://api.github.com/repos/acme/api/pulls?page=`+
					strconv.Itoa(doer.listPagesServed+1)+`>; rel="next"`)
			}
		}
		return &http.Response{
			StatusCode: http.StatusOK, Header: header,
			Body: io.NopCloser(strings.NewReader(body)), Request: request,
		}, nil
	default:
		// /pulls/1 detail (the only PR that should ever be detail-fetched).
		return &http.Response{
			StatusCode: http.StatusOK, Header: header,
			Body:    io.NopCloser(strings.NewReader(gitHubPullRequestDetailFixture42)),
			Request: request,
		}, nil
	}
}

// gitHubPullRequestPagingDoer serves `totalPages` pages of one PR each (all
// with an updated_at inside nativeTestClaim's window) via the Link header,
// plus the repo GET and every /pulls/{number} detail GET.
type gitHubPullRequestPagingDoer struct {
	t          *testing.T
	totalPages int
	page       int
}

func (doer *gitHubPullRequestPagingDoer) Do(request *http.Request) (*http.Response, error) {
	doer.t.Helper()
	path := request.URL.Path
	header := http.Header{"Content-Type": []string{"application/json"}}
	switch {
	case path == "/repos/acme/api":
		return &http.Response{
			StatusCode: http.StatusOK, Header: header,
			Body: io.NopCloser(strings.NewReader(gitHubPullRequestRepoFixture)), Request: request,
		}, nil
	case path == "/repos/acme/api/pulls":
		doer.page++
		number := doer.page
		body := `[{"number": ` + strconv.Itoa(number) +
			`, "updated_at": "2026-07-21T15:30:00Z"}]`
		if doer.page < doer.totalPages {
			header.Set("Link", `<https://api.github.com/repos/acme/api/pulls?page=`+
				strconv.Itoa(doer.page+1)+`>; rel="next"`)
		}
		return &http.Response{
			StatusCode: http.StatusOK, Header: header,
			Body: io.NopCloser(strings.NewReader(body)), Request: request,
		}, nil
	default:
		// Any /pulls/{number} detail request.
		return &http.Response{
			StatusCode: http.StatusOK, Header: header,
			Body:    io.NopCloser(strings.NewReader(gitHubPullRequestDetailFixture42)),
			Request: request,
		}, nil
	}
}

// TestFilterGitHubPullWindowClauseCoverage mutation-tests
// pullOutsideKnownWindow's three clauses independently (codex H3): each
// sub-test isolates exactly one of "is updated_at known", "is it before
// since", "is it after before" so a coverage gap in any single clause
// cannot hide behind the other two already being satisfied in the same
// case -- the failure mode the shared mutation harness's "mutate compound
// predicates clause by clause" rule exists to catch.
func TestFilterGitHubPullWindowClauseCoverage(t *testing.T) {
	t.Parallel()
	since := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	before := time.Date(2026, 7, 31, 23, 59, 59, 0, time.UTC)
	claim := Claim{Unit: Unit{SinceAt: &since, BeforeAt: &before}}

	for name, test := range map[string]struct {
		updatedAt time.Time
		want      bool
	}{
		"unknown updated_at is never excluded (H3's fix)": {
			time.Time{}, false,
		},
		"known, inside the window": {
			time.Date(2026, 7, 15, 0, 0, 0, 0, time.UTC), false,
		},
		"known, exactly at since (inclusive)": {
			since, false,
		},
		"known, exactly at before (inclusive)": {
			before, false,
		},
		"known, before since": {
			time.Date(2026, 6, 30, 0, 0, 0, 0, time.UTC), true,
		},
		"known, after before": {
			time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC), true,
		},
	} {
		test := test
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if got := pullOutsideKnownWindow(test.updatedAt, claim); got != test.want {
				t.Errorf("pullOutsideKnownWindow = %v want %v", got, test.want)
			}
		})
	}

	// Clause isolation: SinceAt-only and BeforeAt-only claims, so the
	// "before" and "after" clauses are each provable with the OTHER bound
	// entirely absent -- a mutation that only fires when both bounds are
	// set (e.g. an accidental `&&` between the two comparisons) cannot hide
	// behind the paired-bound cases above.
	sinceOnly := Claim{Unit: Unit{SinceAt: &since}}
	if pullOutsideKnownWindow(time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC), sinceOnly) != true {
		t.Error("since-only claim: an item before `since` must be excluded")
	}
	if pullOutsideKnownWindow(time.Date(2099, 1, 1, 0, 0, 0, 0, time.UTC), sinceOnly) != false {
		t.Error("since-only claim: an item far in the future must not be excluded")
	}
	beforeOnly := Claim{Unit: Unit{BeforeAt: &before}}
	if pullOutsideKnownWindow(time.Date(2099, 1, 1, 0, 0, 0, 0, time.UTC), beforeOnly) != true {
		t.Error("before-only claim: an item after `before` must be excluded")
	}
	if pullOutsideKnownWindow(time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC), beforeOnly) != false {
		t.Error("before-only claim: an item far in the past must not be excluded")
	}
}

func TestGitHubPullRequestRouteFailsClosedOnScopeAndPayloadFaults(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC)
	handler := GitHubPullRequestRouteHandler{Now: func() time.Time { return now }}
	client := gitHubPullRequestClient(
		t, &gitHubPullRequestDoer{t: t, bodies: defaultGitHubPullRequestFixtures()},
		"https://api.github.com",
	)
	for name, claim := range map[string]Claim{
		"wrong dataset":  nativeTestClaim("github", "repo-metadata"),
		"wrong provider": nativeTestClaim("gitlab", "prs"),
	} {
		if _, err := handler.Collect(
			context.Background(), claim, providerfoundation.Credential{}, client, now,
		); err != ErrInvalidConfiguration {
			t.Errorf("%s error=%v", name, err)
		}
	}

	malformedList := gitHubPullRequestClient(t, &gitHubPullRequestDoer{
		t: t, bodies: map[string]string{
			"/repos/acme/api":       gitHubPullRequestRepoFixture,
			"/repos/acme/api/pulls": `[{"number":"not-a-number"}]`,
		},
	}, "https://api.github.com")
	if _, err := handler.Collect(
		context.Background(), nativeTestClaim("github", "prs"),
		providerfoundation.Credential{}, malformedList, now,
	); err == nil {
		t.Error("malformed PR list item was accepted")
	}

	malformedRepo := gitHubPullRequestClient(t, &gitHubPullRequestDoer{
		t: t, bodies: map[string]string{
			"/repos/acme/api":       `{"id":"not-a-number"}`,
			"/repos/acme/api/pulls": `[]`,
		},
	}, "https://api.github.com")
	if _, err := handler.Collect(
		context.Background(), nativeTestClaim("github", "prs"),
		providerfoundation.Credential{}, malformedRepo, now,
	); err == nil {
		t.Error("malformed repository payload was accepted")
	}

	// codex H4: a repository payload with no full_name MUST fail closed, not
	// fall back to claim.SourceExternalID. Python's get_repo_uuid_from_repo
	// raises on a falsy repo string and models/git.py::Repo.__init__ never
	// even attempts the call, leaving `id` unset; every downstream
	// ClickHouse insert then raises on that None. Falling back here would
	// write PRs under a repo_id nothing else in the system would derive --
	// this test previously blessed exactly that divergence and is now
	// inverted to require the fail-closed behavior.
	noFullName := gitHubPullRequestClient(t, &gitHubPullRequestDoer{
		t: t, bodies: map[string]string{
			"/repos/acme/api":       `{"id":4567}`,
			"/repos/acme/api/pulls": `[]`,
		},
	}, "https://api.github.com")
	if _, err := handler.Collect(
		context.Background(), nativeTestClaim("github", "prs"),
		providerfoundation.Credential{}, noFullName, now,
	); err == nil {
		t.Error("repository payload with no full_name was accepted " +
			"(must fail closed, not guess claim.SourceExternalID)")
	}
}

// TestGitHubPullRequestRoutePreservesEnterpriseBasePath is the GHE
// regression, mirroring TestGitHubRepositoryRoutePreservesEnterpriseBasePath:
// an absolute request path would replace a configured /api/v3 prefix instead
// of extending it.
func TestGitHubPullRequestRoutePreservesEnterpriseBasePath(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC)
	doer := &gitHubPullRequestDoer{t: t, bodies: map[string]string{
		"/api/v3/repos/acme/api":          gitHubPullRequestRepoFixture,
		"/api/v3/repos/acme/api/pulls":    gitHubPullRequestListFixture,
		"/api/v3/repos/acme/api/pulls/42": gitHubPullRequestDetailFixture42,
	}}
	client := gitHubPullRequestClient(t, doer, "https://ghe.acme.test/api/v3")
	batch, err := (GitHubPullRequestRouteHandler{
		Now: func() time.Time { return now },
	}).Collect(
		context.Background(), nativeTestClaim("github", "prs"),
		providerfoundation.Credential{}, client, now,
	)
	if err != nil {
		t.Fatal(err)
	}
	if batch.Evidence.Records != 1 {
		t.Fatalf("records=%d want 1", batch.Evidence.Records)
	}
}

// TestNormalizePRStateMatchesPython is a byte-for-byte port of
// providers/pr_state.py::normalize_pr_state's own semantics -- every branch
// pinned by an independent run of the Python function.
//
//	>>> from dev_health_ops.providers.pr_state import normalize_pr_state
//	>>> from datetime import datetime, timezone
//	>>> mergedAt = datetime(2026, 1, 1, tzinfo=timezone.utc)
//	>>> [normalize_pr_state(s, m) for s, m in [
//	...     ("open", None), ("opened", None), ("OPEN", None),
//	...     ("closed", None), ("closed", mergedAt), ("merged", None),
//	...     ("MERGED", None), ("", None), (None, None), ("unknown", None),
//	... ]]
//	['open', 'open', 'open', 'closed', 'merged', 'merged', 'merged', 'open', 'open', 'open']
func TestNormalizePRStateMatchesPython(t *testing.T) {
	t.Parallel()
	mergedAt := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for name, test := range map[string]struct {
		raw      string
		mergedAt *time.Time
		want     string
	}{
		"open":                  {"open", nil, "open"},
		"opened normalizes":     {"opened", nil, "open"},
		"case insensitive":      {"OPEN", nil, "open"},
		"closed unmerged":       {"closed", nil, "closed"},
		"closed merged":         {"closed", &mergedAt, "merged"},
		"merged literal":        {"merged", nil, "merged"},
		"merged case":           {"MERGED", nil, "merged"},
		"empty defaults open":   {"", nil, "open"},
		"unknown defaults open": {"unknown", nil, "open"},
		"whitespace tolerant":   {"  closed  ", &mergedAt, "merged"},
	} {
		test := test
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if got := normalizePRState(test.raw, test.mergedAt); got != test.want {
				t.Errorf("normalizePRState(%q) = %q want %q", test.raw, got, test.want)
			}
		})
	}
}

// TestGitHubPullRequestCoerceCreatedAtFallsBackLikePython mirrors
// BaseGitProcessor.coerce_created_at: created_at or merged_at or closed_at or
// now().
func TestGitHubPullRequestCoerceCreatedAtFallsBackLikePython(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC)
	claim := nativeTestClaim("github", "prs")

	// No created_at, no merged_at: falls back to closed_at.
	closedAt := "2026-07-15T00:00:00Z"
	row, err := normalizeGitHubPullRequest(claim, "c7198fbc-1945-3717-05d8-eb78866b4e79",
		gitHubPullDetailPayload{Number: 7, State: strPtr("closed"), ClosedAt: &closedAt},
		now,
	)
	if err != nil {
		t.Fatal(err)
	}
	want := time.Date(2026, 7, 15, 0, 0, 0, 0, time.UTC)
	if !row.CreatedAt.Equal(want) {
		t.Fatalf("created_at=%v want %v (fallback to closed_at)", row.CreatedAt, want)
	}

	// No created_at, no merged_at, no closed_at: falls back to normalizedAt.
	row, err = normalizeGitHubPullRequest(claim, "c7198fbc-1945-3717-05d8-eb78866b4e79",
		gitHubPullDetailPayload{Number: 8, State: strPtr("open")}, now,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !row.CreatedAt.Equal(now) {
		t.Fatalf("created_at=%v want normalizedAt=%v", row.CreatedAt, now)
	}
}

func strPtr(value string) *string { return &value }

// TestParseGitHubPullTimeTruncatesToMilliseconds is codex M5: ClickHouse
// persists DateTime64(3), so a provider timestamp with sub-millisecond
// precision must be truncated at construction time or a later readback
// (which only ever sees millisecond precision) would compare unequal to the
// in-memory value and report a false conflict.
func TestParseGitHubPullTimeTruncatesToMilliseconds(t *testing.T) {
	t.Parallel()
	value := "2026-07-10T09:00:00.123456789Z"
	got := parseGitHubPullTime(&value)
	if got == nil {
		t.Fatal("parseGitHubPullTime returned nil")
	}
	want := time.Date(2026, 7, 10, 9, 0, 0, 123_000_000, time.UTC)
	if !got.Equal(want) {
		t.Fatalf("parseGitHubPullTime(%q) = %v want %v (truncated to ms)", value, got, want)
	}
	if got.Nanosecond()%int(time.Millisecond) != 0 {
		t.Fatalf("parseGitHubPullTime(%q) retained sub-millisecond precision: %v", value, got)
	}
}

// TestGitHubPullRequestRouteTruncatesNormalizedAtToMilliseconds is codex M5's
// follow-up: `last_synced` (and created_at's now() fallback) come from
// Collect's OWN `normalizedAt = normalizedAt.UTC().Truncate(time.Millisecond)`
// line -- a SEPARATE truncation point from parseGitHubPullTime's, which only
// covers provider-supplied created_at/merged_at/closed_at. Every other test
// in this file constructs `now` with zero sub-second precision
// (`time.Date(..., 0, time.UTC)`), so removing Collect's own truncate call
// would be invisible to them: truncating an already-round value is a no-op.
// This test exists specifically so that gap has a fixture -- codex flagged
// that the mutation plan would have let a deleted truncation survive for
// exactly this reason.
func TestGitHubPullRequestRouteTruncatesNormalizedAtToMilliseconds(t *testing.T) {
	t.Parallel()
	subMillisecondNow := time.Date(2026, 7, 23, 12, 30, 0, 123_456_789, time.UTC)
	doer := &gitHubPullRequestDoer{t: t, bodies: defaultGitHubPullRequestFixtures()}
	client := gitHubPullRequestClient(t, doer, "https://api.github.com")
	batch, err := (GitHubPullRequestRouteHandler{
		Now: func() time.Time { return subMillisecondNow },
	}).Collect(
		context.Background(), nativeTestClaim("github", "prs"),
		providerfoundation.Credential{}, client, subMillisecondNow,
	)
	if err != nil {
		t.Fatal(err)
	}
	var row pullRequestRow
	if err := json.Unmarshal(batch.Effects[0].Rows[0], &row); err != nil {
		t.Fatal(err)
	}
	wantLastSynced := time.Date(2026, 7, 23, 12, 30, 0, 123_000_000, time.UTC)
	if !row.LastSynced.Equal(wantLastSynced) {
		t.Fatalf("last_synced=%v want %v (truncated to ms)", row.LastSynced, wantLastSynced)
	}
	if row.LastSynced.Nanosecond()%int(time.Millisecond) != 0 {
		t.Fatalf("last_synced retained sub-millisecond precision: %v", row.LastSynced)
	}
}

// TestGitHubPullUserLoginStringifiesNonStringLogins is codex M8 (and M4's
// follow-up on it): Python's _pull_from_item does `str(user["login"])` for
// ANY non-null login value, not only a numeric one. codex M4 found that an
// earlier version of this test claimed "ANY non-null" while covering only
// an integer -- stringValue's own switch handled numbers but fell through
// to "" for a boolean, silently different from Python's str(True)=="True".
// This table now genuinely covers every JSON scalar type a login could
// plausibly be: string, number, and boolean. list/dict/null are
// deliberately NOT claimed here -- see stringValue's own doc comment for
// why those are out of scope rather than silently unhandled.
func TestGitHubPullUserLoginStringifiesNonStringLogins(t *testing.T) {
	t.Parallel()
	for name, test := range map[string]struct {
		raw  string
		want string
	}{
		"string login":          {`{"login":"octocat"}`, "octocat"},
		"numeric login":         {`{"login":12345}`, "12345"},
		"boolean login (true)":  {`{"login":true}`, "True"},
		"boolean login (false)": {`{"login":false}`, "False"},
		"null login":            {`{"login":null}`, ""},
		"absent login":          {`{}`, ""},
		"user is JSON null":     {`null`, ""},
	} {
		test := test
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if got := gitHubPullUserLogin([]byte(test.raw)); got != test.want {
				t.Errorf("gitHubPullUserLogin(%s) = %q want %q", test.raw, got, test.want)
			}
		})
	}
}
