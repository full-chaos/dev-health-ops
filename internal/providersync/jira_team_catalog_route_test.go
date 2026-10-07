package providersync

import (
	"context"
	"errors"
	"fmt"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// jiraTeamCatalogFixtureDoer routes by exact path+query (jira's board/sprint
// walk issues several distinct GETs to the SAME path root -- e.g. every
// project's /rest/agile/1.0/board listing -- so path-only routing, as
// githubTeamCatalogFixtureDoer uses, is not precise enough here).
type jiraTeamCatalogFixtureDoer struct {
	t        *testing.T
	byURI    map[string]jiraTeamCatalogFixtureResponse
	requests []string
}

type jiraTeamCatalogFixtureResponse struct {
	status int
	body   string
}

func (doer *jiraTeamCatalogFixtureDoer) Do(request *http.Request) (*http.Response, error) {
	doer.t.Helper()
	uri := request.URL.RequestURI()
	doer.requests = append(doer.requests, uri)
	fixture, ok := doer.byURI[uri]
	if !ok && uri == jiraTeamCatalogArchivedProjectSearchURI {
		// A site with no archived project, unless the test says otherwise.
		fixture, ok = jiraTeamCatalogFixtureResponse{body: `{"values":[],"isLast":true}`}, true
	}
	if !ok {
		doer.t.Fatalf("unexpected request %q", uri)
	}
	status := fixture.status
	if status == 0 {
		status = http.StatusOK
	}
	return &http.Response{
		StatusCode: status,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(fixture.body)),
		Request:    request,
	}, nil
}

func jiraTeamCatalogTestClient(t *testing.T, doer providerfoundation.HTTPDoer) *providerfoundation.HTTPClient {
	t.Helper()
	client, err := providerfoundation.NewHTTPClient(
		"jira", "https://jira.example.com", fakehttp.Client(doer),
		func(*http.Request) error { return nil },
		providerfoundation.RetryPolicy{MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond},
		providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
	)
	if err != nil {
		t.Fatal(err)
	}
	return client
}

const jiraTeamCatalogProjectSearchURI = "/rest/api/3/project/search?maxResults=100"

// jiraTeamCatalogArchivedProjectSearchURI is the second read of every walk:
// the archived projects.
const jiraTeamCatalogArchivedProjectSearchURI = "/rest/api/3/project/search?maxResults=100&status=archived"

// TestJiraTeamCatalogCollectCountsFailedAndRetriedAttempts verifies that
// JiraTeamCatalogEvidence.Requests has a single source -- the counting Doer
// at the HTTP boundary -- across two distinct paths in one
// CollectTeamCatalog call: the project search fails its first wire attempt
// and succeeds on retry, and the boards listing fails every attempt
// through to the retry policy's exhaustion (a best-effort failure under
// Strict=false, which skips just the sprint walk rather than aborting the
// whole collection). A double count or a dropped count makes the exact
// assertion below go red.
func TestJiraTeamCatalogCollectCountsFailedAndRetriedAttempts(t *testing.T) {
	t.Parallel()
	searchAttempts := 0
	boardsAttempts := 0
	doer := jiraWorkItemsDoerFunc(func(request *http.Request) (*http.Response, error) {
		uri := request.URL.RequestURI()
		switch uri {
		case jiraTeamCatalogProjectSearchURI:
			searchAttempts++
			if searchAttempts == 1 {
				return nil, errors.New("simulated transient transport failure")
			}
			body := `{"values":[{"id":"10001","key":"OPS","name":"Ops Project"}]}`
			return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
		case jiraTeamCatalogArchivedProjectSearchURI:
			body := `{"values":[],"isLast":true}`
			return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
		case "/rest/api/3/project/OPS":
			body := `{"projectTypeKey":"software"}`
			return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
		case "/rest/agile/1.0/board?maxResults=100&projectKeyOrId=OPS&startAt=0":
			boardsAttempts++
			return nil, errors.New("simulated transient transport failure")
		default:
			t.Fatalf("unexpected request %q", uri)
			return nil, nil
		}
	})
	client, err := providerfoundation.NewHTTPClient(
		"jira", "https://jira.example.com", fakehttp.Client(doer),
		func(*http.Request) error { return nil },
		providerfoundation.RetryPolicy{
			MaxAttempts: 2, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond,
		},
		providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
	)
	if err != nil {
		t.Fatal(err)
	}
	handler := JiraTeamCatalogRouteHandler{}
	credential := providerfoundation.Credential{Provider: "jira"}
	batch, err := handler.CollectTeamCatalog(
		context.Background(),
		TeamCatalogReference{OrgID: "org-1", SyncRunID: "run-1", Strict: false},
		credential, client,
		TeamCatalogSelections{Teams: true},
		time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatalf("a board-listing failure must soft-skip the sprint walk under non-strict: %v", err)
	}
	if searchAttempts != 2 {
		t.Fatalf("search attempts=%d want 2 (one failed, one retried)", searchAttempts)
	}
	if boardsAttempts != 2 {
		t.Fatalf("boards attempts=%d want 2 (both exhausted by the retry policy)", boardsAttempts)
	}
	if len(batch.Rows.Sprints) != 0 {
		t.Fatalf("sprints=%+v want none (the board listing never recovered)", batch.Rows.Sprints)
	}
	// 2 (search, retried) + 1 (archived search) + 1 (project detail) +
	// 2 (boards, exhausted) = 6.
	if batch.Evidence.Requests != 6 {
		t.Fatalf("evidence=%+v want Requests=6 (every physical attempt, from one source)", batch.Evidence)
	}
}

// TestJiraTeamCatalogCollectSkipsOneBoardsSprint400UnderStrict ports
// team_autoimport_jira's test_jira_populate_skips_one_boards_sprint_400_
// under_strict_reference_discovery: board 81 answers the documented
// "no sprint support" 400, board 82 returns a sprint normally -- the whole
// walk must still succeed, strict included, with the healthy board's
// sprint landing and the failing board simply absent.
func TestJiraTeamCatalogCollectSkipsOneBoardsSprint400UnderStrict(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	doer := &jiraTeamCatalogFixtureDoer{t: t, byURI: map[string]jiraTeamCatalogFixtureResponse{
		jiraTeamCatalogProjectSearchURI: {body: `{"values":[{"id":"10001","key":"OPS","name":"Ops Project"}]}`},
		"/rest/api/3/project/OPS":       {body: `{"projectTypeKey":"software"}`},
		"/rest/agile/1.0/board?maxResults=100&projectKeyOrId=OPS&startAt=0": {
			body: `{"values":[{"id":81},{"id":82}],"isLast":true}`,
		},
		"/rest/agile/1.0/board/81/sprint?maxResults=100&startAt=0": {
			status: http.StatusBadRequest,
			body:   `{"errorMessages":["The board does not support sprints"]}`,
		},
		"/rest/agile/1.0/board/82/sprint?maxResults=100&startAt=0": {
			body: `{"values":[{"id":501,"name":"Sprint 1","state":"active"}],"isLast":true}`,
		},
	}}
	handler := JiraTeamCatalogRouteHandler{}
	client := jiraTeamCatalogTestClient(t, fakehttp.Client(doer))
	credential := providerfoundation.Credential{Provider: "jira"}

	batch, err := handler.CollectTeamCatalog(
		context.Background(),
		TeamCatalogReference{OrgID: "org-1", SyncRunID: "run-1", Strict: true},
		credential, client,
		TeamCatalogSelections{Teams: true},
		now,
	)
	if err != nil {
		t.Fatalf("a skippable per-board 400 must not fail the whole walk, even under strict: %v", err)
	}
	if batch.Result.WalkSkipped {
		t.Fatalf("a healthy project must not report WalkSkipped: %+v", batch.Result)
	}
	if len(batch.Rows.Teams) != 1 || batch.Rows.Teams[0].ID != "OPS" {
		t.Fatalf("teams=%+v", batch.Rows.Teams)
	}
	if len(batch.Rows.Sprints) != 1 || batch.Rows.Sprints[0].SprintID != "501" {
		t.Fatalf("want exactly sprint 501 (board 81's benign 400 skipped), got %+v", batch.Rows.Sprints)
	}
}

// TestJiraTeamCatalogCollectResolvesSprintsWhenNothingSelectedUnderStrict
// ports test_jira_strict_reference_discovery_still_resolves_sprints_when_
// all_categories_off: sprint/cycle reference discovery is unconditional
// under strict, even with every category selection off.
func TestJiraTeamCatalogCollectResolvesSprintsWhenNothingSelectedUnderStrict(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	doer := &jiraTeamCatalogFixtureDoer{t: t, byURI: map[string]jiraTeamCatalogFixtureResponse{
		jiraTeamCatalogProjectSearchURI: {body: `{"values":[{"id":"10001","key":"OPS","name":"Ops Project"}]}`},
		"/rest/api/3/project/OPS":       {body: `{"projectTypeKey":"software"}`},
		"/rest/agile/1.0/board?maxResults=100&projectKeyOrId=OPS&startAt=0": {
			body: `{"values":[{"id":82}],"isLast":true}`,
		},
		"/rest/agile/1.0/board/82/sprint?maxResults=100&startAt=0": {
			body: `{"values":[{"id":501,"name":"Sprint 1","state":"active"}],"isLast":true}`,
		},
	}}
	handler := JiraTeamCatalogRouteHandler{}
	client := jiraTeamCatalogTestClient(t, fakehttp.Client(doer))
	credential := providerfoundation.Credential{Provider: "jira"}

	batch, err := handler.CollectTeamCatalog(
		context.Background(),
		TeamCatalogReference{OrgID: "org-1", SyncRunID: "run-1", Strict: true},
		credential, client,
		TeamCatalogSelections{}, // every category off
		now,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(batch.Rows.Sprints) != 1 || batch.Rows.Sprints[0].SprintID != "501" {
		t.Fatalf("sprints must still resolve with nothing selected under strict, got %+v", batch.Rows.Sprints)
	}
	if batch.Result.TeamsImported != 1 {
		// The walk still discovers the team row internally (it is
		// unconditional, like project search itself), but nothing writable
		// is selected -- the collector layer, not the Handler, is what
		// gates the actual write. Pinning TeamsImported here documents
		// that the Handler's own row-building is selection-agnostic.
		t.Fatalf("teams imported = %d, want 1 (row-building itself is unconditional)", batch.Result.TeamsImported)
	}
}

// TestJiraTeamCatalogCollectReraisesA403SprintListingFailureUnderStrict
// ports test_jira_populate_reraises_a_403_sprint_listing_failure_under_
// strict_reference_discovery: only the documented 400+errorMessages shape
// is a per-board skip. A 403 (revoked/insufficiently-scoped credentials)
// must fail the whole call under strict.
func TestJiraTeamCatalogCollectReraisesA403SprintListingFailureUnderStrict(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	doer := &jiraTeamCatalogFixtureDoer{t: t, byURI: map[string]jiraTeamCatalogFixtureResponse{
		jiraTeamCatalogProjectSearchURI: {body: `{"values":[{"id":"10001","key":"OPS","name":"Ops Project"}]}`},
		"/rest/api/3/project/OPS":       {body: `{"projectTypeKey":"software"}`},
		"/rest/agile/1.0/board?maxResults=100&projectKeyOrId=OPS&startAt=0": {
			body: `{"values":[{"id":81}],"isLast":true}`,
		},
		"/rest/agile/1.0/board/81/sprint?maxResults=100&startAt=0": {
			status: http.StatusForbidden,
			body:   `{"errorMessages":["Forbidden"]}`,
		},
	}}
	handler := JiraTeamCatalogRouteHandler{}
	client := jiraTeamCatalogTestClient(t, fakehttp.Client(doer))
	credential := providerfoundation.Credential{Provider: "jira"}

	_, err := handler.CollectTeamCatalog(
		context.Background(),
		TeamCatalogReference{OrgID: "org-1", SyncRunID: "run-1", Strict: true},
		credential, client,
		TeamCatalogSelections{Teams: true},
		now,
	)
	if err == nil {
		t.Fatal("a 403 sprint-listing failure must propagate under strict, not be silently absorbed")
	}
}

// TestJiraTeamCatalogCollectReraisesABoardListing400UnderStrict ports
// test_jira_populate_reraises_a_board_listing_400_under_strict_reference_
// discovery: board LISTING has no documented benign 400 shape, unlike
// sprint listing -- even the same errorMessages envelope must propagate.
func TestJiraTeamCatalogCollectReraisesABoardListing400UnderStrict(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	doer := &jiraTeamCatalogFixtureDoer{t: t, byURI: map[string]jiraTeamCatalogFixtureResponse{
		jiraTeamCatalogProjectSearchURI: {body: `{"values":[{"id":"10001","key":"OPS","name":"Ops Project"}]}`},
		"/rest/api/3/project/OPS":       {body: `{"projectTypeKey":"software"}`},
		"/rest/agile/1.0/board?maxResults=100&projectKeyOrId=OPS&startAt=0": {
			status: http.StatusBadRequest,
			body:   `{"errorMessages":["The project key or id 'OPS' does not exist"]}`,
		},
	}}
	handler := JiraTeamCatalogRouteHandler{}
	client := jiraTeamCatalogTestClient(t, fakehttp.Client(doer))
	credential := providerfoundation.Credential{Provider: "jira"}

	_, err := handler.CollectTeamCatalog(
		context.Background(),
		TeamCatalogReference{OrgID: "org-1", SyncRunID: "run-1", Strict: true},
		credential, client,
		TeamCatalogSelections{Teams: true},
		now,
	)
	if err == nil {
		t.Fatal("a board-listing 400 must propagate under strict, never be treated as a per-project skip")
	}
}

// TestJiraTeamCatalogCollectSkipsBoardDiscoveryForNonSoftwareProjectUnderStrict
// A project the search returns with no native id (or with a value in the
// retired key-built form) keeps its team row and gets NO project row and NO
// ownership row: an id built from the key would name a project no work item
// points to. The skip is counted in the result.
func TestJiraTeamCatalogCollectWritesNoProjectIdentityWithoutANativeID(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	doer := &jiraTeamCatalogFixtureDoer{t: t, byURI: map[string]jiraTeamCatalogFixtureResponse{
		jiraTeamCatalogProjectSearchURI: {
			body: `{"values":[{"key":"NOID","name":"No id"},{"id":"  ","key":"BLANK","name":"Blank id"},` +
				`{"id":"org-1:jira:OLD","key":"OLD","name":"Key-built id"},{"id":"10001","key":"OPS","name":"Ops Project"}]}`,
		},
		"/rest/api/3/project/NOID":  {body: `{"projectTypeKey":"business"}`},
		"/rest/api/3/project/BLANK": {body: `{"projectTypeKey":"business"}`},
		"/rest/api/3/project/OLD":   {body: `{"projectTypeKey":"business"}`},
		"/rest/api/3/project/OPS":   {body: `{"projectTypeKey":"business"}`},
	}}
	batch, err := JiraTeamCatalogRouteHandler{}.CollectTeamCatalog(
		context.Background(),
		TeamCatalogReference{OrgID: "org-1", SyncRunID: "run-1", Strict: true},
		providerfoundation.Credential{Provider: "jira"}, jiraTeamCatalogTestClient(t, fakehttp.Client(doer)),
		TeamCatalogSelections{Teams: true, Projects: true},
		now,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(batch.Rows.Teams) != 4 {
		t.Fatalf("teams=%+v, want all 4 projects as teams", batch.Rows.Teams)
	}
	if len(batch.Rows.Ownership) != 1 || batch.Rows.Ownership[0].ProjectID != "10001" || batch.Rows.Ownership[0].TeamID != "OPS" {
		t.Fatalf("ownership=%+v, want only OPS on its native id", batch.Rows.Ownership)
	}
	if len(batch.Rows.Projects) != 1 || batch.Rows.Projects[0].ID != "10001" {
		t.Fatalf("projects=%+v, want only OPS on its native id", batch.Rows.Projects)
	}
	if batch.Result.ProjectsSkippedNoNativeID != 3 {
		t.Fatalf("skipped=%d, want 3", batch.Result.ProjectsSkippedNoNativeID)
	}
}

// jiraProjectSearchPage is one page of the project search: `count` projects
// numbered from `first`, with the end-of-data fields given.
func jiraProjectSearchPage(first, count int, tail string) string {
	entries := make([]string, 0, count)
	for index := first; index < first+count; index++ {
		entries = append(entries, fmt.Sprintf(`{"id":"%d","key":"P%d","name":"Project %d"}`, 10000+index, index, index))
	}
	return `{"values":[` + strings.Join(entries, ",") + `]` + tail + `}`
}

func collectJiraProjectSearch(t *testing.T, byURI map[string]jiraTeamCatalogFixtureResponse) (JiraTeamCatalogBatch, *jiraTeamCatalogFixtureDoer) {
	t.Helper()
	doer := &jiraTeamCatalogFixtureDoer{t: t, byURI: byURI}
	// Projects only and not strict: no member lookup; the sprint walk has no
	// fixture and is skipped, which is not what these tests are about.
	client := jiraTeamCatalogTestClient(t, fakehttp.Client(jiraProjectSearchOnlyDoer{doer}))
	batch, err := JiraTeamCatalogRouteHandler{}.CollectTeamCatalog(context.Background(),
		TeamCatalogReference{OrgID: "org-1", SyncRunID: "run-1"}, providerfoundation.Credential{Provider: "jira"}, client,
		TeamCatalogSelections{Projects: true}, time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	return batch, doer
}

// jiraProjectSearchOnlyDoer answers the project search from the fixtures and
// every other request with 404.
type jiraProjectSearchOnlyDoer struct{ search *jiraTeamCatalogFixtureDoer }

func (doer jiraProjectSearchOnlyDoer) Do(request *http.Request) (*http.Response, error) {
	if strings.HasPrefix(request.URL.Path, "/rest/api/3/project/search") {
		return doer.search.Do(request)
	}
	return &http.Response{StatusCode: http.StatusNotFound, Header: http.Header{"Content-Type": []string{"application/json"}},
		Body: io.NopCloser(strings.NewReader(`{}`)), Request: request}, nil
}

const jiraTeamCatalogProjectSearchPage2URI = "/rest/api/3/project/search?maxResults=100&startAt=100"

// The project search is read to the provider's end-of-data signal. A first
// page that is full and not the last one is a PART of the projects: the walk
// reads the next page, and only the page that says it is the last makes the
// search complete.
func TestJiraTeamCatalogCollectReadsEveryPageOfTheProjectSearch(t *testing.T) {
	t.Parallel()
	batch, doer := collectJiraProjectSearch(t, map[string]jiraTeamCatalogFixtureResponse{
		jiraTeamCatalogProjectSearchURI:      {body: jiraProjectSearchPage(0, 100, `,"isLast":false,"total":150`)},
		jiraTeamCatalogProjectSearchPage2URI: {body: jiraProjectSearchPage(100, 50, `,"isLast":true,"total":150`)},
	})
	if len(batch.Rows.Ownership) != 150 || len(batch.Rows.Projects) != 150 || len(batch.Rows.Teams) != 150 {
		t.Fatalf("teams=%d ownership=%d projects=%d, want 150 each (both pages)", len(batch.Rows.Teams), len(batch.Rows.Ownership), len(batch.Rows.Projects))
	}
	if !batch.Result.ProjectSearchComplete || batch.Result.ProjectSearchPages != 2 {
		t.Fatalf("result=%+v, want the search complete after 2 pages", batch.Result)
	}
	if len(doer.requests) != 3 || doer.requests[2] != jiraTeamCatalogArchivedProjectSearchURI {
		t.Fatalf("search requests=%v, want the two pages, then the archived read", doer.requests)
	}
}

// The end-of-data signal decides, one case per form of it.
func TestJiraTeamCatalogProjectSearchIsCompleteOnlyOnAnEndOfDataSignal(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		pages               map[string]jiraTeamCatalogFixtureResponse
		complete            bool
		pagesRead, projects int
	}{
		"a later page fails": {map[string]jiraTeamCatalogFixtureResponse{
			jiraTeamCatalogProjectSearchURI:      {body: jiraProjectSearchPage(0, 100, `,"isLast":false,"total":150`)},
			jiraTeamCatalogProjectSearchPage2URI: {status: http.StatusForbidden, body: `{}`},
		}, false, 1, 100},
		"a full page with no signal is not the end; an empty page stops the walk": {map[string]jiraTeamCatalogFixtureResponse{
			jiraTeamCatalogProjectSearchURI:      {body: jiraProjectSearchPage(0, 100, ``)},
			jiraTeamCatalogProjectSearchPage2URI: {body: `{"values":[],"isLast":false}`},
		}, false, 2, 100},
		"isLast false on a short page is not the end": {map[string]jiraTeamCatalogFixtureResponse{
			jiraTeamCatalogProjectSearchURI:                       {body: jiraProjectSearchPage(0, 2, `,"isLast":false`)},
			"/rest/api/3/project/search?maxResults=100&startAt=2": {status: http.StatusForbidden, body: `{}`},
		}, false, 1, 2},
		"total not reached is not the end": {map[string]jiraTeamCatalogFixtureResponse{
			jiraTeamCatalogProjectSearchURI:                       {body: jiraProjectSearchPage(0, 2, `,"total":3`)},
			"/rest/api/3/project/search?maxResults=100&startAt=2": {status: http.StatusForbidden, body: `{}`},
		}, false, 1, 2},
		"total reached is the end": {map[string]jiraTeamCatalogFixtureResponse{
			jiraTeamCatalogProjectSearchURI: {body: jiraProjectSearchPage(0, 2, `,"total":2`)},
		}, true, 1, 2},
		"a short page with no signal is the end": {map[string]jiraTeamCatalogFixtureResponse{
			jiraTeamCatalogProjectSearchURI: {body: jiraProjectSearchPage(0, 2, ``)},
		}, true, 1, 2},
		"an empty object as the first page is not the end": {map[string]jiraTeamCatalogFixtureResponse{
			jiraTeamCatalogProjectSearchURI: {body: `{}`},
		}, false, 1, 0},
		"an error-shaped body under HTTP 200 is not the end": {map[string]jiraTeamCatalogFixtureResponse{
			jiraTeamCatalogProjectSearchURI: {body: `{"errorMessages":["no"],"errors":{}}`},
		}, false, 1, 0},
		"an empty object after a full page is not the end": {map[string]jiraTeamCatalogFixtureResponse{
			jiraTeamCatalogProjectSearchURI:      {body: jiraProjectSearchPage(0, 100, ``)},
			jiraTeamCatalogProjectSearchPage2URI: {body: `{}`},
		}, false, 2, 100},
		"no entries with total zero is the end": {map[string]jiraTeamCatalogFixtureResponse{
			jiraTeamCatalogProjectSearchURI: {body: `{"values":[],"total":0}`},
		}, true, 1, 0},
		"isLast true on a full page is the end": {map[string]jiraTeamCatalogFixtureResponse{
			jiraTeamCatalogProjectSearchURI: {body: jiraProjectSearchPage(0, 100, `,"isLast":true`)},
		}, true, 1, 100},
	} {
		batch, _ := collectJiraProjectSearch(t, tc.pages)
		if batch.Result.ProjectSearchComplete != tc.complete || batch.Result.ProjectSearchPages != tc.pagesRead || len(batch.Rows.Projects) != tc.projects {
			t.Errorf("%s: complete=%v pages=%d projects=%d, want %v, %d, %d", name,
				batch.Result.ProjectSearchComplete, batch.Result.ProjectSearchPages, len(batch.Rows.Projects), tc.complete, tc.pagesRead, tc.projects)
		}
	}
}

// The walk is bounded. A provider that never says "last" stops the walk at
// the bound with what was read, and the search is not complete.
func TestJiraTeamCatalogProjectSearchStopsAtThePageBoundAndIsNotComplete(t *testing.T) {
	t.Parallel()
	pages := map[string]jiraTeamCatalogFixtureResponse{}
	for page := 0; page < jiraTeamCatalogProjectSearchMaxPages; page++ {
		uri := jiraTeamCatalogProjectSearchURI
		if page > 0 {
			uri += "&startAt=" + strconv.Itoa(page*2)
		}
		pages[uri] = jiraTeamCatalogFixtureResponse{body: jiraProjectSearchPage(page*2, 2, `,"isLast":false`)}
	}
	// No fixture for the page after the bound: a request for it fails the
	// test. The one request more is the archived read.
	batch, doer := collectJiraProjectSearch(t, pages)
	if batch.Result.ProjectSearchComplete || batch.Result.ProjectSearchPages != jiraTeamCatalogProjectSearchMaxPages ||
		len(batch.Rows.Projects) != 2*jiraTeamCatalogProjectSearchMaxPages || len(doer.requests) != jiraTeamCatalogProjectSearchMaxPages+1 {
		t.Fatalf("complete=%v pages=%d projects=%d requests=%d, want not complete at the bound of %d pages",
			batch.Result.ProjectSearchComplete, batch.Result.ProjectSearchPages, len(batch.Rows.Projects), len(doer.requests), jiraTeamCatalogProjectSearchMaxPages)
	}
}

// The project search returns live projects only. The walk reads the archived
// projects with a second search (status=archived), every page of it, and
// gives them ownership rows to HOLD only: no team, project or member row, and
// no row among the fresh ownership rows.
func TestJiraTeamCatalogCollectReadsArchivedProjectsToHoldOwnershipOnly(t *testing.T) {
	t.Parallel()
	const archivedPage2 = "/rest/api/3/project/search?maxResults=100&startAt=100&status=archived"
	live := map[string]jiraTeamCatalogFixtureResponse{
		jiraTeamCatalogProjectSearchURI: {body: jiraProjectSearchPage(0, 1, `,"isLast":true`)},
	}
	with := func(extra map[string]jiraTeamCatalogFixtureResponse) map[string]jiraTeamCatalogFixtureResponse {
		pages := map[string]jiraTeamCatalogFixtureResponse{}
		for uri, response := range live {
			pages[uri] = response
		}
		for uri, response := range extra {
			pages[uri] = response
		}
		return pages
	}
	for name, tc := range map[string]struct {
		pages    map[string]jiraTeamCatalogFixtureResponse
		complete bool
		archived []string
		requests int
	}{
		"one archived project": {with(map[string]jiraTeamCatalogFixtureResponse{
			jiraTeamCatalogArchivedProjectSearchURI: {body: `{"values":[{"id":"20001","key":" OLD ","name":"Old"}],"isLast":true}`},
		}), true, []string{"OLD/20001"}, 2},
		"every page of the archived projects": {with(map[string]jiraTeamCatalogFixtureResponse{
			jiraTeamCatalogArchivedProjectSearchURI: {body: jiraProjectSearchPage(500, 100, `,"isLast":false`)},
			archivedPage2:                           {body: `{"values":[{"id":"20001","key":"OLD","name":"Old"}],"isLast":true}`},
		}), true, nil, 3},
		"the archived read fails: the walk goes on, not complete": {with(map[string]jiraTeamCatalogFixtureResponse{
			jiraTeamCatalogArchivedProjectSearchURI: {status: http.StatusBadRequest, body: `{}`},
		}), false, []string{}, 2},
		"a later archived page fails: not complete": {with(map[string]jiraTeamCatalogFixtureResponse{
			jiraTeamCatalogArchivedProjectSearchURI: {body: jiraProjectSearchPage(500, 100, `,"isLast":false`)},
			archivedPage2:                           {status: http.StatusForbidden, body: `{}`},
		}), false, nil, 3},
		"the archived read answers an empty object: not complete": {with(map[string]jiraTeamCatalogFixtureResponse{
			jiraTeamCatalogArchivedProjectSearchURI: {body: `{}`},
		}), false, []string{}, 2},
		"an archived project with no id or a key-built id is held by nothing": {with(map[string]jiraTeamCatalogFixtureResponse{
			jiraTeamCatalogArchivedProjectSearchURI: {body: `{"values":[{"id":"","key":"A","name":"A"},{"id":"20002","key":" ","name":"B"},{"id":"org-1:jira:C","key":"C","name":"C"},` +
				`{"id":"20001","key":"OLD","name":"Old"},{"id":"20001","key":"OLD","name":"Old"}],"isLast":true}`},
		}), true, []string{"OLD/20001"}, 2},
	} {
		batch, doer := collectJiraProjectSearch(t, tc.pages)
		if len(batch.Rows.Teams) != 1 || len(batch.Rows.Projects) != 1 || len(batch.Rows.Ownership) != 1 || len(batch.Rows.Memberships) != 0 {
			t.Errorf("%s: teams=%d projects=%d ownership=%d memberships=%d, want the one live project only", name,
				len(batch.Rows.Teams), len(batch.Rows.Projects), len(batch.Rows.Ownership), len(batch.Rows.Memberships))
		}
		if batch.Result.ProjectSearchComplete != tc.complete || batch.Result.ProjectSearchPages != 1 || len(doer.requests) != tc.requests {
			t.Errorf("%s: complete=%v live pages=%d requests=%v, want complete=%v, 1 live page, %d requests", name,
				batch.Result.ProjectSearchComplete, batch.Result.ProjectSearchPages, doer.requests, tc.complete, tc.requests)
		}
		if tc.archived == nil {
			if len(batch.ArchivedProjects) < 100 {
				t.Errorf("%s: %d archived projects, want the first page's 100 at least", name, len(batch.ArchivedProjects))
			}
			if tc.complete && len(batch.ArchivedProjects) != 101 {
				t.Errorf("%s: %d archived projects, want both pages (101)", name, len(batch.ArchivedProjects))
			}
			continue
		}
		got := []string{}
		for _, project := range batch.ArchivedProjects {
			got = append(got, project.Key+"/"+project.ID)
		}
		if !slices.Equal(got, tc.archived) {
			t.Errorf("%s: archived projects = %v, want %v", name, got, tc.archived)
		}
	}
}

// An open row is held when its project is an archived project, named by the
// native id or by the id built from the project key. Team and source do not
// decide: every open row of this writer for that project is held. A held row
// is taken out of what the snapshot rule may close.
func TestJiraHoldArchivedOwnershipHoldsBothIDFormsOfAnArchivedProject(t *testing.T) {
	t.Parallel()
	row := func(team, project, source string) jiraTeamCatalogOwnershipRow {
		return jiraTeamCatalogOwnershipRow{TeamID: team, ProjectID: project, Source: source}
	}
	archived := []JiraArchivedProject{{ID: "20001", Key: "OLD"}, {ID: "20002", Key: "GONE"}}
	open := []jiraTeamCatalogOwnershipRow{
		row("OLD", "20001", "native"),               // native id
		row("OLD", "org-1:jira:OLD", "native"),      // key-built id
		row("ops", "org-1:jira:OLD", "jira_legacy"), // key-built id, another team and source
		row("ops", "20002", "jira_legacy"),          // native id of the second archived project
		row("LIVE", "10001", "native"),              // a live project
		row("LIVE", "org-1:jira:LIVE", "native"),    // a live project, key-built
		row("OLD", "org-2:jira:OLD", "native"),      // the key-built id of another organization
		row("OLD", "OLD", "native"),                 // the bare key is no id form
		row("OLD", "org-1:jira:20001", "native"),    // the native id is not a key
		row("GONE", "org-1:jira:gone", "native"),    // the key is compared as stored
		row("X", "org-1:jira:OLDER", "native"),      // a key that only starts like an archived one
		row("X", "200011", "native"),                // an id that only starts like an archived one
	}
	held, rest := jiraHoldArchivedOwnership("org-1", archived, open)
	if !reflect.DeepEqual(held, open[:4]) || !reflect.DeepEqual(rest, open[4:]) {
		t.Fatalf("held = %+v\nrest = %+v\nwant the first four rows held and the others left to the snapshot rule", held, rest)
	}
	if held, rest := jiraHoldArchivedOwnership("org-1", nil, open); len(held) != 0 || len(rest) != len(open) {
		t.Fatalf("no archived project: held %d, rest %d; want none held", len(held), len(rest))
	}
}

// ports test_jira_populate_skips_board_discovery_for_a_non_software_
// project_under_strict_reference_discovery: a service_desk
// project's board discovery is skipped entirely (iter_boards must never
// even be called for it), but its project/ownership rows still land --
// the skip is boards-only, never project-level.
func TestJiraTeamCatalogCollectSkipsBoardDiscoveryForNonSoftwareProjectUnderStrict(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	doer := &jiraTeamCatalogFixtureDoer{t: t, byURI: map[string]jiraTeamCatalogFixtureResponse{
		jiraTeamCatalogProjectSearchURI: {
			body: `{"values":[{"id":"10002","key":"SUP","name":"Support"},{"id":"10001","key":"OPS","name":"Ops Project"}]}`,
		},
		"/rest/api/3/project/SUP": {body: `{"projectTypeKey":"service_desk"}`},
		"/rest/api/3/project/OPS": {body: `{"projectTypeKey":"software"}`},
		"/rest/agile/1.0/board?maxResults=100&projectKeyOrId=OPS&startAt=0": {
			body: `{"values":[{"id":91}],"isLast":true}`,
		},
		"/rest/agile/1.0/board/91/sprint?maxResults=100&startAt=0": {
			body: `{"values":[{"id":601,"name":"Sprint 1","state":"active"}],"isLast":true}`,
		},
		// Deliberately NO fixture for a SUP board listing -- iter_boards
		// must never be called for it. The doer fails the test (Fatalf) if
		// it is.
	}}
	handler := JiraTeamCatalogRouteHandler{}
	client := jiraTeamCatalogTestClient(t, fakehttp.Client(doer))
	credential := providerfoundation.Credential{Provider: "jira"}

	batch, err := handler.CollectTeamCatalog(
		context.Background(),
		TeamCatalogReference{OrgID: "org-1", SyncRunID: "run-1", Strict: true},
		credential, client,
		TeamCatalogSelections{Teams: true, Projects: true},
		now,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(batch.Rows.Teams) != 2 {
		t.Fatalf("teams=%+v", batch.Rows.Teams)
	}
	if len(batch.Rows.Sprints) != 1 || batch.Rows.Sprints[0].SprintID != "601" {
		t.Fatalf("want exactly OPS's sprint 601 (SUP has no boards), got %+v", batch.Rows.Sprints)
	}
	foundSUPOwnership := false
	for _, row := range batch.Rows.Ownership {
		if row.TeamID == "SUP" {
			foundSUPOwnership = true
		}
	}
	if !foundSUPOwnership {
		t.Fatalf("SUP's project/ownership row must still land even though its board discovery is skipped: %+v", batch.Rows.Ownership)
	}
}

// TestJiraTeamCatalogCollectRaisesOnUnrecognizedProjectTypeUnderStrict ports
// test_jira_populate_raises_on_an_unrecognized_project_type_under_strict_
// reference_discovery: an unrecognized projectTypeKey is not the same as a
// confirmed non-board-capable type and must propagate, never be silently
// skipped.
func TestJiraTeamCatalogCollectRaisesOnUnrecognizedProjectTypeUnderStrict(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	doer := &jiraTeamCatalogFixtureDoer{t: t, byURI: map[string]jiraTeamCatalogFixtureResponse{
		jiraTeamCatalogProjectSearchURI: {body: `{"values":[{"id":"10001","key":"OPS","name":"Ops Project"}]}`},
		"/rest/api/3/project/OPS":       {body: `{"projectTypeKey":"some_future_jira_template"}`},
		// Deliberately no board-listing fixture -- it must never be called.
	}}
	handler := JiraTeamCatalogRouteHandler{}
	client := jiraTeamCatalogTestClient(t, fakehttp.Client(doer))
	credential := providerfoundation.Credential{Provider: "jira"}

	_, err := handler.CollectTeamCatalog(
		context.Background(),
		TeamCatalogReference{OrgID: "org-1", SyncRunID: "run-1", Strict: true},
		credential, client,
		TeamCatalogSelections{Teams: true},
		now,
	)
	if err == nil {
		t.Fatal("an unrecognized project type must propagate as an error under strict")
	}
}

// TestJiraTeamCatalogCollectHappyPathTeamsMembersProjects proves the
// non-strict, all-selected happy path: a project's lead lands as its sole
// membership, the roster carries that member's facets, and the project/
// ownership rows are native-sourced.
func TestJiraTeamCatalogCollectHappyPathTeamsMembersProjects(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	doer := &jiraTeamCatalogFixtureDoer{t: t, byURI: map[string]jiraTeamCatalogFixtureResponse{
		jiraTeamCatalogProjectSearchURI: {
			body: `{"values":[{"id":"10001","key":"OPS","name":"Ops Project","description":"Ops team project"}]}`,
		},
		// Fetched twice: once for the member/lead lookup, once for the
		// sprint walk's project-type gate -- see jiraTeamCatalogProjectDetailPayload's
		// doc comment on why these are the SAME response body read for two
		// different purposes (mirroring Python's two separate call sites).
		"/rest/api/3/project/OPS": {
			body: `{"projectTypeKey":"software","lead":{"accountId":"account-1","emailAddress":"ops@example.com","displayName":"Ops Lead"}}`,
		},
		"/rest/agile/1.0/board?maxResults=100&projectKeyOrId=OPS&startAt=0": {
			body: `{"values":[],"isLast":true}`,
		},
	}}
	handler := JiraTeamCatalogRouteHandler{}
	client := jiraTeamCatalogTestClient(t, fakehttp.Client(doer))
	credential := providerfoundation.Credential{Provider: "jira"}

	batch, err := handler.CollectTeamCatalog(
		context.Background(),
		TeamCatalogReference{OrgID: "org-1", SyncRunID: "run-1", Strict: false},
		credential, client,
		TeamCatalogSelections{Teams: true, Members: true, Projects: true},
		now,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(batch.Rows.Teams) != 1 {
		t.Fatalf("teams=%+v", batch.Rows.Teams)
	}
	team := batch.Rows.Teams[0]
	if team.ID != "OPS" || team.OrgID != "org-1" || team.Provider != "jira" ||
		team.NativeTeamKey == nil || *team.NativeTeamKey != "OPS" || team.ParentTeamID != nil {
		t.Fatalf("team=%+v", team)
	}
	if !team.MembersAuthoritative {
		t.Fatal("MembersAuthoritative must be true once the walk completes with Members selected")
	}
	if len(batch.Rows.Memberships) != 1 {
		t.Fatalf("memberships=%+v", batch.Rows.Memberships)
	}
	membership := batch.Rows.Memberships[0]
	if membership.TeamID != "OPS" || membership.MemberID != "jira:account-1" ||
		membership.RawProviderUserID == nil || *membership.RawProviderUserID != "jira:accountid:account-1" ||
		membership.RawEmail == nil || *membership.RawEmail != "ops@example.com" ||
		membership.IsPrimary != 1 || membership.Source != "native" {
		t.Fatalf("membership=%+v", membership)
	}
	if len(membership.IdentityFacets) != 2 ||
		membership.IdentityFacets[0] != "jira:accountid:account-1" ||
		membership.IdentityFacets[1] != "ops@example.com" {
		t.Fatalf("identity_facets=%+v", membership.IdentityFacets)
	}
	if len(batch.Rows.Ownership) != 1 {
		t.Fatalf("ownership=%+v", batch.Rows.Ownership)
	}
	ownership := batch.Rows.Ownership[0]
	if ownership.TeamID != "OPS" || ownership.ProjectID != "10001" ||
		ownership.Source != "native" || ownership.Specificity != jiraTeamCatalogNativeSpecificity ||
		ownership.Priority != jiraTeamCatalogNativePriority {
		t.Fatalf("ownership=%+v", ownership)
	}
	if len(batch.Rows.Projects) != 1 || batch.Rows.Projects[0].ID != "10001" {
		t.Fatalf("projects=%+v", batch.Rows.Projects)
	}
	if len(batch.Rows.Sprints) != 0 {
		t.Fatalf("sprints=%+v", batch.Rows.Sprints)
	}
}

// TestJiraTeamCatalogCollectNonStrictWalkFailureSkipsCleanly proves the
// non-strict mirror of Python's populate() catching discover_jira's own
// failure: a project-search error degrades to a clean, successful
// WalkSkipped zero result instead of propagating.
func TestJiraTeamCatalogCollectNonStrictWalkFailureSkipsCleanly(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	doer := &jiraTeamCatalogFixtureDoer{t: t, byURI: map[string]jiraTeamCatalogFixtureResponse{
		jiraTeamCatalogProjectSearchURI: {status: http.StatusForbidden, body: `{}`},
	}}
	handler := JiraTeamCatalogRouteHandler{}
	client := jiraTeamCatalogTestClient(t, fakehttp.Client(doer))
	credential := providerfoundation.Credential{Provider: "jira"}

	batch, err := handler.CollectTeamCatalog(
		context.Background(),
		TeamCatalogReference{OrgID: "org-1", SyncRunID: "run-1", Strict: false},
		credential, client,
		TeamCatalogSelections{Teams: true},
		now,
	)
	if err != nil {
		t.Fatalf("a non-strict discovery failure must degrade to a clean skip, not propagate: %v", err)
	}
	if !batch.Result.WalkSkipped || batch.Result.WalkSkipReason != "project_discovery_failed" {
		t.Fatalf("result=%+v", batch.Result)
	}

	// The strict counterpart of the same failure must propagate unchanged.
	_, err = handler.CollectTeamCatalog(
		context.Background(),
		TeamCatalogReference{OrgID: "org-1", SyncRunID: "run-1", Strict: true},
		credential, client,
		TeamCatalogSelections{Teams: true},
		now,
	)
	if err == nil {
		t.Fatal("the same discovery failure must propagate under strict")
	}
}

// TestJiraTeamCatalogCollectNonStrictWalkFailureStampsRequestsOnTheSkipBatch
// proves that jiraTeamCatalogWalkFailure must not
// discard the counting Doer's running total along with the rest of the
// walk's state -- the physical requests already spent before a non-strict
// abort are real wire cost regardless of the abort. The project search fails
// every attempt through to the retry policy's exhaustion.
func TestJiraTeamCatalogCollectNonStrictWalkFailureStampsRequestsOnTheSkipBatch(t *testing.T) {
	t.Parallel()
	searchAttempts := 0
	doer := jiraWorkItemsDoerFunc(func(request *http.Request) (*http.Response, error) {
		if request.URL.RequestURI() != jiraTeamCatalogProjectSearchURI {
			t.Fatalf("unexpected request %q", request.URL.RequestURI())
		}
		searchAttempts++
		return nil, errors.New("simulated transient transport failure")
	})
	client, err := providerfoundation.NewHTTPClient(
		"jira", "https://jira.example.com", fakehttp.Client(doer),
		func(*http.Request) error { return nil },
		providerfoundation.RetryPolicy{MaxAttempts: 2, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond},
		providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
	)
	if err != nil {
		t.Fatal(err)
	}
	handler := JiraTeamCatalogRouteHandler{}
	batch, err := handler.CollectTeamCatalog(
		context.Background(),
		TeamCatalogReference{OrgID: "org-1", SyncRunID: "run-1", Strict: false},
		providerfoundation.Credential{Provider: "jira"}, client,
		TeamCatalogSelections{Teams: true},
		time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatalf("a non-strict discovery failure must degrade to a clean skip, not propagate: %v", err)
	}
	if !batch.Result.WalkSkipped || batch.Result.WalkSkipReason != "project_discovery_failed" {
		t.Fatalf("result=%+v", batch.Result)
	}
	if searchAttempts != 2 {
		t.Fatalf("search attempts=%d want 2 (both exhausted by the retry policy)", searchAttempts)
	}
	if batch.Evidence.Requests != 2 {
		t.Fatalf("evidence=%+v want Requests=2 (the skip batch must carry the real wire cost, not discard it)", batch.Evidence)
	}
}

// TestJiraTeamCatalogCollectorSkipsCleanlyWithNothingSelectedNonStrict is the
// collector-level contract test (mirrors GitHub's
// TestGitHubCollectTeamCatalogSkipsCleanlyUnderStrictWithNothingUsableSelected):
// a non-strict call with nothing selected must return a zero result WITHOUT
// ever touching ClickHouse or issuing an HTTP request.
func TestJiraTeamCatalogCollectorSkipsCleanlyWithNothingSelectedNonStrict(t *testing.T) {
	t.Parallel()
	collector := JiraTeamCatalogCollector{
		Sink: JiraTeamCatalogClickHouseEffects{Conn: unreachableConn{t: t}},
	}
	credential := providerfoundation.Credential{Provider: "jira"}
	client := &providerfoundation.HTTPClient{Provider: "jira"}

	result, err := collector.CollectTeamCatalog(
		context.Background(),
		TeamCatalogReference{OrgID: "org-1", SyncRunID: "run-1", Strict: false},
		credential, client,
		TeamCatalogSelections{},
		time.Now(),
	)
	if err != nil {
		t.Fatalf("nothing selected, non-strict, must skip cleanly: %v", err)
	}
	if result.TeamsWritten != 0 || result.MembershipsWritten != 0 || result.ProjectsWritten != 0 ||
		result.OwnershipWritten != 0 || result.SprintsWritten != 0 {
		t.Fatalf("want a zero result, got %+v", result)
	}
}

// TestJiraTeamCatalogCollectResolvesMemberIdentityThroughAliasMap is a collector-level proof: a project lead's membership row must
// carry the org's ALIAS-RESOLVED canonical identity for its account id, not
// the raw "jira:accountid:<id>" qualified id the collector would otherwise
// write. Deliberately not t.Parallel(): it sets IDENTITY_MAPPING_PATH via
// t.Setenv, which panics if called from a parallel subtest sibling.
func TestJiraTeamCatalogCollectResolvesMemberIdentityThroughAliasMap(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "identity_mapping.yaml")
	if err := os.WriteFile(path, []byte(`
version: 1
identities:
  - canonical: "person-b@example.com"
    aliases:
      - "jira:accountid:account-1"
`), 0o600); err != nil {
		t.Fatalf("write seeded identity_mapping.yaml: %v", err)
	}
	t.Setenv("IDENTITY_MAPPING_PATH", path)

	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	doer := &jiraTeamCatalogFixtureDoer{t: t, byURI: map[string]jiraTeamCatalogFixtureResponse{
		jiraTeamCatalogProjectSearchURI: {
			body: `{"values":[{"id":"10001","key":"OPS","name":"Ops Project"}]}`,
		},
		"/rest/api/3/project/OPS": {
			body: `{"projectTypeKey":"software","lead":{"accountId":"account-1","emailAddress":"ops@example.com","displayName":"Ops Lead"}}`,
		},
		"/rest/agile/1.0/board?maxResults=100&projectKeyOrId=OPS&startAt=0": {
			body: `{"values":[],"isLast":true}`,
		},
	}}
	handler := JiraTeamCatalogRouteHandler{}
	client := jiraTeamCatalogTestClient(t, fakehttp.Client(doer))
	credential := providerfoundation.Credential{Provider: "jira"}

	batch, err := handler.CollectTeamCatalog(
		context.Background(),
		TeamCatalogReference{OrgID: "org-1", SyncRunID: "run-1", Strict: false},
		credential, client,
		TeamCatalogSelections{Members: true},
		now,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(batch.Rows.Memberships) != 1 {
		t.Fatalf("memberships=%+v", batch.Rows.Memberships)
	}
	membership := batch.Rows.Memberships[0]
	if membership.RawProviderUserID == nil || *membership.RawProviderUserID != "person-b@example.com" {
		t.Fatalf("RawProviderUserID=%v, want alias-resolved person-b@example.com", membership.RawProviderUserID)
	}
	if len(membership.IdentityFacets) != 3 ||
		membership.IdentityFacets[0] != "person-b@example.com" ||
		membership.IdentityFacets[1] != "jira:accountid:account-1" ||
		membership.IdentityFacets[2] != "ops@example.com" {
		t.Fatalf("IdentityFacets=%v, want [person-b@example.com jira:accountid:account-1 ops@example.com]", membership.IdentityFacets)
	}
}

var _ TeamCatalogCollector = JiraTeamCatalogCollector{}
