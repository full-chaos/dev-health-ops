package providersync

import (
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
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
		"jira", "https://jira.example.com", doer,
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
			body := `{"values":[{"key":"OPS","name":"Ops Project"}]}`
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
		"jira", "https://jira.example.com", doer,
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
	// 2 (search, retried) + 1 (project detail) + 2 (boards, exhausted) = 5.
	if batch.Evidence.Requests != 5 {
		t.Fatalf("evidence=%+v want Requests=5 (every physical attempt, from one source)", batch.Evidence)
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
		jiraTeamCatalogProjectSearchURI: {body: `{"values":[{"key":"OPS","name":"Ops Project"}]}`},
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
	client := jiraTeamCatalogTestClient(t, doer)
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
		jiraTeamCatalogProjectSearchURI: {body: `{"values":[{"key":"OPS","name":"Ops Project"}]}`},
		"/rest/api/3/project/OPS":       {body: `{"projectTypeKey":"software"}`},
		"/rest/agile/1.0/board?maxResults=100&projectKeyOrId=OPS&startAt=0": {
			body: `{"values":[{"id":82}],"isLast":true}`,
		},
		"/rest/agile/1.0/board/82/sprint?maxResults=100&startAt=0": {
			body: `{"values":[{"id":501,"name":"Sprint 1","state":"active"}],"isLast":true}`,
		},
	}}
	handler := JiraTeamCatalogRouteHandler{}
	client := jiraTeamCatalogTestClient(t, doer)
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
		jiraTeamCatalogProjectSearchURI: {body: `{"values":[{"key":"OPS","name":"Ops Project"}]}`},
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
	client := jiraTeamCatalogTestClient(t, doer)
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
		jiraTeamCatalogProjectSearchURI: {body: `{"values":[{"key":"OPS","name":"Ops Project"}]}`},
		"/rest/api/3/project/OPS":       {body: `{"projectTypeKey":"software"}`},
		"/rest/agile/1.0/board?maxResults=100&projectKeyOrId=OPS&startAt=0": {
			status: http.StatusBadRequest,
			body:   `{"errorMessages":["The project key or id 'OPS' does not exist"]}`,
		},
	}}
	handler := JiraTeamCatalogRouteHandler{}
	client := jiraTeamCatalogTestClient(t, doer)
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
			body: `{"values":[{"key":"SUP","name":"Support"},{"key":"OPS","name":"Ops Project"}]}`,
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
	client := jiraTeamCatalogTestClient(t, doer)
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
		jiraTeamCatalogProjectSearchURI: {body: `{"values":[{"key":"OPS","name":"Ops Project"}]}`},
		"/rest/api/3/project/OPS":       {body: `{"projectTypeKey":"some_future_jira_template"}`},
		// Deliberately no board-listing fixture -- it must never be called.
	}}
	handler := JiraTeamCatalogRouteHandler{}
	client := jiraTeamCatalogTestClient(t, doer)
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
			body: `{"values":[{"key":"OPS","name":"Ops Project","description":"Ops team project"}]}`,
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
	client := jiraTeamCatalogTestClient(t, doer)
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
	if ownership.TeamID != "OPS" || ownership.ProjectID != "org-1:jira:OPS" ||
		ownership.Source != "native" || ownership.Specificity != jiraTeamCatalogNativeSpecificity ||
		ownership.Priority != jiraTeamCatalogNativePriority {
		t.Fatalf("ownership=%+v", ownership)
	}
	if len(batch.Rows.Projects) != 1 || batch.Rows.Projects[0].ID != "org-1:jira:OPS" {
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
	client := jiraTeamCatalogTestClient(t, doer)
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
		"jira", "https://jira.example.com", doer,
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
			body: `{"values":[{"key":"OPS","name":"Ops Project"}]}`,
		},
		"/rest/api/3/project/OPS": {
			body: `{"projectTypeKey":"software","lead":{"accountId":"account-1","emailAddress":"ops@example.com","displayName":"Ops Lead"}}`,
		},
		"/rest/agile/1.0/board?maxResults=100&projectKeyOrId=OPS&startAt=0": {
			body: `{"values":[],"isLast":true}`,
		},
	}}
	handler := JiraTeamCatalogRouteHandler{}
	client := jiraTeamCatalogTestClient(t, doer)
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
