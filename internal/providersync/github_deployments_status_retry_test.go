package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// CHAOS-9678: a deployment whose status a run could not store (a failed or
// rate-limited lookup, or a lookup skipped after one) is looked up again by a
// later run, whatever that run's window.

type recordingStatusHolds struct {
	ids        []string
	err        error
	calls      int
	orgID      string
	repoID     string
	since      time.Time
	limit      int
	gotContext context.Context
}

func (holds *recordingStatusHolds) DeploymentsWithoutStatus(ctx context.Context, orgID, repoID string, since time.Time, limit int) ([]string, error) {
	holds.calls++
	holds.gotContext, holds.orgID, holds.repoID, holds.since, holds.limit = ctx, orgID, repoID, since, limit
	return holds.ids, holds.err
}

// githubDeploymentStatusRetryDoer serves three deployments: 701 and 702 were
// deployed on 2026-07-22 (inside the window of the first run, outside the
// window of the second), 703 on 2026-06-10 (outside both). Statuses are
// served per path from `statuses`; a path in `limited` answers 429.
type githubDeploymentStatusRetryDoer struct {
	statuses map[string]string
	limited  map[string]bool
	// single serves GET /deployments/{id}, the deployment the list did not return; any other id is 404.
	single map[string]string
	asked  []string
}

func (doer *githubDeploymentStatusRetryDoer) Do(request *http.Request) (*http.Response, error) {
	header := http.Header{"Content-Type": {"application/json"}}
	respond := func(code int, body string) (*http.Response, error) {
		return &http.Response{StatusCode: code, Header: header, Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
	}
	switch request.URL.Path {
	case "/repos/acme/api":
		return respond(http.StatusOK, gitHubRepositoryFixture)
	case "/repos/acme/api/releases":
		return respond(http.StatusOK, `[]`)
	case "/repos/acme/api/deployments":
		return respond(http.StatusOK, `[{"id":701,"created_at":"2026-07-22T10:00:00Z"},{"id":702,"created_at":"2026-07-22T10:05:00Z"},{"id":703,"created_at":"2026-06-10T10:00:00Z"}]`)
	}
	if strings.HasPrefix(request.URL.Path, "/repos/acme/api/deployments/") && !strings.HasSuffix(request.URL.Path, "/statuses") {
		doer.asked = append(doer.asked, request.URL.Path)
		if doer.limited[request.URL.Path] {
			return respond(http.StatusTooManyRequests, `{"message":"rate limited"}`)
		}
		if body, ok := doer.single[request.URL.Path]; ok {
			return respond(http.StatusOK, body)
		}
		return respond(http.StatusNotFound, `{"message":"Not Found"}`)
	}
	if strings.HasSuffix(request.URL.Path, "/statuses") {
		doer.asked = append(doer.asked, request.URL.Path)
		if doer.limited[request.URL.Path] {
			return respond(http.StatusTooManyRequests, `{"message":"rate limited"}`)
		}
		if body, ok := doer.statuses[request.URL.Path]; ok {
			return respond(http.StatusOK, body)
		}
		return respond(http.StatusOK, `[]`)
	}
	return respond(http.StatusOK, `[]`)
}

func collectStatusRetryRows(t *testing.T, doer *githubDeploymentStatusRetryDoer, holds GitHubDeploymentStatusHolds, claim Claim, now time.Time) map[string]deploymentRow {
	t.Helper()
	client := gitHubRepositoryClient(t, fakehttp.Client(doer), "https://api.github.com")
	batch, err := (GitHubDeploymentsRouteHandler{Holds: holds}).Collect(context.Background(), claim, providerfoundation.Credential{}, client, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.Effects) != 1 {
		t.Fatalf("effects=%+v", batch.Effects)
	}
	rows := map[string]deploymentRow{}
	for _, raw := range batch.Effects[0].Rows {
		var row deploymentRow
		if err := json.Unmarshal(raw, &row); err != nil {
			t.Fatal(err)
		}
		rows[row.DeploymentID] = row
	}
	return rows
}

func TestGitHubDeploymentsRouteLooksUpAgainADeploymentWhoseStatusIsEmptyOutsideTheWindow(t *testing.T) {
	// Not t.Parallel(): captureSlog swaps the process-global slog default.
	log := captureSlog(t)
	now := time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC)

	// Run 1: the first lookup exhausts the rate limit and the second is
	// skipped, so both rows are written with no status.
	first := &githubDeploymentStatusRetryDoer{
		limited:  map[string]bool{"/repos/acme/api/deployments/701/statuses": true},
		statuses: map[string]string{"/repos/acme/api/deployments/702/statuses": `[{"id":1,"state":"success","created_at":"2026-07-22T10:06:00Z"}]`},
	}
	firstRows := collectStatusRetryRows(t, first, &recordingStatusHolds{}, nativeTestClaim("github", "deployments"), now)
	if len(firstRows) != 2 {
		t.Fatalf("run 1 rows = %d, want the two deployments of the window (703 is outside it)", len(firstRows))
	}
	for id, row := range firstRows {
		if row.Status != nil || !row.LifecycleLookupFailed {
			t.Fatalf("run 1 row %s status %v lookupFailed %v, want no status and a failed lookup", id, row.Status, row.LifecycleLookupFailed)
		}
	}
	if len(first.asked) != 1 {
		t.Fatalf("run 1 asked %v, want exactly one statuses call (the rest skipped)", first.asked)
	}

	// Run 2, a later window that holds neither deployment: the store names
	// 701 and 702 as still having no status, so both are looked up again; 703
	// is not named and stays out.
	second := &githubDeploymentStatusRetryDoer{statuses: map[string]string{
		"/repos/acme/api/deployments/701/statuses": `[{"id":1,"state":"failure","created_at":"2026-07-22T10:01:00Z"}]`,
		"/repos/acme/api/deployments/702/statuses": `[{"id":2,"state":"success","created_at":"2026-07-22T10:06:00Z"}]`,
		"/repos/acme/api/deployments/703/statuses": `[{"id":3,"state":"success","created_at":"2026-06-10T10:01:00Z"}]`,
	}}
	holds := &recordingStatusHolds{ids: []string{"701", "702"}}
	laterWindow := nativeTestClaim("github", "deployments")
	since, before := time.Date(2026, 7, 23, 0, 0, 0, 0, time.UTC), time.Date(2026, 7, 23, 23, 59, 59, 0, time.UTC)
	laterWindow.SinceAt, laterWindow.BeforeAt = &since, &before
	secondRows := collectStatusRetryRows(t, second, holds, laterWindow, now)
	if len(secondRows) != 2 {
		t.Fatalf("run 2 rows = %d, want exactly the two deployments the store names", len(secondRows))
	}
	if got := secondRows["701"].Status; got == nil || *got != "failure" {
		t.Fatalf("run 2 status of 701 = %v, want failure", got)
	}
	if got := secondRows["702"].Status; got == nil || *got != "success" {
		t.Fatalf("run 2 status of 702 = %v, want success", got)
	}
	if secondRows["701"].LifecycleLookupFailed || secondRows["702"].LifecycleLookupFailed {
		t.Fatal("a successful lookup is marked as failed")
	}
	for _, path := range second.asked {
		if strings.Contains(path, "/703/") {
			t.Fatalf("run 2 looked up 703, which the store does not name and the window excludes: %v", second.asked)
		}
	}
	if holds.calls != 1 || holds.orgID != "org-acme" || holds.repoID == "" {
		t.Fatalf("holds read %d times for org %q repo %q, want once for the unit's org and repository", holds.calls, holds.orgID, holds.repoID)
	}
	if want := now.Add(-deploymentStatusRetryHorizon); !holds.since.Equal(want) || holds.limit != deploymentStatusRetryMax {
		t.Fatalf("holds read since %v limit %d, want %v and %d", holds.since, holds.limit, want, deploymentStatusRetryMax)
	}
	for _, want := range []string{"github_deployments.status_lookups", "stored=2", "retried=2", "failed=0", "skipped=0"} {
		if !strings.Contains(log.String(), want) {
			t.Fatalf("run 2 log lacks %q: %s", want, log.String())
		}
	}
}

func TestGitHubDeploymentsRouteCountsEachStatusLookupOutcomeOnce(t *testing.T) {
	log := captureSlog(t)
	now := time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC)
	doer := &githubDeploymentStatusRetryDoer{
		limited:  map[string]bool{"/repos/acme/api/deployments/701/statuses": true},
		statuses: map[string]string{},
	}
	collectStatusRetryRows(t, doer, nil, nativeTestClaim("github", "deployments"), now)
	// 701 failed on the rate limit, 702 was skipped after it.
	for _, want := range []string{"github_deployments.status_lookups", "stored=0", "empty=0", "failed=1", "skipped=1", "retried=0", "rows=2"} {
		if !strings.Contains(log.String(), want) {
			t.Fatalf("log lacks %q: %s", want, log.String())
		}
	}

	log = captureSlog(t)
	empty := &githubDeploymentStatusRetryDoer{statuses: map[string]string{
		"/repos/acme/api/deployments/702/statuses": `[{"id":2,"state":"success","created_at":"2026-07-22T10:06:00Z"}]`,
	}}
	collectStatusRetryRows(t, empty, nil, nativeTestClaim("github", "deployments"), now)
	// 701 has no statuses at all (an honest empty), 702 stored one.
	for _, want := range []string{"stored=1", "empty=1", "failed=0", "skipped=0"} {
		if !strings.Contains(log.String(), want) {
			t.Fatalf("log lacks %q: %s", want, log.String())
		}
	}
}

func TestGitHubDeploymentsRouteKeepsTheWindowWhenTheHeldReadFails(t *testing.T) {
	log := captureSlog(t)
	now := time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC)
	doer := &githubDeploymentStatusRetryDoer{statuses: map[string]string{
		"/repos/acme/api/deployments/701/statuses": `[{"id":1,"state":"success","created_at":"2026-07-22T10:01:00Z"}]`,
		"/repos/acme/api/deployments/702/statuses": `[{"id":2,"state":"success","created_at":"2026-07-22T10:06:00Z"}]`,
	}}
	rows := collectStatusRetryRows(t, doer, &recordingStatusHolds{err: errors.New("store unavailable")}, nativeTestClaim("github", "deployments"), now)
	if len(rows) != 2 || rows["701"].Status == nil || rows["702"].Status == nil {
		t.Fatalf("rows = %+v, want the two deployments of the window with a status: a failed held read never fails the unit", rows)
	}
	if !strings.Contains(log.String(), "github_deployments.status_retry_read_failed") {
		t.Fatalf("the failed held read is not logged: %s", log.String())
	}
}

// A held deployment that the window also reaches is a normal lookup, not a
// retry: only a deployment outside the window counts as retried.
func TestGitHubDeploymentsRouteCountsOnlyDeploymentsOutsideTheWindowAsRetried(t *testing.T) {
	log := captureSlog(t)
	now := time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC)
	doer := &githubDeploymentStatusRetryDoer{statuses: map[string]string{
		"/repos/acme/api/deployments/701/statuses": `[{"id":1,"state":"success","created_at":"2026-07-22T10:01:00Z"}]`,
		"/repos/acme/api/deployments/702/statuses": `[{"id":2,"state":"success","created_at":"2026-07-22T10:06:00Z"}]`,
		"/repos/acme/api/deployments/703/statuses": `[{"id":3,"state":"failure","created_at":"2026-06-10T10:01:00Z"}]`,
	}}
	// 701 and 702 are inside the window of the claim, 703 is outside it; the store holds 701 and 703.
	rows := collectStatusRetryRows(t, doer, &recordingStatusHolds{ids: []string{"701", "703"}}, nativeTestClaim("github", "deployments"), now)
	if len(rows) != 3 {
		t.Fatalf("rows = %d, want the two of the window and the held one outside it", len(rows))
	}
	for _, want := range []string{"github_deployments.status_lookups", "rows=3", "stored=3", "retried=1"} {
		if !strings.Contains(log.String(), want) {
			t.Fatalf("log lacks %q (701 is held but inside the window: not a retry; 703 is the one retry): %s", want, log.String())
		}
	}
}

// A held deployment the list did not return (the list holds the repository's
// newest deployments only) is fetched by id and looked up again, so the cap of
// the list never keeps an old held deployment out of the retry.
func TestGitHubDeploymentsRouteLooksUpAgainAHeldDeploymentTheListDidNotReturn(t *testing.T) {
	log := captureSlog(t)
	now := time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC)
	doer := &githubDeploymentStatusRetryDoer{
		single: map[string]string{"/repos/acme/api/deployments/704": `{"id":704,"created_at":"2026-06-05T10:00:00Z"}`},
		statuses: map[string]string{
			"/repos/acme/api/deployments/701/statuses": `[{"id":1,"state":"success","created_at":"2026-07-22T10:01:00Z"}]`,
			"/repos/acme/api/deployments/702/statuses": `[{"id":2,"state":"success","created_at":"2026-07-22T10:06:00Z"}]`,
			"/repos/acme/api/deployments/704/statuses": `[{"id":4,"state":"success","created_at":"2026-06-05T10:01:00Z"}]`,
		},
	}
	rows := collectStatusRetryRows(t, doer, &recordingStatusHolds{ids: []string{"704"}}, nativeTestClaim("github", "deployments"), now)
	if got := rows["704"].Status; len(rows) != 3 || got == nil || *got != "success" {
		t.Fatalf("rows = %d, status of 704 = %v, want the two of the window and 704 stored as success", len(rows), got)
	}
	for _, want := range []string{"rows=3", "stored=3", "retried=1", "unlisted_fetched=1", "unlisted_failed=0"} {
		if !strings.Contains(log.String(), want) {
			t.Fatalf("log lacks %q: %s", want, log.String())
		}
	}
}

func TestGitHubDeploymentsRouteSurvivesAHeldDeploymentThatCannotBeFetched(t *testing.T) {
	log := captureSlog(t)
	now := time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC)
	doer := &githubDeploymentStatusRetryDoer{statuses: map[string]string{}}
	// 704 does not exist (404): the unit goes on with the deployments it has.
	rows := collectStatusRetryRows(t, doer, &recordingStatusHolds{ids: []string{"704"}}, nativeTestClaim("github", "deployments"), now)
	if len(rows) != 2 {
		t.Fatalf("rows = %d, want the two deployments of the window", len(rows))
	}
	for _, want := range []string{"github_deployments.held_deployment_fetch_failed", "deployment_id=704", "unlisted_fetched=0", "unlisted_failed=1"} {
		if !strings.Contains(log.String(), want) {
			t.Fatalf("log lacks %q: %s", want, log.String())
		}
	}
}

func TestGitHubDeploymentsRouteStopsFetchingHeldDeploymentsAfterARateLimit(t *testing.T) {
	captureSlog(t)
	now := time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC)
	doer := &githubDeploymentStatusRetryDoer{
		limited:  map[string]bool{"/repos/acme/api/deployments/704": true},
		statuses: map[string]string{},
	}
	collectStatusRetryRows(t, doer, &recordingStatusHolds{ids: []string{"704", "705"}}, nativeTestClaim("github", "deployments"), now)
	for _, path := range doer.asked {
		if strings.Contains(path, "/705") {
			t.Fatalf("705 was fetched after 704 exhausted the rate limit: %v", doer.asked)
		}
	}
}
