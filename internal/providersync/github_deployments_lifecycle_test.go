package providersync

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// captureSlog redirects the default slog logger to a buffer for the
// duration of the test, restoring it on cleanup -- the same pattern any
// caller of the standard library's slog.SetDefault would use to assert
// on an emitted log line rather than trusting a code path ran silently.
func captureSlog(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	swapDefaultLogger(t, slog.New(slog.NewTextHandler(&buf, nil)))
	return &buf
}

// TestDeploymentLifecycleFromStatusesIsOrderIndependent pins
// deploymentLifecycleFromStatuses' own doc comment: it compares every
// entry's created_at rather than trusting response order, so a reversed
// slice (standing in for a differently-ordered page) yields the same
// started_at/finished_at.
func TestDeploymentLifecycleFromStatusesIsOrderIndependent(t *testing.T) {
	t.Parallel()
	// Two in_progress entries (a retry) and two terminal entries (a
	// failed attempt then a successful one) so the earliest/latest
	// selection is exercised, not just a single-entry pass-through: the
	// later in_progress and the earlier terminal entry must both lose.
	forward := []gitHubDeploymentStatusPayload{
		{State: "queued", CreatedAt: strPtr("2026-07-22T09:58:00Z")},
		{State: "in_progress", CreatedAt: strPtr("2026-07-22T09:59:00Z")},
		{State: "in_progress", CreatedAt: strPtr("2026-07-22T09:59:30Z")},
		{State: "failure", CreatedAt: strPtr("2026-07-22T10:00:30Z")},
		{State: "success", CreatedAt: strPtr("2026-07-22T10:01:00Z")},
	}
	reversed := []gitHubDeploymentStatusPayload{forward[4], forward[3], forward[2], forward[1], forward[0]}

	wantStarted := time.Date(2026, 7, 22, 9, 59, 0, 0, time.UTC)
	wantFinished := time.Date(2026, 7, 22, 10, 1, 0, 0, time.UTC)

	cases := []struct {
		name     string
		statuses []gitHubDeploymentStatusPayload
	}{
		{name: "forward", statuses: forward},
		{name: "reversed", statuses: reversed},
	}
	for _, testCase := range cases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			startedAt, finishedAt := deploymentLifecycleFromStatuses(testCase.statuses)
			if startedAt == nil || !startedAt.Equal(wantStarted) || finishedAt == nil || !finishedAt.Equal(wantFinished) {
				t.Fatalf("startedAt=%v finishedAt=%v want started=%v finished=%v", startedAt, finishedAt, wantStarted, wantFinished)
			}
		})
	}
}

// TestDeploymentLifecycleFromStatusesSkipsUnparseableTimestamp pins the
// `at == nil` guard: a status entry whose created_at fails to parse is
// skipped outright, never treated as an in_progress/terminal signal. Each
// unparseable entry here follows a VALID entry of the same class (so
// startedAt/finishedAt is already non-nil when the guard is reached) --
// without the guard, the nil `at` would be compared against that non-nil
// value and panic, not just produce a wrong answer.
func TestDeploymentLifecycleFromStatusesSkipsUnparseableTimestamp(t *testing.T) {
	t.Parallel()
	startedAt, finishedAt := deploymentLifecycleFromStatuses([]gitHubDeploymentStatusPayload{
		{State: "in_progress", CreatedAt: strPtr("2026-07-22T09:59:00Z")},
		{State: "in_progress", CreatedAt: strPtr("not-a-timestamp")},
		{State: "success", CreatedAt: strPtr("2026-07-22T10:01:00Z")},
		{State: "success", CreatedAt: nil},
	})
	wantStarted := time.Date(2026, 7, 22, 9, 59, 0, 0, time.UTC)
	wantFinished := time.Date(2026, 7, 22, 10, 1, 0, 0, time.UTC)
	if startedAt == nil || !startedAt.Equal(wantStarted) || finishedAt == nil || !finishedAt.Equal(wantFinished) {
		t.Fatalf("startedAt=%v finishedAt=%v want started=%v finished=%v", startedAt, finishedAt, wantStarted, wantFinished)
	}
}

// TestDeploymentLifecycleFromStatusesTerminalStates pins the three states
// that represent this deployment's own pipeline finishing (success,
// failure, error), and that pending/queued/in_progress/inactive never
// resolve finished_at from a single entry alone -- inactive gets its own
// dedicated test (TestDeploymentLifecycleFromStatusesInactiveNeverExtendsFinishedAt)
// for the supersession case, since a bare single-entry check can't
// distinguish "inactive is never terminal" from "inactive alone,
// coincidentally, never had anything to extend."
func TestDeploymentLifecycleFromStatusesTerminalStates(t *testing.T) {
	t.Parallel()
	for _, state := range []string{"success", "failure", "error"} {
		state := state
		t.Run(state, func(t *testing.T) {
			t.Parallel()
			_, finishedAt := deploymentLifecycleFromStatuses([]gitHubDeploymentStatusPayload{
				{State: state, CreatedAt: strPtr("2026-07-22T10:01:00Z")},
			})
			if finishedAt == nil {
				t.Fatalf("state %q did not resolve finished_at", state)
			}
		})
	}
	for _, state := range []string{"pending", "queued", "in_progress", "inactive"} {
		state := state
		t.Run(state, func(t *testing.T) {
			t.Parallel()
			_, finishedAt := deploymentLifecycleFromStatuses([]gitHubDeploymentStatusPayload{
				{State: state, CreatedAt: strPtr("2026-07-22T10:01:00Z")},
			})
			if finishedAt != nil {
				t.Fatalf("open state %q wrongly resolved finished_at=%v", state, finishedAt)
			}
		})
	}
}

// TestDeploymentLifecycleFromStatusesInactiveNeverExtendsFinishedAt pins
// that an "inactive" status -- fired when a NEWER deployment on the same
// environment supersedes this one -- never becomes finished_at, even when
// it is the latest entry in the history: a deployment that completed
// (success) long before a much-later supersession keeps its real
// completion time, not the supersession instant.
func TestDeploymentLifecycleFromStatusesInactiveNeverExtendsFinishedAt(t *testing.T) {
	t.Parallel()
	_, finishedAt := deploymentLifecycleFromStatuses([]gitHubDeploymentStatusPayload{
		{State: "in_progress", CreatedAt: strPtr("2026-07-22T10:00:00Z")},
		{State: "success", CreatedAt: strPtr("2026-07-22T10:05:00Z")},
		{State: "inactive", CreatedAt: strPtr("2026-08-15T09:00:00Z")},
	})
	wantFinished := time.Date(2026, 7, 22, 10, 5, 0, 0, time.UTC)
	if finishedAt == nil || !finishedAt.Equal(wantFinished) {
		t.Fatalf("finishedAt=%v, want %v (the real completion, not the later supersession)", finishedAt, wantFinished)
	}
}

// TestDeploymentLifecycleFromStatusesNoSignalStaysNil pins that an absent
// signal is nil, never a value copied from another column -- the class
// ruling this ticket enforces.
func TestDeploymentLifecycleFromStatusesNoSignalStaysNil(t *testing.T) {
	t.Parallel()
	startedAt, finishedAt := deploymentLifecycleFromStatuses(nil)
	if startedAt != nil || finishedAt != nil {
		t.Fatalf("empty input produced startedAt=%v finishedAt=%v", startedAt, finishedAt)
	}
	startedAt, finishedAt = deploymentLifecycleFromStatuses([]gitHubDeploymentStatusPayload{
		{State: "queued", CreatedAt: strPtr("2026-07-22T09:58:00Z")},
	})
	if startedAt != nil || finishedAt != nil {
		t.Fatalf("queued-only input produced startedAt=%v finishedAt=%v", startedAt, finishedAt)
	}
}

// TestDeploymentLatestStatusIsOrderIndependent pins deploymentLatestStatus'
// own doc comment: it picks the entry with the greatest created_at
// regardless of response order, sharing the exact forward/reversed fixture
// TestDeploymentLifecycleFromStatusesIsOrderIndependent uses above (the
// last entry by time, "success" at 10:01:00, must win in either order).
func TestDeploymentLatestStatusIsOrderIndependent(t *testing.T) {
	t.Parallel()
	forward := []gitHubDeploymentStatusPayload{
		{State: "queued", CreatedAt: strPtr("2026-07-22T09:58:00Z")},
		{State: "in_progress", CreatedAt: strPtr("2026-07-22T09:59:00Z")},
		{State: "in_progress", CreatedAt: strPtr("2026-07-22T09:59:30Z")},
		{State: "failure", CreatedAt: strPtr("2026-07-22T10:00:30Z")},
		{State: "success", CreatedAt: strPtr("2026-07-22T10:01:00Z")},
	}
	reversed := []gitHubDeploymentStatusPayload{forward[4], forward[3], forward[2], forward[1], forward[0]}

	for _, testCase := range []struct {
		name     string
		statuses []gitHubDeploymentStatusPayload
	}{
		{name: "forward", statuses: forward},
		{name: "reversed", statuses: reversed},
	} {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			got := deploymentLatestStatus(testCase.statuses)
			if got == nil || *got != "success" {
				t.Fatalf("deploymentLatestStatus=%v want=%q", got, "success")
			}
		})
	}
}

// TestDeploymentLatestStatusStillInProgressReturnsInProgress proves the
// running case directly: when the latest entry by created_at is
// in_progress (no terminal entry exists yet), that is exactly the string
// deploymentStatusIsRunning must see for the flame reader's request-clock
// fallback to fire.
func TestDeploymentLatestStatusStillInProgressReturnsInProgress(t *testing.T) {
	t.Parallel()
	got := deploymentLatestStatus([]gitHubDeploymentStatusPayload{
		{State: "queued", CreatedAt: strPtr("2026-07-22T09:58:00Z")},
		{State: "in_progress", CreatedAt: strPtr("2026-07-22T09:59:00Z")},
	})
	if got == nil || *got != "in_progress" {
		t.Fatalf("deploymentLatestStatus=%v want=%q", got, "in_progress")
	}
}

// TestDeploymentLatestStatusNoSignalStaysNil mirrors
// TestDeploymentLifecycleFromStatusesNoSignalStaysNil for the status
// derivation: no entries, and entries whose created_at fails to parse,
// both leave the status nil -- never an invented value.
func TestDeploymentLatestStatusNoSignalStaysNil(t *testing.T) {
	t.Parallel()
	if got := deploymentLatestStatus(nil); got != nil {
		t.Fatalf("empty input produced status=%v", got)
	}
	if got := deploymentLatestStatus([]gitHubDeploymentStatusPayload{
		{State: "in_progress", CreatedAt: strPtr("not-a-timestamp")},
	}); got != nil {
		t.Fatalf("unparseable-timestamp-only input produced status=%v", got)
	}
}

// githubDeploymentStatusPagedDoer serves a Link-headered, two-page statuses
// response -- the in_progress and terminal entries land on different
// pages, so a correct fetch must aggregate both pages before
// deploymentLifecycleFromStatuses runs, the same "second page changes
// nothing but its own presence" property the order-independence test above
// pins for a single page.
type githubDeploymentStatusPagedDoer struct {
	t        *testing.T
	requests []string
}

func (doer *githubDeploymentStatusPagedDoer) Do(request *http.Request) (*http.Response, error) {
	doer.requests = append(doer.requests, request.URL.Path+"?"+request.URL.RawQuery)
	header := http.Header{"Content-Type": {"application/json"}}
	switch request.URL.Path {
	case "/repos/acme/api":
		return &http.Response{StatusCode: http.StatusOK, Header: header, Body: io.NopCloser(strings.NewReader(gitHubRepositoryFixture)), Request: request}, nil
	case "/repos/acme/api/releases":
		return &http.Response{StatusCode: http.StatusOK, Header: header, Body: io.NopCloser(strings.NewReader(`[]`)), Request: request}, nil
	case "/repos/acme/api/deployments":
		return &http.Response{StatusCode: http.StatusOK, Header: header, Body: io.NopCloser(strings.NewReader(`[{"id":201,"state":"success","created_at":"2026-07-22T10:00:00Z"}]`)), Request: request}, nil
	case "/repos/acme/api/deployments/201/statuses":
		page := request.URL.Query().Get("page")
		if page == "" || page == "1" {
			next := *request.URL
			forward := next.Query()
			forward.Set("page", "2")
			next.RawQuery = forward.Encode()
			header.Set("Link", "<"+next.String()+">; rel=\"next\"")
			body := `[{"id":1,"state":"success","created_at":"2026-07-22T10:01:00Z"}]`
			return &http.Response{StatusCode: http.StatusOK, Header: header, Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
		}
		body := `[{"id":2,"state":"in_progress","created_at":"2026-07-22T09:59:00Z"}]`
		return &http.Response{StatusCode: http.StatusOK, Header: header, Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
	}
	doer.t.Fatalf("unexpected request %s", request.URL.String())
	return nil, nil
}

func TestGitHubDeploymentsRouteAggregatesStatusesAcrossPages(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC)
	doer := &githubDeploymentStatusPagedDoer{t: t}
	client := gitHubRepositoryClient(t, doer, "https://api.github.com")
	claim := nativeTestClaim("github", "deployments")
	batch, err := (GitHubDeploymentsRouteHandler{}).Collect(context.Background(), claim, providerfoundation.Credential{}, client, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.Effects) != 1 || len(batch.Effects[0].Rows) != 1 {
		t.Fatalf("effects=%+v", batch.Effects)
	}
	var row deploymentRow
	if err := json.Unmarshal(batch.Effects[0].Rows[0], &row); err != nil {
		t.Fatal(err)
	}
	wantStarted := time.Date(2026, 7, 22, 9, 59, 0, 0, time.UTC)
	wantFinished := time.Date(2026, 7, 22, 10, 1, 0, 0, time.UTC)
	if row.StartedAt == nil || !row.StartedAt.Equal(wantStarted) || row.FinishedAt == nil || !row.FinishedAt.Equal(wantFinished) {
		t.Fatalf("row started/finished not aggregated across pages: %+v", row)
	}
	if row.Status == nil || *row.Status != "success" {
		t.Fatalf("row status not derived from the statuses lookup: %+v -- want %q (the latest entry by created_at, page 1's own success at 10:01:00)", row, "success")
	}
	sawPage2 := false
	for _, path := range doer.requests {
		if strings.Contains(path, "/deployments/201/statuses") && strings.Contains(path, "page=2") {
			sawPage2 = true
		}
	}
	if !sawPage2 {
		t.Fatalf("second statuses page was never requested: %v", doer.requests)
	}
}

// TestGitHubDeploymentsRouteStatusLookupFailureLeavesLifecycleNil pins that
// a failed statuses lookup never fails the unit and never invents a
// timestamp -- ruling: a failed or empty lookup leaves NULLs.
func TestGitHubDeploymentsRouteStatusLookupFailureLeavesLifecycleNil(t *testing.T) {
	// Not t.Parallel(): captureSlog swaps the process-global slog default,
	// which a concurrently running captureSlog test would race.
	log := captureSlog(t)
	now := time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC)
	doer := &githubDeploymentStatusFailingDoer{}
	client := gitHubRepositoryClient(t, doer, "https://api.github.com")
	claim := nativeTestClaim("github", "deployments")
	batch, err := (GitHubDeploymentsRouteHandler{}).Collect(context.Background(), claim, providerfoundation.Credential{}, client, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.Effects) != 1 || len(batch.Effects[0].Rows) != 1 {
		t.Fatalf("effects=%+v", batch.Effects)
	}
	var row deploymentRow
	if err := json.Unmarshal(batch.Effects[0].Rows[0], &row); err != nil {
		t.Fatal(err)
	}
	if row.StartedAt != nil || row.FinishedAt != nil {
		t.Fatalf("failed statuses lookup invented a timestamp: %+v", row)
	}
	if !row.LifecycleLookupFailed {
		t.Fatalf("row is not marked LifecycleLookupFailed despite a failed statuses lookup -- the write-once carry-forward guard would never protect it")
	}
	if !strings.Contains(log.String(), "github_deployments.status_lookup_failed") || !strings.Contains(log.String(), "deployment_id=301") {
		t.Fatalf("expected a status_lookup_failed log line for deployment_id=301, got: %s", log.String())
	}
}

// TestGitHubDeploymentsRouteEmptyStatusListLeavesLifecycleNil pins that a
// 200 response with zero statuses (a deployment whose status history has
// not been recorded) leaves both fields NULL, the same as a failed lookup
// -- never an invented value.
func TestGitHubDeploymentsRouteEmptyStatusListLeavesLifecycleNil(t *testing.T) {
	// Not t.Parallel(): captureSlog swaps the process-global slog default,
	// which a concurrently running captureSlog test would race.
	log := captureSlog(t)
	now := time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC)
	doer := &githubDeploymentStatusEmptyDoer{}
	client := gitHubRepositoryClient(t, doer, "https://api.github.com")
	claim := nativeTestClaim("github", "deployments")
	batch, err := (GitHubDeploymentsRouteHandler{}).Collect(context.Background(), claim, providerfoundation.Credential{}, client, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.Effects) != 1 || len(batch.Effects[0].Rows) != 1 {
		t.Fatalf("effects=%+v", batch.Effects)
	}
	var row deploymentRow
	if err := json.Unmarshal(batch.Effects[0].Rows[0], &row); err != nil {
		t.Fatal(err)
	}
	if row.StartedAt != nil || row.FinishedAt != nil {
		t.Fatalf("empty statuses list invented a timestamp: %+v", row)
	}
	if row.LifecycleLookupFailed {
		t.Fatalf("row is wrongly marked LifecycleLookupFailed for a SUCCESSFUL empty lookup -- the write-once carry-forward guard would incorrectly protect an honest empty result from ever being written")
	}
	if !strings.Contains(log.String(), "github_deployments.status_lookup_empty") || !strings.Contains(log.String(), "deployment_id=401") {
		t.Fatalf("expected a status_lookup_empty log line for deployment_id=401, got: %s", log.String())
	}
}

type githubDeploymentStatusEmptyDoer struct{}

func (doer *githubDeploymentStatusEmptyDoer) Do(request *http.Request) (*http.Response, error) {
	header := http.Header{"Content-Type": {"application/json"}}
	switch request.URL.Path {
	case "/repos/acme/api":
		return &http.Response{StatusCode: http.StatusOK, Header: header, Body: io.NopCloser(strings.NewReader(gitHubRepositoryFixture)), Request: request}, nil
	case "/repos/acme/api/releases":
		return &http.Response{StatusCode: http.StatusOK, Header: header, Body: io.NopCloser(strings.NewReader(`[]`)), Request: request}, nil
	case "/repos/acme/api/deployments":
		return &http.Response{StatusCode: http.StatusOK, Header: header, Body: io.NopCloser(strings.NewReader(`[{"id":401,"state":"success","created_at":"2026-07-22T10:00:00Z"}]`)), Request: request}, nil
	case "/repos/acme/api/deployments/401/statuses":
		return &http.Response{StatusCode: http.StatusOK, Header: header, Body: io.NopCloser(strings.NewReader(`[]`)), Request: request}, nil
	}
	return &http.Response{StatusCode: http.StatusOK, Header: header, Body: io.NopCloser(strings.NewReader(`[]`)), Request: request}, nil
}

// githubDeploymentStatusStillRunningDoer serves one deployment whose
// statuses history has only an in_progress entry -- no terminal entry
// exists yet, the shape a deployment carries while it is genuinely still
// running.
type githubDeploymentStatusStillRunningDoer struct{}

func (doer *githubDeploymentStatusStillRunningDoer) Do(request *http.Request) (*http.Response, error) {
	header := http.Header{"Content-Type": {"application/json"}}
	switch request.URL.Path {
	case "/repos/acme/api":
		return &http.Response{StatusCode: http.StatusOK, Header: header, Body: io.NopCloser(strings.NewReader(gitHubRepositoryFixture)), Request: request}, nil
	case "/repos/acme/api/releases":
		return &http.Response{StatusCode: http.StatusOK, Header: header, Body: io.NopCloser(strings.NewReader(`[]`)), Request: request}, nil
	case "/repos/acme/api/deployments":
		return &http.Response{StatusCode: http.StatusOK, Header: header, Body: io.NopCloser(strings.NewReader(`[{"id":501,"created_at":"2026-07-22T10:00:00Z"}]`)), Request: request}, nil
	case "/repos/acme/api/deployments/501/statuses":
		return &http.Response{StatusCode: http.StatusOK, Header: header, Body: io.NopCloser(strings.NewReader(`[{"id":1,"state":"in_progress","created_at":"2026-07-22T10:01:00Z"}]`)), Request: request}, nil
	}
	return &http.Response{StatusCode: http.StatusOK, Header: header, Body: io.NopCloser(strings.NewReader(`[]`)), Request: request}, nil
}

// TestGitHubDeploymentsRoutePersistsRunningStatusForFlameReader is this
// PR's own load-bearing proof: a genuinely still-running GitHub deployment
// (no terminal status entry recorded yet) must persist a real "in_progress"
// status, not nil -- internal/queryapi/flame's own
// deploymentStatusIsRunning treats a nil Status as terminal (its own doc
// comment: "never invents a duration for a status it cannot affirmatively
// confirm is still running"), so a writer that leaves Status nil for a
// running deployment silently defeats the reader's entire fix. Note this
// deployment's own list-response payload carries NO state/status field at
// all (github.com's List Deployments response never does -- only a
// statuses_url) -- deploymentLatestStatus, not normalizeGitHubDeployment's
// list-derived optionalString(deployment.State, deployment.Status), is the
// only source that can ever produce a non-nil Status for a GitHub row.
func TestGitHubDeploymentsRoutePersistsRunningStatusForFlameReader(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC)
	doer := &githubDeploymentStatusStillRunningDoer{}
	client := gitHubRepositoryClient(t, doer, "https://api.github.com")
	claim := nativeTestClaim("github", "deployments")
	batch, err := (GitHubDeploymentsRouteHandler{}).Collect(context.Background(), claim, providerfoundation.Credential{}, client, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.Effects) != 1 || len(batch.Effects[0].Rows) != 1 {
		t.Fatalf("effects=%+v", batch.Effects)
	}
	var row deploymentRow
	if err := json.Unmarshal(batch.Effects[0].Rows[0], &row); err != nil {
		t.Fatal(err)
	}
	if row.Status == nil || *row.Status != "in_progress" {
		t.Fatalf("row.Status=%v want=%q -- a still-running deployment must persist its real status, or the flame reader's request-clock fallback can never fire for it", row.Status, "in_progress")
	}
	if row.StartedAt == nil {
		t.Fatalf("row.StartedAt is nil despite an in_progress status entry: %+v", row)
	}
	if row.FinishedAt != nil {
		t.Fatalf("row.FinishedAt=%v want nil: no terminal status entry exists yet", row.FinishedAt)
	}
}

// TestGitHubDeploymentsRouteSkipsRemainingStatusLookupsAfterRateLimitExhaustion
// pins the class ruling: once one deployment's statuses lookup exhausts its
// retry budget on a rate-limited response, every remaining deployment in
// the same sync unit skips its own statuses call outright (leaving NULLs)
// rather than each paying the same exhausted backoff again -- and the
// skip is reported once, with the count, not once per deployment.
func TestGitHubDeploymentsRouteSkipsRemainingStatusLookupsAfterRateLimitExhaustion(t *testing.T) {
	// Not t.Parallel(): captureSlog swaps the process-global slog default,
	// which a concurrently running captureSlog test would race.
	log := captureSlog(t)
	now := time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC)
	doer := &githubDeploymentStatusRateLimitedDoer{}
	client := gitHubRepositoryClient(t, doer, "https://api.github.com")
	claim := nativeTestClaim("github", "deployments")
	batch, err := (GitHubDeploymentsRouteHandler{}).Collect(context.Background(), claim, providerfoundation.Credential{}, client, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.Effects) != 1 || len(batch.Effects[0].Rows) != 2 {
		t.Fatalf("effects=%+v", batch.Effects)
	}
	var rows []deploymentRow
	for _, raw := range batch.Effects[0].Rows {
		var row deploymentRow
		if err := json.Unmarshal(raw, &row); err != nil {
			t.Fatal(err)
		}
		rows = append(rows, row)
	}
	for _, row := range rows {
		if row.StartedAt != nil || row.FinishedAt != nil {
			t.Fatalf("row %s carries a lifecycle timestamp despite the rate-limit skip: %+v", row.DeploymentID, row)
		}
		if !row.LifecycleLookupFailed {
			t.Fatalf("row %s is not marked LifecycleLookupFailed despite a failed/skipped statuses lookup -- the write-once carry-forward guard would never protect it", row.DeploymentID)
		}
	}
	if doer.statusesRequests != 1 {
		t.Fatalf("statusesRequests=%d, want exactly 1 (the second deployment's lookup must be skipped, not attempted)", doer.statusesRequests)
	}
	if !strings.Contains(log.String(), "github_deployments.status_lookup_rate_limited_skip") || !strings.Contains(log.String(), "count=1") {
		t.Fatalf("expected a status_lookup_rate_limited_skip log line with count=1, got: %s", log.String())
	}
}

type githubDeploymentStatusRateLimitedDoer struct {
	statusesRequests int
}

func (doer *githubDeploymentStatusRateLimitedDoer) Do(request *http.Request) (*http.Response, error) {
	header := http.Header{"Content-Type": {"application/json"}}
	switch request.URL.Path {
	case "/repos/acme/api":
		return &http.Response{StatusCode: http.StatusOK, Header: header, Body: io.NopCloser(strings.NewReader(gitHubRepositoryFixture)), Request: request}, nil
	case "/repos/acme/api/releases":
		return &http.Response{StatusCode: http.StatusOK, Header: header, Body: io.NopCloser(strings.NewReader(`[]`)), Request: request}, nil
	case "/repos/acme/api/deployments":
		body := `[{"id":501,"state":"success","created_at":"2026-07-22T10:00:00Z"},{"id":502,"state":"success","created_at":"2026-07-22T10:00:00Z"}]`
		return &http.Response{StatusCode: http.StatusOK, Header: header, Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
	case "/repos/acme/api/deployments/501/statuses":
		doer.statusesRequests++
		return &http.Response{StatusCode: http.StatusTooManyRequests, Header: header, Body: io.NopCloser(strings.NewReader(`{"message":"rate limited"}`)), Request: request}, nil
	case "/repos/acme/api/deployments/502/statuses":
		doer.statusesRequests++
		return &http.Response{StatusCode: http.StatusOK, Header: header, Body: io.NopCloser(strings.NewReader(`[{"id":1,"state":"success","created_at":"2026-07-22T10:01:00Z"}]`)), Request: request}, nil
	}
	return &http.Response{StatusCode: http.StatusOK, Header: header, Body: io.NopCloser(strings.NewReader(`[]`)), Request: request}, nil
}

// TestGitHubDeploymentsRouteLogsWhenStatusPagesAreTruncated pins that a
// deployment whose status history exceeds maxDeploymentStatusPages logs
// the truncation instead of staying silent about it: the derived
// started_at/finished_at is honest for what was fetched, but may not be
// the true earliest/latest entry.
func TestGitHubDeploymentsRouteLogsWhenStatusPagesAreTruncated(t *testing.T) {
	// Not t.Parallel(): captureSlog swaps the process-global slog default,
	// which a concurrently running captureSlog test would race.
	log := captureSlog(t)
	now := time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC)
	doer := &githubDeploymentStatusTruncatedDoer{}
	client := gitHubRepositoryClient(t, doer, "https://api.github.com")
	claim := nativeTestClaim("github", "deployments")
	batch, err := (GitHubDeploymentsRouteHandler{}).Collect(context.Background(), claim, providerfoundation.Credential{}, client, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.Effects) != 1 || len(batch.Effects[0].Rows) != 1 {
		t.Fatalf("effects=%+v", batch.Effects)
	}
	if doer.statusesPagesFetched != 3 {
		t.Fatalf("statusesPagesFetched=%d, want exactly 3 (maxDeploymentStatusPages)", doer.statusesPagesFetched)
	}
	if !strings.Contains(log.String(), "github_deployments.status_lookup_truncated") || !strings.Contains(log.String(), "deployment_id=601") {
		t.Fatalf("expected a status_lookup_truncated log line for deployment_id=601, got: %s", log.String())
	}
}

type githubDeploymentStatusTruncatedDoer struct {
	statusesPagesFetched int
}

func (doer *githubDeploymentStatusTruncatedDoer) Do(request *http.Request) (*http.Response, error) {
	header := http.Header{"Content-Type": {"application/json"}}
	switch {
	case request.URL.Path == "/repos/acme/api":
		return &http.Response{StatusCode: http.StatusOK, Header: header, Body: io.NopCloser(strings.NewReader(gitHubRepositoryFixture)), Request: request}, nil
	case request.URL.Path == "/repos/acme/api/releases":
		return &http.Response{StatusCode: http.StatusOK, Header: header, Body: io.NopCloser(strings.NewReader(`[]`)), Request: request}, nil
	case request.URL.Path == "/repos/acme/api/deployments":
		return &http.Response{StatusCode: http.StatusOK, Header: header, Body: io.NopCloser(strings.NewReader(`[{"id":601,"state":"success","created_at":"2026-07-22T10:00:00Z"}]`)), Request: request}, nil
	case request.URL.Path == "/repos/acme/api/deployments/601/statuses":
		doer.statusesPagesFetched++
		page := request.URL.Query().Get("page")
		if page == "" {
			page = "1"
		}
		next := *request.URL
		forward := next.Query()
		forward.Set("page", nextPageNumber(page))
		next.RawQuery = forward.Encode()
		header.Set("Link", "<"+next.String()+`>; rel="next"`)
		body := `[{"id":` + page + `,"state":"success","created_at":"2026-07-22T10:0` + page + `:00Z"}]`
		return &http.Response{StatusCode: http.StatusOK, Header: header, Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
	}
	return &http.Response{StatusCode: http.StatusOK, Header: header, Body: io.NopCloser(strings.NewReader(`[]`)), Request: request}, nil
}

func nextPageNumber(page string) string {
	switch page {
	case "1":
		return "2"
	case "2":
		return "3"
	default:
		return "4"
	}
}

type githubDeploymentStatusFailingDoer struct{}

func (doer *githubDeploymentStatusFailingDoer) Do(request *http.Request) (*http.Response, error) {
	header := http.Header{"Content-Type": {"application/json"}}
	switch request.URL.Path {
	case "/repos/acme/api":
		return &http.Response{StatusCode: http.StatusOK, Header: header, Body: io.NopCloser(strings.NewReader(gitHubRepositoryFixture)), Request: request}, nil
	case "/repos/acme/api/releases":
		return &http.Response{StatusCode: http.StatusOK, Header: header, Body: io.NopCloser(strings.NewReader(`[]`)), Request: request}, nil
	case "/repos/acme/api/deployments":
		return &http.Response{StatusCode: http.StatusOK, Header: header, Body: io.NopCloser(strings.NewReader(`[{"id":301,"state":"success","created_at":"2026-07-22T10:00:00Z"}]`)), Request: request}, nil
	case "/repos/acme/api/deployments/301/statuses":
		return &http.Response{StatusCode: http.StatusInternalServerError, Header: header, Body: io.NopCloser(strings.NewReader(`{"message":"boom"}`)), Request: request}, nil
	}
	return &http.Response{StatusCode: http.StatusOK, Header: header, Body: io.NopCloser(strings.NewReader(`[]`)), Request: request}, nil
}
