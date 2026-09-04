package providersync

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// githubTestsReportWindowDoer serves one default-branch run whose updated_at
// the test controls. It records every path so a test can assert on the ABSENCE
// of the per-run artifacts call, which is the observable that separates "we
// skipped this run" from "we re-downloaded it and happened to emit the same
// rows" -- the latter still costs the provider request and still rewrites the
// ReplacingMergeTree row with a new last_synced.
type githubTestsReportWindowDoer struct {
	t             *testing.T
	archive       []byte
	updatedAt     string
	omitUpdatedAt bool
	requests      []string
}

func (doer *githubTestsReportWindowDoer) Do(request *http.Request) (*http.Response, error) {
	doer.t.Helper()
	doer.requests = append(doer.requests, request.URL.Path)
	status := http.StatusOK
	header := http.Header{"Content-Type": {"application/json"}}
	body := "{}"
	switch request.URL.Path {
	case "/repos/acme/api":
		body = gitHubRepositoryFixture
	case "/repos/acme/api/actions/runs":
		updated := `,"updated_at":"` + doer.updatedAt + `"`
		if doer.omitUpdatedAt {
			updated = ""
		}
		body = `{"workflow_runs":[{"id":9001,"name":"CI","status":"completed","conclusion":"success",` +
			`"created_at":"2026-07-22T10:00:00Z","run_started_at":"2026-07-22T10:01:00Z"` + updated +
			`,"run_attempt":1,"event":"push","head_sha":"abc","head_branch":"main",` +
			`"html_url":"https://github.com/acme/api/actions/runs/9001","pull_requests":[]}]}`
	case "/repos/acme/api/actions/runs/9001/jobs":
		body = `{"jobs":[{"id":11,"name":"unit","status":"completed","conclusion":"success",` +
			`"started_at":"2026-07-22T10:01:00Z","completed_at":"2026-07-22T10:04:00Z","labels":["ubuntu-latest"]}]}`
	case "/repos/acme/api/actions/runs/9001/artifacts":
		body = `{"artifacts":[{"id":77,"expired":false}]}`
	case "/repos/acme/api/actions/artifacts/77/zip":
		status = http.StatusFound
		header.Set("Location", "https://blob.example/report.zip")
		body = ""
	case "/report.zip":
		header.Set("Content-Type", "application/zip")
		body = string(doer.archive)
	default:
		doer.t.Fatalf("unexpected request %s", request.URL.String())
	}
	return &http.Response{
		StatusCode: status, Header: header,
		Body: io.NopCloser(strings.NewReader(body)), Request: request,
	}, nil
}

func (doer *githubTestsReportWindowDoer) fetchedArtifacts() bool {
	for _, path := range doer.requests {
		if path == "/repos/acme/api/actions/runs/9001/artifacts" {
			return true
		}
	}
	return false
}

// githubTestsWindowClaim is nativeTestClaim narrowed to one hourly incremental
// window, which is the shape production actually dispatches (sync_run_units
// rows carry since_at = the previous hour, before_at = this hour).
func githubTestsWindowClaim(t *testing.T, since, before time.Time) Claim {
	t.Helper()
	claim := nativeTestClaim("github", "tests")
	claim.SinceAt, claim.BeforeAt = &since, &before
	return claim
}

// collectGitHubTestsReportRows runs the production (chunked) route for one
// window and returns the projected suite/case counts plus the doer.
func collectGitHubTestsReportRows(
	t *testing.T, doer *githubTestsReportWindowDoer, claim Claim, normalizedAt time.Time,
) (suites, cases int) {
	t.Helper()
	err := GitHubTestsRouteHandler{}.CollectChunks(
		context.Background(), claim, providerfoundation.Credential{},
		githubTestsClient(t, doer), normalizedAt, "",
		func(emission ChunkRouteEmission) error {
			for _, effect := range emission.Batch.Effects {
				switch effect.Destination {
				case "test_suite_results":
					rows, decodeErr := decodeEffectRows[testSuiteResultRow](effect)
					if decodeErr != nil {
						return decodeErr
					}
					suites += len(rows)
				case "test_case_results":
					rows, decodeErr := decodeEffectRows[testCaseResultRow](effect)
					if decodeErr != nil {
						return decodeErr
					}
					cases += len(rows)
				}
			}
			return nil
		},
	)
	if err != nil {
		t.Fatalf("CollectChunks: %v", err)
	}
	return suites, cases
}

func githubTestsReportWindowDoerFor(t *testing.T, updatedAt string) *githubTestsReportWindowDoer {
	t.Helper()
	return &githubTestsReportWindowDoer{
		t: t, updatedAt: updatedAt,
		archive: githubTestsZip(t, map[string]string{"junit.xml": githubTestsJUnitFixture}),
	}
}

// CHAOS-5045. The report phase's server-side run filter is date-granular
// (created>=<day of SinceAt>) and is pinned as a Python-parity observation, so
// GitHub keeps returning a run for every remaining pass of the same day. Before
// the updated_at lower bound, each of those passes re-downloaded the artifact
// and re-emitted every case row with a fresh last_synced -- on one production
// repo, 7,072,971 raw test_case_results rows for 3,795,833 distinct keys.
//
// Window 2 must skip the run outright: no artifacts request, no rows.
func TestGitHubTestsReportPhaseSkipsARunUnchangedSinceAnEarlierWindow(t *testing.T) {
	t.Parallel()
	normalizedAt := time.Date(2026, 7, 22, 13, 5, 0, 0, time.UTC)

	first := githubTestsReportWindowDoerFor(t, "2026-07-22T10:05:00Z")
	suites, cases := collectGitHubTestsReportRows(t, first,
		githubTestsWindowClaim(t,
			time.Date(2026, 7, 22, 10, 0, 0, 0, time.UTC),
			time.Date(2026, 7, 22, 11, 0, 0, 0, time.UTC)),
		normalizedAt)
	if !first.fetchedArtifacts() {
		t.Fatal("window 1 never fetched the run's artifacts")
	}
	if suites == 0 || cases == 0 {
		t.Fatalf("window 1 projected suites=%d cases=%d, want both > 0", suites, cases)
	}

	// Window 2: the run is unchanged, and its updated_at is older than
	// SinceAt minus the indexing grace.
	second := githubTestsReportWindowDoerFor(t, "2026-07-22T10:05:00Z")
	repeatSuites, repeatCases := collectGitHubTestsReportRows(t, second,
		githubTestsWindowClaim(t,
			time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC),
			time.Date(2026, 7, 22, 13, 0, 0, 0, time.UTC)),
		normalizedAt)
	if second.fetchedArtifacts() {
		t.Fatalf("window 2 re-downloaded an unchanged run's artifacts: %v", second.requests)
	}
	if repeatSuites != 0 || repeatCases != 0 {
		t.Fatalf("window 2 re-projected suites=%d cases=%d, want 0/0 -- "+
			"every re-projection rewrites the ReplacingMergeTree row with a new last_synced",
			repeatSuites, repeatCases)
	}
}

// The parity constraint: narrowing the window must never drop a run that
// legitimately changed. A re-run, a retried job, or a late-arriving suite all
// bump the run's updated_at, and GitHub reports that bumped value on the very
// next listing -- so the run lands inside the new window and is re-collected.
func TestGitHubTestsReportPhaseStillCollectsARunUpdatedInsideTheWindow(t *testing.T) {
	t.Parallel()
	normalizedAt := time.Date(2026, 7, 22, 13, 5, 0, 0, time.UTC)

	// Same run id, same start, but updated_at moved into window 2 (a re-run).
	doer := githubTestsReportWindowDoerFor(t, "2026-07-22T12:30:00Z")
	suites, cases := collectGitHubTestsReportRows(t, doer,
		githubTestsWindowClaim(t,
			time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC),
			time.Date(2026, 7, 22, 13, 0, 0, 0, time.UTC)),
		normalizedAt)
	if !doer.fetchedArtifacts() {
		t.Fatalf("a run updated inside the window was skipped: %v", doer.requests)
	}
	if suites == 0 || cases == 0 {
		t.Fatalf("re-run projected suites=%d cases=%d, want both > 0", suites, cases)
	}
}

// The grace band. GitHub publishes no bound on how long after a run completes
// its artifacts become listable, so githubTestsArtifactIndexingGrace gives a
// run exactly one more window. A run updated just before SinceAt is still
// offered; one updated well before it is not.
func TestGitHubTestsReportPhaseGraceCoversOneWindowOfIndexingLag(t *testing.T) {
	t.Parallel()
	normalizedAt := time.Date(2026, 7, 22, 13, 5, 0, 0, time.UTC)
	since := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	before := time.Date(2026, 7, 22, 13, 0, 0, 0, time.UTC)

	for _, testCase := range []struct {
		name      string
		updatedAt string
		want      bool
	}{
		{"inside the grace band", "2026-07-22T11:50:00Z", true},
		{"exactly at the grace boundary", "2026-07-22T11:45:00Z", true},
		{"one second before the boundary", "2026-07-22T11:44:59Z", false},
		{"well before the boundary", "2026-07-22T11:00:00Z", false},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			doer := githubTestsReportWindowDoerFor(t, testCase.updatedAt)
			collectGitHubTestsReportRows(t, doer,
				githubTestsWindowClaim(t, since, before), normalizedAt)
			if got := doer.fetchedArtifacts(); got != testCase.want {
				t.Fatalf("updated_at=%s fetched artifacts=%v, want %v (grace=%s)",
					testCase.updatedAt, got, testCase.want, githubTestsArtifactIndexingGrace)
			}
		})
	}
}

// Fail-open, deliberately: a run we cannot date is a run we must not silently
// drop. A payload with no updated_at keeps the pre-CHAOS-5045 disposition.
func TestGitHubTestsReportPhaseNeverSkipsARunWithoutAnUpdatedAt(t *testing.T) {
	t.Parallel()
	doer := githubTestsReportWindowDoerFor(t, "")
	doer.omitUpdatedAt = true
	collectGitHubTestsReportRows(t, doer,
		githubTestsWindowClaim(t,
			time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC),
			time.Date(2026, 7, 22, 13, 0, 0, 0, time.UTC)),
		time.Date(2026, 7, 22, 13, 5, 0, 0, time.UTC))
	if !doer.fetchedArtifacts() {
		t.Fatalf("a run with no updated_at was skipped: %v", doer.requests)
	}
}

// A backfill claim carries no SinceAt. It must filter nothing, or the first
// sync of a repository would collect no reports at all.
func TestGitHubTestsReportPhaseFiltersNothingWithoutASinceBound(t *testing.T) {
	t.Parallel()
	claim := nativeTestClaim("github", "tests")
	claim.SinceAt = nil
	doer := githubTestsReportWindowDoerFor(t, "2020-01-01T00:00:00Z")
	if _, cases := collectGitHubTestsReportRows(t, doer, claim,
		time.Date(2026, 7, 22, 13, 5, 0, 0, time.UTC)); cases == 0 {
		t.Fatalf("backfill claim projected no cases: %v", doer.requests)
	}
}

// The non-chunked Collect is production-dead for cicd/tests but survives as the
// oracle/comparison implementation, so the two must share one disposition --
// otherwise the parity oracle grades the fixed route against the unfixed one.
func TestGitHubTestsReportWindowDispositionIsSharedByBothRoutes(t *testing.T) {
	t.Parallel()
	since := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	before := time.Date(2026, 7, 22, 13, 0, 0, 0, time.UTC)
	claim := githubTestsWindowClaim(t, since, before)
	stale := time.Date(2026, 7, 22, 10, 5, 0, 0, time.UTC)
	fresh := time.Date(2026, 7, 22, 12, 30, 0, 0, time.UTC)
	started := time.Date(2026, 7, 22, 10, 1, 0, 0, time.UTC)

	if !githubTestsReportRunOutsideWindow(
		githubTestsPipelineRow{StartedAt: started, FinishedAt: &stale}, claim) {
		t.Fatal("a run unchanged since an earlier window was treated as in-window")
	}
	if githubTestsReportRunOutsideWindow(
		githubTestsPipelineRow{StartedAt: started, FinishedAt: &fresh}, claim) {
		t.Fatal("a run updated inside the window was treated as out-of-window")
	}
	// The pre-existing upper bound is retained, not replaced.
	late := time.Date(2026, 7, 22, 14, 0, 0, 0, time.UTC)
	if !githubTestsReportRunOutsideWindow(
		githubTestsPipelineRow{StartedAt: late, FinishedAt: &late}, claim) {
		t.Fatal("a run started after BeforeAt was treated as in-window")
	}
}

// GitLab is the in-repo precedent this fix mirrors: its report phase bounds
// BOTH ends on updated_at, server-side (updated_after/updated_before), which is
// why the shared six-destination projection never duplicated on that provider.
// Asserting the live query here keeps the two providers' report windows from
// drifting apart silently the next time either route is touched.
func TestGitLabTestsReportPhaseBoundsBothEndsOnUpdatedAt(t *testing.T) {
	t.Parallel()
	doer := &gitLabTestsRouteDoer{t: t, archive: githubTestsZip(t, map[string]string{
		"coverage.info": githubTestsLCOVFixture,
	})}
	claim := nativeTestClaim("gitlab", "tests")
	since := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	before := time.Date(2026, 7, 22, 13, 0, 0, 0, time.UTC)
	claim.SinceAt, claim.BeforeAt = &since, &before

	if err := (GitLabTestsRouteHandler{}).CollectChunks(
		context.Background(), claim, providerfoundation.Credential{},
		gitLabRepositoryClient(t, doer, "https://gitlab.example"),
		time.Date(2026, 7, 22, 13, 5, 0, 0, time.UTC), "",
		func(ChunkRouteEmission) error { return nil },
	); err != nil {
		t.Fatalf("CollectChunks: %v", err)
	}

	listings := 0
	for _, raw := range doer.requests {
		parsed, err := url.Parse(raw)
		if err != nil {
			t.Fatalf("unparsable recorded request %q: %v", raw, err)
		}
		if parsed.Path != "/api/v4/projects/123/pipelines" {
			continue
		}
		listings++
		query := parsed.Query()
		if query.Get("updated_after") == "" || query.Get("updated_before") == "" {
			t.Fatalf("pipeline listing %q lost a window bound -- "+
				"the GitHub report phase duplicated for exactly this reason (CHAOS-5045)", raw)
		}
	}
	if listings == 0 {
		t.Fatalf("no pipeline listing was issued; requests=%v", doer.requests)
	}
}
