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
