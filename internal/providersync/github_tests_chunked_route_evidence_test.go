package providersync

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// TestGitHubTestsChunkRouteCountsFailedAndRetriedAttempts verifies that
// cursor.Requests has a single source -- the counting Doer at the HTTP
// boundary, added to whatever the cursor already carried from prior chunk
// invocations -- not a per-page or per-loop tally: the initial repo fetch
// fails its first wire attempt and succeeds on retry, and the exact count
// below (which includes that extra attempt) would go red on a double count
// or a dropped one.
func TestGitHubTestsChunkRouteCountsFailedAndRetriedAttempts(t *testing.T) {
	repoAttempts := 0
	doer := jiraWorkItemsDoerFunc(func(request *http.Request) (*http.Response, error) {
		header := http.Header{"Content-Type": {"application/json"}}
		switch request.URL.Path {
		case "/repos/acme/api":
			repoAttempts++
			if repoAttempts == 1 {
				return nil, errors.New("simulated transient transport failure")
			}
			return githubTestsHTTPResponse(request, header, gitHubRepositoryFixture), nil
		case "/repos/acme/api/actions/runs":
			return githubTestsHTTPResponse(request, header, githubTestsWorkflowRunsFixture(9001, 9001)), nil
		case "/repos/acme/api/actions/runs/9001/jobs":
			return githubTestsHTTPResponse(request, header, `{"jobs":[]}`), nil
		case "/repos/acme/api/actions/runs/9001/artifacts":
			return githubTestsHTTPResponse(request, header, `{"artifacts":[]}`), nil
		default:
			t.Fatalf("unexpected request %s", request.URL.String())
			return nil, nil
		}
	})
	client, err := providerfoundation.NewHTTPClient(
		"github", "https://api.github.com", doer,
		func(*http.Request) error { return nil },
		providerfoundation.RetryPolicy{
			MaxAttempts: 2, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond,
		},
		providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
	)
	if err != nil {
		t.Fatal(err)
	}
	claim := nativeTestClaim("github", "tests")
	walk := walkGitHubTestsChunks(t, GitHubTestsRouteHandler{}, claim, client, 10)

	if repoAttempts != 2 {
		t.Fatalf("repo attempts=%d want 2 (one failed, one retried)", repoAttempts)
	}
	if walk.cursor.Phase != "done" {
		t.Fatalf("terminal phase=%q, want done", walk.cursor.Phase)
	}
	// 2 (repo, retried) + 1 (runs-phase listing) + 1 (jobs) + 1
	// (artifacts-phase listing) + 1 (artifacts listing) = 6.
	if walk.cursor.Requests != 6 {
		t.Fatalf("cursor=%+v want Requests=6 (every physical attempt, from one source)", walk.cursor)
	}
}

// TestGitHubTestsChunkRouteCountsListingPagesWhoseRunsAllFailNormalization
// proves that a runs-listing page fetch is a real
// physical wire attempt even when every run on it is normalize-excluded
// (normalizeGitHubTestsPipeline's include=false, here because the run has
// neither created_at nor run_started_at) before it can ever reach a per-run
// job or artifact fetch -- the OLD code's ONLY two cursor.Requests refresh
// points, one per phase. Two listing pages, each with one such run, so
// NEITHER phase's refresh ever fires under the old code -- yet two real
// listing requests were made per phase.
func TestGitHubTestsChunkRouteCountsListingPagesWhoseRunsAllFailNormalization(t *testing.T) {
	unnormalizableRun := func(id int) string {
		return `{"id":` + strconv.Itoa(id) + `,"name":"CI","status":"completed","conclusion":"success","event":"push","head_sha":"abc","head_branch":"main","html_url":"https://github.com/acme/api/actions/runs/` + strconv.Itoa(id) + `","pull_requests":[]}`
	}
	runsPage := 0
	doer := jiraWorkItemsDoerFunc(func(request *http.Request) (*http.Response, error) {
		header := http.Header{"Content-Type": {"application/json"}}
		switch request.URL.Path {
		case "/repos/acme/api":
			return githubTestsHTTPResponse(request, header, gitHubRepositoryFixture), nil
		case "/repos/acme/api/actions/runs":
			runsPage++
			if runsPage == 1 {
				header.Set("Link", `<https://api.github.com/repos/acme/api/actions/runs?page=2>; rel="next"`)
				return githubTestsHTTPResponse(request, header, `{"workflow_runs":[`+unnormalizableRun(9001)+`]}`), nil
			}
			return githubTestsHTTPResponse(request, header, `{"workflow_runs":[`+unnormalizableRun(9002)+`]}`), nil
		default:
			t.Fatalf("unexpected request %s (every run fails normalization; no job/artifact fetch should ever fire)", request.URL.String())
			return nil, nil
		}
	})
	client, err := providerfoundation.NewHTTPClient(
		"github", "https://api.github.com", doer,
		func(*http.Request) error { return nil },
		providerfoundation.RetryPolicy{
			MaxAttempts: 2, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond,
		},
		providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
	)
	if err != nil {
		t.Fatal(err)
	}
	claim := nativeTestClaim("github", "tests")
	walk := walkGitHubTestsChunks(t, GitHubTestsRouteHandler{}, claim, client, 10)

	// The artifacts phase re-lists the same /actions/runs endpoint (a 3rd
	// hit) -- runsPage covers both phases.
	if runsPage != 3 {
		t.Fatalf("runs listing pages served=%d want 3 (2 runs-phase + 1 artifacts-phase)", runsPage)
	}
	if walk.cursor.Phase != "done" {
		t.Fatalf("terminal phase=%q, want done", walk.cursor.Phase)
	}
	// 1 (repo) + 2 (runs-phase listing pages) + 1 (artifacts-phase listing) =
	// 4. No job or artifact fetch ever fires because every run fails
	// normalization in both phases.
	if walk.cursor.Requests != 4 {
		t.Fatalf("cursor=%+v want Requests=4 (every listing page counted even though no item ever reached the old refresh point)", walk.cursor)
	}
}

// TestGitHubTestsChunkRouteJobsFetchExhaustsTheRetryPolicyOnPermanentFailure
// is the failed-final-attempt half of the same proof: a per-run jobs fetch
// that fails every attempt spends exactly the retry policy's attempt budget
// on the shared counting Doer, not a single logical try -- the chunked
// route fails the whole invocation closed here, so this checks the real
// wire attempts directly rather than cursor.Requests.
func TestGitHubTestsChunkRouteJobsFetchExhaustsTheRetryPolicyOnPermanentFailure(t *testing.T) {
	jobsAttempts := 0
	doer := jiraWorkItemsDoerFunc(func(request *http.Request) (*http.Response, error) {
		header := http.Header{"Content-Type": {"application/json"}}
		switch request.URL.Path {
		case "/repos/acme/api":
			return githubTestsHTTPResponse(request, header, gitHubRepositoryFixture), nil
		case "/repos/acme/api/actions/runs":
			return githubTestsHTTPResponse(request, header, githubTestsWorkflowRunsFixture(9001, 9001)), nil
		case "/repos/acme/api/actions/runs/9001/jobs":
			jobsAttempts++
			return nil, errors.New("simulated transient transport failure")
		default:
			t.Fatalf("unexpected request %s", request.URL.String())
			return nil, nil
		}
	})
	client, err := providerfoundation.NewHTTPClient(
		"github", "https://api.github.com", doer,
		func(*http.Request) error { return nil },
		providerfoundation.RetryPolicy{
			MaxAttempts: 2, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond,
		},
		providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
	)
	if err != nil {
		t.Fatal(err)
	}
	claim := nativeTestClaim("github", "tests")
	err = (GitHubTestsRouteHandler{}).CollectChunks(
		context.Background(), claim, providerfoundation.Credential{}, client,
		time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC), "",
		func(ChunkRouteEmission) error { return nil },
	)
	if err == nil {
		t.Fatal("want an error from the exhausted jobs fetch")
	}
	if jobsAttempts != 2 {
		t.Fatalf("jobs attempts=%d want 2 (both exhausted by the retry policy, none dropped)", jobsAttempts)
	}
}
