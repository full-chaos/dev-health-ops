package providersync

import (
	"context"
	"errors"
	"net/http"
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
