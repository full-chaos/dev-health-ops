package providersync

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// githubTestsLateArtifactDoer models the case CHAOS-5045 r1 raised as P1: a run
// whose artifact listing is EMPTY when its own window collects it, and which
// only becomes listable LATER, without the run's updated_at ever changing.
//
// GitHub gives no documented bound on that listing lag, and the artifact payload
// carries no availability timestamp we could check instead.
type githubTestsLateArtifactDoer struct {
	t             *testing.T
	archive       []byte
	updatedAt     string
	artifactsLive bool // false => listing is still empty
	requests      []string
}

func (doer *githubTestsLateArtifactDoer) Do(request *http.Request) (*http.Response, error) {
	doer.t.Helper()
	doer.requests = append(doer.requests, request.URL.Path)
	status := http.StatusOK
	header := http.Header{"Content-Type": {"application/json"}}
	body := "{}"
	switch request.URL.Path {
	case "/repos/acme/api":
		body = gitHubRepositoryFixture
	case "/repos/acme/api/actions/runs":
		body = `{"workflow_runs":[{"id":9001,"name":"CI","status":"completed","conclusion":"success",` +
			`"created_at":"2026-07-22T10:00:00Z","run_started_at":"2026-07-22T10:01:00Z",` +
			`"updated_at":"` + doer.updatedAt + `","run_attempt":1,"event":"push","head_sha":"abc",` +
			`"head_branch":"main","html_url":"https://github.com/acme/api/actions/runs/9001","pull_requests":[]}]}`
	case "/repos/acme/api/actions/runs/9001/jobs":
		body = `{"jobs":[{"id":11,"name":"unit","status":"completed","conclusion":"success",` +
			`"started_at":"2026-07-22T10:01:00Z","completed_at":"2026-07-22T10:04:00Z","labels":["ubuntu-latest"]}]}`
	case "/repos/acme/api/actions/runs/9001/artifacts":
		if !doer.artifactsLive {
			body = `{"artifacts":[]}` // published, but not yet listable
			break
		}
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
	return &http.Response{StatusCode: status, Header: header, Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
}

func (doer *githubTestsLateArtifactDoer) listedArtifacts() bool {
	for _, p := range doer.requests {
		if p == "/repos/acme/api/actions/runs/9001/artifacts" {
			return true
		}
	}
	return false
}

// CHAOS-5045 r1 P1. A report artifact that becomes listable AFTER the run's
// updated_at has settled must still reach ClickHouse eventually. Before this
// change the unbounded lower bound re-collected it on the next pass; the
// updated_at bound skips it, and after midnight the day-granular server filter
// stops returning the run at all, so the rows are lost permanently and the
// watermark has already advanced over the gap.
//
// The assertion is deliberately OUTCOME-based, not mechanism-based: it does not
// care whether the fix re-fetches, hashes content, or withholds the watermark —
// only that a late-published report is not silently lost forever.
func TestGitHubTestsLatePublishedArtifactIsNotLostForever(t *testing.T) {
	t.Parallel()
	normalizedAt := time.Date(2026, 7, 22, 13, 5, 0, 0, time.UTC)
	archive := githubTestsZip(t, map[string]string{"junit.xml": githubTestsJUnitFixture})
	const settled = "2026-07-22T10:44:59Z" // one second outside window 2's grace

	// Window 1 collects the run while its artifact listing is still empty.
	first := &githubTestsLateArtifactDoer{t: t, archive: archive, updatedAt: settled, artifactsLive: false}
	claim1 := githubTestsWindowClaim(t,
		time.Date(2026, 7, 22, 10, 0, 0, 0, time.UTC),
		time.Date(2026, 7, 22, 11, 0, 0, 0, time.UTC))
	if _, cases := collectLateArtifact(t, first, claim1, normalizedAt); cases != 0 {
		t.Fatalf("window 1 should see no cases yet, got %d", cases)
	}
	if !first.listedArtifacts() {
		t.Fatal("window 1 never listed artifacts at all")
	}

	// The artifact is now listable. The run itself did not change.
	second := &githubTestsLateArtifactDoer{t: t, archive: archive, updatedAt: settled, artifactsLive: true}
	claim2 := githubTestsWindowClaim(t,
		time.Date(2026, 7, 22, 11, 0, 0, 0, time.UTC),
		time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC))
	_, cases := collectLateArtifact(t, second, claim2, normalizedAt)

	if !second.listedArtifacts() {
		t.Fatalf("window 2 never listed the run's artifacts, so the late-published report "+
			"can never be collected: requests=%v", second.requests)
	}
	if cases == 0 {
		t.Fatal("window 2 listed artifacts but projected no cases: the late report was still lost")
	}
}

func collectLateArtifact(
	t *testing.T, doer *githubTestsLateArtifactDoer, claim Claim, normalizedAt time.Time,
) (suites, cases int) {
	t.Helper()
	err := GitHubTestsRouteHandler{}.CollectChunks(
		context.Background(), claim, providerfoundation.Credential{},
		githubTestsClient(t, doer), normalizedAt, "",
		func(emission ChunkRouteEmission) error {
			for _, effect := range emission.Batch.Effects {
				switch effect.Destination {
				case "test_suite_results":
					rows, err := decodeEffectRows[testSuiteResultRow](effect)
					if err != nil {
						return err
					}
					suites += len(rows)
				case "test_case_results":
					rows, err := decodeEffectRows[testCaseResultRow](effect)
					if err != nil {
						return err
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
