package providersync

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// githubTestsExpiredCommitDoer is githubTestsExpiredClassDoer plus a switch that makes every artifact download answer 503, and a
// per-artifact download counter.
type githubTestsExpiredCommitDoer struct {
	t         *testing.T
	artifacts string
	archive   []byte
	failZip   bool
	zipHits   map[string]int
}

func (doer *githubTestsExpiredCommitDoer) Do(request *http.Request) (*http.Response, error) {
	doer.t.Helper()
	header := http.Header{"Content-Type": {"application/json"}}
	path := request.URL.Path
	switch {
	case path == "/repos/acme/api":
		return githubTestsHTTPResponse(request, header, gitHubRepositoryFixture), nil
	case path == "/repos/acme/api/actions/runs":
		return githubTestsHTTPResponse(request, header, githubTestsWorkflowRunsFixture(1, 1)), nil
	case strings.HasSuffix(path, "/jobs"):
		return githubTestsHTTPResponse(request, header, `{"jobs":[]}`), nil
	case strings.HasSuffix(path, "/artifacts"):
		return githubTestsHTTPResponse(request, header, doer.artifacts), nil
	case strings.HasPrefix(path, "/repos/acme/api/actions/artifacts/") && strings.HasSuffix(path, "/zip"):
		if doer.zipHits == nil {
			doer.zipHits = map[string]int{}
		}
		doer.zipHits[path]++
		if doer.failZip {
			return &http.Response{StatusCode: http.StatusServiceUnavailable, Header: header, Body: io.NopCloser(strings.NewReader(`{}`)), Request: request}, nil
		}
		return &http.Response{
			StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"application/zip"}},
			Body: io.NopCloser(bytes.NewReader(doer.archive)), Request: request,
		}, nil
	}
	doer.t.Fatalf("unexpected request %s", request.URL.String())
	return nil, nil
}

func githubTestsExpiredMetricText(t *testing.T, metrics *providerfoundation.Metrics) string {
	t.Helper()
	var buffer bytes.Buffer
	if err := metrics.WritePrometheus(&buffer); err != nil {
		t.Fatalf("WritePrometheus: %v", err)
	}
	return buffer.String()
}

// CHAOS-13980 (codex round 2): a failed attempt must not charge the expired skip. One expired artifact and one readable artifact
// whose download fails; the unit is retried and completes. The route-level counter stays free of artifact_expired on both
// attempts (providerunit counts the committed observation once, after Complete).
func TestGitHubTestsExpiredMetricIsNotChargedByAFailedAttempt(t *testing.T) {
	artifacts := githubTestsExpiredArtifactsJSON(githubTestsExpiredEntry(1, "test-results-1", false), githubTestsExpiredEntry(2, "test-results-2", true))
	normalizedAt := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	for _, route := range []string{"chunked", "nonchunked/tests", "nonchunked/cicd"} {
		t.Run(route, func(t *testing.T) {
			doer := &githubTestsExpiredCommitDoer{t: t, artifacts: artifacts, archive: githubTestsHealthyArchive(t), failZip: true}
			client := githubTestsClient(t, fakehttp.Client(doer))
			client.Metrics = providerfoundation.NewMetrics()
			attempt := func() (CompleteRouteBatch, error) {
				if route == "chunked" {
					walk, err := walkGitHubTestsChunksResult(t, client, 4)
					return walk.final, err
				}
				claim := nativeTestClaim("github", strings.TrimPrefix(route, "nonchunked/"))
				return (GitHubTestsRouteHandler{}).Collect(context.Background(), claim, providerfoundation.Credential{}, client, normalizedAt)
			}
			if _, err := attempt(); err == nil {
				t.Fatal("first attempt: want the failing download to fail the unit")
			}
			doer.failZip = false
			batch, err := attempt()
			if err != nil {
				t.Fatalf("retry: %v", err)
			}
			if incomplete, _ := batch.Result["incomplete"].([]GitHubTestsIncomplete); len(incomplete) != 1 || incomplete[0].Count != 1 {
				t.Fatalf("retry incomplete=%+v, want one expired observation of count 1", incomplete)
			}
			if text := githubTestsExpiredMetricText(t, client.Metrics); strings.Contains(text, `reason="artifact_expired"`) {
				t.Errorf("route counted the expired skip (once per attempt, charged again by the retry):\n%s", text)
			}
		})
	}
}

// CHAOS-13980 (codex round 2): a clean non-chunked completion carries no skip-marker fields, exactly as before; the expired case
// carries all four.
func TestGitHubTestsNonChunkedResultCarriesSkipFieldsOnlyWithEvidence(t *testing.T) {
	normalizedAt := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	keys := []string{"skipped_artifacts", "skipped_artifacts_overflow", "skipped_artifact_cause_overflow", "skipped_artifact_cause_count"}
	for _, dataset := range []string{"tests", "cicd"} {
		for name, artifacts := range map[string]string{
			"clean":   githubTestsExpiredArtifactsJSON(githubTestsExpiredEntry(1, "test-results-1", false)),
			"expired": githubTestsExpiredArtifactsJSON(githubTestsExpiredEntry(1, "test-results-1", false), githubTestsExpiredEntry(2, "test-results-2", true)),
		} {
			t.Run(dataset+"/"+name, func(t *testing.T) {
				doer := &githubTestsExpiredCommitDoer{t: t, artifacts: artifacts, archive: githubTestsHealthyArchive(t)}
				client := githubTestsClient(t, fakehttp.Client(doer))
				batch, err := (GitHubTestsRouteHandler{}).Collect(context.Background(), nativeTestClaim("github", dataset), providerfoundation.Credential{}, client, normalizedAt)
				if err != nil {
					t.Fatal(err)
				}
				for _, key := range keys {
					if _, present := batch.Result[key]; present != (name == "expired") {
						t.Errorf("result key %q present=%v on a %s completion", key, present, name)
					}
				}
			})
		}
	}
}

// CHAOS-13980 (acp ruling): newest-per-name stays. A newer attempt that expired next to an older readable attempt of the same name
// is ONE artifact_expired loss; the older attempt is never downloaded and its data is never used.
func TestGitHubTestsNewerExpiredAttemptIsALossAndOlderIsNeverDownloaded(t *testing.T) {
	artifacts := githubTestsExpiredArtifactsJSON(githubTestsExpiredEntry(1, "test-results", false), githubTestsExpiredEntry(2, "test-results", true))
	normalizedAt := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	for _, route := range []string{"chunked", "nonchunked"} {
		for _, dataset := range []string{"tests", "cicd"} {
			t.Run(route+"/"+dataset, func(t *testing.T) {
				doer := &githubTestsExpiredCommitDoer{t: t, artifacts: artifacts, archive: githubTestsHealthyArchive(t)}
				client := githubTestsClient(t, fakehttp.Client(doer))
				client.Metrics = providerfoundation.NewMetrics()
				claim := nativeTestClaim("github", dataset)
				var batch CompleteRouteBatch
				if route == "chunked" {
					batch = walkGitHubTestsChunks(t, GitHubTestsRouteHandler{}, claim, client, 4).final
				} else {
					var err error
					if batch, err = (GitHubTestsRouteHandler{}).Collect(context.Background(), claim, providerfoundation.Credential{}, client, normalizedAt); err != nil {
						t.Fatal(err)
					}
				}
				if len(doer.zipHits) != 0 {
					t.Errorf("downloads=%v, want none for that name (the older attempt is superseded, the newer is expired)", doer.zipHits)
				}
				incomplete, _ := batch.Result["incomplete"].([]GitHubTestsIncomplete)
				if len(incomplete) != 1 || incomplete[0].Cause != githubTestsArtifactExpiredCause || incomplete[0].Count != 1 {
					t.Errorf("incomplete=%+v, want one report_member/artifact_expired", incomplete)
				}
				if _, err := (ProductionContractComparator{}).CompareCompleteRoute(context.Background(), claim, batch); err != nil {
					t.Errorf("production comparator rejected the completion: %v", err)
				}
			})
		}
	}
}

// CHAOS-13980 (acp delta read): any skip evidence keeps all four fields, including per-cause counts with an empty marker sample
// (a state the route cannot reach today, so the builder is called directly).
func TestAddGitHubTestsNonChunkedSkipFieldsGatesOnAnyEvidence(t *testing.T) {
	keys := []string{"skipped_artifacts", "skipped_artifacts_overflow", "skipped_artifact_cause_overflow", "skipped_artifact_cause_count"}
	cases := map[string]struct {
		skipped       []GitHubTestsSkippedArtifact
		overflow      int
		causeOverflow map[string]bool
		causeCount    map[string]int
		want          bool
	}{
		"none":                   {want: false},
		"empty non-nil maps":     {causeOverflow: map[string]bool{}, causeCount: map[string]int{}, want: false},
		"marker":                 {skipped: []GitHubTestsSkippedArtifact{{RunID: "1", ArtifactID: "2", Cause: githubTestsArtifactExpiredCause}}, want: true},
		"overflow only":          {overflow: 1, want: true},
		"cause overflow only":    {causeOverflow: map[string]bool{githubTestsArtifactExpiredCause: true}, want: true},
		"cause count, no sample": {causeCount: map[string]int{githubTestsArtifactExpiredCause: 2}, want: true},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			result := map[string]any{}
			addGitHubTestsNonChunkedSkipFields(result, tc.skipped, tc.overflow, tc.causeOverflow, tc.causeCount)
			for _, key := range keys {
				if _, present := result[key]; present != tc.want {
					t.Errorf("key %q present=%v, want %v", key, present, tc.want)
				}
			}
		})
	}
}
