package providersync

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

type gitLabAllRefPipeline struct {
	ID, Ref                             string
	Created, Started, Finished, Updated string
	ReportStatus                        int
	ReportPad                           int
	ArtifactJobs                        []gitLabAllRefArtifactJob
}

type gitLabAllRefArtifactJob struct {
	ID        string
	Oversized bool
	Gone      bool
}

// gitLabAllRefDoer serves a project and its pipelines. The pipelines listing honours
// updated_after / updated_before on each pipeline's Updated stamp, as GitLab does.
type gitLabAllRefDoer struct {
	t         *testing.T
	archive   []byte
	pipelines []gitLabAllRefPipeline
	listings  []string
	requests  []string
}

func gitLabAllRefStamp(value string) time.Time {
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		panic(err)
	}
	return parsed
}

func (doer *gitLabAllRefDoer) Do(request *http.Request) (*http.Response, error) {
	doer.t.Helper()
	status, header, body := http.StatusOK, http.Header{"Content-Type": {"application/json"}}, "{}"
	path := request.URL.Path
	contentLength := int64(-1)
	findPipeline := func(id string) gitLabAllRefPipeline {
		for _, pipeline := range doer.pipelines {
			if pipeline.ID == id {
				return pipeline
			}
		}
		doer.t.Fatalf("unknown pipeline %s", id)
		return gitLabAllRefPipeline{}
	}
	switch {
	case path == "/api/v4/projects/123":
		body = `{"id":123,"path_with_namespace":"acme/api","default_branch":"main","only_allow_merge_if_pipeline_succeeds":true}`
	case path == "/api/v4/projects/123/pipelines":
		doer.listings = append(doer.listings, request.URL.RawQuery)
		query := request.URL.Query()
		items := []string{}
		for _, pipeline := range doer.pipelines {
			updated := gitLabAllRefStamp(pipeline.Updated)
			if raw := query.Get("updated_after"); raw != "" && updated.Before(gitLabAllRefStamp(raw)) {
				continue
			}
			if raw := query.Get("updated_before"); raw != "" && updated.After(gitLabAllRefStamp(raw)) {
				continue
			}
			items = append(items, `{"id":`+pipeline.ID+`,"name":"CI","ref":"`+pipeline.Ref+`","status":"success",`+
				`"created_at":"`+pipeline.Created+`","started_at":"`+pipeline.Started+`","finished_at":"`+pipeline.Finished+`",`+
				`"source":"merge_request_event","sha":"abc","updated_at":"`+pipeline.Updated+`",`+
				`"web_url":"https://gitlab.example/acme/api/-/pipelines/`+pipeline.ID+`"}`)
		}
		body = "[" + strings.Join(items, ",") + "]"
	case strings.HasSuffix(path, "/merge_requests"):
		body = "[]"
	case strings.HasSuffix(path, "/test_report"):
		id := strings.TrimSuffix(strings.TrimPrefix(path, "/api/v4/projects/123/pipelines/"), "/test_report")
		doer.requests = append(doer.requests, id+" test_report")
		pipeline := findPipeline(id)
		if pipeline.ReportStatus != 0 {
			status, body = pipeline.ReportStatus, `{"message":"refused"}`
			break
		}
		pad := strings.Repeat("x", pipeline.ReportPad)
		body = `{"test_suites":[{"name":"api","total_time":"1.25","test_cases":[{"name":"case-` + id +
			`","classname":"tests.TestAPI","status":"success","execution_time":"1.25","system_output":"` + pad + `"}]}]}`
	case strings.HasSuffix(path, "/jobs"):
		id := strings.TrimSuffix(strings.TrimPrefix(path, "/api/v4/projects/123/pipelines/"), "/jobs")
		pipeline := findPipeline(id)
		if request.URL.Query().Get("include_retried") == "true" {
			doer.requests = append(doer.requests, id+" jobs include_retried")
			body = `[{"id":11,"name":"unit","stage":"test","status":"success","started_at":"` + pipeline.Started +
				`","finished_at":"` + pipeline.Finished + `","runner":{"runner_type":"instance_type"}}]`
			break
		}
		doer.requests = append(doer.requests, id+" jobs")
		jobs := []string{}
		for _, job := range pipeline.ArtifactJobs {
			jobs = append(jobs, `{"id":`+job.ID+`,"name":"unit","artifacts_file":{"filename":"reports.zip"}}`)
		}
		body = "[" + strings.Join(jobs, ",") + "]"
	case strings.Contains(path, "/api/v4/projects/123/jobs/") && strings.HasSuffix(path, "/artifacts"):
		jobID := strings.TrimSuffix(strings.TrimPrefix(path, "/api/v4/projects/123/jobs/"), "/artifacts")
		doer.requests = append(doer.requests, "artifact "+jobID)
		header = http.Header{"Content-Type": {"application/zip"}}
		body = string(doer.archive)
		for _, pipeline := range doer.pipelines {
			for _, job := range pipeline.ArtifactJobs {
				if job.ID != jobID {
					continue
				}
				if job.Oversized {
					contentLength, body = int64(gitLabTestsMaxDownload)+500, ""
				}
				if job.Gone {
					status, body = http.StatusGone, ""
				}
			}
		}
	default:
		doer.t.Fatalf("unexpected request %s", request.URL.String())
	}
	return &http.Response{StatusCode: status, Header: header, ContentLength: contentLength,
		Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
}

type gitLabAllRefWalk struct {
	batch CompleteRouteBatch
	final CompleteRouteBatch
	err   error
}

func gitLabAllRefCollectChunks(t *testing.T, doer *gitLabAllRefDoer, claim Claim) (gitLabAllRefWalk, *providerfoundation.HTTPClient) {
	t.Helper()
	client := gitLabRepositoryClient(t, doer, "https://gitlab.example")
	client.Metrics = providerfoundation.NewMetrics()
	walk := gitLabAllRefWalk{}
	walk.err = (GitLabTestsRouteHandler{}).CollectChunks(context.Background(), claim, providerfoundation.Credential{}, client,
		time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC), "",
		func(emission ChunkRouteEmission) error {
			walk.batch.Effects = append(walk.batch.Effects, emission.Batch.Effects...)
			if emission.Final {
				walk.final = emission.Batch
			}
			return nil
		})
	return walk, client
}

func gitLabAllRefCollect(t *testing.T, doer *gitLabAllRefDoer, claim Claim) CompleteRouteBatch {
	t.Helper()
	client := gitLabRepositoryClient(t, doer, "https://gitlab.example")
	batch, err := (GitLabTestsRouteHandler{}).Collect(context.Background(), claim, providerfoundation.Credential{}, client,
		time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	return batch
}

func gitLabAllRefRowKeys(t *testing.T, batch CompleteRouteBatch, destination string, fields ...string) []string {
	t.Helper()
	var keys []string
	for _, effect := range batch.Effects {
		if effect.Destination != destination {
			continue
		}
		for _, raw := range effect.Rows {
			var row map[string]any
			if err := json.Unmarshal(raw, &row); err != nil {
				t.Fatal(err)
			}
			parts := make([]string, 0, len(fields))
			for _, field := range fields {
				parts = append(parts, field+"="+row[field].(string))
			}
			keys = append(keys, strings.Join(parts, ","))
		}
	}
	slices.Sort(keys)
	return keys
}

func gitLabAllRefRuns(t *testing.T, batch CompleteRouteBatch, destination string) []string {
	t.Helper()
	runs := []string{}
	for _, key := range gitLabAllRefRowKeys(t, batch, destination, "run_id") {
		runs = append(runs, strings.TrimPrefix(key, "run_id="))
	}
	return slices.Compact(runs)
}

func gitLabAllRefPipelines() []gitLabAllRefPipeline {
	at := func(id, ref string) gitLabAllRefPipeline {
		return gitLabAllRefPipeline{ID: id, Ref: ref, Created: "2026-07-22T10:00:00Z", Started: "2026-07-22T10:01:00Z",
			Finished: "2026-07-22T10:05:00Z", Updated: "2026-07-22T10:05:00Z"}
	}
	return []gitLabAllRefPipeline{at("9001", "main"), at("9002", "refs/merge-requests/12/head"), at("9003", "feature/x")}
}

// A merge-request pipeline and a feature-branch pipeline get suite and case rows, on both routes,
// through the same requests the default-branch pipeline makes.
func TestGitLabTestsReportPhaseFetchesPipelinesOfEveryRef(t *testing.T) {
	claim := nativeTestClaim("gitlab", "tests")
	for name, collect := range map[string]func(*testing.T, *gitLabAllRefDoer) CompleteRouteBatch{
		"chunked": func(t *testing.T, doer *gitLabAllRefDoer) CompleteRouteBatch {
			walk, _ := gitLabAllRefCollectChunks(t, doer, claim)
			if walk.err != nil {
				t.Fatal(walk.err)
			}
			return walk.batch
		},
		"non-chunked": func(t *testing.T, doer *gitLabAllRefDoer) CompleteRouteBatch {
			return gitLabAllRefCollect(t, doer, claim)
		},
	} {
		t.Run(name, func(t *testing.T) {
			doer := &gitLabAllRefDoer{t: t, archive: githubTestsZip(t, map[string]string{"junit.xml": githubTestsJUnitFixture}),
				pipelines: gitLabAllRefPipelines()}
			for index := range doer.pipelines {
				doer.pipelines[index].ArtifactJobs = []gitLabAllRefArtifactJob{{ID: "2" + strconv.Itoa(index)}}
			}
			batch := collect(t, doer)
			if runs := gitLabAllRefRuns(t, batch, "test_suite_results"); !slices.Equal(runs, []string{"9001", "9002", "9003"}) {
				t.Fatalf("suite runs = %v, want the default-branch, merge-request and feature pipelines", runs)
			}
			if runs := gitLabAllRefRuns(t, batch, "test_case_results"); !slices.Equal(runs, []string{"9001", "9002", "9003"}) {
				t.Fatalf("case runs = %v", runs)
			}
			for _, id := range []string{"9001", "9002", "9003"} {
				for _, want := range []string{id + " test_report", id + " jobs"} {
					if !slices.Contains(doer.requests, want) {
						t.Fatalf("pipeline %s: no request %q (requests %v)", id, want, doer.requests)
					}
				}
			}
			downloads := 0
			for _, request := range doer.requests {
				if strings.HasPrefix(request, "artifact ") {
					downloads++
				}
			}
			if downloads != 3 {
				t.Fatalf("artifact downloads = %d, want 3 (the same path for every ref)", downloads)
			}
		})
	}
}

// The default-branch pipeline's rows do not change when other refs are present.
func TestGitLabTestsDefaultBranchRowsAreUnchangedByOtherRefs(t *testing.T) {
	claim := nativeTestClaim("gitlab", "tests")
	rowsOf := func(pipelines []gitLabAllRefPipeline) []string {
		doer := &gitLabAllRefDoer{t: t, pipelines: pipelines}
		walk, _ := gitLabAllRefCollectChunks(t, doer, claim)
		if walk.err != nil {
			t.Fatal(walk.err)
		}
		keys := gitLabAllRefRowKeys(t, walk.batch, "test_case_results", "run_id", "suite_id", "case_id")
		return slices.DeleteFunc(keys, func(key string) bool { return !strings.Contains(key, "run_id=9001,") })
	}
	only := rowsOf(gitLabAllRefPipelines()[:1])
	mixed := rowsOf(gitLabAllRefPipelines())
	if len(only) == 0 || !slices.Equal(only, mixed) {
		t.Fatalf("default-branch case keys changed: only=%v mixed=%v", only, mixed)
	}
}

// A merge-request pipeline and a main pipeline on one commit are two runs with two key sets, and a
// second window over the same runs writes the same keys.
func TestGitLabTestsMergeRequestAndMainPipelineKeepDistinctKeysOnReplay(t *testing.T) {
	claim := nativeTestClaim("gitlab", "tests")
	doer := &gitLabAllRefDoer{t: t, pipelines: gitLabAllRefPipelines()[:2]}
	first, _ := gitLabAllRefCollectChunks(t, doer, claim)
	second, _ := gitLabAllRefCollectChunks(t, doer, claim)
	keys := func(batch CompleteRouteBatch) []string {
		return gitLabAllRefRowKeys(t, batch, "test_suite_results", "run_id", "suite_id")
	}
	if len(keys(first.batch)) != 2 || !slices.Equal(keys(first.batch), keys(second.batch)) {
		t.Fatalf("suite keys first=%v second=%v, want 2 distinct keys, equal on replay", keys(first.batch), keys(second.batch))
	}
}

func gitLabAllRefWindow(claim Claim, since, before string) Claim {
	sinceAt, beforeAt := gitLabAllRefStamp(since), gitLabAllRefStamp(before)
	claim.SinceAt, claim.BeforeAt = &sinceAt, &beforeAt
	return claim
}

// A merge-request pipeline that starts at 23:58:30 on day D-1 and finishes at 00:03 on day D has its
// updated_at in D: the D window lists it and writes its report rows; the D-1 window does not list it.
// Both phases send the window as exact timestamps, never a date, and no created_* parameter.
func TestGitLabTestsPipelineCrossingMidnightIsListedInTheWindowOfItsLastUpdate(t *testing.T) {
	crossing := gitLabAllRefPipeline{ID: "9101", Ref: "refs/merge-requests/12/head", Created: "2026-08-01T23:58:00Z",
		Started: "2026-08-01T23:58:30Z", Finished: "2026-08-02T00:03:00Z", Updated: "2026-08-02T00:03:00Z"}
	base := nativeTestClaim("gitlab", "tests")
	dayD := gitLabAllRefWindow(base, "2026-08-02T00:00:00Z", "2026-08-02T01:00:00Z")
	dayBefore := gitLabAllRefWindow(base, "2026-08-01T23:00:00Z", "2026-08-02T00:00:00Z")

	doer := &gitLabAllRefDoer{t: t, pipelines: []gitLabAllRefPipeline{crossing}}
	walk, _ := gitLabAllRefCollectChunks(t, doer, dayD)
	if walk.err != nil {
		t.Fatal(walk.err)
	}
	if runs := gitLabAllRefRuns(t, walk.batch, "test_suite_results"); !slices.Equal(runs, []string{"9101"}) {
		t.Fatalf("day D suite runs = %v, want the crossing pipeline", runs)
	}
	for _, listing := range doer.listings {
		if !strings.Contains(listing, "updated_after=2026-08-02T00%3A00%3A00Z") ||
			!strings.Contains(listing, "updated_before=2026-08-02T01%3A00%3A00Z") || strings.Contains(listing, "created_") {
			t.Fatalf("listing %q is not the exact updated_after/updated_before window", listing)
		}
	}

	before := &gitLabAllRefDoer{t: t, pipelines: []gitLabAllRefPipeline{crossing}}
	earlier, _ := gitLabAllRefCollectChunks(t, before, dayBefore)
	if earlier.err != nil || len(gitLabAllRefRuns(t, earlier.batch, "test_suite_results")) != 0 {
		t.Fatalf("day D-1 listed a pipeline last updated in D: err=%v", earlier.err)
	}

	replay, _ := gitLabAllRefCollectChunks(t, &gitLabAllRefDoer{t: t, pipelines: []gitLabAllRefPipeline{crossing}}, dayD)
	if !slices.Equal(gitLabAllRefRowKeys(t, walk.batch, "test_case_results", "run_id", "suite_id", "case_id"),
		gitLabAllRefRowKeys(t, replay.batch, "test_case_results", "run_id", "suite_id", "case_id")) {
		t.Fatal("the same window replayed wrote different case keys")
	}

	// Pinned current behaviour of the pipelines phase (a separate defect): the crossing pipeline's START is before
	// the window, so it gets no ci_pipeline_runs row while its report rows are written.
	if pipelines := gitLabAllRefRowKeys(t, walk.batch, "ci_pipeline_runs", "run_id"); len(pipelines) != 0 {
		t.Fatalf("ci_pipeline_runs rows = %v: the start-window drop changed; update the limit named in the PR", pipelines)
	}
}

// A one-sided claim keeps its one-sided parameter and a pipeline started after the upper bound is dropped.
func TestGitLabTestsListingKeepsOneSidedBoundsAndDropsLateStarts(t *testing.T) {
	late := gitLabAllRefPipeline{ID: "9201", Ref: "refs/merge-requests/12/head", Created: "2026-07-31T23:59:59.5Z",
		Started: "2026-07-31T23:59:59.9Z", Finished: "2026-08-01T00:05:00Z", Updated: "2026-07-31T23:59:59.9Z"}
	claim := nativeTestClaim("gitlab", "tests")
	doer := &gitLabAllRefDoer{t: t, pipelines: []gitLabAllRefPipeline{late}}
	walk, _ := gitLabAllRefCollectChunks(t, doer, claim)
	if walk.err != nil || len(gitLabAllRefRuns(t, walk.batch, "test_suite_results")) != 0 {
		t.Fatalf("a pipeline started after the upper bound was kept: err=%v", walk.err)
	}
	sinceOnly := claim
	sinceOnly.BeforeAt = nil
	oneSided := &gitLabAllRefDoer{t: t, pipelines: []gitLabAllRefPipeline{late}}
	if walk, _ := gitLabAllRefCollectChunks(t, oneSided, sinceOnly); walk.err != nil {
		t.Fatal(walk.err)
	}
	for _, listing := range oneSided.listings {
		if !strings.Contains(listing, "updated_after=") || strings.Contains(listing, "updated_before=") {
			t.Fatalf("since-only claim listing %q", listing)
		}
	}
}

// An expired report or artifact is absence: no rows, no error, the watermark advances.
func TestGitLabTestsExpiredReportAndArtifactAreAbsence(t *testing.T) {
	claim := nativeTestClaim("gitlab", "tests")
	pipelines := gitLabAllRefPipelines()[1:2]
	pipelines[0].ReportStatus = http.StatusNotFound
	pipelines[0].ArtifactJobs = []gitLabAllRefArtifactJob{{ID: "21", Gone: true}}
	walk, _ := gitLabAllRefCollectChunks(t, &gitLabAllRefDoer{t: t, pipelines: pipelines}, claim)
	if walk.err != nil || len(gitLabAllRefRuns(t, walk.batch, "test_suite_results")) != 0 {
		t.Fatalf("expired items: err=%v", walk.err)
	}
	if walk.final.Watermark == nil || !walk.final.Watermark.Equal(*claim.BeforeAt) {
		t.Fatalf("watermark = %v, want %v", walk.final.Watermark, claim.BeforeAt)
	}
}

func gitLabAllRefPrometheus(t *testing.T, client *providerfoundation.HTTPClient) string {
	t.Helper()
	var buffer bytes.Buffer
	if err := client.Metrics.WritePrometheus(&buffer); err != nil {
		t.Fatal(err)
	}
	return buffer.String()
}

func gitLabAllRefSkipCount(t *testing.T, client *providerfoundation.HTTPClient, reason string) int {
	t.Helper()
	count := 0
	for _, line := range strings.Split(gitLabAllRefPrometheus(t, client), "\n") {
		if strings.HasPrefix(line, "dev_health_provider_artifact_skipped_total{") && strings.Contains(line, `reason="`+reason+`"`) {
			value, err := strconv.Atoi(line[strings.LastIndex(line, " ")+1:])
			if err != nil {
				t.Fatal(err)
			}
			count += value
		}
	}
	return count
}

func gitLabAllRefTruncated(final CompleteRouteBatch) []string {
	raw, _ := final.Result["inventory_truncated"].([]string)
	return raw
}

// S1: one oversize report in a claim with two good reports: the two reports are stored, the watermark
// advances, report_too_large is counted once, and the unit says its coverage is not complete.
func TestGitLabTestsOversizeReportIsSkippedAndTheUnitGoesOn(t *testing.T) {
	for _, ref := range []string{"refs/merge-requests/12/head", "main"} {
		t.Run(ref, func(t *testing.T) {
			claim := nativeTestClaim("gitlab", "tests")
			pipelines := gitLabAllRefPipelines()
			pipelines[1].Ref = ref
			pipelines[1].ReportPad = nativeMaxObjectBytes + 1024
			walk, client := gitLabAllRefCollectChunks(t, &gitLabAllRefDoer{t: t, pipelines: pipelines}, claim)
			if walk.err != nil {
				t.Fatalf("an oversize report failed the claim: %v", walk.err)
			}
			if runs := gitLabAllRefRuns(t, walk.batch, "test_suite_results"); !slices.Equal(runs, []string{"9001", "9003"}) {
				t.Fatalf("suite runs = %v, want the two good reports and none for the oversize run", runs)
			}
			if walk.final.Watermark == nil || !walk.final.Watermark.Equal(*claim.BeforeAt) {
				t.Fatalf("watermark = %v, want %v (not held)", walk.final.Watermark, claim.BeforeAt)
			}
			if walk.final.Result["coverage_complete"] != false || !slices.Equal(gitLabAllRefTruncated(walk.final), []string{"report_too_large"}) {
				t.Fatalf("result = %v", walk.final.Result)
			}
			if got := gitLabAllRefSkipCount(t, client, "report_too_large"); got != 1 {
				t.Fatalf("report_too_large counted %d times, want 1", got)
			}
		})
	}
}

// S2: one oversize artifact (a Content-Length above the cap, nothing read) beside a good one.
func TestGitLabTestsOversizeArtifactIsSkippedAndTheUnitGoesOn(t *testing.T) {
	claim := nativeTestClaim("gitlab", "tests")
	pipelines := gitLabAllRefPipelines()[1:2]
	pipelines[0].ArtifactJobs = []gitLabAllRefArtifactJob{{ID: "21", Oversized: true}, {ID: "22"}}
	doer := &gitLabAllRefDoer{t: t, archive: githubTestsZip(t, map[string]string{"junit.xml": githubTestsJUnitFixture}), pipelines: pipelines}
	walk, client := gitLabAllRefCollectChunks(t, doer, claim)
	if walk.err != nil {
		t.Fatalf("an oversize artifact failed the claim: %v", walk.err)
	}
	if !slices.Contains(doer.requests, "artifact 21") || !slices.Contains(doer.requests, "artifact 22") {
		t.Fatalf("requests %v: the artifact after the oversize one was not fetched", doer.requests)
	}
	if runs := gitLabAllRefRuns(t, walk.batch, "test_suite_results"); !slices.Equal(runs, []string{"9002"}) {
		t.Fatalf("suite runs = %v: the native report must still be stored", runs)
	}
	if walk.final.Watermark == nil || !walk.final.Watermark.Equal(*claim.BeforeAt) {
		t.Fatalf("watermark = %v, want %v (not held)", walk.final.Watermark, claim.BeforeAt)
	}
	if walk.final.Result["coverage_complete"] != false || !slices.Equal(gitLabAllRefTruncated(walk.final), []string{"artifact_oversized"}) {
		t.Fatalf("result = %v", walk.final.Result)
	}
	if got := gitLabAllRefSkipCount(t, client, "artifact_oversized"); got != 1 {
		t.Fatalf("artifact_oversized counted %d times, want 1", got)
	}
}

// S3: only the size case changed. A refused report (403) or a server error still fails the unit.
func TestGitLabTestsRefusedReportStillFailsTheUnit(t *testing.T) {
	for _, status := range []int{http.StatusForbidden, http.StatusInternalServerError, http.StatusTooManyRequests} {
		pipelines := gitLabAllRefPipelines()
		pipelines[1].ReportStatus = status
		walk, _ := gitLabAllRefCollectChunks(t, &gitLabAllRefDoer{t: t, pipelines: pipelines}, nativeTestClaim("gitlab", "tests"))
		if walk.err == nil || errors.Is(walk.err, errGitLabTestsReportTooLarge) {
			t.Fatalf("status %d: err=%v, want the unit to fail", status, walk.err)
		}
	}
}

// S5: the two size components are in the closed vocabulary, advance the watermark, and decode from a checkpoint.
func TestGitLabTestsSizeSkipComponentsAreKnownAndAdvance(t *testing.T) {
	for _, component := range []string{gitLabReportTooLargeComponent, gitLabArtifactOversizedComponent} {
		if !gitLabTestsTruncationKnown(component) {
			t.Fatalf("%s must be in the closed vocabulary", component)
		}
		if gitLabTestsBlocksWatermark([]string{component}) {
			t.Fatalf("%s must NOT block the watermark", component)
		}
		if _, err := decodeGitLabTestsChunkCursor(`{"phase":"pipelines","truncated":["` + component + `"]}`); err != nil {
			t.Fatalf("%s did not decode from a checkpoint: %v", component, err)
		}
	}
}

// The helpers keep their typed errors: an oversize report and artifact are told apart, and both keep the class they had.
func TestGitLabTestsSizeCapErrorsKeepTheirClass(t *testing.T) {
	_, _, err := fetchGitLabTestsReport(context.Background(), sizeClient(t, "gitlab", nativeMaxObjectBytes+10), "/projects/1", "9")
	if !errors.Is(err, errGitLabTestsReportTooLarge) || !errors.Is(err, providerfoundation.ErrNormalizationInvalid) {
		t.Fatalf("report err=%v", err)
	}
	_, err = downloadGitLabTestsArtifact(context.Background(), sizeClient(t, "gitlab", gitLabTestsMaxDownload+10), "/projects/1", "9")
	if !errors.Is(err, errGitLabTestsArtifactTooLarge) || !errors.Is(err, ErrGitLabTestsIncomplete) {
		t.Fatalf("artifact err=%v", err)
	}
}
