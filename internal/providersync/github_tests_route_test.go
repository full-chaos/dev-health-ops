package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

type githubTestsRouteDoer struct {
	t            *testing.T
	archive      []byte
	requests     []string
	capRuns      bool
	failJobs     bool
	headBranch   string
	artifactOnly bool
}

func (doer *githubTestsRouteDoer) Do(request *http.Request) (*http.Response, error) {
	doer.t.Helper()
	doer.requests = append(doer.requests, request.URL.Host+request.URL.Path)
	status := http.StatusOK
	header := http.Header{"Content-Type": {"application/json"}}
	body := "{}"
	switch request.URL.Path {
	case "/repos/acme/api":
		body = gitHubRepositoryFixture
	case "/repos/acme/api/actions/runs":
		if doer.artifactOnly && request.URL.Query().Get("branch") == "" {
			body = `{"workflow_runs":[]}`
			break
		}
		branch := doer.headBranch
		if branch == "" {
			branch = "main"
		}
		if selected := request.URL.Query().Get("branch"); selected != "" && selected != branch {
			body = `{"workflow_runs":[]}`
			break
		}
		body = `{"workflow_runs":[{"id":9001,"name":"CI","status":"completed","conclusion":"success","created_at":"2026-07-22T10:00:00Z","run_started_at":"2026-07-22T10:01:00Z","updated_at":"2026-07-22T10:05:00Z","run_attempt":1,"event":"pull_request","head_sha":"abc","head_branch":"` + branch + `","html_url":"https://github.com/acme/api/actions/runs/9001","pull_requests":[]}]}`
		if doer.capRuns && request.URL.Query().Get("branch") == "" {
			header.Set("Link", `<https://api.github.com/repos/acme/api/actions/runs?page=2>; rel="next"`)
		}
	case "/repos/acme/api/actions/runs/9001/jobs":
		if doer.failJobs {
			status = http.StatusServiceUnavailable
			body = `{"message":"unavailable"}`
			break
		}
		body = `{"jobs":[{"id":11,"name":"unit","status":"completed","conclusion":"success","started_at":"2026-07-22T10:01:00Z","completed_at":"2026-07-22T10:04:00Z","labels":["ubuntu-latest"]}]}`
	case "/repos/acme/api/actions/runs/9001/artifacts":
		body = `{"artifacts":[{"id":77,"expired":false}]}`
	case "/repos/acme/api/actions/artifacts/77/zip":
		status = http.StatusFound
		header.Set("Location", "https://blob.example/report.zip")
		body = ""
	case "/report.zip":
		if request.Header.Get("Authorization") != "" {
			doer.t.Fatal("provider Authorization leaked to artifact blob host")
		}
		header.Set("Content-Type", "application/zip")
		body = string(doer.archive)
	default:
		doer.t.Fatalf("unexpected request %s", request.URL.String())
	}
	return &http.Response{StatusCode: status, Header: header, Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
}

func githubTestsClient(t *testing.T, doer providerfoundation.HTTPDoer) *providerfoundation.HTTPClient {
	t.Helper()
	client, err := providerfoundation.NewHTTPClient("github", "https://api.github.com", doer, func(request *http.Request) error { request.Header.Set("Authorization", "Bearer secret"); return nil }, providerfoundation.RetryPolicy{MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond}, providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }))
	if err != nil {
		t.Fatal(err)
	}
	return client
}

// githubTestsHighVolumeDoer models three complete Actions listing pages. The
// 201st in-window run is deliberate: the historical 200-run default failed
// before it could fetch any jobs, whereas a bounded multi-page default must
// collect all three pages.
type githubTestsHighVolumeDoer struct {
	t               *testing.T
	runListRequests int
}

func (doer *githubTestsHighVolumeDoer) Do(request *http.Request) (*http.Response, error) {
	doer.t.Helper()
	header := http.Header{"Content-Type": {"application/json"}}
	path := request.URL.Path
	switch {
	case path == "/repos/acme/api":
		return githubTestsHTTPResponse(request, header, gitHubRepositoryFixture), nil
	case path == "/repos/acme/api/actions/runs":
		if request.URL.Query().Get("branch") != "" {
			return githubTestsHTTPResponse(request, header, `{"workflow_runs":[]}`), nil
		}
		doer.runListRequests++
		page := 1
		if raw := request.URL.Query().Get("page"); raw != "" {
			parsed, err := strconv.Atoi(raw)
			if err != nil {
				doer.t.Fatalf("invalid page query %q: %v", raw, err)
			}
			page = parsed
		}
		first := (page-1)*nativePerPage + 1
		last := first + nativePerPage - 1
		if page == 3 {
			last = 201
		}
		if page < 1 || page > 3 {
			doer.t.Fatalf("unexpected workflow-runs page %d", page)
		}
		if page < 3 {
			header.Set("Link", "<https://api.github.com/repos/acme/api/actions/runs?page="+strconv.Itoa(page+1)+">; rel=\"next\"")
		}
		return githubTestsHTTPResponse(request, header, githubTestsWorkflowRunsFixture(first, last)), nil
	case strings.HasPrefix(path, "/repos/acme/api/actions/runs/") && strings.HasSuffix(path, "/jobs"):
		return githubTestsHTTPResponse(request, header, `{"jobs":[]}`), nil
	default:
		doer.t.Fatalf("unexpected request %s", request.URL.String())
		return nil, nil
	}
}

func githubTestsHTTPResponse(request *http.Request, header http.Header, body string) *http.Response {
	return &http.Response{StatusCode: http.StatusOK, Header: header, Body: io.NopCloser(strings.NewReader(body)), Request: request}
}

func githubTestsWorkflowRunsFixture(first, last int) string {
	var body strings.Builder
	body.WriteString(`{"workflow_runs":[`)
	for id := first; id <= last; id++ {
		if id > first {
			body.WriteByte(',')
		}
		body.WriteString(`{"id":`)
		body.WriteString(strconv.Itoa(id))
		body.WriteString(`,"name":"CI","status":"completed","conclusion":"success","created_at":"2026-07-22T10:00:00Z","run_started_at":"2026-07-22T10:01:00Z","updated_at":"2026-07-22T10:05:00Z","run_attempt":1,"event":"push","head_sha":"abc","head_branch":"main","html_url":"https://github.com/acme/api/actions/runs/`)
		body.WriteString(strconv.Itoa(id))
		body.WriteString(`","pull_requests":[]}`)
	}
	body.WriteString(`]}`)
	return body.String()
}

func TestGitHubTestsRouteEmitsSixCompleteEffectsAndStripsRedirectAuth(t *testing.T) {
	now := time.Date(2026, 7, 23, 12, 30, 0, 456789000, time.UTC)
	doer := &githubTestsRouteDoer{t: t, archive: githubTestsZip(t, map[string]string{"junit.xml": githubTestsJUnitFixture, "lcov.info": githubTestsLCOVFixture})}
	claim := nativeTestClaim("github", "tests")
	batch, err := (GitHubTestsRouteHandler{}).Collect(context.Background(), claim, providerfoundation.Credential{}, githubTestsClient(t, doer), now)
	if err != nil {
		t.Fatal(err)
	}
	descriptor, _ := Descriptor("github", "tests")
	if err := batch.validate(descriptor); err != nil {
		t.Fatal(err)
	}
	if len(batch.Effects) != 6 {
		t.Fatalf("effects=%d", len(batch.Effects))
	}
	counts := map[string]int{}
	for _, effect := range batch.Effects {
		counts[effect.Destination] = len(effect.Rows)
	}
	for destination, want := range map[string]int{"ci_pipeline_runs": 1, "ci_job_runs": 1, "ci_acceptance_checks": 1, "test_suite_results": 1, "test_case_results": 2, "coverage_snapshots": 1} {
		if counts[destination] != want {
			t.Fatalf("%s rows=%d want=%d", destination, counts[destination], want)
		}
	}
	if batch.Watermark == nil || !batch.Watermark.Equal(*claim.BeforeAt) {
		t.Fatalf("watermark=%v", batch.Watermark)
	}
	if len(doer.requests) != 7 || doer.requests[6] != "blob.example/report.zip" {
		t.Fatalf("requests=%v", doer.requests)
	}
	// cicd and tests delegate to ONE complete-row unit, so this same batch must
	// also satisfy the github/cicd descriptor's destination contract.
	//
	// Only the tests descriptor was asserted before, which left github/cicd's
	// six-destination list entirely unpinned. Found by splitting the mutation
	// entry that spanned both descriptors: while one entry mutated both sites,
	// the assertion above absorbed the kill and the cicd site was never
	// measured -- a wholesale mutation reading as coverage for two sites while
	// covering one.
	cicd, known := Descriptor("github", "cicd")
	if !known {
		t.Fatal("github/cicd capability disappeared")
	}
	if err := batch.validate(cicd); err != nil {
		t.Fatalf("github/cicd descriptor rejects the shared complete batch: %v", err)
	}
}

func TestGitHubTestsRouteExcludesFeatureBranchArtifactsLikePythonProducer(t *testing.T) {
	doer := &githubTestsRouteDoer{t: t, headBranch: "feature", archive: githubTestsZip(t, map[string]string{"junit.xml": githubTestsJUnitFixture})}
	claim := nativeTestClaim("github", "tests")
	batch, err := (GitHubTestsRouteHandler{}).Collect(context.Background(), claim, providerfoundation.Credential{}, githubTestsClient(t, doer), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	for _, effect := range batch.Effects {
		if effect.Destination == "test_suite_results" || effect.Destination == "test_case_results" || effect.Destination == "coverage_snapshots" {
			if len(effect.Rows) != 0 {
				t.Fatalf("feature-branch report reached %s: %+v", effect.Destination, effect.Rows)
			}
		}
	}
	for _, request := range doer.requests {
		if strings.Contains(request, "/artifacts") {
			t.Fatalf("feature-branch artifact inventory was fetched: %v", doer.requests)
		}
	}
}

func TestGitHubTestsRouteKeepsPythonDateFloorArtifactSelectionIndependent(t *testing.T) {
	doer := &githubTestsRouteDoer{t: t, artifactOnly: true, archive: githubTestsZip(t, map[string]string{"junit.xml": githubTestsJUnitFixture})}
	batch, err := (GitHubTestsRouteHandler{}).Collect(context.Background(), nativeTestClaim("github", "tests"), providerfoundation.Credential{}, githubTestsClient(t, doer), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	counts := map[string]int{}
	for _, effect := range batch.Effects {
		counts[effect.Destination] = len(effect.Rows)
	}
	if counts["ci_pipeline_runs"] != 0 || counts["test_suite_results"] != 1 || counts["test_case_results"] != 2 {
		t.Fatalf("independent Python artifact selection lost: %v", counts)
	}
}

func TestGitHubTestsRouteFailsClosedOnRunPaginationCap(t *testing.T) {
	doer := &githubTestsRouteDoer{t: t, capRuns: true}
	claim := nativeTestClaim("github", "tests")
	batch, err := (GitHubTestsRouteHandler{MaxRuns: githubTestsMaxRuns}).Collect(context.Background(), claim, providerfoundation.Credential{}, githubTestsClient(t, doer), time.Now())
	if !errors.Is(err, ErrPaginationCapExceeded) {
		t.Fatalf("error=%v", err)
	}
	if len(batch.Effects) != 0 || batch.Watermark != nil {
		t.Fatalf("partial batch=%+v", batch)
	}
}

func TestGitHubTestsRouteDefaultCollectsMoreThanTwoPagesOfInWindowRuns(t *testing.T) {
	doer := &githubTestsHighVolumeDoer{t: t}
	claim := nativeTestClaim("github", "cicd")
	batch, err := (GitHubTestsRouteHandler{}).Collect(
		context.Background(), claim, providerfoundation.Credential{}, githubTestsClient(t, doer),
		time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatal(err)
	}
	if doer.runListRequests != 3 {
		t.Fatalf("workflow run pages=%d want 3", doer.runListRequests)
	}
	for _, effect := range batch.Effects {
		if effect.Destination == "ci_pipeline_runs" && len(effect.Rows) == 201 {
			return
		}
	}
	t.Fatalf("did not retain all 201 in-window workflow runs: %+v", batch.Effects)
}

func TestGitHubTestsRouteFetchFailureCannotCommitEffectsOrWatermark(t *testing.T) {
	doer := &githubTestsRouteDoer{t: t, failJobs: true}
	claim := nativeTestClaim("github", "tests")
	batch, err := (GitHubTestsRouteHandler{}).Collect(context.Background(), claim, providerfoundation.Credential{}, githubTestsClient(t, doer), time.Now())
	if err == nil {
		t.Fatal("expected job inventory failure")
	}
	if len(batch.Effects) != 0 || batch.Watermark != nil {
		t.Fatalf("partial batch=%+v", batch)
	}
}

func TestGitHubTestsRoutePreservesValidReportsAndRecordsSkippedMember(t *testing.T) {
	now := time.Date(2026, 8, 12, 12, 30, 0, 0, time.UTC)
	doer := &githubTestsRouteDoer{t: t, archive: githubTestsZip(t, map[string]string{
		"reports/good.xml":  githubTestsJUnitFixture,
		"reports/good.info": githubTestsLCOVFixture,
		"reports/bad.xml":   `<!DOCTYPE x [<!ENTITY x "boom">]><testsuite>&x;</testsuite>`,
	})}
	claim := nativeTestClaim("github", "tests")
	batch, err := (GitHubTestsRouteHandler{}).Collect(
		context.Background(), claim, providerfoundation.Credential{},
		githubTestsClient(t, doer), now,
	)
	if err != nil {
		t.Fatal(err)
	}
	counts := map[string]int{}
	for _, effect := range batch.Effects {
		counts[effect.Destination] = len(effect.Rows)
	}
	if counts["test_suite_results"] != 1 || counts["test_case_results"] != 2 ||
		counts["coverage_snapshots"] != 1 {
		t.Fatalf("valid report rows were lost: %v", counts)
	}
	if batch.Watermark != nil {
		t.Fatalf("incomplete report inventory advanced watermark=%v", batch.Watermark)
	}
	encoded, marshalErr := json.Marshal(batch.Result)
	if marshalErr != nil {
		t.Fatal(marshalErr)
	}
	var durable struct {
		ReportsComplete bool                    `json:"reports_complete"`
		ReportsSkipped  int                     `json:"reports_skipped"`
		Incomplete      []GitHubTestsIncomplete `json:"incomplete"`
	}
	if err := json.Unmarshal(encoded, &durable); err != nil {
		t.Fatal(err)
	}
	wantIncomplete := []GitHubTestsIncomplete{{
		Component: "report_member", Cause: "malformed", Count: 1,
	}}
	if durable.ReportsComplete || durable.ReportsSkipped != 1 ||
		!reflect.DeepEqual(durable.Incomplete, wantIncomplete) {
		t.Fatalf("durable result=%+v", durable)
	}
	if batch.Result["reports_complete"] != false || batch.Result["reports_skipped"] != 1 {
		t.Fatalf("result=%+v", batch.Result)
	}
	comparison, compareErr := (ProductionContractComparator{}).CompareCompleteRoute(
		context.Background(), claim, batch,
	)
	if compareErr != nil || !comparison.Match || comparison.NativeRecords != 7 ||
		comparison.PythonRecords != 7 {
		t.Fatalf("comparison=%+v error=%v", comparison, compareErr)
	}
	invalidWatermark := batch
	invalidWatermark.Watermark = claim.BeforeAt
	if _, err := (ProductionContractComparator{}).CompareCompleteRoute(
		context.Background(), claim, invalidWatermark,
	); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("incomplete watermark comparison error=%v", err)
	}
	invalidCount := batch
	invalidCount.Result = make(map[string]any, len(batch.Result))
	for key, value := range batch.Result {
		invalidCount.Result[key] = value
	}
	invalidCount.Result["reports_skipped"] = 2
	if _, err := (ProductionContractComparator{}).CompareCompleteRoute(
		context.Background(), claim, invalidCount,
	); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("inconsistent skipped count comparison error=%v", err)
	}
	invalidComplete := batch
	invalidComplete.Result = make(map[string]any, len(batch.Result))
	for key, value := range batch.Result {
		invalidComplete.Result[key] = value
	}
	invalidComplete.Result["reports_complete"] = true
	if _, err := (ProductionContractComparator{}).CompareCompleteRoute(
		context.Background(), claim, invalidComplete,
	); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("false complete comparison error=%v", err)
	}
}

func TestGitHubTestsRouteUnsafeArchiveFailureRemainsFailClosed(t *testing.T) {
	doer := &githubTestsRouteDoer{t: t, archive: []byte("not a zip archive")}
	claim := nativeTestClaim("github", "tests")
	batch, err := (GitHubTestsRouteHandler{}).Collect(
		context.Background(), claim, providerfoundation.Credential{},
		githubTestsClient(t, doer), time.Now(),
	)
	if !errors.Is(err, ErrGitHubTestsIncomplete) {
		t.Fatalf("error=%v", err)
	}
	if len(batch.Effects) != 0 || batch.Watermark != nil || batch.Result != nil {
		t.Fatalf("unsafe archive returned partial batch=%+v", batch)
	}
}

func TestGitHubTestsRouteRetryProducesStableEffects(t *testing.T) {
	now := time.Date(2026, 7, 23, 12, 30, 0, 456789000, time.UTC)
	claim := nativeTestClaim("github", "tests")
	collect := func() CompleteRouteBatch {
		doer := &githubTestsRouteDoer{t: t, archive: githubTestsZip(t, map[string]string{"junit.xml": githubTestsJUnitFixture, "lcov.info": githubTestsLCOVFixture})}
		batch, err := (GitHubTestsRouteHandler{}).Collect(context.Background(), claim, providerfoundation.Credential{}, githubTestsClient(t, doer), now)
		if err != nil {
			t.Fatal(err)
		}
		return batch
	}
	first, second := collect(), collect()
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("retry changed effect identity or payload\nfirst=%+v\nsecond=%+v", first, second)
	}
}

func TestGitHubCICDAndTestsAliasesEmitTheSameCompleteEffects(t *testing.T) {
	now := time.Date(2026, 7, 23, 12, 30, 0, 456789000, time.UTC)
	collect := func(dataset string) CompleteRouteBatch {
		doer := &githubTestsRouteDoer{t: t, archive: githubTestsZip(t, map[string]string{"junit.xml": githubTestsJUnitFixture, "lcov.info": githubTestsLCOVFixture})}
		batch, err := (GitHubTestsRouteHandler{}).Collect(context.Background(), nativeTestClaim("github", dataset), providerfoundation.Credential{}, githubTestsClient(t, doer), now)
		if err != nil {
			t.Fatal(err)
		}
		return batch
	}
	testsBatch, cicdBatch := collect("tests"), collect("cicd")
	if !reflect.DeepEqual(testsBatch.Effects, cicdBatch.Effects) {
		t.Fatalf("shared complete unit diverged\ntests=%+v\ncicd=%+v", testsBatch.Effects, cicdBatch.Effects)
	}
	testsPlan := ProviderRequestPlan("github", "tests", 1, nil)
	cicdPlan := ProviderRequestPlan("github", "cicd", 1, nil)
	if !reflect.DeepEqual(cicdPlan, testsPlan) {
		t.Fatalf("shared complete unit admission identity diverged\ntests=%+v\ncicd=%+v", testsPlan, cicdPlan)
	}
	for _, estimate := range testsPlan {
		if estimate.RouteFamily != "tests" {
			t.Fatalf("shared complete unit route family = %q, want tests", estimate.RouteFamily)
		}
	}
}

func TestGitHubTestsNormalizesEveryProviderTimestampBeforeHashing(t *testing.T) {
	created := "2026-07-22T10:00:00.123456789Z"
	started := "2026-07-22T10:01:00.234567891Z"
	finished := "2026-07-22T10:05:00.345678912Z"
	claim := nativeTestClaim("github", "tests")
	pipeline, ok := normalizeGitHubTestsPipeline(claim, "c7198fbc-1945-3717-05d8-eb78866b4e79", gitHubWorkflowRunPayload{ID: "1", CreatedAt: &created, RunStartedAt: &started, UpdatedAt: &finished}, time.Now())
	if !ok {
		t.Fatal("pipeline excluded")
	}
	job, ok := normalizeGitHubTestsJob(claim, pipeline.RepoID, pipeline.RunID, 0, githubTestsJobPayload{ID: "2", StartedAt: &started, CompletedAt: &finished}, time.Now())
	if !ok {
		t.Fatal("job excluded")
	}
	for name, value := range map[string]*time.Time{"queued": pipeline.QueuedAt, "pipeline_started": &pipeline.StartedAt, "pipeline_finished": pipeline.FinishedAt, "job_started": job.StartedAt, "job_finished": job.FinishedAt} {
		if value == nil || value.Nanosecond()%int(time.Millisecond) != 0 {
			t.Fatalf("%s was not normalized before effect hashing: %v", name, value)
		}
	}
}
