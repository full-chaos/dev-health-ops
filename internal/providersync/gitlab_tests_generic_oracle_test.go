package providersync

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

var gitLabTestsGoOnlyFields = map[string]string{
	"last_synced": "stamped by the Go complete-route effect boundary",
}

func gitLabTestsOracleCase() oracleCase {
	return oracleCase{ID: "active_gitlab_testops_rows", Input: map[string]any{
		"repo_id": "c7198fbc-1945-3717-05d8-eb78866b4e79", "org_id": "org-acme", "run_id": "9001",
		"since_at": "2026-07-22T00:00:00Z", "before_at": "2026-07-23T00:00:00Z",
		"started_at": "2026-07-22T10:01:00Z", "finished_at": "2026-07-22T10:05:00Z",
		"raw_pipeline": map[string]any{
			"id": 9001, "name": "CI", "ref": "main", "status": "success",
			"created_at": "2026-07-22T10:00:00Z", "started_at": "2026-07-22T10:01:00Z",
			"finished_at": "2026-07-22T10:05:00Z", "source": "push", "sha": "abc",
			"web_url": "https://gitlab.test/acme/api/-/pipelines/9001",
		},
		"raw_job": map[string]any{
			"id": 11, "name": "unit", "stage": "test", "status": "success",
			"started_at": "2026-07-22T10:01:00Z", "finished_at": "2026-07-22T10:04:00Z",
			"runner": map[string]any{"runner_type": "instance_type"}, "retried": true,
		},
		"native_report": map[string]any{"test_suites": []any{
			map[string]any{"name": "api", "total_time": "bad-duration", "test_cases": []any{
				map[string]any{"name": "fails", "classname": "tests.TestAPI", "status": "failed", "execution_time": "bad-duration", "system_output": "stderr"},
			}},
		}},
		"lcov": "SF:services/api/main.go\nLF:2\nLH:1\nBRF:2\nBRH:1\nFNF:1\nFNH:1\nend_of_record\n",
	}}
}

func gitLabTestsGoRows(t *testing.T, input map[string]any) (githubTestsPipelineRow, githubTestsJobRow, githubTestsAcceptanceRow, testSuiteResultRow, testCaseResultRow, coverageSnapshotRow) {
	t.Helper()
	decode := func(key string, target any) {
		encoded, err := json.Marshal(input[key])
		if err != nil {
			t.Fatal(err)
		}
		decoder := json.NewDecoder(bytes.NewReader(encoded))
		decoder.UseNumber()
		if err := decoder.Decode(target); err != nil {
			t.Fatal(err)
		}
	}
	var rawPipeline gitLabTestsPipelinePayload
	var rawJob gitLabTestsJobPayload
	var report gitLabTestsReportPayload
	decode("raw_pipeline", &rawPipeline)
	decode("raw_job", &rawJob)
	decode("native_report", &report)
	claim := nativeTestClaim("gitlab", "tests")
	claim.OrgID = input["org_id"].(string)
	at := time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC)
	pipeline, ok := normalizeGitLabTestsPipeline(claim, input["repo_id"].(string), rawPipeline, at)
	if !ok {
		t.Fatal("pipeline excluded")
	}
	job, ok := normalizeGitLabTestsJob(claim, input["repo_id"].(string), pipeline.RunID, rawJob, at)
	if !ok {
		t.Fatal("job excluded")
	}
	checks := projectGitLabTestsChecks(claim, pipeline.RepoID, pipeline, rawPipeline, []githubTestsJobRow{job}, map[string]struct{}{"pipeline": {}}, "gitlab.project_merge_policy", at)
	var acceptance githubTestsAcceptanceRow
	for _, check := range checks {
		if check.CheckName == "unit" {
			acceptance = check
		}
	}
	started, _ := time.Parse(time.RFC3339, input["started_at"].(string))
	finished, _ := time.Parse(time.RFC3339, input["finished_at"].(string))
	suites, cases, err := normalizeGitLabNativeTestReport(claim, pipeline.RepoID, pipeline.RunID, report, &started, &finished, at)
	if err != nil || len(suites) != 1 || len(cases) != 1 {
		t.Fatalf("native rows suites=%+v cases=%+v err=%v", suites, cases, err)
	}
	coverage, err := parseLCOVRow([]byte(input["lcov"].(string)), "reports/lcov.info", pipeline.RepoID, pipeline.RunID, claim.OrgID, at)
	if err != nil {
		t.Fatal(err)
	}
	return pipeline, job, acceptance, suites[0], cases[0], coverage
}

func TestGenericOracleMatchesActivePythonGitLabTestsRows(t *testing.T) {
	testCase := gitLabTestsOracleCase()
	compareRowsAgainstPythonOracle(t, "gitlab/tests/pipeline", []oracleCase{testCase}, func(t *testing.T, input map[string]any) githubTestsPipelineRow {
		a, _, _, _, _, _ := gitLabTestsGoRows(t, input)
		return a
	}, gitLabTestsGoOnlyFields)
	compareRowsAgainstPythonOracle(t, "gitlab/tests/job", []oracleCase{testCase}, func(t *testing.T, input map[string]any) githubTestsJobRow {
		_, a, _, _, _, _ := gitLabTestsGoRows(t, input)
		return a
	}, gitLabTestsGoOnlyFields)
	compareRowsAgainstPythonOracle(t, "gitlab/tests/acceptance", []oracleCase{testCase}, func(t *testing.T, input map[string]any) githubTestsAcceptanceRow {
		_, _, a, _, _, _ := gitLabTestsGoRows(t, input)
		return a
	}, gitLabTestsGoOnlyFields)
	compareRowsAgainstPythonOracle(t, "gitlab/tests/suite", []oracleCase{testCase}, func(t *testing.T, input map[string]any) testSuiteResultRow {
		_, _, _, a, _, _ := gitLabTestsGoRows(t, input)
		return a
	}, gitLabTestsGoOnlyFields)
	compareRowsAgainstPythonOracle(t, "gitlab/tests/case", []oracleCase{testCase}, func(t *testing.T, input map[string]any) testCaseResultRow {
		_, _, _, _, a, _ := gitLabTestsGoRows(t, input)
		return a
	}, gitLabTestsGoOnlyFields)
	compareRowsAgainstPythonOracle(t, "gitlab/tests/coverage", []oracleCase{testCase}, func(t *testing.T, input map[string]any) coverageSnapshotRow {
		_, _, _, _, _, a := gitLabTestsGoRows(t, input)
		return a
	}, gitLabTestsGoOnlyFields)
}

type gitLabTestsSelectionObservation struct {
	ReportRunIDs      []string                      `json:"report_run_ids"`
	CoverageRunIDs    []string                      `json:"coverage_run_ids"`
	ArtifactJobIDs    []string                      `json:"artifact_job_ids"`
	AdapterRunIDs     []string                      `json:"adapter_run_ids"`
	UsageObservations []gitLabTestsUsageObservation `json:"usage_observations"`
	MaxPipelines      int                           `json:"max_pipelines"`
	MaxArtifacts      int                           `json:"max_artifacts"`
}

type gitLabTestsUsageObservation struct {
	Transport    string `json:"transport"`
	RouteFamily  string `json:"route_family"`
	Dimension    string `json:"dimension"`
	RequestCount int    `json:"request_count"`
}

type gitLabTestsSelectionDoerFunc func(*http.Request) (*http.Response, error)

func (do gitLabTestsSelectionDoerFunc) Do(request *http.Request) (*http.Response, error) {
	return do(request)
}

func TestGenericOracleMatchesActivePythonGitLabTestsSelection(t *testing.T) {
	jobs := make([]any, 0, gitLabTestsMaxArtifacts+2)
	jobs = append(jobs, map[string]any{"id": "empty", "artifacts_file": map[string]any{}, "artifacts": []any{}})
	for id := 1; id <= gitLabTestsMaxArtifacts+1; id++ {
		jobs = append(jobs, map[string]any{"id": id, "artifacts_file": map[string]any{"filename": "reports.zip"}})
	}
	testCase := oracleCase{ID: "filter_cap_window_artifacts_and_usage", Input: map[string]any{
		"since_at": "2026-07-22T00:00:00Z", "before_at": "2026-07-23T00:00:00.0005Z",
		"default_branch": "main", "max_pipelines": 3, "job_run_id": "old-main",
		"raw_pipelines": []any{
			map[string]any{"id": "feature-old", "ref": "feature", "created_at": "2020-01-01T00:00:00Z", "started_at": "2020-01-01T00:00:00Z"},
			map[string]any{"id": "old-main", "ref": "main", "created_at": "2020-01-01T00:00:00Z", "started_at": "2026-07-22T10:00:00Z"},
			map[string]any{"id": "future-main", "ref": "main", "created_at": "2026-07-24T00:00:00Z", "started_at": "2026-07-24T00:00:00Z"},
			map[string]any{"id": "valid-main", "ref": "main", "created_at": "2026-07-22T11:00:00Z", "started_at": "2026-07-22T11:00:00Z"},
			map[string]any{"id": "over-cap-main", "ref": "main", "created_at": "2026-07-22T12:00:00Z", "started_at": "2026-07-22T12:00:00Z"},
		},
		"jobs": jobs,
		"adapter_pipelines": []any{
			map[string]any{"id": "subms-after", "ref": "main", "created_at": "2026-07-22T23:59:59Z", "started_at": "2026-07-23T00:00:00.0009Z"},
			map[string]any{"id": "at-before", "ref": "main", "created_at": "2026-07-22T23:59:59Z", "started_at": "2026-07-23T00:00:00.0005Z"},
		},
	}}
	compareRowsAgainstPythonOracle(t, "gitlab/tests/selection", []oracleCase{testCase}, func(t *testing.T, input map[string]any) gitLabTestsSelectionObservation {
		decodeRaw := func(key string) []json.RawMessage {
			t.Helper()
			encoded, err := json.Marshal(input[key])
			if err != nil {
				t.Fatal(err)
			}
			var rows []json.RawMessage
			if err := json.Unmarshal(encoded, &rows); err != nil {
				t.Fatal(err)
			}
			return rows
		}
		before, err := time.Parse(time.RFC3339, input["before_at"].(string))
		if err != nil {
			t.Fatal(err)
		}
		pipelines, err := selectGitLabTestsReportPipelines(
			decodeRaw("raw_pipelines"), input["default_branch"].(string), input["max_pipelines"].(int), &before,
		)
		if err != nil {
			t.Fatal(err)
		}
		artifactJobs, err := selectGitLabTestsArtifactJobs(decodeRaw("jobs"), gitLabTestsMaxArtifacts)
		if err != nil {
			t.Fatal(err)
		}
		result := gitLabTestsSelectionObservation{
			ReportRunIDs: make([]string, 0, len(pipelines)), ArtifactJobIDs: make([]string, 0, len(artifactJobs)),
			MaxPipelines: gitLabTestsMaxPipelines, MaxArtifacts: gitLabTestsMaxArtifacts,
		}
		jobRunID := input["job_run_id"].(string)
		for _, pipeline := range pipelines {
			runID := stringValue(pipeline.ID)
			result.ReportRunIDs = append(result.ReportRunIDs, runID)
			if runID == jobRunID && len(artifactJobs) > 0 {
				result.CoverageRunIDs = append(result.CoverageRunIDs, runID)
			}
		}
		for _, job := range artifactJobs {
			result.ArtifactJobIDs = append(result.ArtifactJobIDs, stringValue(job.ID))
		}
		claim := nativeTestClaim("gitlab", "tests")
		since, err := time.Parse(time.RFC3339, input["since_at"].(string))
		if err != nil {
			t.Fatal(err)
		}
		claim.SinceAt, claim.BeforeAt = &since, &before
		for _, raw := range decodeRaw("adapter_pipelines") {
			pipeline, err := decodeGitLabTestsPipeline(raw)
			if err != nil {
				t.Fatal(err)
			}
			started := gitLabTestsPipelineStartedAt(pipeline)
			if started != nil && !ciPipelineRunOutsideWindow(*started, claim) {
				result.AdapterRunIDs = append(result.AdapterRunIDs, stringValue(pipeline.ID))
			}
		}

		adapterBody := []byte(`[{"id":"at-before","ref":"main","created_at":"2026-07-22T10:59:00Z","started_at":"2026-07-22T11:00:00Z"}]`)
		reportBody, err := json.Marshal(input["raw_pipelines"])
		if err != nil {
			t.Fatal(err)
		}
		jobBody, err := json.Marshal(input["jobs"])
		if err != nil {
			t.Fatal(err)
		}
		archive := githubTestsZip(t, map[string]string{"coverage.info": githubTestsLCOVFixture})
		pipelineCalls, physicalRequests := 0, 0
		requestPaths := make([]string, 0)
		doer := gitLabTestsSelectionDoerFunc(func(request *http.Request) (*http.Response, error) {
			physicalRequests++
			requestPaths = append(requestPaths, request.URL.Path)
			var body []byte
			switch path := request.URL.Path; {
			case path == "/api/v4/projects/123":
				body = []byte(`{"id":123,"path_with_namespace":"acme/api","default_branch":"main","only_allow_merge_if_pipeline_succeeds":true}`)
			case path == "/api/v4/projects/123/pipelines":
				if pipelineCalls == 0 {
					body = adapterBody
				} else {
					body = reportBody
				}
				pipelineCalls++
			case path == "/api/v4/projects/123/pipelines/at-before/jobs":
				body = []byte(`[]`)
			case path == "/api/v4/projects/123/pipelines/old-main/test_report" ||
				path == "/api/v4/projects/123/pipelines/valid-main/test_report":
				body = []byte(`{"test_suites":[{"name":"suite","test_cases":[{"name":"case","status":"success"}]}]}`)
			case path == "/api/v4/projects/123/pipelines/old-main/jobs":
				body = jobBody
			case path == "/api/v4/projects/123/pipelines/valid-main/jobs":
				body = []byte(`[]`)
			case strings.HasPrefix(path, "/api/v4/projects/123/jobs/") && strings.HasSuffix(path, "/artifacts"):
				body = archive
			default:
				t.Fatalf("unexpected selection oracle request %s", request.URL.String())
			}
			return &http.Response{
				StatusCode: http.StatusOK, Header: make(http.Header),
				Body: io.NopCloser(bytes.NewReader(body)), Request: request,
			}, nil
		})
		fullClaim := nativeTestClaim("gitlab", "tests")
		fullBefore := time.Date(2026, 7, 22, 11, 30, 0, 0, time.UTC)
		fullClaim.SinceAt, fullClaim.BeforeAt = &since, &fullBefore
		batch, err := (GitLabTestsRouteHandler{}).Collect(
			context.Background(), fullClaim, providerfoundation.Credential{},
			gitLabRepositoryClient(t, doer, "https://gitlab.test"), time.Now(),
		)
		if err != nil {
			t.Fatal(err)
		}
		if batch.Evidence.Requests != physicalRequests {
			t.Fatalf("evidence requests=%d physical requests=%d paths=%v", batch.Evidence.Requests, physicalRequests, requestPaths)
		}
		if physicalRequests != 33 {
			t.Fatalf("physical requests=%d want=33 paths=%v", physicalRequests, requestPaths)
		}
		observations, ok := batch.Result["observations"].(map[string]any)
		if !ok {
			t.Fatalf("observations=%#v", batch.Result["observations"])
		}
		usage, ok := observations["provider_usage"].([]any)
		if !ok {
			t.Fatalf("provider usage=%#v", observations["provider_usage"])
		}
		for _, raw := range usage {
			observation, ok := raw.(map[string]any)
			if !ok {
				t.Fatalf("provider usage observation=%#v", raw)
			}
			result.UsageObservations = append(result.UsageObservations, gitLabTestsUsageObservation{
				Transport: stringValue(observation["transport"]), RouteFamily: stringValue(observation["route_family"]),
				Dimension: stringValue(observation["dimension"]), RequestCount: observation["request_count"].(int),
			})
		}
		return result
	}, nil)
}
