package providersync

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"
)

var githubTestsOracleGoOnlyFields = map[string]string{
	"last_synced": "ClickHouse sinks stamp persistence time after Python's active report-ingestion boundary; Go stabilizes it in the effect row for crash recovery",
}

var githubTestsProducerGoOnlyFields = map[string]string{
	"last_synced": "stamped by the Go complete-route boundary after the active Python producer returns its row",
}

func githubTestsProducerCase() oracleCase {
	return oracleCase{ID: "required_main_branch_job", Input: map[string]any{
		"repo_id": "c7198fbc-1945-3717-05d8-eb78866b4e79", "org_id": "org-acme",
		"since_at": "2026-07-22T00:00:00Z", "before_at": "2026-07-23T00:00:00Z",
		"required_contexts": []any{"unit"},
		"raw_run":           map[string]any{"id": 9001, "name": "CI", "status": "completed", "conclusion": "success", "created_at": "2026-07-22T10:00:00Z", "run_started_at": "2026-07-22T10:01:00Z", "updated_at": "2026-07-22T10:05:00Z", "run_attempt": 2, "event": "pull_request", "head_sha": "abc", "head_branch": "feature", "html_url": "https://github.com/acme/api/actions/runs/9001", "pull_requests": []any{map[string]any{"number": 42, "base": map[string]any{"ref": "main"}}}},
		"raw_job":           map[string]any{"id": 11, "name": "unit", "status": "completed", "conclusion": "success", "started_at": "2026-07-22T10:01:00Z", "completed_at": "2026-07-22T10:04:00Z", "labels": []any{"ubuntu-latest"}},
	}}
}

func githubTestsProducerRows(t *testing.T, input map[string]any) (githubTestsPipelineRow, githubTestsJobRow, githubTestsAcceptanceRow) {
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
	var run gitHubWorkflowRunPayload
	var job githubTestsJobPayload
	decode("raw_run", &run)
	decode("raw_job", &job)
	claim := nativeTestClaim("github", "tests")
	claim.OrgID = input["org_id"].(string)
	normalizedAt := time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC)
	pipeline, ok := normalizeGitHubTestsPipeline(claim, input["repo_id"].(string), run, normalizedAt)
	if !ok {
		t.Fatal("pipeline excluded")
	}
	jobRow, ok := normalizeGitHubTestsJob(claim, input["repo_id"].(string), pipeline.RunID, pipeline.RetryCount, job, normalizedAt)
	if !ok {
		t.Fatal("job excluded")
	}
	branch, pr := gitHubTestsTarget(run)
	checks := projectGitHubTestsChecks(claim, input["repo_id"].(string), pipeline, []githubTestsJobRow{jobRow}, githubTestsPolicy{required: map[string]struct{}{"unit": {}}, known: true, provenance: "github.branch_protection.required_status_checks"}, branch, pr, testsOptionalString(stringValue(run.HTMLURL)), normalizedAt)
	if len(checks) != 1 {
		t.Fatalf("checks=%+v", checks)
	}
	return pipeline, jobRow, checks[0]
}

func TestGenericOracleMatchesActivePythonGitHubTestsProducer(t *testing.T) {
	testCase := githubTestsProducerCase()
	compareRowsAgainstPythonOracle(t, "github/tests/pipeline", []oracleCase{testCase}, func(t *testing.T, input map[string]any) githubTestsPipelineRow {
		pipeline, _, _ := githubTestsProducerRows(t, input)
		return pipeline
	}, githubTestsProducerGoOnlyFields)
	compareRowsAgainstPythonOracle(t, "github/tests/job", []oracleCase{testCase}, func(t *testing.T, input map[string]any) githubTestsJobRow {
		_, job, _ := githubTestsProducerRows(t, input)
		return job
	}, githubTestsProducerGoOnlyFields)
	compareRowsAgainstPythonOracle(t, "github/tests/acceptance", []oracleCase{testCase}, func(t *testing.T, input map[string]any) githubTestsAcceptanceRow {
		_, _, acceptance := githubTestsProducerRows(t, input)
		return acceptance
	}, nil)
}

type githubTestsSelectionObservation struct {
	Branch  *string `json:"branch"`
	Created *string `json:"created"`
}

func TestGenericOracleMatchesActivePythonGitHubArtifactSelection(t *testing.T) {
	compareRowsAgainstPythonOracle(t, "github/tests/selection", []oracleCase{{
		ID: "default_branch_and_date_floor",
		Input: map[string]any{
			"default_branch": "main", "since_at": "2026-07-22T10:31:00Z", "before_at": "2026-07-23T00:00:00Z",
		},
	}}, func(t *testing.T, input map[string]any) githubTestsSelectionObservation {
		branch := input["default_branch"].(string)
		since, err := time.Parse(time.RFC3339, input["since_at"].(string))
		if err != nil {
			t.Fatal(err)
		}
		created := ">=" + since.UTC().Format(time.DateOnly)
		return githubTestsSelectionObservation{Branch: &branch, Created: &created}
	}, nil)
}

func githubTestsOracleCase() oracleCase {
	return oracleCase{ID: "junit_and_lcov", Input: map[string]any{
		"repo_id": "c7198fbc-1945-3717-05d8-eb78866b4e79",
		"run_id":  "9001", "org_id": "org-a",
		"started_at":  "2026-07-23T12:00:00Z",
		"finished_at": "2026-07-23T12:05:00Z",
	}}
}

func githubTestsOracleTimes(t *testing.T, input map[string]any) (*time.Time, *time.Time, time.Time) {
	t.Helper()
	parse := func(key string) *time.Time {
		value, err := time.Parse(time.RFC3339, input[key].(string))
		if err != nil {
			t.Fatal(err)
		}
		return &value
	}
	return parse("started_at"), parse("finished_at"), time.Date(2026, 7, 23, 12, 10, 0, 0, time.UTC)
}

func TestGenericOracleMatchesLivePythonForGitHubTestsSuiteRow(t *testing.T) {
	compareRowsAgainstPythonOracle(t, "github/tests/suite", []oracleCase{githubTestsOracleCase()},
		func(t *testing.T, input map[string]any) testSuiteResultRow {
			started, finished, normalizedAt := githubTestsOracleTimes(t, input)
			suites, _, err := parseJUnitRows(
				[]byte(githubTestsJUnitFixture), input["repo_id"].(string), input["run_id"].(string), input["org_id"].(string),
				started, finished, normalizedAt,
			)
			if err != nil || len(suites) != 1 {
				t.Fatalf("suites=%+v error=%v", suites, err)
			}
			return suites[0]
		}, githubTestsOracleGoOnlyFields)
}

func TestGenericOracleMatchesLivePythonForGitHubTestsCaseRow(t *testing.T) {
	compareRowsAgainstPythonOracle(t, "github/tests/case", []oracleCase{githubTestsOracleCase()},
		func(t *testing.T, input map[string]any) testCaseResultRow {
			started, finished, normalizedAt := githubTestsOracleTimes(t, input)
			_, cases, err := parseJUnitRows(
				[]byte(githubTestsJUnitFixture), input["repo_id"].(string), input["run_id"].(string), input["org_id"].(string),
				started, finished, normalizedAt,
			)
			if err != nil || len(cases) != 2 {
				t.Fatalf("cases=%+v error=%v", cases, err)
			}
			return cases[1]
		}, githubTestsOracleGoOnlyFields)
}

func TestGenericOracleMatchesLivePythonForGitHubTestsCoverageRow(t *testing.T) {
	fallback := githubTestsOracleCase()
	fallback.ID = "da_fallback_without_summaries"
	fallback.Input["lcov"] = "SF:services/api/main.go\nDA:1,1\nDA:2,0\nDA:2,3\nend_of_record\n"
	majority := githubTestsOracleCase()
	majority.ID = "majority_file_service"
	majority.Input["lcov"] = "SF:services/api/main.go\nLF:1\nLH:1\nend_of_record\n" +
		"SF:services/web/handler.go\nLF:1\nLH:0\nend_of_record\n" +
		"SF:services/web/router.go\nLF:1\nLH:1\nend_of_record\n"
	cobertura := githubTestsOracleCase()
	cobertura.ID = "cobertura"
	cobertura.Input["coverage_name"] = "reports/coverage.xml"
	cobertura.Input["coverage"] = `<coverage lines-valid="2" lines-covered="1" branches-valid="2" branches-covered="1"><packages><package><classes><class filename="services/api/main.go"><lines><line number="1" hits="1" branch="true" condition-coverage="50% (1/2)"/><line number="2" hits="0"/></lines></class></classes></package></packages></coverage>`
	compareRowsAgainstPythonOracle(t, "github/tests/coverage", []oracleCase{
		githubTestsOracleCase(), fallback, majority, cobertura,
	},
		func(t *testing.T, input map[string]any) coverageSnapshotRow {
			_, _, normalizedAt := githubTestsOracleTimes(t, input)
			body := githubTestsLCOVFixture
			path := "reports/lcov.info"
			if value, ok := input["lcov"].(string); ok {
				body = value
			}
			if value, ok := input["coverage"].(string); ok {
				body = value
			}
			if value, ok := input["coverage_name"].(string); ok {
				path = value
			}
			row, err := parseGitHubCoverageRow(
				[]byte(body), path, input["repo_id"].(string), input["run_id"].(string), input["org_id"].(string), normalizedAt,
			)
			if err != nil {
				t.Fatal(err)
			}
			return row
		}, githubTestsOracleGoOnlyFields)
}
