package providersync

import (
	"testing"
	"time"
)

var githubTestsOracleGoOnlyFields = map[string]string{
	"last_synced": "ClickHouse sinks stamp persistence time after Python's active report-ingestion boundary; Go stabilizes it in the effect row for crash recovery",
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
	compareRowsAgainstPythonOracle(t, "github/tests/coverage", []oracleCase{
		githubTestsOracleCase(), fallback, majority,
	},
		func(t *testing.T, input map[string]any) coverageSnapshotRow {
			_, _, normalizedAt := githubTestsOracleTimes(t, input)
			lcov := githubTestsLCOVFixture
			if value, ok := input["lcov"].(string); ok {
				lcov = value
			}
			row, err := parseLCOVRow(
				[]byte(lcov), "reports/lcov.info", input["repo_id"].(string), input["run_id"].(string), input["org_id"].(string), normalizedAt,
			)
			if err != nil {
				t.Fatal(err)
			}
			return row
		}, githubTestsOracleGoOnlyFields)
}
