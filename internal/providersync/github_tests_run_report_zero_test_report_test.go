package providersync

import (
	"testing"
)

// A report that parses whole and holds no test (a filter that selected none) is a report source of the run: the attempt
// gets a verdict, and it counts in reports_expected and reports_admitted like a report with rows.
func TestGitHubTestsRunReportCompletenessCountsAZeroTestReport(t *testing.T) {
	zeroTestReports := []struct{ name, body string }{
		{"empty_testsuites", `<testsuites tests="0"></testsuites>`},
		{"empty_root_suite", `<testsuite name="filtered" tests="0" failures="0"/>`},
		{"empty_child_suite", `<testsuites><testsuite name="filtered" tests="0" failures="0"/></testsuites>`},
		{"empty_nested_suite", `<testsuites><testsuite name="matrix"><testsuite name="filtered" tests="0"/></testsuite></testsuites>`},
	}
	for _, report := range zeroTestReports {
		for _, withRows := range []bool{false, true} {
			name := report.name + "/alone"
			if withRows {
				name = report.name + "/beside_a_report_with_rows"
			}
			t.Run(name, func(t *testing.T) {
				archives := []completenessArtifact{{name: "test-results-filtered", body: githubTestsZip(t, map[string]string{"junit.xml": report.body})}}
				if withRows {
					archives = append(archives, completenessArtifact{name: "test-results-kept", body: githubTestsZip(t, map[string]string{"junit.xml": completenessJUnit("keep-", 2)})})
				}
				rows, _ := completenessRows(t, GitHubTestsRouteHandler{}, &completenessDoer{t: t, runs: completenessRuns(1, 1), artifacts: archives})
				want := int64(len(archives))
				if len(rows) != 1 || !rows[0].Complete || len(rows[0].Reasons) != 0 || rows[0].ReportsExpected != want || rows[0].ReportsAdmitted != want {
					t.Fatalf("rows = %+v, want one complete verdict with %d/%d reports", rows, want, want)
				}
			})
		}
	}

	// A rerun whose only report holds no test still names its attempt, so readers take attempt 2 as the newest.
	t.Run("zero_test_rerun_names_its_attempt", func(t *testing.T) {
		first, _ := completenessRows(t, GitHubTestsRouteHandler{}, &completenessDoer{t: t, runs: completenessRuns(1, 1), artifacts: []completenessArtifact{
			{name: "test-results", body: githubTestsZip(t, map[string]string{"junit.xml": completenessJUnit("matched-", 2)})},
		}})
		second, _ := completenessRows(t, GitHubTestsRouteHandler{}, &completenessDoer{t: t, runs: completenessRuns(2, 1), artifacts: []completenessArtifact{
			{name: "test-results", body: githubTestsZip(t, map[string]string{"junit.xml": `<testsuite name="filtered" tests="0" failures="0"/>`})},
		}})
		if len(first) != 1 || first[0].RunAttempt != 1 || len(second) != 1 || second[0].RunAttempt != 2 || !second[0].Complete {
			t.Fatalf("first walk = %+v, rerun = %+v, want attempt 1 then a complete attempt 2", first, second)
		}
	})

	// A non-report archive stays outside the counts.
	t.Run("non_report_archive_beside_a_report", func(t *testing.T) {
		rows, _ := completenessRows(t, GitHubTestsRouteHandler{}, &completenessDoer{t: t, runs: completenessRuns(1, 1), artifacts: []completenessArtifact{
			{name: "test-results-log", body: githubTestsZip(t, map[string]string{"output.log": "build log"})},
			{name: "test-results-kept", body: githubTestsZip(t, map[string]string{"junit.xml": completenessJUnit("keep-", 2)})},
		}})
		if len(rows) != 1 || !rows[0].Complete || rows[0].ReportsExpected != 1 || rows[0].ReportsAdmitted != 1 {
			t.Fatalf("rows = %+v, want one complete verdict with 1/1 reports", rows)
		}
	})
}
