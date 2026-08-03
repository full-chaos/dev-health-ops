package providersync

import (
	"archive/zip"
	"bytes"
	"strings"
	"testing"
	"time"
)

const githubTestsJUnitFixture = `<testsuites><testsuite name="api" time="3.5">
<testcase name="passes" classname="tests/test_api.py::TestAPI" file="services/api/test_api.py" time="1.25"/>
<testcase name="fails" classname="tests/test_api.py::TestAPI" file="services/api/test_api.py" time="2.25"><failure message="expected 200" type="AssertionError">trace</failure><system-err>stderr</system-err></testcase>
</testsuite></testsuites>`

const githubTestsLCOVFixture = `TN:
SF:services/api/main.go
DA:1,1
DA:2,0
LF:2
LH:1
BRF:2
BRH:1
FNF:1
FNH:1
end_of_record
`

func TestGitHubTestsArtifactParsesJUnitCasesAndCoverage(t *testing.T) {
	started := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	finished := started.Add(5 * time.Minute)
	normalizedAt := started.Add(10*time.Minute + 987654321*time.Nanosecond)
	rows, err := parseGitHubTestsArtifact(
		githubTestsZip(t, map[string]string{
			"reports/junit.xml": githubTestsJUnitFixture,
			"reports/lcov.info": githubTestsLCOVFixture,
			"ignored.txt":       "not a report",
		}),
		"c7198fbc-1945-3717-05d8-eb78866b4e79", "9001", "org-a",
		&started, &finished, normalizedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows.Suites) != 1 || len(rows.Cases) != 2 || len(rows.Coverage) != 1 || rows.Skipped != 0 {
		t.Fatalf("rows=%+v", rows)
	}
	suite := rows.Suites[0]
	if suite.SuiteName != "api" || suite.Framework == nil || *suite.Framework != "pytest" ||
		suite.TotalCount != 2 || suite.PassedCount != 1 || suite.FailedCount != 1 ||
		suite.ServiceID == nil || *suite.ServiceID != "api" || suite.StartedAt == nil || !suite.StartedAt.Equal(started) ||
		suite.FinishedAt == nil || !suite.FinishedAt.Equal(finished) {
		t.Fatalf("suite=%+v", suite)
	}
	failed := rows.Cases[1]
	if failed.Status != "failed" || failed.FailureMessage == nil || *failed.FailureMessage != "expected 200" ||
		failed.FailureType == nil || *failed.FailureType != "AssertionError" || failed.StackTrace == nil || *failed.StackTrace != "trace\nstderr" {
		t.Fatalf("failed=%+v", failed)
	}
	coverage := rows.Coverage[0]
	if coverage.LinesTotal == nil || *coverage.LinesTotal != 2 || coverage.LinesCovered == nil || *coverage.LinesCovered != 1 ||
		coverage.LineCoveragePct == nil || *coverage.LineCoveragePct != 50 || coverage.BranchCoveragePct == nil || *coverage.BranchCoveragePct != 50 {
		t.Fatalf("coverage=%+v", coverage)
	}
	if !suite.LastSynced.Equal(normalizedAt.Truncate(time.Millisecond)) || !coverage.LastSynced.Equal(normalizedAt.Truncate(time.Millisecond)) {
		t.Fatal("sink timestamps were not stabilized to millisecond precision")
	}
}

func TestGitHubTestsArtifactSkipsMalformedMemberWithoutDroppingValidReports(t *testing.T) {
	rows, err := parseGitHubTestsArtifact(
		githubTestsZip(t, map[string]string{
			"bad.xml":   `<!DOCTYPE x [<!ENTITY x "boom">]><testsuite>&x;</testsuite>`,
			"good.info": githubTestsLCOVFixture,
		}),
		"c7198fbc-1945-3717-05d8-eb78866b4e79", "9001", "org-a",
		nil, nil, time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatal(err)
	}
	if rows.Skipped != 1 || len(rows.Coverage) != 1 || len(rows.Suites) != 0 {
		t.Fatalf("rows=%+v", rows)
	}
}

func TestGitHubTestsArtifactEnforcesReportCountCap(t *testing.T) {
	members := make(map[string]string, githubTestsMaxReportsPerRun+1)
	for index := 0; index <= githubTestsMaxReportsPerRun; index++ {
		members[hashTestIdentifier(string(rune(index)))+".info"] = githubTestsLCOVFixture
	}
	rows, err := parseGitHubTestsArtifact(
		githubTestsZip(t, members),
		"c7198fbc-1945-3717-05d8-eb78866b4e79", "9001", "org-a",
		nil, nil, time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows.Coverage) != githubTestsMaxReportsPerRun || rows.Skipped != 1 {
		t.Fatalf("coverage=%d skipped=%d", len(rows.Coverage), rows.Skipped)
	}
}

func TestGitHubTestsJUnitParserRejectsDTDAndTruncatesStack(t *testing.T) {
	_, _, err := parseJUnitRows(
		[]byte(`<!DOCTYPE testsuite><testsuite name="x"><testcase name="x"/></testsuite>`),
		"repo", "run", "org", nil, nil, time.Now(),
	)
	if err == nil {
		t.Fatal("entity-bearing document was accepted")
	}
	large := strings.Repeat("x", 5000)
	body := []byte(`<testsuite name="x"><testcase name="x"><failure>` + large + `</failure></testcase></testsuite>`)
	_, cases, err := parseJUnitRows(body, "repo", "run", "org", nil, nil, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if len(cases) != 1 || cases[0].StackTrace == nil || len(*cases[0].StackTrace) != 4096 {
		t.Fatalf("cases=%+v", cases)
	}
}

func TestGitHubTestsCoverageRejectsCoveredGreaterThanTotal(t *testing.T) {
	_, err := parseLCOVRow(
		[]byte("SF:main.go\nLF:1\nLH:2\nend_of_record\n"), "lcov.info",
		"repo", "run", "org", time.Now(),
	)
	if err == nil {
		t.Fatal("incoherent coverage row was accepted")
	}
}

func TestGitHubTestsCoverageFallsBackToUniqueDALines(t *testing.T) {
	row, err := parseLCOVRow(
		[]byte("SF:services/api/main.go\nDA:1,1\nDA:2,0\nDA:2,3\nend_of_record\n"),
		"lcov.info", "repo", "run", "org", time.Now(),
	)
	if err != nil {
		t.Fatal(err)
	}
	if row.LinesTotal == nil || *row.LinesTotal != 2 ||
		row.LinesCovered == nil || *row.LinesCovered != 2 ||
		row.LineCoveragePct == nil || *row.LineCoveragePct != 100 {
		t.Fatalf("coverage=%+v", row)
	}
}

func TestGitHubTestsCoverageAttributesMajorityFileService(t *testing.T) {
	row, err := parseLCOVRow([]byte(`SF:services/api/main.go
LF:1
LH:1
end_of_record
SF:services/web/handler.go
LF:1
LH:0
end_of_record
SF:services/web/router.go
LF:1
LH:1
end_of_record
`), "lcov.info", "repo", "run", "org", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if row.ServiceID == nil || *row.ServiceID != "web" {
		t.Fatalf("service_id=%v", row.ServiceID)
	}
	if row.LinesTotal == nil || *row.LinesTotal != 3 ||
		row.LinesCovered == nil || *row.LinesCovered != 2 {
		t.Fatalf("coverage=%+v", row)
	}
}

func githubTestsZip(t *testing.T, members map[string]string) []byte {
	t.Helper()
	var buffer bytes.Buffer
	writer := zip.NewWriter(&buffer)
	for name, body := range members {
		member, err := writer.Create(name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := member.Write([]byte(body)); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return buffer.Bytes()
}
