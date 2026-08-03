package providersync

import (
	"archive/zip"
	"bytes"
	"fmt"
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

func TestGitHubTestsArchiveMemberNameMatchesPythonSafetyContract(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name string
		want bool
	}{
		{name: "", want: false},
		{name: "reports/", want: false},
		{name: "../../reports/junit.xml", want: false},
		{name: "/reports/junit.xml", want: false},
		{name: `\reports\junit.xml`, want: false},
		{name: `\\server\share\junit.xml`, want: false},
		{name: `C:\reports\junit.xml`, want: false},
		{name: "reports/nested/junit.xml", want: true},
		{name: `reports\nested\junit.xml`, want: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := isSafeGitHubTestsArchiveMemberName(test.name); got != test.want {
				t.Fatalf("safe(%q) = %t, want %t", test.name, got, test.want)
			}
		})
	}
}

func TestGitHubTestsArtifactSkipsUnsafeNamesBeforeClassification(t *testing.T) {
	for _, unsafeName := range []string{
		"../../reports/junit.xml",
		"/reports/junit.xml",
		`\reports\junit.xml`,
		`\\server\share\junit.xml`,
		`C:\reports\junit.xml`,
	} {
		t.Run(unsafeName, func(t *testing.T) {
			rows, err := parseGitHubTestsArtifact(
				githubTestsZip(t, map[string]string{
					unsafeName:          githubTestsJUnitFixture,
					"reports/good.info": githubTestsLCOVFixture,
				}),
				"repo", "run", "org", nil, nil, time.Now(),
			)
			if err != nil {
				t.Fatal(err)
			}
			if len(rows.Suites) != 0 || len(rows.Cases) != 0 || len(rows.Coverage) != 1 || rows.Skipped != 0 {
				t.Fatalf("unsafe member %q was classified: %+v", unsafeName, rows)
			}
		})
	}

	rows, err := parseGitHubTestsArtifact(
		githubTestsZip(t, map[string]string{"reports/nested/junit.xml": githubTestsJUnitFixture}),
		"repo", "run", "org", nil, nil, time.Now(),
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows.Suites) != 1 || len(rows.Cases) != 2 || rows.Skipped != 0 {
		t.Fatalf("valid nested report was not classified: %+v", rows)
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

func TestGitHubTestsArtifactParsesCoberturaCoverage(t *testing.T) {
	const cobertura = `<coverage lines-valid="2" lines-covered="1" branches-valid="2" branches-covered="1"><packages><package><classes><class filename="services/api/main.go"><lines><line number="1" hits="1" branch="true" condition-coverage="50% (1/2)"/><line number="2" hits="0"/></lines></class></classes></package></packages></coverage>`
	if _, err := parseCoberturaRow([]byte(cobertura), "coverage.xml", "c7198fbc-1945-3717-05d8-eb78866b4e79", "9001", "org-a", time.Now()); err != nil {
		t.Fatalf("parse cobertura: %v", err)
	}
	rows, err := parseGitHubTestsArtifact(
		githubTestsZip(t, map[string]string{"coverage.xml": cobertura}),
		"c7198fbc-1945-3717-05d8-eb78866b4e79", "9001", "org-a", nil, nil,
		time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows.Coverage) != 1 || rows.Coverage[0].ReportFormat == nil || *rows.Coverage[0].ReportFormat != "cobertura" {
		t.Fatalf("coverage=%+v skipped=%d", rows.Coverage, rows.Skipped)
	}
	row := rows.Coverage[0]
	if row.LinesTotal == nil || *row.LinesTotal != 2 || row.LinesCovered == nil || *row.LinesCovered != 1 ||
		row.BranchesTotal == nil || *row.BranchesTotal != 2 || row.BranchesCovered == nil || *row.BranchesCovered != 1 ||
		row.ServiceID == nil || *row.ServiceID != "api" {
		t.Fatalf("coverage=%+v", row)
	}
}

func TestGitHubTestsArtifactDoesNotTraversePastPythonEntryCap(t *testing.T) {
	var buffer bytes.Buffer
	writer := zip.NewWriter(&buffer)
	for index := 0; index < githubTestsMaxArchiveEntries+1; index++ {
		_, err := writer.Create(fmt.Sprintf("ignored/%04d.txt", index))
		if err != nil {
			t.Fatal(err)
		}
	}
	report, err := writer.Create("reports/after-cap.xml")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := report.Write([]byte(githubTestsJUnitFixture)); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	rows, err := parseGitHubTestsArtifact(buffer.Bytes(), "repo", "run", "org", nil, nil, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if len(rows.Suites) != 0 || len(rows.Cases) != 0 {
		t.Fatalf("entry beyond Python's total-entry cap was parsed: %+v", rows)
	}
}

func TestGitHubTestsArtifactSkipsPythonOverRatioMember(t *testing.T) {
	var buffer bytes.Buffer
	writer := zip.NewWriter(&buffer)
	header := &zip.FileHeader{Name: "reports/bomb.xml", Method: zip.Deflate}
	member, err := writer.CreateHeader(header)
	if err != nil {
		t.Fatal(err)
	}
	body := `<testsuite name="bomb"><testcase name="case"><system-out>` + strings.Repeat(" ", 1<<20) + `</system-out></testcase></testsuite>`
	if _, err := member.Write([]byte(body)); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	rows, err := parseGitHubTestsArtifact(buffer.Bytes(), "repo", "run", "org", nil, nil, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if len(rows.Suites) != 0 || rows.Skipped != 0 {
		t.Fatalf("over-ratio member did not match Python skip semantics: %+v", rows)
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
