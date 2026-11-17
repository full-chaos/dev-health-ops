package providersync

// Frozen differential oracle for CHAOS-13985: the Cobertura and JUnit row
// decoders exactly as on main (6ccff348), copied verbatim from
// internal/providersync/github_tests_reports.go and renamed. The only change is
// the guard parameter: guard=true keeps main's blanket <!DOCTYPE refusal,
// guard=false is main's decoder without it. The <!ENTITY refusal is kept in both.

import (
	"bytes"
	"encoding/xml"
	"time"
)

func oracleMainParseCoberturaRow(guard bool, body []byte, reportPath, artifactID, repoID, runID, orgID string, normalizedAt time.Time) (coverageSnapshotRow, error) {
	upper := bytes.ToUpper(body)
	if len(body) > githubTestsMaxReportBytes || (guard && bytes.Contains(upper, []byte("<!DOCTYPE"))) || bytes.Contains(upper, []byte("<!ENTITY")) {
		return coverageSnapshotRow{}, ErrGitHubTestsReportInvalid
	}
	var document coberturaDocument
	if err := xml.Unmarshal(body, &document); err != nil {
		return coverageSnapshotRow{}, err
	}
	type metrics struct {
		path                                                     string
		linesTotal, linesCovered, branchesTotal, branchesCovered int64
	}
	files := make([]metrics, 0, len(document.Classes))
	positions := map[string]int{}
	for _, class := range document.Classes {
		if class.Filename == "" {
			continue
		}
		current := metrics{path: class.Filename}
		current.linesTotal = int64(len(class.Lines))
		for _, line := range class.Lines {
			if hits := parseOptionalInt64(line.Hits); hits != nil && *hits > 0 {
				current.linesCovered++
			}
			total, covered := coberturaConditionCounts(line.ConditionCoverage)
			current.branchesTotal += total
			current.branchesCovered += covered
		}
		if index, ok := positions[current.path]; ok {
			files[index] = current
		} else {
			positions[current.path] = len(files)
			files = append(files, current)
		}
	}
	linesTotal := parseOptionalInt64(document.LinesValid)
	linesCovered := parseOptionalInt64(document.LinesCovered)
	branchesTotal := parseOptionalInt64(document.BranchesValid)
	branchesCovered := parseOptionalInt64(document.BranchesCovered)
	if linesTotal == nil {
		var value int64
		for _, file := range files {
			value += file.linesTotal
		}
		linesTotal = nilIfZero(value)
	}
	if linesCovered == nil {
		var value int64
		for _, file := range files {
			value += file.linesCovered
		}
		linesCovered = nilIfZero(value)
	}
	if branchesTotal == nil {
		var value int64
		for _, file := range files {
			value += file.branchesTotal
		}
		branchesTotal = nilIfZero(value)
	}
	if branchesCovered == nil {
		var value int64
		for _, file := range files {
			value += file.branchesCovered
		}
		branchesCovered = nilIfZero(value)
	}
	if linesCovered != nil && linesTotal != nil && *linesCovered > *linesTotal {
		return coverageSnapshotRow{}, ErrGitHubTestsReportInvalid
	}
	if branchesCovered != nil && branchesTotal != nil && *branchesCovered > *branchesTotal {
		return coverageSnapshotRow{}, ErrGitHubTestsReportInvalid
	}
	serviceCounts := map[string]int{}
	serviceOrder := []string{}
	for _, file := range files {
		if service := testServiceID(file.path); service != nil {
			if _, ok := serviceCounts[*service]; !ok {
				serviceOrder = append(serviceOrder, *service)
			}
			serviceCounts[*service]++
		}
	}
	var serviceID *string
	best := 0
	for _, service := range serviceOrder {
		if serviceCounts[service] > best {
			value := service
			serviceID = &value
			best = serviceCounts[service]
		}
	}
	format := "cobertura"
	row := coverageSnapshotRow{RepoID: repoID, RunID: runID, SnapshotID: githubTestsSnapshotID(runID, artifactID, format, reportPath), ReportFormat: &format, LinesTotal: linesTotal, LinesCovered: linesCovered, BranchesTotal: branchesTotal, BranchesCovered: branchesCovered, ServiceID: serviceID, OrgID: orgID, LastSynced: normalizedAt}
	row.LineCoveragePct = coveragePercent(row.LinesCovered, row.LinesTotal)
	row.BranchCoveragePct = coveragePercent(row.BranchesCovered, row.BranchesTotal)
	return row, nil
}

func oracleMainParseJUnitRows(
	guard bool,
	body []byte, artifactID, repoID, runID, orgID string,
	fallbackStarted, fallbackFinished *time.Time,
	normalizedAt time.Time,
	suiteOccurrence map[string]int,
) ([]testSuiteResultRow, []testCaseResultRow, int, error) {
	nonFinite := 0
	if len(body) > githubTestsMaxReportBytes || (guard && bytes.Contains(bytes.ToUpper(body), []byte("<!DOCTYPE"))) || bytes.Contains(bytes.ToUpper(body), []byte("<!ENTITY")) {
		return nil, nil, 0, ErrGitHubTestsReportInvalid
	}
	var document junitDocument
	if err := xml.Unmarshal(body, &document); err != nil {
		return nil, nil, 0, err
	}
	suites := document.Suites
	if document.XMLName.Local == "testsuite" {
		suites = []junitSuite{{Cases: document.Cases}}
		var root junitSuite
		if err := xml.Unmarshal(body, &root); err != nil {
			return nil, nil, 0, err
		}
		suites = []junitSuite{root}
	}
	flat := make([]junitSuite, 0, len(suites))
	var visit func(junitSuite)
	visit = func(suite junitSuite) {
		if len(suite.Cases) > 0 {
			flat = append(flat, suite)
		}
		for _, child := range suite.Suites {
			visit(child)
		}
	}
	for _, suite := range suites {
		visit(suite)
	}
	resultSuites := make([]testSuiteResultRow, 0, len(flat))
	resultCases := make([]testCaseResultRow, 0)
	for _, suite := range flat {
		name := suite.Name
		if name == "" {
			name = "unnamed"
		}
		framework := inferJUnitFramework(suite)
		// The ordinal suiteOccurrence[name] disambiguates two SIBLING suite
		// OBJECTS that would hash to the identical suiteID because they share a
		// (run, artifact, name) triple (CHAOS-4508, prod sync_run
		// a7b60282-4af1-58ad-a094-8dc8baab7a1f, run_id=33248832747): an
		// ordinary shape when one artifact bundles multiple report files or
		// matrix legs that each emit a generically-named root suite (e.g.
		// two files both producing <testsuite name="pytest">). Neither
		// CHAOS-4190 (artifact scoping) nor CHAOS-4392's caseOccurrence
		// below (reset fresh per suite OBJECT, so two colliding objects each
		// start their first case at occurrence=0 and collide too) covers
		// this. Folded into the hash ONLY when non-zero -- hash-of-hash, not
		// a three-way join through hashTestIdentifier's unescaped "::"
		// separator, matching caseOccurrence's own fix below -- so the first
		// (and every non-colliding) suite keeps the exact suiteID it always
		// has: existing rows for single-suite artifacts are unaffected.
		suiteID := githubTestsSuiteID(runID, artifactID, name, suiteOccurrence[name])
		suiteOccurrence[name]++
		started := parseGitHubTestsTime(suite.Timestamp)
		hasSuiteTimestamp := started != nil
		if started == nil {
			started = cloneTime(fallbackStarted)
		}
		duration, durationNonFinite := parseOptionalFloat(suite.Time)
		if durationNonFinite {
			nonFinite++
		}
		finished := cloneTime(fallbackFinished)
		if hasSuiteTimestamp && started != nil && duration != nil {
			value := started.Add(time.Duration(*duration * float64(time.Second))).UTC().Truncate(time.Millisecond)
			finished = &value
		}
		service := junitServiceID(suite)
		row := testSuiteResultRow{
			RepoID: repoID, RunID: runID, SuiteID: suiteID, SuiteName: name,
			Framework: &framework, TotalCount: int64(len(suite.Cases)), DurationSeconds: duration,
			StartedAt: started, FinishedAt: finished, ServiceID: service,
			OrgID: orgID, LastSynced: normalizedAt,
		}
		// caseOccurrence disambiguates within-suite duplicate case names
		// (CHAOS-4392): two <testcase> elements sharing a name in the same
		// suite otherwise hash to the identical case_id in newJUnitCaseRow,
		// and WriteEffect's recordGitHubTestsKey rejects the whole batch with
		// a bare ErrInvalidConfiguration -- the unit then burns all 5 River
		// attempts on the same deterministic collision (prod run 33149651369,
		// full-chaos/dev-health-ops). Keyed on the NORMALIZED name (codex
		// review finding, P2): an empty name="" and a literal name="unnamed"
		// both normalize to "unnamed" in newJUnitCaseRow, so keying on the
		// raw pre-normalization string let each reach occurrence=0
		// independently and still collide on the same CaseID -- the exact
		// defect this fix exists to remove.
		caseOccurrence := map[string]int{}
		for _, testCase := range suite.Cases {
			name := testCase.Name
			if name == "" {
				name = "unnamed"
			}
			occurrence := caseOccurrence[name]
			caseOccurrence[name] = occurrence + 1
			caseRow, caseNonFinite := newJUnitCaseRow(testCase, row, normalizedAt, occurrence)
			if caseNonFinite {
				nonFinite++
			}
			switch caseRow.Status {
			case "passed":
				row.PassedCount++
			case "failed":
				row.FailedCount++
			case "skipped":
				row.SkippedCount++
			case "error":
				row.ErrorCount++
			case "quarantined":
				row.QuarantinedCount++
			}
			resultCases = append(resultCases, caseRow)
		}
		resultSuites = append(resultSuites, row)
	}
	return resultSuites, resultCases, nonFinite, nil
}
