package providersync

import (
	"encoding/json"
	"slices"
	"strconv"
	"testing"
	"time"
)

// CHAOS-13991: the search for an aggregate's parts is bounded (githubTestsPartitionBound). Running out of work before a partition is
// proven keeps the artifact, with no marker; a proven partition that differs is a mismatch.

type boundSuite struct {
	name     string
	cases    int
	duration float64
}

// boundReport builds one parsed report: per suite, a suite row and its cases named <suite>-<n>, each with the given duration.
func boundReport(id string, suites ...boundSuite) githubTestsParsedReport {
	report := githubTestsParsedReport{ID: json.Number(id), Name: "n" + id}
	class := "pkg"
	for _, suite := range suites {
		suiteID := id + "/" + suite.name
		report.Suites = append(report.Suites, testSuiteResultRow{SuiteID: suiteID, SuiteName: suite.name, TotalCount: int64(suite.cases)})
		for n := 0; n < suite.cases; n++ {
			duration := suite.duration
			report.Cases = append(report.Cases, testCaseResultRow{SuiteID: suiteID, CaseID: suiteID + "/" + strconv.Itoa(n), ClassName: &class,
				CaseName: suite.name + "-" + strconv.Itoa(n), Status: "passed", DurationSeconds: &duration})
		}
	}
	return report
}

func mustPlanWithin(t *testing.T, bound githubTestsPartitionBound, reports ...githubTestsParsedReport) githubTestsRunReportPlan {
	t.Helper()
	plan, err := githubTestsPlanRunReportsWithin(reports, bound)
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

// The work bound: at exactly the work the decision needs the aggregate is dropped; one below it, it is kept with no marker.
func TestGitHubTestsPlanRunReportsPartitionWorkBound(t *testing.T) {
	reports := []githubTestsParsedReport{
		boundReport("1", boundSuite{"a", 10, 0}), boundReport("2", boundSuite{"b", 10, 0}),
		boundReport("3", boundSuite{"a", 10, 0}, boundSuite{"b", 10, 0}),
	}
	free := mustPlanWithin(t, githubTestsDefaultPartitionBound, reports...)
	if len(free.Dropped) != 1 || free.Dropped[0].Index != 2 || free.PartitionWork == 0 {
		t.Fatalf("unbounded: dropped %+v work %d, want the aggregate (index 2) dropped", free.Dropped, free.PartitionWork)
	}
	at := mustPlanWithin(t, githubTestsPartitionBound{Work: free.PartitionWork}, reports...)
	if !slices.Equal(at.Dropped, free.Dropped) || len(at.Mismatch) != 0 {
		t.Fatalf("at the bound (%d): dropped %+v mismatch %+v, want the aggregate dropped", free.PartitionWork, at.Dropped, at.Mismatch)
	}
	below := mustPlanWithin(t, githubTestsPartitionBound{Work: free.PartitionWork - 1}, reports...)
	if len(below.Dropped) != 0 || len(below.Mismatch) != 0 || !slices.Equal(below.Admitted, []int{0, 1, 2}) {
		t.Fatalf("one below the bound: dropped %+v mismatch %+v admitted %v, want all kept and nothing marked", below.Dropped, below.Mismatch, below.Admitted)
	}
}

// Parts are searched first among artifacts whose suites are all in the total: the identity partition 1+2 comes first in id order
// but differs in durations, 3+4 holds every row, so the total is dropped against 3+4. When no partition holds every row (the total's
// own durations differ from all), the first identity partition proves a mismatch.
func TestGitHubTestsPlanRunReportsSearchesExactPartsFirst(t *testing.T) {
	reports := []githubTestsParsedReport{
		boundReport("1", boundSuite{"a", 1, 0.5}, boundSuite{"b", 1, 0.5}), boundReport("2", boundSuite{"c", 1, 0.5}, boundSuite{"d", 1, 0.5}),
		boundReport("3", boundSuite{"a", 1, 0.1}, boundSuite{"c", 1, 0.1}), boundReport("4", boundSuite{"b", 1, 0.1}, boundSuite{"d", 1, 0.1}),
		boundReport("5", boundSuite{"a", 1, 0.1}, boundSuite{"b", 1, 0.1}, boundSuite{"c", 1, 0.1}, boundSuite{"d", 1, 0.1}),
	}
	exact := mustPlanWithin(t, githubTestsDefaultPartitionBound, reports...)
	if len(exact.Dropped) != 1 || exact.Dropped[0].Index != 4 || len(exact.Mismatch) != 0 {
		t.Fatalf("exact parts present: dropped %+v mismatch %+v, want index 4 dropped", exact.Dropped, exact.Mismatch)
	}
	reports[4] = boundReport("5", boundSuite{"a", 1, 0.9}, boundSuite{"b", 1, 0.9}, boundSuite{"c", 1, 0.9}, boundSuite{"d", 1, 0.9})
	none := mustPlanWithin(t, githubTestsDefaultPartitionBound, reports...)
	if len(none.Dropped) != 0 || len(none.Mismatch) != 1 || none.Mismatch[0].Index != 4 || !slices.Equal(none.Mismatch[0].Parts, []int{0, 1}) {
		t.Fatalf("no exact parts: dropped %+v mismatch %+v, want index 4 a mismatch against 0 and 1", none.Dropped, none.Mismatch)
	}
}

// The default bound against acr's run (ten shards and their aggregate, about 73,000 held rows): the aggregate is dropped well
// inside the bound.
func TestGitHubTestsPlanRunReportsDefaultBoundCoversACR(t *testing.T) {
	parts, aggregate, _ := acrRun()
	var reports []githubTestsParsedReport
	at := time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)
	for _, artifact := range append(parts, aggregate) {
		rows, err := parseGitHubTestsReportArtifact(githubTestsZip(t, map[string]string{"junit.xml": artifact.body}), artifact.id,
			githubTestsReportSlot{Name: "n" + artifact.id, Attempt: 1}, nil, "repo", "run", "org", &at, &at, at)
		if err != nil {
			t.Fatal(err)
		}
		reports = append(reports, githubTestsParsedReport{ID: json.Number(artifact.id), Name: "n" + artifact.id, Suites: rows.Suites, Cases: rows.Cases})
	}
	plan := mustPlanWithin(t, githubTestsDefaultPartitionBound, reports...)
	if len(plan.Dropped) != 1 || plan.Dropped[0].Index != len(parts) || len(plan.Mismatch) != 0 {
		t.Fatalf("dropped %+v mismatch %+v, want only the aggregate", plan.Dropped, plan.Mismatch)
	}
	t.Logf("acr run: partition work %d of %d", plan.PartitionWork, githubTestsDefaultPartitionBound.Work)
	if plan.PartitionWork*8 > githubTestsDefaultPartitionBound.Work {
		t.Fatalf("acr run spent %d of %d: want under an eighth of the bound", plan.PartitionWork, githubTestsDefaultPartitionBound.Work)
	}
}
