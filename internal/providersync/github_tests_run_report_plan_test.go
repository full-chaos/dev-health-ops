package providersync

import (
	"encoding/json"
	"slices"
	"strconv"
	"testing"
)

func planRunReportCases(prefix string, count int) githubTestsParsedReport {
	class := "pkg"
	cases := make([]testCaseResultRow, count)
	for index := range cases {
		cases[index] = testCaseResultRow{ClassName: &class, CaseName: prefix + strconv.Itoa(index), Status: "passed"}
	}
	return githubTestsParsedReport{ID: json.Number(prefix), Name: prefix, Cases: cases}
}

func mustPlanRunReports(t *testing.T, reports ...githubTestsParsedReport) githubTestsRunReportPlan {
	t.Helper()
	plan, err := githubTestsPlanRunReports(reports)
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

// The bound is exact: githubTestsMaxReportRowsPerRun rows (99,975: maxEffectRows less one retraction row per listed artifact,
// CHAOS-14259) are admitted, one more is not.
func TestGitHubTestsPlanRunReportsBoundaryIsExact(t *testing.T) {
	at := mustPlanRunReports(t, planRunReportCases("a", 60_000), planRunReportCases("b", githubTestsMaxReportRowsPerRun-60_000))
	if !slices.Equal(at.Admitted, []int{0, 1}) || len(at.OverBound) != 0 {
		t.Fatalf("at the bound: admitted %v over %v, want both admitted", at.Admitted, at.OverBound)
	}
	over := mustPlanRunReports(t, planRunReportCases("a", 60_000), planRunReportCases("b", githubTestsMaxReportRowsPerRun-60_000+1))
	if !slices.Equal(over.Admitted, []int{0}) || !slices.Equal(over.OverBound, []int{1}) {
		t.Fatalf("one row over the bound: admitted %v over %v, want [0] and [1]", over.Admitted, over.OverBound)
	}
}

// Artifacts of 60k, 30k and 60k rows in admission order (smallest declared size first): the first two fit, the last does not.
func TestGitHubTestsPlanRunReportsAdmitsInOrderUntilTheBound(t *testing.T) {
	plan := mustPlanRunReports(t, planRunReportCases("small", 60_000), planRunReportCases("middle", 30_000), planRunReportCases("big", 60_000))
	if !slices.Equal(plan.Admitted, []int{0, 1}) || !slices.Equal(plan.OverBound, []int{2}) {
		t.Fatalf("admitted %v over %v, want [0 1] and [2]", plan.Admitted, plan.OverBound)
	}
}

// An artifact that alone exceeds the bound is skipped whole, and the artifacts after it are still admitted.
func TestGitHubTestsPlanRunReportsSkipsAnArtifactThatAloneExceedsTheBound(t *testing.T) {
	plan := mustPlanRunReports(t, planRunReportCases("big", githubTestsMaxReportRowsPerRun+2), planRunReportCases("s1", 400), planRunReportCases("s2", 400))
	if !slices.Equal(plan.OverBound, []int{0}) || !slices.Equal(plan.Admitted, []int{1, 2}) {
		t.Fatalf("admitted %v over %v, want [1 2] and [0]", plan.Admitted, plan.OverBound)
	}
}

// The budget is per run: the plan is one run's decision, so two runs of 51,000 rows each (102,000 over the unit) are both admitted
// by two plans, while the same two artifacts in ONE run are not.
func TestGitHubTestsPlanRunReportsBudgetIsPerRun(t *testing.T) {
	for run := 0; run < 2; run++ {
		plan := mustPlanRunReports(t, planRunReportCases("r"+strconv.Itoa(run), 51_000))
		if !slices.Equal(plan.Admitted, []int{0}) || len(plan.OverBound) != 0 {
			t.Fatalf("run %d: admitted %v over %v, want its one artifact admitted", run, plan.Admitted, plan.OverBound)
		}
	}
	same := mustPlanRunReports(t, planRunReportCases("a", 51_000), planRunReportCases("b", 51_000))
	if !slices.Equal(same.Admitted, []int{0}) || !slices.Equal(same.OverBound, []int{1}) {
		t.Fatalf("one run: admitted %v over %v, want [0] and [1]", same.Admitted, same.OverBound)
	}
}

func listedArtifact(id string, size int64) githubTestsArtifactPayload {
	return githubTestsArtifactPayload{ID: json.Number(id), SizeInBytes: json.RawMessage(strconv.FormatInt(size, 10))}
}

func artifactListingOrders(items []githubTestsArtifactPayload) [][]githubTestsArtifactPayload {
	if len(items) <= 1 {
		return [][]githubTestsArtifactPayload{slices.Clone(items)}
	}
	var out [][]githubTestsArtifactPayload
	for index := range items {
		rest := slices.Concat(items[:index:index], items[index+1:])
		for _, tail := range artifactListingOrders(rest) {
			out = append(out, append([]githubTestsArtifactPayload{items[index]}, tail...))
		}
	}
	return out
}

// N = 4 artifacts, all 4! = 24 listing orders: one admission order, smallest declared size first, equal sizes by id (as a
// number, so 9 < 10), and an artifact with no usable size last.
func TestGitHubTestsArtifactAdmissionOrderDoesNotDependOnListingOrder(t *testing.T) {
	items := []githubTestsArtifactPayload{
		listedArtifact("10", 1000), listedArtifact("9", 1000), listedArtifact("5", 500),
		{ID: json.Number("1"), SizeInBytes: json.RawMessage("null")},
	}
	want := []string{"5", "9", "10", "1"}
	orders := artifactListingOrders(items)
	if len(orders) != 24 {
		t.Fatalf("%d listing orders, want 24", len(orders))
	}
	for _, order := range orders {
		githubTestsSortArtifactsForAdmission(order)
		var got []string
		for _, item := range order {
			got = append(got, string(item.ID))
		}
		if !slices.Equal(got, want) {
			t.Fatalf("admission order %v, want %v", got, want)
		}
	}
}
