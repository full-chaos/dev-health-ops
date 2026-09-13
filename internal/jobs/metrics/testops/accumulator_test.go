package testops

import (
	"fmt"
	"math"
	"reflect"
	"testing"

	"github.com/google/uuid"
)

// ----------------------------------------------------------------------
// CHAOS-4284. Two things are proved here:
//
//  1. avg_queue_seconds matches CPython's own sum() (Neumaier compensated),
//     not a naive accumulation -- against the LIVE Python authority.
//  2. The streaming accumulators added for the native testops_pipeline/
//     testops_test families produce byte-identical output to the slice APIs
//     the four existing live-Python oracles already cover, including via the
//     ClickHouse-reduced CaseGroup shape the native reader actually returns.
//
// (2) is what lets the native executors reuse the oracle-proved compute
// instead of carrying a second implementation that has to be kept in sync by
// hand.
// ----------------------------------------------------------------------

// TestComputePipelineMetricsAvgQueueMatchesLivePythonSum (the live-Python
// mean()/Neumaier-sum oracle) was deleted by CHAOS-5245 alongside
// compute_testops.py -- there is no more live Python compute to compare
// against. The Neumaier-sum requirement itself is still enforced: see
// internal/pythonparity's own TestFMAGoldenMatchesLivePython-style guards,
// and ComputePipelineMetrics's use of pythonparity.Sum directly.

// TestPipelineAccumulatorMatchesSliceAPI proves the streaming path the native
// executor uses is identical to the slice path the live-Python oracles cover.
// Inputs deliberately span the grouping subtleties CHAOS-4294's codex round 1
// found: nil vs "" service_id (two distinct groups) and a nil team_id that
// must resolve through the repo-pattern resolver.
func TestPipelineAccumulatorMatchesSliceAPI(t *testing.T) {
	repoID := uuid.MustParse("d4f322ad-2102-1fbf-8425-7400573194f7")
	const orgID = "70d529e0-3c06-4597-8480-794fd02328b6"
	resolver := fakeRepoTeamResolver{repoName: "acme/widgets", teamID: "team-a", teamName: "Team A"}
	rows := []PipelineRunRow{
		{RepoID: repoID, Status: strPtr("success"), StartedAt: floatTime(2026, 8, 27, 1, 0, 0),
			FinishedAt: floatTimePtr(2026, 8, 27, 1, 5, 0), QueueSeconds: floatPtr(0.1),
			ServiceID: nil, OrgID: orgID},
		{RepoID: repoID, Status: strPtr("FAILED"), StartedAt: floatTime(2026, 8, 27, 2, 0, 0),
			FinishedAt: floatTimePtr(2026, 8, 27, 2, 7, 0), QueueSeconds: floatPtr(0.1),
			ServiceID: strPtr(""), OrgID: orgID},
		{RepoID: repoID, Status: strPtr("  Cancelled  "), StartedAt: floatTime(2026, 8, 27, 3, 0, 0),
			FinishedAt: floatTimePtr(2026, 8, 27, 3, 1, 0), RetryCount: 2,
			ServiceID: strPtr("svc-1"), TeamID: strPtr("team-explicit"), OrgID: orgID},
		{RepoID: repoID, Status: strPtr("success"), StartedAt: floatTime(2026, 8, 27, 4, 0, 0),
			FinishedAt: floatTimePtr(2026, 8, 27, 4, 9, 0), QueueSeconds: floatPtr(0.1),
			ServiceID: nil, OrgID: orgID},
	}

	want := ComputePipelineMetrics(repoID, rows, "acme/widgets", resolver)
	accumulator := NewPipelineAccumulator(repoID, "acme/widgets", resolver)
	for _, row := range rows {
		accumulator.Add(row)
	}
	got := accumulator.Finish()

	if len(want) != len(got) {
		t.Fatalf("slice API produced %d rows, accumulator %d", len(want), len(got))
	}
	if len(want) < 3 {
		t.Fatalf("fixture collapsed to %d groups; it must exercise >=3 distinct (team, service) groups", len(want))
	}
	for index := range want {
		if !pipelineMetricsIdentical(want[index], got[index]) {
			t.Fatalf("row %d differs:\nslice=%+v\naccum=%+v", index, want[index], got[index])
		}
	}
}

// TestTestAccumulatorCaseGroupMatchesRawCaseRows proves the ClickHouse
// pushdown shape (one CaseGroup per case_name, carrying the distinct RAW
// status strings and max retry_attempt) reduces to exactly what feeding the
// individual rows produces. This is the in-process half of the pushdown
// differential; testops_native_integration_test.go proves the same equality
// against a real ClickHouse, where the reduction is actually performed.
func TestTestAccumulatorCaseGroupMatchesRawCaseRows(t *testing.T) {
	repoID := uuid.MustParse("d4f322ad-2102-1fbf-8425-7400573194f7")
	const orgID = "70d529e0-3c06-4597-8480-794fd02328b6"
	suites := []SuiteRow{{
		RepoID: repoID, RunID: "run-1", SuiteID: "suite-1",
		TotalCount: 6, PassedCount: 3, FailedCount: 2, SkippedCount: 1, ErrorCount: 1,
		QuarantinedCount: 1, DurationSeconds: floatPtr(12.5),
		StartedAt: floatTimePtr(2026, 8, 27, 1, 0, 0), OrgID: orgID,
	}}
	// flaky: passes and fails, and only ever on a retry. recurring: fails,
	// and is in the historical set. clean: passes on attempt 0 only.
	// Whitespace and case variants are included because normalisation is the
	// step deliberately NOT pushed into SQL.
	cases := []CaseRow{
		{RepoID: repoID, RunID: "run-1", SuiteID: "suite-1", CaseName: "flaky", Status: strPtr("FAILED"), RetryAttempt: 0},
		{RepoID: repoID, RunID: "run-1", SuiteID: "suite-1", CaseName: "flaky", Status: strPtr(" Passed "), RetryAttempt: 2},
		{RepoID: repoID, RunID: "run-1", SuiteID: "suite-1", CaseName: "recurring", Status: strPtr("timed_out"), RetryAttempt: 0},
		{RepoID: repoID, RunID: "run-1", SuiteID: "suite-1", CaseName: "clean", Status: strPtr("succeeded"), RetryAttempt: 0},
		{RepoID: repoID, RunID: "run-1", SuiteID: "suite-1", CaseName: "", Status: strPtr("passed"), RetryAttempt: 0},
	}
	historical := map[string]struct{}{"recurring": {}}

	want := ComputeTestMetrics(repoID, suites, cases, historical, "", nil)

	// The same information after ClickHouse's GROUP BY case_name.
	groups := []CaseGroup{
		{CaseName: "flaky", Statuses: []string{"FAILED", " Passed "}, MaxRetry: 2},
		{CaseName: "recurring", Statuses: []string{"timed_out"}, MaxRetry: 0},
		{CaseName: "clean", Statuses: []string{"succeeded"}, MaxRetry: 0},
	}
	accumulator := NewTestAccumulator(repoID, "", nil)
	for _, suite := range suites {
		accumulator.AddSuite(suite)
	}
	for _, group := range groups {
		accumulator.AddCaseGroup(group)
	}
	got := accumulator.Finish(historical)

	if len(want) != 1 || len(got) != 1 {
		t.Fatalf("want 1 row from each path, got slice=%d accumulator=%d", len(want), len(got))
	}
	if !testMetricsIdentical(want[0], got[0]) {
		t.Fatalf("case-group path differs:\nrows  =%+v\ngroups=%+v", want[0], got[0])
	}
	// Guard against a vacuous pass: the fixture must actually exercise the
	// three derived rates, or "identical" would prove nothing.
	if want[0].FlakeRate == 0 || want[0].RetryDependencyRate == 0 || want[0].FailureRecurrence == 0 {
		t.Fatalf("fixture is vacuous -- flake/retry/recurrence rates must all be nonzero, got %+v", want[0])
	}
}

// TestTestAccumulatorEmptyInputMatchesSliceAPI pins the nil-return contract
// (Python's `if not repo_suites and not repo_cases: continue`) on both paths.
func TestTestAccumulatorEmptyInputMatchesSliceAPI(t *testing.T) {
	repoID := uuid.MustParse("d4f322ad-2102-1fbf-8425-7400573194f7")
	if got := ComputeTestMetrics(repoID, nil, nil, nil, "", nil); got != nil {
		t.Fatalf("slice API on empty input = %+v, want nil", got)
	}
	if got := NewTestAccumulator(repoID, "", nil).Finish(nil); got != nil {
		t.Fatalf("accumulator on empty input = %+v, want nil", got)
	}
}

// testMetricsIdentical compares two TestMetrics field by field. A plain `!=`
// on the struct compares the *float64 fields by ADDRESS, so two runs holding
// equal values but distinct pointers always differ -- a false red that says
// nothing about parity.
func testMetricsIdentical(a, b TestMetric) bool {
	return a.RepoID == b.RepoID &&
		a.TotalCases == b.TotalCases && a.PassedCount == b.PassedCount &&
		a.FailedCount == b.FailedCount && a.SkippedCount == b.SkippedCount &&
		a.QuarantinedCount == b.QuarantinedCount &&
		identicalFloat(a.PassRate, b.PassRate) && identicalFloat(a.FailureRate, b.FailureRate) &&
		identicalFloat(a.FlakeRate, b.FlakeRate) &&
		identicalFloat(a.RetryDependencyRate, b.RetryDependencyRate) &&
		a.TotalSuites == b.TotalSuites &&
		identicalFloatPtr(a.SuiteDurationP50Seconds, b.SuiteDurationP50Seconds) &&
		identicalFloatPtr(a.SuiteDurationP95Seconds, b.SuiteDurationP95Seconds) &&
		identicalFloat(a.FailureRecurrence, b.FailureRecurrence) &&
		identicalStrPtr(a.TeamID, b.TeamID) && identicalStrPtr(a.ServiceID, b.ServiceID) &&
		a.OrgID == b.OrgID
}

// pipelineMetricsIdentical compares two PipelineMetrics field by field,
// dereferencing the pointer fields, so a difference reports as a value
// mismatch rather than two never-equal addresses.
func pipelineMetricsIdentical(a, b PipelineMetric) bool {
	return a.RepoID == b.RepoID &&
		a.PipelinesCount == b.PipelinesCount && a.SuccessCount == b.SuccessCount &&
		a.FailureCount == b.FailureCount && a.CancelledCount == b.CancelledCount &&
		identicalFloat(a.SuccessRate, b.SuccessRate) && identicalFloat(a.FailureRate, b.FailureRate) &&
		identicalFloat(a.CancelRate, b.CancelRate) && identicalFloat(a.RerunRate, b.RerunRate) &&
		identicalFloatPtr(a.MedianDurationSeconds, b.MedianDurationSeconds) &&
		identicalFloatPtr(a.P95DurationSeconds, b.P95DurationSeconds) &&
		identicalFloatPtr(a.AvgQueueSeconds, b.AvgQueueSeconds) &&
		identicalFloatPtr(a.P95QueueSeconds, b.P95QueueSeconds) &&
		identicalStrPtr(a.TeamID, b.TeamID) && identicalStrPtr(a.ServiceID, b.ServiceID) &&
		a.OrgID == b.OrgID
}

// identicalFloat compares by BIT PATTERN, not by ==, so this differential can
// never pass on a value that merely rounds to the same printed decimal -- the
// whole point of the parity work is bit-exactness. NaN compares equal to NaN
// here, which == would not do.
func identicalFloat(a, b float64) bool {
	return math.Float64bits(a) == math.Float64bits(b)
}

func identicalFloatPtr(a, b *float64) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return identicalFloat(*a, *b)
}

func identicalStrPtr(a, b *string) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

// A record finished without a resolver and resolved afterwards must equal the
// record finished with that resolver, for every way the first suite's team
// can be set, and a record with no suites must keep a nil team.
func TestTestMetricResolveTeamMatchesFinishWithResolver(t *testing.T) {
	repoID := uuid.MustParse("00000000-0000-4000-8000-0000000000d4")
	repoName := repoID.String()
	explicit := "suite-team"
	empty := ""
	resolvers := map[string]RepoTeamResolver{
		"nil":      nil,
		"matching": fixedRepoTeamResolver{repoName: repoName, teamID: "pattern-team"},
		"missing":  fixedRepoTeamResolver{repoName: "another-repo", teamID: "pattern-team"},
	}
	suiteTeams := map[string]*string{"nil": nil, "empty": &empty, "explicit": &explicit}

	finish := func(resolver RepoTeamResolver, suiteTeam *string, withSuite bool) []TestMetric {
		accumulator := NewTestAccumulator(repoID, repoName, resolver)
		if withSuite {
			accumulator.AddSuite(SuiteRow{RepoID: repoID, RunID: "run", SuiteID: "suite", TotalCount: 2, PassedCount: 1, TeamID: suiteTeam})
		}
		accumulator.AddCaseGroup(CaseGroup{CaseName: "case", Statuses: []string{"failed", "passed"}})
		return accumulator.Finish(map[string]struct{}{"case": {}})
	}

	for resolverName, resolver := range resolvers {
		for teamName, suiteTeam := range suiteTeams {
			for _, withSuite := range []bool{true, false} {
				want := finish(resolver, suiteTeam, withSuite)
				unresolved := finish(nil, suiteTeam, withSuite)
				if len(want) != 1 || len(unresolved) != 1 {
					t.Fatalf("resolver=%s team=%s suite=%v: want one record each, got %d and %d",
						resolverName, teamName, withSuite, len(want), len(unresolved))
				}
				got := unresolved[0].ResolveTeam(repoName, resolver)
				if !sameTeam(want[0].TeamID, got.TeamID) {
					t.Fatalf("resolver=%s team=%s suite=%v: ResolveTeam gave %v, Finish with the resolver gave %v",
						resolverName, teamName, withSuite, derefForMessage(got.TeamID), derefForMessage(want[0].TeamID))
				}
				got.TeamID, want[0].TeamID = nil, nil
				if !reflect.DeepEqual(want[0], got) {
					t.Fatalf("resolver=%s team=%s suite=%v: ResolveTeam changed more than the team:\nwant %+v\ngot  %+v",
						resolverName, teamName, withSuite, want[0], got)
				}
			}
		}
	}
}

type fixedRepoTeamResolver struct{ repoName, teamID string }

func (resolver fixedRepoTeamResolver) ResolveRepo(repoName string) (string, string) {
	if repoName != resolver.repoName {
		return "", ""
	}
	return resolver.teamID, resolver.teamID
}

func sameTeam(a, b *string) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

func derefForMessage(value *string) string {
	if value == nil {
		return "<nil>"
	}
	return fmt.Sprintf("%q", *value)
}
