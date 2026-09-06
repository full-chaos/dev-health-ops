package testops

import "time"

// This file holds the small test-only helpers that survive
// CHAOS-5245's deletion of compute_test.go (the live-Python-oracle rot
// guards for compute_pipeline_metrics_daily/compute_test_metrics_daily/
// compute_coverage_metrics_daily -- compute_testops.py itself is deleted,
// so nothing can execute those oracles anymore). accumulator_test.go's
// three PURE-GO tests (TestPipelineAccumulatorMatchesSliceAPI,
// TestTestAccumulatorCaseGroupMatchesRawCaseRows,
// TestTestAccumulatorEmptyInputMatchesSliceAPI -- streaming accumulator
// vs slice-API parity, no Python involved) still need these.

func floatPtr(v float64) *float64 { return &v }

func floatTime(year int, month time.Month, day, hour, minute, second int) time.Time {
	return time.Date(year, month, day, hour, minute, second, 0, time.UTC)
}

func floatTimePtr(year int, month time.Month, day, hour, minute, second int) *time.Time {
	v := floatTime(year, month, day, hour, minute, second)
	return &v
}

// fakeRepoTeamResolver is a minimal RepoTeamResolver for tests -- an exact
// repoName match returns the fixed team, everything else resolves to
// ("", "") (Python's None, None miss).
type fakeRepoTeamResolver struct {
	repoName, teamID, teamName string
}

func (r fakeRepoTeamResolver) ResolveRepo(repoName string) (string, string) {
	if repoName == r.repoName {
		return r.teamID, r.teamName
	}
	return "", ""
}
