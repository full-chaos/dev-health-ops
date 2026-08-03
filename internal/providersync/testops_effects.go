package providersync

import (
	"slices"
	"strings"
)

// sortTestOpsAcceptanceNames establishes a total order for provider-supplied
// check names. The case-insensitive primary key preserves Python ordering,
// while the case-sensitive tie-breaker prevents Go map iteration order from
// changing effect digests for distinct names such as "Unit" and "unit".
func sortTestOpsAcceptanceNames(names []string) {
	slices.SortFunc(names, func(a, b string) int {
		if folded := strings.Compare(strings.ToLower(a), strings.ToLower(b)); folded != 0 {
			return folded
		}
		return strings.Compare(a, b)
	})
}

// testOpsEffects is the single six-destination effect projection shared by
// GitHub and GitLab TestOps handlers. Keeping it provider-neutral prevents an
// alias or provider from silently omitting one destination while still
// claiming complete shared-row ownership.
func testOpsEffects(
	pipelines []githubTestsPipelineRow,
	jobs []githubTestsJobRow,
	acceptance []githubTestsAcceptanceRow,
	suites []testSuiteResultRow,
	cases []testCaseResultRow,
	coverage []coverageSnapshotRow,
) ([]EffectBatch, error) {
	values := []struct {
		destination string
		rows        any
	}{
		{"ci_pipeline_runs", pipelines}, {"ci_job_runs", jobs},
		{"ci_acceptance_checks", acceptance}, {"test_suite_results", suites},
		{"test_case_results", cases}, {"coverage_snapshots", coverage},
	}
	effects := make([]EffectBatch, 0, len(values))
	for _, value := range values {
		var effect EffectBatch
		var err error
		switch rows := value.rows.(type) {
		case []githubTestsPipelineRow:
			effect, err = effectBatchFromValues(value.destination, EffectReadbackRequired, rows)
		case []githubTestsJobRow:
			effect, err = effectBatchFromValues(value.destination, EffectReadbackRequired, rows)
		case []githubTestsAcceptanceRow:
			effect, err = effectBatchFromValues(value.destination, EffectReadbackRequired, rows)
		case []testSuiteResultRow:
			effect, err = effectBatchFromValues(value.destination, EffectReadbackRequired, rows)
		case []testCaseResultRow:
			effect, err = effectBatchFromValues(value.destination, EffectReadbackRequired, rows)
		case []coverageSnapshotRow:
			effect, err = effectBatchFromValues(value.destination, EffectReadbackRequired, rows)
		}
		if err != nil {
			return nil, err
		}
		effects = append(effects, effect)
	}
	return effects, nil
}
