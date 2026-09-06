package providersync

import (
	"encoding/json"
	"testing"
)

func linearizeWorkItemOracleCases(cases []oracleCase) []oracleCase {
	result := make([]oracleCase, 0, len(cases))
	for _, testCase := range cases {
		encoded, err := json.Marshal(testCase.Input)
		if err != nil {
			panic(err)
		}
		var input map[string]any
		if err := json.Unmarshal(encoded, &input); err != nil {
			panic(err)
		}
		linearizeOracleProviders(input)
		result = append(result, oracleCase{ID: testCase.ID, Input: input})
	}
	return result
}

func linearizeOracleProviders(value any) {
	switch typed := value.(type) {
	case map[string]any:
		for key, child := range typed {
			if (key == "provider" || key == "Provider") && child == "github" {
				typed[key] = "linear"
				continue
			}
			linearizeOracleProviders(child)
		}
	case []any:
		for _, child := range typed {
			linearizeOracleProviders(child)
		}
	}
}

func TestLinearWorkItemDerivedSurfacesMatchLivePythonProduction(t *testing.T) {
	cases := linearizeWorkItemOracleCases(githubDerivedOracleCases())
	// CHAOS-5323/CHAOS-3092: no "linear/work-items/estimate-coverage" oracle
	// pair here anymore -- compute_estimate_coverage_metrics_daily is
	// deleted entirely (work_item_estimate is fully native, no remaining
	// Python caller), and its oracle_pairs script (linear_work-
	// items_estimate-coverage.py) is deleted with it.
	// CHAOS-5321/CHAOS-3092 (R6): compute_work_item_team_attributions/
	// compute_work_item_state_durations_daily are deleted (native Go
	// executor + providersync own these tables now) -- frozen, see
	// testdata/oracle_frozen/README.md.
	compareRowsAgainstFrozenOracle(t, "linear_work-items_team-attributions", cases,
		func(t *testing.T, input map[string]any) githubTeamAttributionColumns {
			return newGitHubTeamAttributionColumns(buildGitHubDerivedOracleSurfaces(t, input).TeamAttributions)
		}, nil)
	compareRowsAgainstFrozenOracle(t, "linear_work-items_state-durations", cases,
		func(t *testing.T, input map[string]any) githubStateDurationColumns {
			return newGitHubStateDurationColumns(buildGitHubDerivedOracleSurfaces(t, input).StateDurations)
		}, nil)
}

func TestLinearWorkItemMetricTripletMatchesLivePythonProduction(t *testing.T) {
	cases := linearizeWorkItemOracleCases(githubWorkItemMetricTripletOracleCases())
	// CHAOS-5310/CHAOS-3092 (R6): compute_work_item_metrics_daily is deleted
	// (native Go executor + providersync own work_item_metrics_daily now) --
	// frozen, see testdata/oracle_frozen/README.md.
	compareRowsAgainstFrozenOracle(t, "linear_work-items_metrics-daily", cases,
		func(t *testing.T, input map[string]any) githubWorkItemMetricsDailyOracleColumns {
			return githubWorkItemMetricTripletOracleResult(t, input).metricsDaily
		}, nil)
	compareRowsAgainstFrozenOracle(t, "linear_work-items_user-metrics-daily", cases,
		func(t *testing.T, input map[string]any) githubWorkItemUserMetricsDailyOracleColumns {
			return githubWorkItemMetricTripletOracleResult(t, input).userMetricsDaily
		}, nil)
	compareRowsAgainstFrozenOracle(t, "linear_work-items_cycle-times", cases,
		func(t *testing.T, input map[string]any) githubWorkItemCycleTimeOracleColumns {
			return githubWorkItemMetricTripletOracleResult(t, input).cycleTimes
		}, nil)
}

func TestLinearWorkItemEngineDestinationsMatchLivePythonProduction(t *testing.T) {
	cases := linearizeWorkItemOracleCases(githubDerivedOracleCases())
	compareRowsAgainstPythonOracle(t, "linear/work-items/issue-type-metrics", cases,
		func(t *testing.T, input map[string]any) githubIssueTypeMetricsColumns {
			issueTypes, _, _ := buildGitHubWorkItemEngineOracleRows(t, input)
			return newGitHubIssueTypeMetricsColumns(issueTypes)
		}, nil)
	compareRowsAgainstPythonOracle(t, "linear/work-items/investment-classifications", cases,
		func(t *testing.T, input map[string]any) githubInvestmentClassificationColumns {
			_, classifications, _ := buildGitHubWorkItemEngineOracleRows(t, input)
			return newGitHubInvestmentClassificationColumns(classifications)
		}, nil)
	compareRowsAgainstPythonOracle(t, "linear/work-items/investment-metrics", cases,
		func(t *testing.T, input map[string]any) githubInvestmentMetricsColumns {
			_, _, metrics := buildGitHubWorkItemEngineOracleRows(t, input)
			return newGitHubInvestmentMetricsColumns(metrics)
		}, nil)
}

func TestLinearWorkItemDerivedOracleInputsExerciseEveryImplementedDestination(t *testing.T) {
	surfaceCases := linearizeWorkItemOracleCases(githubDerivedOracleCases())
	coverageRows, teamRows, stateRows := 0, 0, 0
	for _, testCase := range surfaceCases {
		surfaces := buildGitHubDerivedOracleSurfaces(t, testCase.Input)
		coverageRows += len(surfaces.EstimateCoverage)
		teamRows += len(surfaces.TeamAttributions)
		stateRows += len(surfaces.StateDurations)
	}
	if coverageRows == 0 || teamRows == 0 || stateRows == 0 {
		t.Fatalf("Linear surface oracle batch is vacuous: coverage=%d team=%d state=%d", coverageRows, teamRows, stateRows)
	}
	metricCases := linearizeWorkItemOracleCases(githubWorkItemMetricTripletOracleCases())
	groupRows, userRows, cycleRows := 0, 0, 0
	for _, testCase := range metricCases {
		result := githubWorkItemMetricTripletOracleResult(t, testCase.Input)
		groupRows += len(result.metricsDaily.Provider)
		userRows += len(result.userMetricsDaily.Provider)
		cycleRows += len(result.cycleTimes.Provider)
	}
	if groupRows == 0 || userRows == 0 || cycleRows == 0 {
		t.Fatalf("Linear metric oracle batch is vacuous: groups=%d users=%d cycles=%d", groupRows, userRows, cycleRows)
	}
}
