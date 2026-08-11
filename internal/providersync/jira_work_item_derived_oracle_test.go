package providersync

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

func jiraDerivedOracleCases(cases []oracleCase) []oracleCase {
	result := make([]oracleCase, 0, len(cases))
	for _, testCase := range cases {
		result = append(result, oracleCase{
			ID:    "jira_" + testCase.ID,
			Input: jiraDerivedOracleValue(testCase.Input).(map[string]any),
		})
	}
	return result
}

func jiraDerivedOracleValue(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		result := make(map[string]any, len(typed))
		for key, nested := range typed {
			result[key] = jiraDerivedOracleValue(nested)
		}
		return result
	case []any:
		result := make([]any, len(typed))
		for index, nested := range typed {
			result[index] = jiraDerivedOracleValue(nested)
		}
		return result
	case string:
		if typed == "github" {
			return "jira"
		}
		if strings.HasPrefix(typed, "gh:") {
			return "jira:" + strings.TrimPrefix(typed, "gh:")
		}
		return typed
	default:
		return value
	}
}

func jiraDerivedOracleRows(
	t *testing.T,
	input map[string]any,
) (Claim, githubWorkItemRows, githubWorkItemDerivationContext) {
	t.Helper()
	_, rows, derived := gitlabWorkItemOracleRows(t, input)
	claim := nativeTestClaim("jira", "work-items")
	claim.OrgID = input["OrgID"].(string)
	return claim, rows, derived
}

func TestJiraWorkItemMetricTripletMatchesLivePythonProduction(t *testing.T) {
	cases := jiraDerivedOracleCases(githubWorkItemMetricTripletOracleCases())
	compareRowsAgainstPythonOracle(t, "jira/work-items/metrics-daily", cases,
		func(t *testing.T, input map[string]any) githubWorkItemMetricsDailyOracleColumns {
			claim, rows, derived := jiraDerivedOracleRows(t, input)
			triplet, err := buildWorkItemMetricTripletForProvider(
				"jira", claim, rows, gitlabWorkItemOracleDay(t, input),
				gitlabWorkItemOracleComputedAt(t, input), derived,
			)
			if err != nil {
				t.Fatal(err)
			}
			return newGitHubWorkItemMetricsDailyOracleColumns(len(triplet.MetricsDaily)).fromRows(triplet.MetricsDaily)
		}, nil)
	compareRowsAgainstPythonOracle(t, "jira/work-items/user-metrics-daily", cases,
		func(t *testing.T, input map[string]any) githubWorkItemUserMetricsDailyOracleColumns {
			claim, rows, derived := jiraDerivedOracleRows(t, input)
			triplet, err := buildWorkItemMetricTripletForProvider(
				"jira", claim, rows, gitlabWorkItemOracleDay(t, input),
				gitlabWorkItemOracleComputedAt(t, input), derived,
			)
			if err != nil {
				t.Fatal(err)
			}
			return newGitHubWorkItemUserMetricsDailyOracleColumns(len(triplet.UserMetricsDaily)).fromRows(triplet.UserMetricsDaily)
		}, nil)
	compareRowsAgainstPythonOracle(t, "jira/work-items/cycle-times", cases,
		func(t *testing.T, input map[string]any) githubWorkItemCycleTimeOracleColumns {
			claim, rows, derived := jiraDerivedOracleRows(t, input)
			triplet, err := buildWorkItemMetricTripletForProvider(
				"jira", claim, rows, gitlabWorkItemOracleDay(t, input),
				gitlabWorkItemOracleComputedAt(t, input), derived,
			)
			if err != nil {
				t.Fatal(err)
			}
			return newGitHubWorkItemCycleTimeOracleColumns(len(triplet.CycleTimes)).fromRows(triplet.CycleTimes)
		}, nil)
}

func TestJiraDerivedSurfacesMatchLivePythonProduction(t *testing.T) {
	cases := jiraDerivedOracleCases(githubDerivedOracleCases())
	compareRowsAgainstPythonOracle(t, "jira/work-items/estimate-coverage", cases,
		func(t *testing.T, input map[string]any) githubEstimateCoverageColumns {
			claim, rows, derived := jiraDerivedOracleRows(t, input)
			surfaces, err := buildWorkItemDerivedSurfacesForProvider(
				"jira", claim, rows, gitlabWorkItemOracleDay(t, input),
				gitlabWorkItemOracleComputedAt(t, input), derived,
			)
			if err != nil {
				t.Fatal(err)
			}
			return newGitHubEstimateCoverageColumns(surfaces.EstimateCoverage)
		}, nil)
	compareRowsAgainstPythonOracle(t, "jira/work-items/team-attributions", cases,
		func(t *testing.T, input map[string]any) githubTeamAttributionColumns {
			claim, rows, derived := jiraDerivedOracleRows(t, input)
			surfaces, err := buildWorkItemDerivedSurfacesForProvider(
				"jira", claim, rows, gitlabWorkItemOracleDay(t, input),
				gitlabWorkItemOracleComputedAt(t, input), derived,
			)
			if err != nil {
				t.Fatal(err)
			}
			return newGitHubTeamAttributionColumns(surfaces.TeamAttributions)
		}, nil)
	compareRowsAgainstPythonOracle(t, "jira/work-items/state-durations", cases,
		func(t *testing.T, input map[string]any) githubStateDurationColumns {
			claim, rows, derived := jiraDerivedOracleRows(t, input)
			surfaces, err := buildWorkItemDerivedSurfacesForProvider(
				"jira", claim, rows, gitlabWorkItemOracleDay(t, input),
				gitlabWorkItemOracleComputedAt(t, input), derived,
			)
			if err != nil {
				t.Fatal(err)
			}
			return newGitHubStateDurationColumns(surfaces.StateDurations)
		}, nil)
}

func TestJiraEngineDestinationsMatchLivePythonProduction(t *testing.T) {
	cases := jiraDerivedOracleCases(githubWorkItemEngineOracleCases())
	compareRowsAgainstPythonOracle(t, "jira/work-items/issue-type-metrics", cases,
		func(t *testing.T, input map[string]any) githubIssueTypeMetricsColumns {
			return newGitHubIssueTypeMetricsColumns(jiraEngineOracleResult(t, input).IssueTypeMetricsDaily)
		}, nil)
	compareRowsAgainstPythonOracle(t, "jira/work-items/investment-classifications", cases,
		func(t *testing.T, input map[string]any) githubInvestmentClassificationColumns {
			return newGitHubInvestmentClassificationColumns(jiraEngineOracleResult(t, input).InvestmentClassificationsDaily)
		}, nil)
	compareRowsAgainstPythonOracle(t, "jira/work-items/investment-metrics", cases,
		func(t *testing.T, input map[string]any) githubInvestmentMetricsColumns {
			return newGitHubInvestmentMetricsColumns(jiraEngineOracleResult(t, input).InvestmentMetricsDaily)
		}, nil)
}

func jiraEngineOracleResult(t *testing.T, input map[string]any) JiraWorkItemDerivedRows {
	t.Helper()
	claim, rows, _ := jiraDerivedOracleRows(t, input)
	facts := githubWorkItemDerivationFacts{}
	encoded, err := json.Marshal(input["Facts"])
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(encoded, &facts); err != nil {
		t.Fatal(err)
	}
	classifier, err := NewInvestmentClassifier(investmentConfigPath(t, "real"))
	if err != nil {
		t.Fatal(err)
	}
	deriver := JiraWorkItemDeriver{
		Source:        &githubMultiDayOracleSource{facts: facts},
		statusMapping: loadRealStatusMapping(t), investmentClassifier: classifier,
	}
	if sinceRaw, ok := input["SinceAt"].(string); ok {
		since := githubDerivedOracleTime(t, sinceRaw)
		before := githubDerivedOracleTime(t, input["BeforeAt"].(string))
		claim.SinceAt, claim.BeforeAt = &since, &before
	} else {
		day := gitlabWorkItemOracleDay(t, input)
		before := day.AddDate(0, 0, 1)
		claim.SinceAt, claim.BeforeAt = &day, &before
	}
	result, err := deriver.Derive(
		context.Background(), claim, jiraRowsFromGitHub(rows),
		gitlabWorkItemOracleComputedAt(t, input),
	)
	if err != nil {
		t.Fatal(err)
	}
	if result.Watermark == nil || !result.Watermark.Equal(*claim.BeforeAt) {
		t.Fatalf("derived watermark=%v want=%v", result.Watermark, claim.BeforeAt)
	}
	return result
}

func jiraRowsFromGitHub(rows githubWorkItemRows) jiraWorkItemRows {
	return jiraWorkItemRows{
		WorkItems: rows.WorkItems, Transitions: rows.StatusTransitions,
		Dependencies: rows.Dependencies, ReopenEvents: rows.ReopenEvents,
		Interactions: rows.Interactions, Sprints: rows.Sprints,
	}
}

// TestJiraWorkItemsRouteIncludesLivePythonMetricEffect is baseline capability
// proof, not a destination-count assertion. It feeds the concrete rows emitted
// by the Jira Atlassian route into Python's checked-in job computation, then
// compares those produced rows with the route's actual effect ledger.
func TestJiraWorkItemsRouteIncludesLivePythonMetricEffect(t *testing.T) {
	claim := jiraAtlassianClaim()
	since := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	before := time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC)
	claim.SinceAt, claim.BeforeAt = &since, &before
	normalizedAt := time.Date(2026, 8, 10, 12, 0, 0, 123456000, time.UTC)
	client := jiraWorkItemsTestClient(
		t,
		&jiraAtlassianDoer{t: t},
		providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
	)
	classifier, err := NewInvestmentClassifier(investmentConfigPath(t, "real"))
	if err != nil {
		t.Fatal(err)
	}
	deriver := JiraWorkItemDeriver{
		Source:               &githubMultiDayOracleSource{},
		statusMapping:        loadRealStatusMapping(t),
		investmentClassifier: classifier,
	}
	batch, err := (JiraAtlassianRouteHandler{
		StatusMapping: loadRealStatusMapping(t),
		Identity:      jiraRouteIdentity,
		Derived:       deriver,
	}).Collect(context.Background(), claim, providerfoundation.Credential{}, client, normalizedAt)
	if err != nil {
		t.Fatal(err)
	}

	input := map[string]any{
		// The fixture's required changelog moves the item to Done at 09:00
		// on August 1, and Jira lifecycle normalization uses that transition
		// as completed_at. This day therefore produces a real completion row.
		"Day":        "2026-08-01",
		"ComputedAt": normalizedAt.Format(time.RFC3339Nano),
		"OrgID":      claim.OrgID,
		"WorkItems":  jiraEffectObjects(t, batch.Effects, "work_items"),
		"Transitions": jiraEffectObjects(
			t, batch.Effects, "work_item_transitions",
		),
		"Dependencies": jiraEffectObjects(
			t, batch.Effects, "work_item_dependencies",
		),
		"Donors": []any{},
		"Facts": map[string]any{
			"Teams": []any{}, "Projects": []any{}, "Repos": []any{},
			"Members": []any{}, "ManualFallbacks": []any{},
		},
	}
	compareRowsAgainstPythonOracle(
		t,
		"jira/work-items/metrics-daily",
		[]oracleCase{{ID: "atlassian_route_non_empty", Input: input}},
		func(t *testing.T, _ map[string]any) githubWorkItemMetricsDailyOracleColumns {
			t.Helper()
			rows := make([]githubWorkItemMetricsDailyRow, 0)
			for _, effect := range batch.Effects {
				if effect.Destination != "work_item_metrics_daily" {
					continue
				}
				for _, raw := range effect.Rows {
					var row githubWorkItemMetricsDailyRow
					if err := json.Unmarshal(raw, &row); err != nil {
						t.Fatal(err)
					}
					rows = append(rows, row)
				}
			}
			return newGitHubWorkItemMetricsDailyOracleColumns(len(rows)).fromRows(rows)
		},
		nil,
	)
}

func jiraEffectObjects(t *testing.T, effects []EffectBatch, destination string) []any {
	t.Helper()
	for _, effect := range effects {
		if effect.Destination != destination {
			continue
		}
		objects := make([]any, 0, len(effect.Rows))
		for _, raw := range effect.Rows {
			var object map[string]any
			if err := json.Unmarshal(raw, &object); err != nil {
				t.Fatal(err)
			}
			objects = append(objects, object)
		}
		return objects
	}
	t.Fatalf("Jira route omitted direct destination %q", destination)
	return nil
}
