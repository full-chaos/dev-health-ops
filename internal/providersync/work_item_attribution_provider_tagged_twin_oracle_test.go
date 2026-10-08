package providersync

import (
	"encoding/json"
	"testing"
)

// The frozen team answers were written for team facts with no provider (an
// admin team: admin create and admin import write provider ""). These twins
// give each such team the item's provider and must reach the same frozen
// answers: a team of the item's provider holds the key exactly as an admin
// team does when no team of the item's provider holds it. The main cases stay
// as they are; the twins are additional.
var providerTaggedTwinCaseIDs = map[string]struct{}{
	"populated_facts_membership_and_project": {},
	"issue_project_scope_needs_trimming":     {},
}

const providerTaggedTwinTripletCaseTeams = 2

func providerTaggedTeamTwins(t *testing.T, cases []oracleCase) []oracleCase {
	t.Helper()
	result := make([]oracleCase, 0, len(cases))
	tagged := 0
	for _, testCase := range cases {
		encoded, err := json.Marshal(testCase.Input)
		if err != nil {
			t.Fatal(err)
		}
		var input map[string]any
		if err := json.Unmarshal(encoded, &input); err != nil {
			t.Fatal(err)
		}
		if facts, ok := input["Facts"].(map[string]any); ok {
			teams, _ := facts["Teams"].([]any)
			for _, team := range teams {
				fact := team.(map[string]any)
				if provider, _ := fact["Provider"].(string); provider == "" {
					fact["Provider"] = "github"
					tagged++
				}
			}
		}
		result = append(result, oracleCase{ID: testCase.ID, Input: input})
	}
	if tagged == 0 {
		t.Fatal("no team fact without a provider was tagged: the twins compare nothing new")
	}
	return result
}

func TestProviderTaggedTwinsTagExactlyTheProviderlessTeams(t *testing.T) {
	count := func(cases []oracleCase) (byCase map[string]int) {
		byCase = map[string]int{}
		for _, testCase := range cases {
			facts, _ := testCase.Input["Facts"].(map[string]any)
			teams, _ := facts["Teams"].([]any)
			for _, team := range teams {
				if provider, _ := team.(map[string]any)["Provider"].(string); provider == "" {
					byCase[testCase.ID]++
				}
			}
		}
		return byCase
	}
	derived := count(githubDerivedOracleCases())
	if len(derived) != len(providerTaggedTwinCaseIDs) {
		t.Errorf("derived cases with provider-less teams = %v, want exactly %v", derived, providerTaggedTwinCaseIDs)
	}
	for id := range providerTaggedTwinCaseIDs {
		if derived[id] != 1 {
			t.Errorf("case %s has %d provider-less teams, want 1", id, derived[id])
		}
	}
	triplet := 0
	for _, n := range count(githubWorkItemMetricTripletOracleCases()) {
		triplet += n
	}
	if triplet != providerTaggedTwinTripletCaseTeams {
		t.Errorf("triplet provider-less teams = %d, want %d", triplet, providerTaggedTwinTripletCaseTeams)
	}
	for _, twins := range [][]oracleCase{
		providerTaggedTeamTwins(t, githubDerivedOracleCases()),
		providerTaggedTeamTwins(t, githubWorkItemMetricTripletOracleCases()),
	} {
		if left := count(twins); len(left) != 0 {
			t.Errorf("twins still carry provider-less teams: %v", left)
		}
	}
}

type providerTwinBuilders struct {
	cases   func([]oracleCase) []oracleCase
	team    func(t *testing.T, input map[string]any) githubTeamAttributionColumns
	triplet func(t *testing.T, input map[string]any) githubWorkItemMetricTripletOracleProjections
}

func providerTwinBuildersFor(provider string) providerTwinBuilders {
	switch provider {
	case "github", "linear":
		cases := func(cases []oracleCase) []oracleCase { return cases }
		if provider == "linear" {
			cases = linearizeWorkItemOracleCases
		}
		return providerTwinBuilders{
			cases: cases,
			team: func(t *testing.T, input map[string]any) githubTeamAttributionColumns {
				return newGitHubTeamAttributionColumns(buildGitHubDerivedOracleSurfaces(t, input).TeamAttributions)
			},
			triplet: githubWorkItemMetricTripletOracleResult,
		}
	}
	rowsOf := gitlabWorkItemOracleRows
	cases := gitlabOracleCases
	if provider == "jira" {
		rowsOf, cases = jiraDerivedOracleRows, jiraDerivedOracleCases
	}
	return providerTwinBuilders{
		cases: cases,
		team: func(t *testing.T, input map[string]any) githubTeamAttributionColumns {
			claim, rows, derived := rowsOf(t, input)
			surfaces, err := buildWorkItemDerivedSurfacesForProvider(provider, claim, rows, gitlabWorkItemOracleDay(t, input), gitlabWorkItemOracleComputedAt(t, input), derived, nil)
			if err != nil {
				t.Fatal(err)
			}
			return newGitHubTeamAttributionColumns(surfaces.TeamAttributions)
		},
		triplet: func(t *testing.T, input map[string]any) githubWorkItemMetricTripletOracleProjections {
			claim, rows, derived := rowsOf(t, input)
			triplet, err := buildWorkItemMetricTripletForProvider(provider, claim, rows, gitlabWorkItemOracleDay(t, input), gitlabWorkItemOracleComputedAt(t, input), derived)
			if err != nil {
				t.Fatal(err)
			}
			return githubWorkItemMetricTripletOracleProjections{
				metricsDaily:     newGitHubWorkItemMetricsDailyOracleColumns(len(triplet.MetricsDaily)).fromRows(triplet.MetricsDaily),
				userMetricsDaily: newGitHubWorkItemUserMetricsDailyOracleColumns(len(triplet.UserMetricsDaily)).fromRows(triplet.UserMetricsDaily),
				cycleTimes:       newGitHubWorkItemCycleTimeOracleColumns(len(triplet.CycleTimes)).fromRows(triplet.CycleTimes),
			}
		},
	}
}

func TestProviderTaggedTeamTwinsMatchTheFrozenAnswersForEveryProvider(t *testing.T) {
	for _, provider := range []string{"github", "gitlab", "jira", "linear"} {
		t.Run(provider, func(t *testing.T) {
			build := providerTwinBuildersFor(provider)
			derivedCases := build.cases(providerTaggedTeamTwins(t, githubDerivedOracleCases()))
			compareRowsAgainstFrozenOracle(t, provider+"_work-items_team-attributions", derivedCases, build.team, nil)
			tripletCases := build.cases(providerTaggedTeamTwins(t, githubWorkItemMetricTripletOracleCases()))
			compareRowsAgainstFrozenOracle(t, provider+"_work-items_metrics-daily", tripletCases,
				func(t *testing.T, input map[string]any) githubWorkItemMetricsDailyOracleColumns {
					return build.triplet(t, input).metricsDaily
				}, nil)
			compareRowsAgainstFrozenOracle(t, provider+"_work-items_user-metrics-daily", tripletCases,
				func(t *testing.T, input map[string]any) githubWorkItemUserMetricsDailyOracleColumns {
					return build.triplet(t, input).userMetricsDaily
				}, nil)
			compareRowsAgainstFrozenOracle(t, provider+"_work-items_cycle-times", tripletCases,
				func(t *testing.T, input map[string]any) githubWorkItemCycleTimeOracleColumns {
					return build.triplet(t, input).cycleTimes
				}, nil)
		})
	}
}
