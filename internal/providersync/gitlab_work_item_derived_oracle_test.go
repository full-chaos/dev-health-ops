package providersync

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// The GitLab cases reuse the same boundary-rich inputs as the already-landed
// provider-neutral Python work-item producers, changing only provider leaves.
// The Python side still executes the real production functions; this helper
// only prevents nine hand-maintained fixture sets from drifting apart.
func gitlabOracleCases(cases []oracleCase) []oracleCase {
	result := make([]oracleCase, 0, len(cases))
	for _, testCase := range cases {
		result = append(result, oracleCase{ID: "gitlab_" + testCase.ID, Input: gitlabOracleValue(testCase.Input).(map[string]any)})
	}
	return result
}

func gitlabOracleValue(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		result := make(map[string]any, len(typed))
		for key, nested := range typed {
			result[key] = gitlabOracleValue(nested)
		}
		return result
	case []any:
		result := make([]any, len(typed))
		for index, nested := range typed {
			result[index] = gitlabOracleValue(nested)
		}
		return result
	case string:
		if typed == "github" {
			return "gitlab"
		}
		return typed
	default:
		return value
	}
}

func gitlabWorkItemOracleClaim(input map[string]any) Claim {
	claim := nativeTestClaim("gitlab", "work-items")
	claim.OrgID = input["OrgID"].(string)
	return claim
}

func gitlabWorkItemOracleRows(
	t *testing.T,
	input map[string]any,
) (Claim, githubWorkItemRows, githubWorkItemDerivationContext) {
	t.Helper()
	claim := gitlabWorkItemOracleClaim(input)
	rows := githubWorkItemRows{}
	for _, raw := range githubDerivedOracleList(input, "WorkItems") {
		rows.WorkItems = append(rows.WorkItems, gitlabWorkItemOracleGoItem(t, raw.(map[string]any)))
	}
	for _, raw := range githubDerivedOracleList(input, "Transitions") {
		rows.StatusTransitions = append(rows.StatusTransitions, githubDerivedOracleGoTransition(t, raw.(map[string]any)))
	}
	for _, raw := range githubDerivedOracleList(input, "Dependencies") {
		rows.Dependencies = append(rows.Dependencies, githubDerivedOracleGoDependency(t, raw.(map[string]any)))
	}
	encodedFacts, err := json.Marshal(input["Facts"])
	if err != nil {
		t.Fatal(err)
	}
	var facts githubWorkItemDerivationFacts
	if err := json.Unmarshal(encodedFacts, &facts); err != nil {
		t.Fatal(err)
	}
	derived := newGitHubWorkItemDerivationContext(facts)
	subjects := make(map[string]githubWorkItemDerivationSubject)
	for _, raw := range githubDerivedOracleList(input, "Donors") {
		donor := githubWorkItemDerivationSubjectFromRow(gitlabWorkItemOracleGoItem(t, raw.(map[string]any)))
		subjects[donor.WorkItemID] = donor
	}
	for _, row := range rows.WorkItems {
		subject := githubWorkItemDerivationSubjectFromRow(row)
		subjects[subject.WorkItemID] = subject
	}
	derived.linkedIssue = derived.buildLinkedIssueIndex(subjects, rows.Dependencies)
	return claim, rows, derived
}

func gitlabWorkItemOracleGoItem(t *testing.T, raw map[string]any) githubWorkItemRow {
	t.Helper()
	item := githubDerivedOracleGoItem(t, raw)
	if nativeTeamKey, ok := raw["native_team_key"].(string); ok {
		item.NativeTeamKey = stringPointer(nativeTeamKey)
	}
	return item
}

func gitlabWorkItemOracleDay(t *testing.T, input map[string]any) time.Time {
	t.Helper()
	dayValue, ok := input["Day"].(string)
	if !ok {
		dayValue = input["SinceAt"].(string)[:len(time.DateOnly)]
	}
	day, err := time.Parse(time.DateOnly, dayValue)
	if err != nil {
		t.Fatal(err)
	}
	return day
}

func gitlabWorkItemOracleComputedAt(t *testing.T, input map[string]any) time.Time {
	t.Helper()
	return githubDerivedOracleTime(t, input["ComputedAt"].(string))
}

func (columns githubWorkItemMetricsDailyOracleColumns) fromRows(
	rows []githubWorkItemMetricsDailyRow,
) githubWorkItemMetricsDailyOracleColumns {
	for _, row := range rows {
		columns.Day = append(columns.Day, row.Day)
		columns.Provider = append(columns.Provider, row.Provider)
		columns.WorkScopeID = append(columns.WorkScopeID, row.WorkScopeID)
		columns.TeamID = append(columns.TeamID, row.TeamID)
		columns.TeamName = append(columns.TeamName, row.TeamName)
		columns.ItemsStarted = append(columns.ItemsStarted, row.ItemsStarted)
		columns.ItemsCompleted = append(columns.ItemsCompleted, row.ItemsCompleted)
		columns.ItemsStartedUnassigned = append(columns.ItemsStartedUnassigned, row.ItemsStartedUnassigned)
		columns.ItemsCompletedUnassigned = append(columns.ItemsCompletedUnassigned, row.ItemsCompletedUnassigned)
		columns.WIPCountEndOfDay = append(columns.WIPCountEndOfDay, row.WIPCountEndOfDay)
		columns.WIPUnassignedEndOfDay = append(columns.WIPUnassignedEndOfDay, row.WIPUnassignedEndOfDay)
		columns.CycleTimeP50Hours = append(columns.CycleTimeP50Hours, row.CycleTimeP50Hours)
		columns.CycleTimeP90Hours = append(columns.CycleTimeP90Hours, row.CycleTimeP90Hours)
		columns.LeadTimeP50Hours = append(columns.LeadTimeP50Hours, row.LeadTimeP50Hours)
		columns.LeadTimeP90Hours = append(columns.LeadTimeP90Hours, row.LeadTimeP90Hours)
		columns.WIPAgeP50Hours = append(columns.WIPAgeP50Hours, row.WIPAgeP50Hours)
		columns.WIPAgeP90Hours = append(columns.WIPAgeP90Hours, row.WIPAgeP90Hours)
		columns.BugCompletedRatio = append(columns.BugCompletedRatio, row.BugCompletedRatio)
		columns.StoryPointsCompleted = append(columns.StoryPointsCompleted, row.StoryPointsCompleted)
		columns.NewBugsCount = append(columns.NewBugsCount, row.NewBugsCount)
		columns.NewItemsCount = append(columns.NewItemsCount, row.NewItemsCount)
		columns.DefectIntroRate = append(columns.DefectIntroRate, row.DefectIntroRate)
		columns.WIPCongestionRatio = append(columns.WIPCongestionRatio, row.WIPCongestionRatio)
		columns.PredictabilityScore = append(columns.PredictabilityScore, row.PredictabilityScore)
		columns.ComputedAt = append(columns.ComputedAt, row.ComputedAt)
		columns.OrgID = append(columns.OrgID, row.OrgID)
	}
	return columns
}

func (columns githubWorkItemUserMetricsDailyOracleColumns) fromRows(
	rows []githubWorkItemUserMetricsDailyRow,
) githubWorkItemUserMetricsDailyOracleColumns {
	for _, row := range rows {
		columns.Day = append(columns.Day, row.Day)
		columns.Provider = append(columns.Provider, row.Provider)
		columns.WorkScopeID = append(columns.WorkScopeID, row.WorkScopeID)
		columns.UserIdentity = append(columns.UserIdentity, row.UserIdentity)
		columns.TeamID = append(columns.TeamID, row.TeamID)
		columns.TeamName = append(columns.TeamName, row.TeamName)
		columns.ItemsStarted = append(columns.ItemsStarted, row.ItemsStarted)
		columns.ItemsCompleted = append(columns.ItemsCompleted, row.ItemsCompleted)
		columns.WIPCountEndOfDay = append(columns.WIPCountEndOfDay, row.WIPCountEndOfDay)
		columns.CycleTimeP50Hours = append(columns.CycleTimeP50Hours, row.CycleTimeP50Hours)
		columns.CycleTimeP90Hours = append(columns.CycleTimeP90Hours, row.CycleTimeP90Hours)
		columns.ComputedAt = append(columns.ComputedAt, row.ComputedAt)
		columns.OrgID = append(columns.OrgID, row.OrgID)
	}
	return columns
}

func (columns githubWorkItemCycleTimeOracleColumns) fromRows(
	rows []githubWorkItemCycleTimeRecord,
) githubWorkItemCycleTimeOracleColumns {
	for _, row := range rows {
		columns.WorkItemID = append(columns.WorkItemID, row.WorkItemID)
		columns.Provider = append(columns.Provider, row.Provider)
		columns.Day = append(columns.Day, row.Day)
		columns.WorkScopeID = append(columns.WorkScopeID, row.WorkScopeID)
		columns.TeamID = append(columns.TeamID, row.TeamID)
		columns.TeamName = append(columns.TeamName, row.TeamName)
		columns.Assignee = append(columns.Assignee, row.Assignee)
		columns.Type = append(columns.Type, row.Type)
		columns.Status = append(columns.Status, row.Status)
		columns.CreatedAt = append(columns.CreatedAt, row.CreatedAt)
		columns.StartedAt = append(columns.StartedAt, row.StartedAt)
		columns.CompletedAt = append(columns.CompletedAt, row.CompletedAt)
		columns.CycleTimeHours = append(columns.CycleTimeHours, row.CycleTimeHours)
		columns.LeadTimeHours = append(columns.LeadTimeHours, row.LeadTimeHours)
		columns.ActiveTimeHours = append(columns.ActiveTimeHours, row.ActiveTimeHours)
		columns.WaitTimeHours = append(columns.WaitTimeHours, row.WaitTimeHours)
		columns.FlowEfficiency = append(columns.FlowEfficiency, row.FlowEfficiency)
		columns.ComputedAt = append(columns.ComputedAt, row.ComputedAt)
		columns.OrgID = append(columns.OrgID, row.OrgID)
	}
	return columns
}

func TestGitLabWorkItemMetricsDailyMatchesLivePythonProduction(t *testing.T) {
	compareRowsAgainstPythonOracle(t, "gitlab/work-items/metrics-daily", gitlabOracleCases(githubWorkItemMetricTripletOracleCases()), func(t *testing.T, input map[string]any) githubWorkItemMetricsDailyOracleColumns {
		claim, rows, derived := gitlabWorkItemOracleRows(t, input)
		triplet, err := buildWorkItemMetricTripletForProvider("gitlab", claim, rows, gitlabWorkItemOracleDay(t, input), gitlabWorkItemOracleComputedAt(t, input), derived)
		if err != nil {
			t.Fatal(err)
		}
		return newGitHubWorkItemMetricsDailyOracleColumns(len(triplet.MetricsDaily)).fromRows(triplet.MetricsDaily)
	}, nil)
}

func TestGitLabWorkItemUserMetricsDailyMatchesLivePythonProduction(t *testing.T) {
	compareRowsAgainstPythonOracle(t, "gitlab/work-items/user-metrics-daily", gitlabOracleCases(githubWorkItemMetricTripletOracleCases()), func(t *testing.T, input map[string]any) githubWorkItemUserMetricsDailyOracleColumns {
		claim, rows, derived := gitlabWorkItemOracleRows(t, input)
		triplet, err := buildWorkItemMetricTripletForProvider("gitlab", claim, rows, gitlabWorkItemOracleDay(t, input), gitlabWorkItemOracleComputedAt(t, input), derived)
		if err != nil {
			t.Fatal(err)
		}
		return newGitHubWorkItemUserMetricsDailyOracleColumns(len(triplet.UserMetricsDaily)).fromRows(triplet.UserMetricsDaily)
	}, nil)
}

func TestGitLabWorkItemCycleTimesMatchLivePythonProduction(t *testing.T) {
	compareRowsAgainstPythonOracle(t, "gitlab/work-items/cycle-times", gitlabOracleCases(githubWorkItemMetricTripletOracleCases()), func(t *testing.T, input map[string]any) githubWorkItemCycleTimeOracleColumns {
		claim, rows, derived := gitlabWorkItemOracleRows(t, input)
		triplet, err := buildWorkItemMetricTripletForProvider("gitlab", claim, rows, gitlabWorkItemOracleDay(t, input), gitlabWorkItemOracleComputedAt(t, input), derived)
		if err != nil {
			t.Fatal(err)
		}
		return newGitHubWorkItemCycleTimeOracleColumns(len(triplet.CycleTimes)).fromRows(triplet.CycleTimes)
	}, nil)
}

func TestGitLabDerivedSurfacesMatchLivePythonProduction(t *testing.T) {
	cases := gitlabOracleCases(githubDerivedOracleCases())
	compareRowsAgainstPythonOracle(t, "gitlab/work-items/estimate-coverage", cases, func(t *testing.T, input map[string]any) githubEstimateCoverageColumns {
		claim, rows, derived := gitlabWorkItemOracleRows(t, input)
		surfaces, err := buildWorkItemDerivedSurfacesForProvider("gitlab", claim, rows, gitlabWorkItemOracleDay(t, input), gitlabWorkItemOracleComputedAt(t, input), derived)
		if err != nil {
			t.Fatal(err)
		}
		return newGitHubEstimateCoverageColumns(surfaces.EstimateCoverage)
	}, nil)
	compareRowsAgainstPythonOracle(t, "gitlab/work-items/team-attributions", cases, func(t *testing.T, input map[string]any) githubTeamAttributionColumns {
		claim, rows, derived := gitlabWorkItemOracleRows(t, input)
		surfaces, err := buildWorkItemDerivedSurfacesForProvider("gitlab", claim, rows, gitlabWorkItemOracleDay(t, input), gitlabWorkItemOracleComputedAt(t, input), derived)
		if err != nil {
			t.Fatal(err)
		}
		return newGitHubTeamAttributionColumns(surfaces.TeamAttributions)
	}, nil)
	compareRowsAgainstPythonOracle(t, "gitlab/work-items/state-durations", cases, func(t *testing.T, input map[string]any) githubStateDurationColumns {
		claim, rows, derived := gitlabWorkItemOracleRows(t, input)
		surfaces, err := buildWorkItemDerivedSurfacesForProvider("gitlab", claim, rows, gitlabWorkItemOracleDay(t, input), gitlabWorkItemOracleComputedAt(t, input), derived)
		if err != nil {
			t.Fatal(err)
		}
		return newGitHubStateDurationColumns(surfaces.StateDurations)
	}, nil)
}

func TestGitLabEngineDestinationsMatchLivePythonProduction(t *testing.T) {
	cases := gitlabOracleCases(githubWorkItemEngineOracleCases())
	compareRowsAgainstPythonOracle(t, "gitlab/work-items/issue-type-metrics", cases,
		func(t *testing.T, input map[string]any) githubIssueTypeMetricsColumns {
			return newGitHubIssueTypeMetricsColumns(gitlabEngineOracleResult(t, input).IssueTypeMetricsDaily)
		}, nil)
	compareRowsAgainstPythonOracle(t, "gitlab/work-items/investment-classifications", cases,
		func(t *testing.T, input map[string]any) githubInvestmentClassificationColumns {
			return newGitHubInvestmentClassificationColumns(gitlabEngineOracleResult(t, input).InvestmentClassificationsDaily)
		}, nil)
	compareRowsAgainstPythonOracle(t, "gitlab/work-items/investment-metrics", cases,
		func(t *testing.T, input map[string]any) githubInvestmentMetricsColumns {
			return newGitHubInvestmentMetricsColumns(gitlabEngineOracleResult(t, input).InvestmentMetricsDaily)
		}, nil)
}

func gitlabEngineOracleResult(t *testing.T, input map[string]any) GitLabWorkItemDerivedRows {
	t.Helper()
	claim, rows, _ := gitlabWorkItemOracleRows(t, input)
	statusMapping := loadRealStatusMapping(t)
	classifier, err := NewInvestmentClassifier(investmentConfigPath(t, "real"))
	if err != nil {
		t.Fatal(err)
	}
	facts := githubWorkItemDerivationFacts{}
	encoded, err := json.Marshal(input["Facts"])
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(encoded, &facts); err != nil {
		t.Fatal(err)
	}
	source := &githubMultiDayOracleSource{facts: facts}
	deriver := GitLabWorkItemDeriver{Source: source, statusMapping: statusMapping, investmentClassifier: classifier}
	if sinceRaw, ok := input["SinceAt"].(string); ok {
		since := githubDerivedOracleTime(t, sinceRaw)
		before := githubDerivedOracleTime(t, input["BeforeAt"].(string))
		claim.SinceAt, claim.BeforeAt = &since, &before
	} else {
		day := gitlabWorkItemOracleDay(t, input)
		before := day.AddDate(0, 0, 1)
		claim.SinceAt, claim.BeforeAt = &day, &before
	}
	result, err := deriver.Derive(context.Background(), claim, gitlabRowsFromGitHub(rows), gitlabWorkItemOracleComputedAt(t, input))
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Gaps) != 1 || result.Watermark != nil {
		t.Fatalf("gap/watermark contract violated: gaps=%v watermark=%v", result.Gaps, result.Watermark)
	}
	return result
}

func gitlabRowsFromGitHub(rows githubWorkItemRows) gitlabWorkItemRows {
	return gitlabWorkItemRows{
		WorkItems: rows.WorkItems, StatusTransitions: rows.StatusTransitions,
		Dependencies: rows.Dependencies, ReopenEvents: rows.ReopenEvents,
		Interactions: rows.Interactions, Sprints: rows.Sprints,
	}
}

func TestGitLabWorkItemDerivedResultIsTypedAndWithholdsAIWatermark(t *testing.T) {
	statusMapping := loadRealStatusMapping(t)
	classifier, err := NewInvestmentClassifier(investmentConfigPath(t, "real"))
	if err != nil {
		t.Fatal(err)
	}
	source := &githubMultiDayOracleSource{}
	deriver := GitLabWorkItemDeriver{
		Source: source, statusMapping: statusMapping, investmentClassifier: classifier,
	}
	counts := [9]int{}
	cases := gitlabOracleCases(githubDerivedOracleCases())
	cases = append(cases, gitlabOracleCases(githubWorkItemEngineOracleCases())...)
	for _, testCase := range cases {
		claim, rows, _ := gitlabWorkItemOracleRows(t, testCase.Input)
		day := gitlabWorkItemOracleDay(t, testCase.Input)
		before := day.AddDate(0, 0, 1)
		claim.SinceAt, claim.BeforeAt = &day, &before
		result, err := deriver.Derive(context.Background(), claim, gitlabRowsFromGitHub(rows), gitlabWorkItemOracleComputedAt(t, testCase.Input))
		if err != nil {
			t.Fatal(err)
		}
		counts[0] += len(result.EstimateCoverageMetricsDaily)
		counts[1] += len(result.InvestmentClassificationsDaily)
		counts[2] += len(result.InvestmentMetricsDaily)
		counts[3] += len(result.IssueTypeMetricsDaily)
		counts[4] += len(result.WorkItemCycleTimes)
		counts[5] += len(result.WorkItemMetricsDaily)
		counts[6] += len(result.WorkItemStateDurationsDaily)
		counts[7] += len(result.WorkItemTeamAttributions)
		counts[8] += len(result.WorkItemUserMetricsDaily)
		if len(result.Gaps) != 1 || result.Gaps[0].Destination != "ai_attribution" ||
			result.Gaps[0].AuthoritativeProducer != "gitlab_mr_ai_attributions" {
			t.Fatalf("unexpected typed gap: %+v", result.Gaps)
		}
		if result.Watermark != nil {
			t.Fatalf("watermark advanced despite AI producer gap: %v", result.Watermark)
		}
	}
	for index, count := range counts {
		if count == 0 {
			t.Fatalf("derived destination index %d produced no semantic rows across oracle cases", index)
		}
	}
}

func TestBuildGitLabWorkItemDerivedEffectsIsCanonicalAndExcludesAIGap(t *testing.T) {
	metric := githubWorkItemMetricTestGroupRow()
	metric.Provider = "gitlab"
	metric.ItemsCompletedUnassigned = 7
	rows := GitLabWorkItemDerivedEffectRows{
		WorkItemMetricsDaily: []gitlabWorkItemMetricsDailyRow{metric},
	}
	effects, err := BuildGitLabWorkItemDerivedEffects(rows)
	if err != nil {
		t.Fatal(err)
	}
	if len(effects) != len(gitlabWorkItemDerivedDestinations) {
		t.Fatalf("effects=%d want=%d", len(effects), len(gitlabWorkItemDerivedDestinations))
	}
	for index, effect := range effects {
		if effect.Destination != gitlabWorkItemDerivedDestinations[index] {
			t.Fatalf("effect[%d]=%q want=%q", index, effect.Destination, gitlabWorkItemDerivedDestinations[index])
		}
		wantRows := 0
		if index == 5 {
			wantRows = 1
		}
		if effect.Recovery != EffectReadbackRequired || len(effect.Rows) != wantRows {
			t.Fatalf("effect[%d] recovery/rows = %s/%d", index, effect.Recovery, len(effect.Rows))
		}
	}
	for _, effect := range effects {
		if effect.Destination == "ai_attribution" {
			t.Fatal("AI producer gap was fabricated as an empty effect")
		}
	}
	var metrics gitlabWorkItemMetricsDailyRow
	if err := json.Unmarshal(effects[5].Rows[0], &metrics); err != nil {
		t.Fatal(err)
	}
	if metrics.ItemsCompletedUnassigned != 7 {
		t.Fatalf("metrics effect lost its typed row: %+v", metrics)
	}
}

func TestGitLabWorkItemDerivedIdentityRejectsRawAndAIDestinations(t *testing.T) {
	claim := nativeTestClaim("gitlab", "work-items")
	effects, err := BuildGitLabWorkItemDerivedEffects(GitLabWorkItemDerivedEffectRows{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := newGitLabWorkItemDerivedEffectIdentity(claim, effects[0]); err != nil {
		t.Fatalf("first derived destination rejected: %v", err)
	}
	ai, err := BuildEffectBatch("ai_attribution", EffectReadbackRequired, []json.RawMessage{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := newGitLabWorkItemDerivedEffectIdentity(claim, ai); err == nil {
		t.Fatal("AI gap accepted as a derived effect")
	}
	raw, err := BuildEffectBatch("work_items", EffectReadbackRequired, []json.RawMessage{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := newGitLabWorkItemDerivedEffectIdentity(claim, raw); err == nil {
		t.Fatal("raw destination accepted by derived identity")
	}
}

func TestGitLabWorkItemsRouteComposesNineDerivedEffectsAndLeavesAIGap(t *testing.T) {
	statusMapping := loadRealStatusMapping(t)
	classifier, err := NewInvestmentClassifier(investmentConfigPath(t, "real"))
	if err != nil {
		t.Fatal(err)
	}
	deriver := GitLabWorkItemDeriver{
		Source:               &githubMultiDayOracleSource{},
		statusMapping:        statusMapping,
		investmentClassifier: classifier,
	}
	doer := &gitLabWorkItemsDoer{responses: gitLabWorkItemResponses()}
	claim := nativeTestClaim("gitlab", "work-items")
	batch, err := (GitLabWorkItemsRouteHandler{
		StatusMapping: loadRealStatusMapping(t), Derived: deriver,
		PerPage: 2, MaxPages: 10, NestedMaxPages: 10,
	}).Collect(
		context.Background(), claim,
		providerfoundation.Credential{Provider: "gitlab", ID: claim.CredentialID},
		gitLabWorkItemsClient(t, doer), time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.Effects) != 15 {
		t.Fatalf("effects=%d want 15 raw+nine-derived", len(batch.Effects))
	}
	for _, effect := range batch.Effects {
		if effect.Destination == "ai_attribution" {
			t.Fatal("AI gap was fabricated as an effect")
		}
	}
	summary, ok := batch.Result["gitlab_work_items"].(GitLabWorkItemsResult)
	if !ok {
		t.Fatalf("summary type=%T", batch.Result["gitlab_work_items"])
	}
	if len(summary.DerivedDestinationsUnimplemented) != 1 ||
		summary.DerivedDestinationsUnimplemented[0] != "ai_attribution" ||
		len(summary.DerivedDestinationsImplemented) != 9 ||
		!summary.WatermarkHeldForDerivedGap || batch.Watermark != nil {
		t.Fatalf("derived summary/watermark=%+v/%v", summary, batch.Watermark)
	}
	implemented, ok := batch.Result["derived_destinations_implemented"].([]string)
	if !ok || len(implemented) != 9 {
		t.Fatalf("implemented destinations=%T/%v", batch.Result["derived_destinations_implemented"], batch.Result["derived_destinations_implemented"])
	}
}
