package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/google/uuid"
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

func TestGitLabAIAttributionMatchesLivePythonProductionRows(t *testing.T) {
	base := map[string]any{
		"iid": 9, "title": "Repair delivery path", "description": "Routine repair",
		"state": "opened", "created_at": "2026-08-01T08:00:00Z",
		"updated_at": "2026-08-03T09:30:00Z", "assignees": []any{},
		"source_branch": "feature/repair",
	}
	labelMR := cloneSemanticOracleMap(base)
	labelMR["created_at"] = nil
	labelMR["labels"] = []any{"codex"}
	labelMR["author"] = map[string]any{"username": "author", "bot": false}
	botMR := cloneSemanticOracleMap(base)
	botMR["labels"] = []any{}
	botMR["author"] = map[string]any{"username": "chatgpt-codex[bot]", "bot": true}
	unknownBotMR := cloneSemanticOracleMap(base)
	unknownBotMR["labels"] = []any{}
	unknownBotMR["author"] = map[string]any{"username": "ai-helper[bot]", "bot": true}
	trailerMR := cloneSemanticOracleMap(base)
	trailerMR["labels"] = []any{}
	trailerMR["description"] = "Repair delivery path\n\nAI-Assisted-By: Claude Code"
	trailerMR["author"] = map[string]any{"username": "author", "bot": false}
	branchMR := cloneSemanticOracleMap(base)
	branchMR["labels"] = []any{}
	branchMR["source_branch"] = "copilot/repair"
	branchMR["author"] = map[string]any{"username": "author", "bot": false}
	bodyMR := cloneSemanticOracleMap(base)
	bodyMR["labels"] = []any{}
	bodyMR["description"] = "Initial scaffold created with Codex."
	bodyMR["author"] = map[string]any{"username": "author", "bot": false}
	cases := []oracleCase{
		{ID: "codex_label", Input: gitlabAIAttributionOracleInput(labelMR, "pr_label")},
		{ID: "known_bot_author", Input: gitlabAIAttributionOracleInput(botMR, "bot_author")},
		{ID: "unknown_bot_author", Input: gitlabAIAttributionOracleInput(unknownBotMR, "bot_author")},
		{ID: "commit_trailer", Input: gitlabAIAttributionOracleInput(trailerMR, "commit_trailer")},
		{ID: "source_branch", Input: gitlabAIAttributionOracleInput(branchMR, "branch_name")},
		{ID: "description_body", Input: gitlabAIAttributionOracleInput(bodyMR, "pr_body")},
	}
	compareRowsAgainstPythonOracle(
		t, "gitlab/work-items/ai-attribution", cases,
		buildGitLabAIAttributionOracleRow, nil,
	)
}

func TestGitLabAIAttributionIsRetryStableAndDoesNotFabricateSignals(t *testing.T) {
	claim := nativeTestClaim("gitlab", "work-items")
	claim.OrgID = "77777777-7777-4777-8777-777777777777"
	repoID := uuid.MustParse("c7198fbc-1945-3717-05d8-eb78866b4e79")
	updated := "2026-08-03T09:30:00Z"
	username := "author"
	bot := false
	payload := gitlabMergeRequestWorkItemPayload{
		IID: 9, Labels: []string{"codex"}, UpdatedAt: &updated,
		Author:       &gitlabWorkItemUserPayload{Username: &username, Bot: bot},
		SourceBranch: "feature/repair",
	}
	normalizedAt := time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC)
	first, err := normalizeGitLabMRAIAttributions(claim, repoID, payload, normalizedAt)
	if err != nil {
		t.Fatal(err)
	}
	second, err := normalizeGitLabMRAIAttributions(claim, repoID, payload, normalizedAt)
	if err != nil {
		t.Fatal(err)
	}
	wantObserved := time.Date(2026, 8, 3, 9, 30, 0, 0, time.UTC)
	if len(first) != 1 || len(second) != 1 || first[0].RecordID != second[0].RecordID ||
		!first[0].IngestedAt.Equal(second[0].IngestedAt) ||
		!first[0].ObservedAt.Equal(wantObserved) {
		t.Fatalf("retry/fallback rows=%+v/%+v", first, second)
	}

	description := "Routine repair without an AI signal."
	nonAI := payload
	nonAI.Labels = []string{"maintenance"}
	nonAI.Description = &description
	nonAI.SourceBranch = "feature/repair"
	if rows, err := normalizeGitLabMRAIAttributions(claim, repoID, nonAI, normalizedAt); err != nil || len(rows) != 0 {
		t.Fatalf("non-AI MR rows=%+v error=%v", rows, err)
	}
	ciUsername := "dependabot[bot]"
	nonAI.Author = &gitlabWorkItemUserPayload{Username: &ciUsername, Bot: true}
	if rows, err := normalizeGitLabMRAIAttributions(claim, repoID, nonAI, normalizedAt); err != nil || len(rows) != 0 {
		t.Fatalf("CI bot rows=%+v error=%v", rows, err)
	}
}

func gitlabAIAttributionOracleInput(rawMR map[string]any, source string) map[string]any {
	return map[string]any{
		"repo_full_name": "acme/api",
		"repo_id":        "c7198fbc-1945-3717-05d8-eb78866b4e79",
		"org_id":         "77777777-7777-4777-8777-777777777777",
		"raw_mr":         rawMR,
		"signal_source":  source,
	}
}

func buildGitLabAIAttributionOracleRow(
	t *testing.T,
	input map[string]any,
) githubAIAttributionRow {
	t.Helper()
	responses := gitLabWorkItemResponses()
	root := "/api/v4/projects/123"
	rawMR, err := json.Marshal([]any{input["raw_mr"]})
	if err != nil {
		t.Fatal(err)
	}
	responses[root+"/issues?page=1"] = []string{"[]"}
	responses[root+"/merge_requests?page=1"] = []string{string(rawMR), "[]"}
	no := false
	claim := nativeTestClaim("gitlab", "work-items")
	claim.OrgID = input["org_id"].(string)
	statusMapping := loadRealStatusMapping(t)
	classifier, err := NewInvestmentClassifier(investmentConfigPath(t, "real"))
	if err != nil {
		t.Fatal(err)
	}
	batch, err := (GitLabWorkItemsRouteHandler{
		StatusMapping: statusMapping,
		Derived: GitLabWorkItemDeriver{
			Source:               &githubMultiDayOracleSource{},
			statusMapping:        statusMapping,
			investmentClassifier: classifier,
		},
		FetchComments: &no, FetchHistory: &no, FetchLabels: &no,
		FetchLinks: &no, FetchMilestones: &no,
	}).Collect(
		context.Background(), claim,
		providerfoundation.Credential{Provider: "gitlab", ID: claim.CredentialID},
		gitLabWorkItemsClient(t, &gitLabWorkItemsDoer{responses: responses}),
		time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, effect := range batch.Effects {
		if effect.Destination != "ai_attribution" {
			continue
		}
		for _, raw := range effect.Rows {
			var row githubAIAttributionRow
			if err := json.Unmarshal(raw, &row); err != nil {
				t.Fatal(err)
			}
			if row.Source == input["signal_source"] {
				return row
			}
		}
	}
	t.Errorf("Go GitLab work-items route omitted authoritative ai_attribution source %q", input["signal_source"])
	return githubAIAttributionRow{
		Evidence: map[string]any{"oracle_failure": fmt.Sprint(input["signal_source"])},
	}
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
	if len(result.Gaps) != 0 || result.Watermark == nil ||
		claim.BeforeAt == nil || !result.Watermark.Equal(*claim.BeforeAt) {
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

func TestGitLabWorkItemDerivedResultIsTypedAndComplete(t *testing.T) {
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
		if len(result.Gaps) != 0 || len(result.producedDestinations()) != 10 {
			t.Fatalf("unexpected derived completeness: gaps=%+v produced=%v", result.Gaps, result.producedDestinations())
		}
		if result.Watermark == nil || !result.Watermark.Equal(before) {
			t.Fatalf("watermark=%v want=%v", result.Watermark, before)
		}
	}
	for index, count := range counts {
		if count == 0 {
			t.Fatalf("derived destination index %d produced no semantic rows across oracle cases", index)
		}
	}
}

func TestBuildGitLabWorkItemDerivedEffectsIsCanonicalAndIncludesAI(t *testing.T) {
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
		if index == 6 {
			wantRows = 1
		}
		if effect.Recovery != EffectReadbackRequired || len(effect.Rows) != wantRows {
			t.Fatalf("effect[%d] recovery/rows = %s/%d", index, effect.Recovery, len(effect.Rows))
		}
	}
	if effects[0].Destination != "ai_attribution" || effects[0].Rows == nil {
		t.Fatalf("AI destination was not evaluated: %+v", effects[0])
	}
	var metrics gitlabWorkItemMetricsDailyRow
	if err := json.Unmarshal(effects[6].Rows[0], &metrics); err != nil {
		t.Fatal(err)
	}
	if metrics.ItemsCompletedUnassigned != 7 {
		t.Fatalf("metrics effect lost its typed row: %+v", metrics)
	}
}

func TestGitLabWorkItemDerivedIdentityAcceptsAIAndRejectsRawDestinations(t *testing.T) {
	claim := nativeTestClaim("gitlab", "work-items")
	effects, err := BuildGitLabWorkItemDerivedEffects(GitLabWorkItemDerivedEffectRows{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := newGitLabWorkItemDerivedEffectIdentity(claim, effects[0]); err != nil {
		t.Fatalf("AI destination rejected: %v", err)
	}
	if _, err := newGitLabWorkItemDerivedEffectIdentity(claim, effects[1]); err != nil {
		t.Fatalf("computed destination rejected: %v", err)
	}
	foreign := claim
	foreign.Provider = "github"
	if _, err := newGitLabWorkItemDerivedEffectIdentity(foreign, effects[0]); err == nil {
		t.Fatal("foreign provider accepted by GitLab derived identity")
	}
	raw, err := BuildEffectBatch("work_items", EffectReadbackRequired, []json.RawMessage{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := newGitLabWorkItemDerivedEffectIdentity(claim, raw); err == nil {
		t.Fatal("raw destination accepted by derived identity")
	}
}

func TestGitLabWorkItemsRouteComposesSixteenEffectsAndAdvancesWatermark(t *testing.T) {
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
	responses := gitLabWorkItemResponses()
	root := "/api/v4/projects/123"
	responses[root+"/merge_requests?page=1"] = []string{
		`[{"iid":9,"title":"Ship the API","description":"","state":"opened","created_at":"2026-07-04T09:00:00Z","updated_at":"2026-07-04T10:00:00Z","labels":["priority::low","codex"],"assignees":[],"author":{"username":"alice","bot":false},"source_branch":"feature/ship-api"}]`,
		`[]`,
	}
	doer := &gitLabWorkItemsDoer{responses: responses}
	claim := nativeTestClaim("gitlab", "work-items")
	claim.OrgID = "77777777-7777-4777-8777-777777777777"
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
	if len(batch.Effects) != 16 {
		t.Fatalf("effects=%d want 16 raw+derived", len(batch.Effects))
	}
	if err := batch.validate(CompleteRouteDescriptor{Destinations: workItemRouteDestinations()}); err != nil {
		t.Fatalf("sixteen-destination governed contract: %v", err)
	}
	var aiEffect *EffectBatch
	for _, effect := range batch.Effects {
		if effect.Destination == "ai_attribution" {
			copy := effect
			aiEffect = &copy
		}
	}
	if aiEffect == nil || len(aiEffect.Rows) != 1 {
		t.Fatalf("AI effect=%+v want one authoritative MR signal", aiEffect)
	}
	var attribution gitlabAIAttributionRow
	if err := json.Unmarshal(aiEffect.Rows[0], &attribution); err != nil {
		t.Fatal(err)
	}
	if attribution.Provider != "gitlab" || attribution.Source != "pr_label" ||
		attribution.SubjectID != "9" || attribution.Confidence != 0.95 {
		t.Fatalf("AI attribution=%+v", attribution)
	}
	summary, ok := batch.Result["gitlab_work_items"].(GitLabWorkItemsResult)
	if !ok {
		t.Fatalf("summary type=%T", batch.Result["gitlab_work_items"])
	}
	if len(summary.DerivedDestinationsUnimplemented) != 0 ||
		len(summary.DerivedDestinationsImplemented) != 10 ||
		summary.WatermarkHeldForDerivedGap || batch.Watermark == nil ||
		claim.BeforeAt == nil || !batch.Watermark.Equal(*claim.BeforeAt) {
		t.Fatalf("derived summary/watermark=%+v/%v", summary, batch.Watermark)
	}
	implemented, ok := batch.Result["derived_destinations_implemented"].([]string)
	if !ok || len(implemented) != 10 {
		t.Fatalf("implemented destinations=%T/%v", batch.Result["derived_destinations_implemented"], batch.Result["derived_destinations_implemented"])
	}
}

type unavailableGitLabAIDeriver struct{}

func (unavailableGitLabAIDeriver) Derive(
	_ context.Context,
	claim Claim,
	_ gitlabWorkItemRows,
	_ time.Time,
) (GitLabWorkItemDerivedRows, error) {
	var watermark *time.Time
	if claim.BeforeAt != nil {
		value := claim.BeforeAt.UTC()
		watermark = &value
	}
	return GitLabWorkItemDerivedRows{Watermark: watermark, Gaps: []GitLabWorkItemDerivedGap{{
		Destination:           "ai_attribution",
		AuthoritativeProducer: "gitlab_mr_ai_attributions",
		Reason:                "authoritative producer unavailable",
	}}}, nil
}

func TestGitLabWorkItemsRouteRejectsUnavailableDerivedProducerBeforeEffects(t *testing.T) {
	claim := nativeTestClaim("gitlab", "work-items")
	_, err := (GitLabWorkItemsRouteHandler{
		StatusMapping: loadRealStatusMapping(t), Derived: unavailableGitLabAIDeriver{},
		PerPage: 2, MaxPages: 10, NestedMaxPages: 10,
	}).Collect(
		context.Background(), claim,
		providerfoundation.Credential{Provider: "gitlab", ID: claim.CredentialID},
		gitLabWorkItemsClient(t, &gitLabWorkItemsDoer{responses: gitLabWorkItemResponses()}),
		time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC),
	)
	if !errors.Is(err, ErrGitLabWorkItemDerivedProducerUnavailable) ||
		!strings.Contains(err.Error(), "ai_attribution") {
		t.Fatalf("unavailable producer error=%v", err)
	}
}

type wrongWatermarkGitLabDeriver struct{}

func (wrongWatermarkGitLabDeriver) Derive(
	_ context.Context,
	_ Claim,
	_ gitlabWorkItemRows,
	normalizedAt time.Time,
) (GitLabWorkItemDerivedRows, error) {
	wrong := normalizedAt.UTC()
	return GitLabWorkItemDerivedRows{Watermark: &wrong, Gaps: []GitLabWorkItemDerivedGap{}}, nil
}

func TestGitLabWorkItemsRouteRejectsDerivedWatermarkOutsideClaimBound(t *testing.T) {
	claim := nativeTestClaim("gitlab", "work-items")
	_, err := (GitLabWorkItemsRouteHandler{
		StatusMapping: loadRealStatusMapping(t), Derived: wrongWatermarkGitLabDeriver{},
		PerPage: 2, MaxPages: 10, NestedMaxPages: 10,
	}).Collect(
		context.Background(), claim,
		providerfoundation.Credential{Provider: "gitlab", ID: claim.CredentialID},
		gitLabWorkItemsClient(t, &gitLabWorkItemsDoer{responses: gitLabWorkItemResponses()}),
		time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC),
	)
	if !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("wrong watermark error=%v", err)
	}
}
