package providersync

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
)

func githubWorkItemEngineOracleCases() []oracleCase {
	repoID := "11111111-1111-4111-8111-111111111111"
	mainFacts := githubDerivedOracleEmptyFacts()
	mainFacts["Repos"] = []any{
		githubDerivedOracleRepoFact(repoID, "acme/api", "payments", "Payments"),
	}
	return []oracleCase{
		{
			ID: "real_engines_grouping_upper_median_and_future_zero_row",
			Input: map[string]any{
				"OrgID": githubDerivedOracleOrg, "Day": "2026-08-04",
				"RequireInvestmentMetrics": true, "AllowEmpty": false,
				"ComputedAt": "2026-08-05T00:30:00Z", "AsOf": "2026-08-05T00:30:00Z",
				"Facts": mainFacts,
				"WorkItems": []any{
					githubDerivedOracleItem("acme/api#1", map[string]any{
						"repo_id": repoID, "type": "pull_request",
						"labels":       []any{"bug", "security"},
						"created_at":   "2026-08-04T01:00:00Z",
						"started_at":   "2026-08-04T02:00:00Z",
						"completed_at": "2026-08-04T10:00:00Z",
						"story_points": 3.7,
					}),
					githubDerivedOracleItem("acme/api#2", map[string]any{
						"repo_id": repoID, "labels": []any{"security", "bug"},
						"started_at":   "2026-08-04T05:00:00Z",
						"completed_at": "2026-08-04T14:00:00Z",
						"story_points": 0,
					}),
					githubDerivedOracleItem("acme/api#3", map[string]any{
						"project_id": "acme/other", "labels": []any{"feature", "chore"},
					}),
					githubDerivedOracleItem("acme/api#4", map[string]any{
						"project_id": "acme/other",
						"created_at": "2026-08-05T00:00:00Z",
					}),
				},
				"Transitions": []any{},
			},
		},
		{
			ID: "boundary_completion_unassigned_and_negative_cycle",
			Input: map[string]any{
				"OrgID": githubDerivedOracleOrg, "Day": "2026-08-04",
				"RequireInvestmentMetrics": true, "AllowEmpty": false,
				"ComputedAt": "2026-08-05T00:30:00Z", "AsOf": "2026-08-05T00:30:00Z",
				"Facts": githubDerivedOracleEmptyFacts(),
				"WorkItems": []any{
					githubDerivedOracleItem("acme/other#1", map[string]any{
						"project_id": "acme/other", "labels": []any{"maintenance"},
						"started_at":   "2026-08-04T01:00:00Z",
						"completed_at": "2026-08-04T00:00:00Z",
					}),
					githubDerivedOracleItem("acme/other#2", map[string]any{
						"project_id": "acme/other", "labels": []any{"maintenance"},
						"completed_at": "2026-08-03T23:59:59Z",
					}),
				},
				"Transitions": []any{},
			},
		},
		{
			ID: "legacy_classifier_default_is_a_real_nonempty_row",
			Input: map[string]any{
				"OrgID": githubDerivedOracleOrg, "Day": "2026-08-04",
				"RequireInvestmentMetrics": false, "AllowEmpty": false,
				"ComputedAt": "2026-08-05T00:30:00Z", "AsOf": "2026-08-05T00:30:00Z",
				"Facts": githubDerivedOracleEmptyFacts(),
				"WorkItems": []any{
					githubDerivedOracleItem("acme/default#1", map[string]any{
						"project_id": "acme/default", "labels": []any{},
					}),
				},
				"Transitions": []any{},
			},
		},
		{
			// The inline producer deliberately ignores closed_at in both active
			// and completed predicates. A closed-only item therefore remains
			// active, is classified, and emits no completed investment metric.
			ID: "closed_at_without_completed_at_remains_active",
			Input: map[string]any{
				"OrgID": githubDerivedOracleOrg, "Day": "2026-08-04",
				"RequireInvestmentMetrics": false, "AllowEmpty": false,
				"ComputedAt": "2026-08-05T00:30:00Z", "AsOf": "2026-08-05T00:30:00Z",
				"Facts": githubDerivedOracleEmptyFacts(),
				"WorkItems": []any{
					githubDerivedOracleItem("acme/closed#1", map[string]any{
						"project_id": "acme/closed", "labels": []any{"bug"},
						"closed_at": "2026-08-03T00:00:00Z",
					}),
				},
				"Transitions": []any{},
			},
		},
		{
			ID: "empty_input_keeps_all_three_destinations_present",
			Input: map[string]any{
				"OrgID": githubDerivedOracleOrg, "Day": "2026-08-04",
				"RequireInvestmentMetrics": false, "AllowEmpty": true,
				"ComputedAt": "2026-08-05T00:30:00Z", "AsOf": "2026-08-05T00:30:00Z",
				"Facts":     githubDerivedOracleEmptyFacts(),
				"WorkItems": []any{}, "Transitions": []any{},
			},
		},
		{
			ID: "three_day_window_uses_exclusive_midnight_before",
			Input: map[string]any{
				"OrgID":                    githubDerivedOracleOrg,
				"SinceAt":                  "2026-08-03T00:00:00Z",
				"BeforeAt":                 "2026-08-06T00:00:00Z",
				"RequireInvestmentMetrics": true, "AllowEmpty": false,
				"ComputedAt": "2026-08-06T00:30:00Z", "AsOf": "2026-08-06T00:30:00Z",
				"Facts": githubDerivedOracleEmptyFacts(),
				"WorkItems": []any{
					githubDerivedOracleItem("acme/window#1", map[string]any{
						"project_id": "acme/window", "labels": []any{"security", "bug"},
						"created_at":   "2026-08-03T01:00:00Z",
						"started_at":   "2026-08-04T02:00:00Z",
						"completed_at": "2026-08-04T10:00:00Z",
						"story_points": 2,
					}),
				},
				"Transitions": []any{},
			},
		},
	}
}

func TestGitHubIssueTypeMetricsMatchLivePythonProduction(t *testing.T) {
	compareRowsAgainstPythonOracle(
		t, "github/work-items/issue-type-metrics", githubWorkItemEngineOracleCases(),
		func(t *testing.T, input map[string]any) githubIssueTypeMetricsColumns {
			issueTypes, _, _ := buildGitHubWorkItemEngineOracleRows(t, input)
			if !input["AllowEmpty"].(bool) && len(issueTypes) == 0 {
				t.Fatal("oracle case produced no issue-type rows")
			}
			return newGitHubIssueTypeMetricsColumns(issueTypes)
		}, nil,
	)
}

func TestGitHubInvestmentClassificationsMatchLivePythonProduction(t *testing.T) {
	compareRowsAgainstPythonOracle(
		t, "github/work-items/investment-classifications", githubWorkItemEngineOracleCases(),
		func(t *testing.T, input map[string]any) githubInvestmentClassificationColumns {
			_, classifications, _ := buildGitHubWorkItemEngineOracleRows(t, input)
			if !input["AllowEmpty"].(bool) && len(classifications) == 0 {
				t.Fatal("oracle case produced no investment-classification rows")
			}
			return newGitHubInvestmentClassificationColumns(classifications)
		}, nil,
	)
}

func TestGitHubInvestmentMetricsMatchLivePythonProduction(t *testing.T) {
	compareRowsAgainstPythonOracle(
		t, "github/work-items/investment-metrics", githubWorkItemEngineOracleCases(),
		func(t *testing.T, input map[string]any) githubInvestmentMetricsColumns {
			_, _, metrics := buildGitHubWorkItemEngineOracleRows(t, input)
			// The open-item default case intentionally has no completed metric;
			// the other two cases must keep the comparison non-vacuous.
			if input["RequireInvestmentMetrics"].(bool) && len(metrics) == 0 {
				t.Fatal("completed-item oracle case produced no investment metric rows")
			}
			return newGitHubInvestmentMetricsColumns(metrics)
		}, nil,
	)
}

func buildGitHubWorkItemEngineOracleRows(
	t *testing.T,
	input map[string]any,
) (
	[]githubIssueTypeMetricsDailyRow,
	[]githubInvestmentClassificationDailyRow,
	[]githubInvestmentMetricsDailyRow,
) {
	t.Helper()
	provider := "github"
	if items, ok := input["WorkItems"].([]any); ok && len(items) > 0 {
		if item, ok := items[0].(map[string]any); ok {
			if value, ok := item["provider"].(string); ok && value != "" {
				provider = value
			}
		}
	}
	claim := nativeTestClaim(provider, "work-items")
	claim.OrgID = input["OrgID"].(string)
	if sinceRaw, ok := input["SinceAt"].(string); ok {
		since := githubDerivedOracleTime(t, sinceRaw)
		before := githubDerivedOracleTime(t, input["BeforeAt"].(string))
		claim.SinceAt, claim.BeforeAt = &since, &before
	} else {
		day, err := time.Parse("2006-01-02", input["Day"].(string))
		if err != nil {
			t.Fatal(err)
		}
		before := day.AddDate(0, 0, 1)
		claim.SinceAt, claim.BeforeAt = &day, &before
	}
	computedAt := githubDerivedOracleTime(t, input["ComputedAt"].(string))
	rows := githubWorkItemRows{}
	for _, raw := range input["WorkItems"].([]any) {
		rows.WorkItems = append(
			rows.WorkItems, githubDerivedOracleGoItem(t, raw.(map[string]any)),
		)
	}
	for _, raw := range githubDerivedOracleList(input, "Dependencies") {
		rows.Dependencies = append(
			rows.Dependencies, githubDerivedOracleGoDependency(t, raw.(map[string]any)),
		)
	}
	encodedFacts, err := json.Marshal(input["Facts"])
	if err != nil {
		t.Fatal(err)
	}
	var facts githubWorkItemDerivationFacts
	if err := json.Unmarshal(encodedFacts, &facts); err != nil {
		t.Fatal(err)
	}
	// The Python helper merges persisted donor subjects with freshly synced
	// rows before resolving linked-issue inheritance. Feed the same donors
	// through the real context loader seam for both provider variants; omitting
	// them makes a linked donor case silently fall back to the unassigned team.
	for _, raw := range githubDerivedOracleList(input, "Donors") {
		facts.DonorItems = append(facts.DonorItems, githubWorkItemDerivationSubjectFromRow(
			githubDerivedOracleGoItem(t, raw.(map[string]any)),
		))
	}
	statusMapping := loadRealStatusMapping(t)
	classifier, err := NewInvestmentClassifier(investmentConfigPath(t, "real"))
	if err != nil {
		t.Fatal(err)
	}
	engine, err := NewGitHubWorkItemEngineDeriver(statusMapping, classifier)
	if err != nil {
		t.Fatal(err)
	}
	source := &githubMultiDayOracleSource{facts: facts}
	deriver := GitHubWorkItemDeriver{Source: source, engine: engine}
	encoded, err := deriver.Derive(context.Background(), claim, rows, computedAt)
	if err != nil {
		t.Fatal(err)
	}
	if source.loads != 1 {
		t.Fatalf("derivation context loads=%d want=1", source.loads)
	}
	for _, destination := range githubWorkItemDerivedEngineDestinations {
		if _, present := encoded[destination]; !present {
			t.Fatalf("engine destination %q absent from full deriver result", destination)
		}
	}
	return decodeGitHubWorkItemEngineRows[githubIssueTypeMetricsDailyRow](
			t, encoded[githubIssueTypeMetricsDestination],
		),
		decodeGitHubWorkItemEngineRows[githubInvestmentClassificationDailyRow](
			t, encoded[githubInvestmentClassificationsDestination],
		),
		decodeGitHubWorkItemEngineRows[githubInvestmentMetricsDailyRow](
			t, encoded[githubInvestmentMetricsDestination],
		)
}

type githubIssueTypeMetricsColumns struct {
	RepoID         []*uuid.UUID               `json:"repo_id"`
	Day            []githubWorkItemDerivedDay `json:"day"`
	Provider       []string                   `json:"provider"`
	TeamID         []string                   `json:"team_id"`
	IssueTypeNorm  []string                   `json:"issue_type_norm"`
	CreatedCount   []int                      `json:"created_count"`
	CompletedCount []int                      `json:"completed_count"`
	ActiveCount    []int                      `json:"active_count"`
	CycleP50Hours  []float64                  `json:"cycle_p50_hours"`
	CycleP90Hours  []float64                  `json:"cycle_p90_hours"`
	LeadP50Hours   []float64                  `json:"lead_p50_hours"`
	ComputedAt     []time.Time                `json:"computed_at"`
	OrgID          []string                   `json:"org_id"`
}

func newGitHubIssueTypeMetricsColumns(
	rows []githubIssueTypeMetricsDailyRow,
) githubIssueTypeMetricsColumns {
	columns := githubIssueTypeMetricsColumns{
		RepoID: []*uuid.UUID{}, Day: []githubWorkItemDerivedDay{}, Provider: []string{},
		TeamID: []string{}, IssueTypeNorm: []string{}, CreatedCount: []int{},
		CompletedCount: []int{}, ActiveCount: []int{}, CycleP50Hours: []float64{},
		CycleP90Hours: []float64{}, LeadP50Hours: []float64{},
		ComputedAt: []time.Time{}, OrgID: []string{},
	}
	for _, row := range rows {
		columns.RepoID = append(columns.RepoID, row.RepoID)
		columns.Day = append(columns.Day, row.Day)
		columns.Provider = append(columns.Provider, row.Provider)
		columns.TeamID = append(columns.TeamID, row.TeamID)
		columns.IssueTypeNorm = append(columns.IssueTypeNorm, row.IssueTypeNorm)
		columns.CreatedCount = append(columns.CreatedCount, row.CreatedCount)
		columns.CompletedCount = append(columns.CompletedCount, row.CompletedCount)
		columns.ActiveCount = append(columns.ActiveCount, row.ActiveCount)
		columns.CycleP50Hours = append(columns.CycleP50Hours, row.CycleP50Hours)
		columns.CycleP90Hours = append(columns.CycleP90Hours, row.CycleP90Hours)
		columns.LeadP50Hours = append(columns.LeadP50Hours, row.LeadP50Hours)
		columns.ComputedAt = append(columns.ComputedAt, row.ComputedAt)
		columns.OrgID = append(columns.OrgID, row.OrgID)
	}
	return columns
}

type githubInvestmentClassificationColumns struct {
	RepoID         []*uuid.UUID               `json:"repo_id"`
	Day            []githubWorkItemDerivedDay `json:"day"`
	ArtifactType   []string                   `json:"artifact_type"`
	ArtifactID     []string                   `json:"artifact_id"`
	Provider       []string                   `json:"provider"`
	InvestmentArea []*string                  `json:"investment_area"`
	ProjectStream  []string                   `json:"project_stream"`
	Confidence     []float64                  `json:"confidence"`
	RuleID         []*string                  `json:"rule_id"`
	ComputedAt     []time.Time                `json:"computed_at"`
	OrgID          []string                   `json:"org_id"`
}

func newGitHubInvestmentClassificationColumns(
	rows []githubInvestmentClassificationDailyRow,
) githubInvestmentClassificationColumns {
	columns := githubInvestmentClassificationColumns{
		RepoID: []*uuid.UUID{}, Day: []githubWorkItemDerivedDay{}, ArtifactType: []string{},
		ArtifactID: []string{}, Provider: []string{}, InvestmentArea: []*string{},
		ProjectStream: []string{}, Confidence: []float64{}, RuleID: []*string{},
		ComputedAt: []time.Time{}, OrgID: []string{},
	}
	for _, row := range rows {
		columns.RepoID = append(columns.RepoID, row.RepoID)
		columns.Day = append(columns.Day, row.Day)
		columns.ArtifactType = append(columns.ArtifactType, row.ArtifactType)
		columns.ArtifactID = append(columns.ArtifactID, row.ArtifactID)
		columns.Provider = append(columns.Provider, row.Provider)
		columns.InvestmentArea = append(columns.InvestmentArea, row.InvestmentArea)
		columns.ProjectStream = append(columns.ProjectStream, row.ProjectStream)
		columns.Confidence = append(columns.Confidence, row.Confidence)
		columns.RuleID = append(columns.RuleID, row.RuleID)
		columns.ComputedAt = append(columns.ComputedAt, row.ComputedAt)
		columns.OrgID = append(columns.OrgID, row.OrgID)
	}
	return columns
}

type githubInvestmentMetricsColumns struct {
	RepoID             []*uuid.UUID               `json:"repo_id"`
	Day                []githubWorkItemDerivedDay `json:"day"`
	TeamID             []string                   `json:"team_id"`
	InvestmentArea     []*string                  `json:"investment_area"`
	ProjectStream      []string                   `json:"project_stream"`
	DeliveryUnits      []int                      `json:"delivery_units"`
	WorkItemsCompleted []int                      `json:"work_items_completed"`
	PRsMerged          []int                      `json:"prs_merged"`
	ChurnLOC           []int                      `json:"churn_loc"`
	CycleP50Hours      []float64                  `json:"cycle_p50_hours"`
	ComputedAt         []time.Time                `json:"computed_at"`
	OrgID              []string                   `json:"org_id"`
}

func newGitHubInvestmentMetricsColumns(
	rows []githubInvestmentMetricsDailyRow,
) githubInvestmentMetricsColumns {
	columns := githubInvestmentMetricsColumns{
		RepoID: []*uuid.UUID{}, Day: []githubWorkItemDerivedDay{}, TeamID: []string{},
		InvestmentArea: []*string{}, ProjectStream: []string{}, DeliveryUnits: []int{},
		WorkItemsCompleted: []int{}, PRsMerged: []int{}, ChurnLOC: []int{},
		CycleP50Hours: []float64{}, ComputedAt: []time.Time{}, OrgID: []string{},
	}
	for _, row := range rows {
		columns.RepoID = append(columns.RepoID, row.RepoID)
		columns.Day = append(columns.Day, row.Day)
		columns.TeamID = append(columns.TeamID, row.TeamID)
		columns.InvestmentArea = append(columns.InvestmentArea, row.InvestmentArea)
		columns.ProjectStream = append(columns.ProjectStream, row.ProjectStream)
		columns.DeliveryUnits = append(columns.DeliveryUnits, row.DeliveryUnits)
		columns.WorkItemsCompleted = append(columns.WorkItemsCompleted, row.WorkItemsCompleted)
		columns.PRsMerged = append(columns.PRsMerged, row.PRsMerged)
		columns.ChurnLOC = append(columns.ChurnLOC, row.ChurnLOC)
		columns.CycleP50Hours = append(columns.CycleP50Hours, row.CycleP50Hours)
		columns.ComputedAt = append(columns.ComputedAt, row.ComputedAt)
		columns.OrgID = append(columns.OrgID, row.OrgID)
	}
	return columns
}
