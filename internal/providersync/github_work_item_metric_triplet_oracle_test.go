package providersync

import (
	"encoding/json"
	"testing"
	"time"
)

// githubWorkItemMetricTripletOracleInput is the ONE case shape all three metric
// triplet pairs share. compute_work_item_metrics_daily returns the three record
// lists together from a single pass, so comparing them from three independently
// wired fixtures could let the Go port agree on each list against a different
// input than Python saw. One decoded input, one Go compute call, three
// projections -- mirroring the Python helper.
type githubWorkItemMetricTripletOracleInput struct {
	Day          string                        `json:"Day"`
	ComputedAt   time.Time                     `json:"ComputedAt"`
	OrgID        string                        `json:"OrgID"`
	WorkItems    []githubWorkItemRow           `json:"WorkItems"`
	Transitions  []githubWorkItemTransitionRow `json:"Transitions"`
	Dependencies []githubWorkItemDependencyRow `json:"Dependencies"`
	Donors       []githubWorkItemRow           `json:"Donors"`
	Facts        githubWorkItemDerivationFacts `json:"Facts"`
}

// The three column structs below transpose an ordered record list into one
// column per persisted field. Their json tags are the Python dataclass field
// names; the comparator walks every exported field exhaustively, so a field
// added to githubWorkItemMetricsDailyRow without a column here is caught as
// "present in Python's row but absent from Go's" rather than silently dropped.
type githubWorkItemMetricsDailyOracleColumns struct {
	Day                      []githubWorkItemMetricDay `json:"day"`
	Provider                 []string                  `json:"provider"`
	WorkScopeID              []string                  `json:"work_scope_id"`
	TeamID                   []string                  `json:"team_id"`
	TeamName                 []string                  `json:"team_name"`
	ItemsStarted             []int                     `json:"items_started"`
	ItemsCompleted           []int                     `json:"items_completed"`
	ItemsStartedUnassigned   []int                     `json:"items_started_unassigned"`
	ItemsCompletedUnassigned []int                     `json:"items_completed_unassigned"`
	WIPCountEndOfDay         []int                     `json:"wip_count_end_of_day"`
	WIPUnassignedEndOfDay    []int                     `json:"wip_unassigned_end_of_day"`
	CycleTimeP50Hours        []*float64                `json:"cycle_time_p50_hours"`
	CycleTimeP90Hours        []*float64                `json:"cycle_time_p90_hours"`
	LeadTimeP50Hours         []*float64                `json:"lead_time_p50_hours"`
	LeadTimeP90Hours         []*float64                `json:"lead_time_p90_hours"`
	WIPAgeP50Hours           []*float64                `json:"wip_age_p50_hours"`
	WIPAgeP90Hours           []*float64                `json:"wip_age_p90_hours"`
	BugCompletedRatio        []float64                 `json:"bug_completed_ratio"`
	StoryPointsCompleted     []float64                 `json:"story_points_completed"`
	NewBugsCount             []int                     `json:"new_bugs_count"`
	NewItemsCount            []int                     `json:"new_items_count"`
	DefectIntroRate          []float64                 `json:"defect_intro_rate"`
	WIPCongestionRatio       []float64                 `json:"wip_congestion_ratio"`
	PredictabilityScore      []float64                 `json:"predictability_score"`
	ComputedAt               []time.Time               `json:"computed_at"`
	OrgID                    []string                  `json:"org_id"`
}

type githubWorkItemUserMetricsDailyOracleColumns struct {
	Day               []githubWorkItemMetricDay `json:"day"`
	Provider          []string                  `json:"provider"`
	WorkScopeID       []string                  `json:"work_scope_id"`
	UserIdentity      []string                  `json:"user_identity"`
	TeamID            []string                  `json:"team_id"`
	TeamName          []string                  `json:"team_name"`
	ItemsStarted      []int                     `json:"items_started"`
	ItemsCompleted    []int                     `json:"items_completed"`
	WIPCountEndOfDay  []int                     `json:"wip_count_end_of_day"`
	CycleTimeP50Hours []*float64                `json:"cycle_time_p50_hours"`
	CycleTimeP90Hours []*float64                `json:"cycle_time_p90_hours"`
	ComputedAt        []time.Time               `json:"computed_at"`
	OrgID             []string                  `json:"org_id"`
}

type githubWorkItemCycleTimeOracleColumns struct {
	WorkItemID      []string                  `json:"work_item_id"`
	Provider        []string                  `json:"provider"`
	Day             []githubWorkItemMetricDay `json:"day"`
	WorkScopeID     []string                  `json:"work_scope_id"`
	TeamID          []string                  `json:"team_id"`
	TeamName        []string                  `json:"team_name"`
	Assignee        []*string                 `json:"assignee"`
	Type            []string                  `json:"type"`
	Status          []string                  `json:"status"`
	CreatedAt       []time.Time               `json:"created_at"`
	StartedAt       []*time.Time              `json:"started_at"`
	CompletedAt     []*time.Time              `json:"completed_at"`
	CycleTimeHours  []*float64                `json:"cycle_time_hours"`
	LeadTimeHours   []*float64                `json:"lead_time_hours"`
	ActiveTimeHours []*float64                `json:"active_time_hours"`
	WaitTimeHours   []*float64                `json:"wait_time_hours"`
	FlowEfficiency  []*float64                `json:"flow_efficiency"`
	ComputedAt      []time.Time               `json:"computed_at"`
	OrgID           []string                  `json:"org_id"`
}

func TestGitHubWorkItemMetricsDailyMatchesLivePythonProduction(t *testing.T) {
	compareRowsAgainstPythonOracle(
		t,
		"github/work-items/metrics-daily",
		githubWorkItemMetricTripletOracleCases(),
		func(t *testing.T, input map[string]any) githubWorkItemMetricsDailyOracleColumns {
			return githubWorkItemMetricTripletOracleResult(t, input).metricsDaily
		},
		nil,
	)
}

func TestGitHubWorkItemUserMetricsDailyMatchesLivePythonProduction(t *testing.T) {
	compareRowsAgainstPythonOracle(
		t,
		"github/work-items/user-metrics-daily",
		githubWorkItemMetricTripletOracleCases(),
		func(t *testing.T, input map[string]any) githubWorkItemUserMetricsDailyOracleColumns {
			return githubWorkItemMetricTripletOracleResult(t, input).userMetricsDaily
		},
		nil,
	)
}

func TestGitHubWorkItemCycleTimesMatchLivePythonProduction(t *testing.T) {
	compareRowsAgainstPythonOracle(
		t,
		"github/work-items/cycle-times",
		githubWorkItemMetricTripletOracleCases(),
		func(t *testing.T, input map[string]any) githubWorkItemCycleTimeOracleColumns {
			return githubWorkItemMetricTripletOracleResult(t, input).cycleTimes
		},
		nil,
	)
}

// TestGitHubWorkItemMetricTripletOracleBatchIsNotVacuous is the loud-failure
// half of the three comparisons above. Every column is a list, and two empty
// lists compare equal: a batch where the Go compute returned nothing for a
// projection in every case would pass all three pairs while proving nothing
// about that projection. This asserts each of the three projections is actually
// populated somewhere in the batch, and -- separately -- that the case that is
// SUPPOSED to produce no rows at all really produces none, so "non-empty
// somewhere" cannot be satisfied by accidentally making every case non-empty.
func TestGitHubWorkItemMetricTripletOracleBatchIsNotVacuous(t *testing.T) {
	groups, users, cycles := 0, 0, 0
	var silent githubWorkItemMetricTripletOracleProjections
	found := false
	for _, testCase := range githubWorkItemMetricTripletOracleCases() {
		result := githubWorkItemMetricTripletOracleResult(t, testCase.Input)
		groups += len(result.metricsDaily.Provider)
		users += len(result.userMetricsDaily.Provider)
		cycles += len(result.cycleTimes.Provider)
		if testCase.ID == "created_after_the_window_is_not_a_row" {
			silent, found = result, true
		}
	}
	if groups == 0 || users == 0 || cycles == 0 {
		t.Fatalf("oracle batch produced groups=%d users=%d cycles=%d -- a projection "+
			"that is empty in every case compares equal to Python's empty list "+
			"without ever exercising the projection", groups, users, cycles)
	}
	if !found {
		t.Fatal("the deliberately-silent oracle case is missing from the batch")
	}
	if len(silent.metricsDaily.Provider) != 0 || len(silent.userMetricsDaily.Provider) != 0 ||
		len(silent.cycleTimes.Provider) != 0 {
		t.Fatalf("an item created after the day window emitted rows: groups=%d users=%d cycles=%d",
			len(silent.metricsDaily.Provider), len(silent.userMetricsDaily.Provider),
			len(silent.cycleTimes.Provider))
	}
}

type githubWorkItemMetricTripletOracleProjections struct {
	metricsDaily     githubWorkItemMetricsDailyOracleColumns
	userMetricsDaily githubWorkItemUserMetricsDailyOracleColumns
	cycleTimes       githubWorkItemCycleTimeOracleColumns
}

func githubWorkItemMetricTripletOracleResult(
	t *testing.T,
	input map[string]any,
) githubWorkItemMetricTripletOracleProjections {
	t.Helper()
	encoded, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	var decoded githubWorkItemMetricTripletOracleInput
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	day, err := time.ParseInLocation(time.DateOnly, decoded.Day, time.UTC)
	if err != nil {
		t.Fatal(err)
	}
	provider := "github"
	if len(decoded.WorkItems) > 0 && decoded.WorkItems[0].Provider != "" {
		provider = decoded.WorkItems[0].Provider
	}
	claim := nativeTestClaim(provider, "work-items")
	claim.OrgID = decoded.OrgID
	rows := githubWorkItemRows{
		WorkItems:         decoded.WorkItems,
		StatusTransitions: decoded.Transitions,
		Dependencies:      decoded.Dependencies,
	}
	// Mirror loadGitHubWorkItemDerivationContext's own merge: persisted donors
	// first, freshly collected rows overriding them.
	derived := newGitHubWorkItemDerivationContext(decoded.Facts)
	subjects := make(map[string]githubWorkItemDerivationSubject, len(decoded.Donors)+len(rows.WorkItems))
	for _, donor := range decoded.Donors {
		subject := githubWorkItemDerivationSubjectFromRow(donor)
		subjects[subject.WorkItemID] = subject
	}
	for _, row := range rows.WorkItems {
		subject := githubWorkItemDerivationSubjectFromRow(row)
		subjects[subject.WorkItemID] = subject
	}
	derived.linkedIssue = derived.buildLinkedIssueIndex(subjects, rows.Dependencies)

	triplet, err := buildGitHubWorkItemMetricTriplet(claim, rows, day, decoded.ComputedAt, derived)
	if err != nil {
		t.Fatal(err)
	}
	result := githubWorkItemMetricTripletOracleProjections{
		metricsDaily:     newGitHubWorkItemMetricsDailyOracleColumns(len(triplet.MetricsDaily)),
		userMetricsDaily: newGitHubWorkItemUserMetricsDailyOracleColumns(len(triplet.UserMetricsDaily)),
		cycleTimes:       newGitHubWorkItemCycleTimeOracleColumns(len(triplet.CycleTimes)),
	}
	for _, row := range triplet.MetricsDaily {
		columns := &result.metricsDaily
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
	for _, row := range triplet.UserMetricsDaily {
		columns := &result.userMetricsDaily
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
	for _, row := range triplet.CycleTimes {
		columns := &result.cycleTimes
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
	return result
}

func newGitHubWorkItemMetricsDailyOracleColumns(size int) githubWorkItemMetricsDailyOracleColumns {
	return githubWorkItemMetricsDailyOracleColumns{
		Day: make([]githubWorkItemMetricDay, 0, size), Provider: make([]string, 0, size),
		WorkScopeID: make([]string, 0, size), TeamID: make([]string, 0, size),
		TeamName: make([]string, 0, size), ItemsStarted: make([]int, 0, size),
		ItemsCompleted: make([]int, 0, size), ItemsStartedUnassigned: make([]int, 0, size),
		ItemsCompletedUnassigned: make([]int, 0, size), WIPCountEndOfDay: make([]int, 0, size),
		WIPUnassignedEndOfDay: make([]int, 0, size), CycleTimeP50Hours: make([]*float64, 0, size),
		CycleTimeP90Hours: make([]*float64, 0, size), LeadTimeP50Hours: make([]*float64, 0, size),
		LeadTimeP90Hours: make([]*float64, 0, size), WIPAgeP50Hours: make([]*float64, 0, size),
		WIPAgeP90Hours: make([]*float64, 0, size), BugCompletedRatio: make([]float64, 0, size),
		StoryPointsCompleted: make([]float64, 0, size), NewBugsCount: make([]int, 0, size),
		NewItemsCount: make([]int, 0, size), DefectIntroRate: make([]float64, 0, size),
		WIPCongestionRatio: make([]float64, 0, size), PredictabilityScore: make([]float64, 0, size),
		ComputedAt: make([]time.Time, 0, size), OrgID: make([]string, 0, size),
	}
}

func newGitHubWorkItemUserMetricsDailyOracleColumns(size int) githubWorkItemUserMetricsDailyOracleColumns {
	return githubWorkItemUserMetricsDailyOracleColumns{
		Day: make([]githubWorkItemMetricDay, 0, size), Provider: make([]string, 0, size),
		WorkScopeID: make([]string, 0, size), UserIdentity: make([]string, 0, size),
		TeamID: make([]string, 0, size), TeamName: make([]string, 0, size),
		ItemsStarted: make([]int, 0, size), ItemsCompleted: make([]int, 0, size),
		WIPCountEndOfDay: make([]int, 0, size), CycleTimeP50Hours: make([]*float64, 0, size),
		CycleTimeP90Hours: make([]*float64, 0, size), ComputedAt: make([]time.Time, 0, size),
		OrgID: make([]string, 0, size),
	}
}

func newGitHubWorkItemCycleTimeOracleColumns(size int) githubWorkItemCycleTimeOracleColumns {
	return githubWorkItemCycleTimeOracleColumns{
		WorkItemID: make([]string, 0, size), Provider: make([]string, 0, size),
		Day: make([]githubWorkItemMetricDay, 0, size), WorkScopeID: make([]string, 0, size),
		TeamID: make([]string, 0, size), TeamName: make([]string, 0, size),
		Assignee: make([]*string, 0, size), Type: make([]string, 0, size),
		Status: make([]string, 0, size), CreatedAt: make([]time.Time, 0, size),
		StartedAt: make([]*time.Time, 0, size), CompletedAt: make([]*time.Time, 0, size),
		CycleTimeHours: make([]*float64, 0, size), LeadTimeHours: make([]*float64, 0, size),
		ActiveTimeHours: make([]*float64, 0, size), WaitTimeHours: make([]*float64, 0, size),
		FlowEfficiency: make([]*float64, 0, size), ComputedAt: make([]time.Time, 0, size),
		OrgID: make([]string, 0, size),
	}
}
