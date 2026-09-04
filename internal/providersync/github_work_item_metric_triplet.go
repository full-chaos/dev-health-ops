package providersync

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/workitemmetrics"
	"github.com/full-chaos/dev-health-ops/internal/teamattribution"
)

const (
	githubWorkItemMetricsDailyDestination     = "work_item_metrics_daily"
	githubWorkItemUserMetricsDailyDestination = "work_item_user_metrics_daily"
	githubWorkItemCycleTimesDestination       = "work_item_cycle_times"
)

// githubWorkItemMetricDay is a ClickHouse Date, not an instant. Keeping it
// distinct from time.Time prevents a local timezone from changing the day on
// either the effect wire or readback path.
type githubWorkItemMetricDay string

func newGitHubWorkItemMetricDay(value time.Time) githubWorkItemMetricDay {
	return githubWorkItemMetricDay(value.UTC().Format(time.DateOnly))
}

func (day githubWorkItemMetricDay) time() (time.Time, error) {
	return time.ParseInLocation(time.DateOnly, string(day), time.UTC)
}

// OracleDate is consumed by the generic differential oracle's typed encoder.
// It keeps Python date and Go Date leaves type-distinct from strings/datetimes.
func (day githubWorkItemMetricDay) OracleDate() string { return string(day) }

type githubWorkItemMetricsDailyRow struct {
	Day                      githubWorkItemMetricDay `json:"day"`
	Provider                 string                  `json:"provider"`
	WorkScopeID              string                  `json:"work_scope_id"`
	TeamID                   string                  `json:"team_id"`
	TeamName                 string                  `json:"team_name"`
	ItemsStarted             int                     `json:"items_started"`
	ItemsCompleted           int                     `json:"items_completed"`
	ItemsStartedUnassigned   int                     `json:"items_started_unassigned"`
	ItemsCompletedUnassigned int                     `json:"items_completed_unassigned"`
	WIPCountEndOfDay         int                     `json:"wip_count_end_of_day"`
	WIPUnassignedEndOfDay    int                     `json:"wip_unassigned_end_of_day"`
	CycleTimeP50Hours        *float64                `json:"cycle_time_p50_hours"`
	CycleTimeP90Hours        *float64                `json:"cycle_time_p90_hours"`
	LeadTimeP50Hours         *float64                `json:"lead_time_p50_hours"`
	LeadTimeP90Hours         *float64                `json:"lead_time_p90_hours"`
	WIPAgeP50Hours           *float64                `json:"wip_age_p50_hours"`
	WIPAgeP90Hours           *float64                `json:"wip_age_p90_hours"`
	BugCompletedRatio        float64                 `json:"bug_completed_ratio"`
	StoryPointsCompleted     float64                 `json:"story_points_completed"`
	NewBugsCount             int                     `json:"new_bugs_count"`
	NewItemsCount            int                     `json:"new_items_count"`
	DefectIntroRate          float64                 `json:"defect_intro_rate"`
	WIPCongestionRatio       float64                 `json:"wip_congestion_ratio"`
	PredictabilityScore      float64                 `json:"predictability_score"`
	ComputedAt               time.Time               `json:"computed_at"`
	OrgID                    string                  `json:"org_id"`
}

type githubWorkItemUserMetricsDailyRow struct {
	Day               githubWorkItemMetricDay `json:"day"`
	Provider          string                  `json:"provider"`
	WorkScopeID       string                  `json:"work_scope_id"`
	UserIdentity      string                  `json:"user_identity"`
	TeamID            string                  `json:"team_id"`
	TeamName          string                  `json:"team_name"`
	ItemsStarted      int                     `json:"items_started"`
	ItemsCompleted    int                     `json:"items_completed"`
	WIPCountEndOfDay  int                     `json:"wip_count_end_of_day"`
	CycleTimeP50Hours *float64                `json:"cycle_time_p50_hours"`
	CycleTimeP90Hours *float64                `json:"cycle_time_p90_hours"`
	ComputedAt        time.Time               `json:"computed_at"`
	OrgID             string                  `json:"org_id"`
}

// githubWorkItemCycleTimeRecord mirrors the complete Python compute record.
// The three flow fields are intentionally absent from the persistence row
// below because Python's ClickHouse sink does not insert them.
type githubWorkItemCycleTimeRecord struct {
	WorkItemID      string                  `json:"work_item_id"`
	Provider        string                  `json:"provider"`
	Day             githubWorkItemMetricDay `json:"day"`
	WorkScopeID     string                  `json:"work_scope_id"`
	TeamID          string                  `json:"team_id"`
	TeamName        string                  `json:"team_name"`
	Assignee        *string                 `json:"assignee"`
	Type            string                  `json:"type"`
	Status          string                  `json:"status"`
	CreatedAt       time.Time               `json:"created_at"`
	StartedAt       *time.Time              `json:"started_at"`
	CompletedAt     *time.Time              `json:"completed_at"`
	CycleTimeHours  *float64                `json:"cycle_time_hours"`
	LeadTimeHours   *float64                `json:"lead_time_hours"`
	ActiveTimeHours *float64                `json:"active_time_hours"`
	WaitTimeHours   *float64                `json:"wait_time_hours"`
	FlowEfficiency  *float64                `json:"flow_efficiency"`
	ComputedAt      time.Time               `json:"computed_at"`
	OrgID           string                  `json:"org_id"`
}

type githubWorkItemCycleTimePersistenceRow struct {
	WorkItemID     string                  `json:"work_item_id"`
	Provider       string                  `json:"provider"`
	Day            githubWorkItemMetricDay `json:"day"`
	WorkScopeID    string                  `json:"work_scope_id"`
	TeamID         string                  `json:"team_id"`
	TeamName       string                  `json:"team_name"`
	Assignee       *string                 `json:"assignee"`
	Type           string                  `json:"type"`
	Status         string                  `json:"status"`
	CreatedAt      time.Time               `json:"created_at"`
	StartedAt      *time.Time              `json:"started_at"`
	CompletedAt    *time.Time              `json:"completed_at"`
	CycleTimeHours *float64                `json:"cycle_time_hours"`
	LeadTimeHours  *float64                `json:"lead_time_hours"`
	ComputedAt     time.Time               `json:"computed_at"`
	OrgID          string                  `json:"org_id"`
}

type githubWorkItemMetricTriplet struct {
	MetricsDaily     []githubWorkItemMetricsDailyRow
	UserMetricsDaily []githubWorkItemUserMetricsDailyRow
	CycleTimes       []githubWorkItemCycleTimeRecord
}

type githubWorkItemMetricGroupBucket struct {
	teamName                                          string
	itemsStarted, itemsCompleted                      int
	itemsStartedUnassigned, itemsCompletedUnassigned  int
	wipCount, wipUnassigned                           int
	wipAgeHours, leadHours, cycleHours                []float64
	bugCompleted, newBugs, newItems, weeklyThroughput int
	storyPointsCompleted                              float64
}

type githubWorkItemMetricUserBucket struct {
	teamName                     string
	itemsStarted, itemsCompleted int
	wipCount                     int
	cycleHours                   []float64
}

type githubWorkItemMetricGroupKey struct {
	provider, scope, teamID string
}

type githubWorkItemMetricUserKey struct {
	provider, scope, user, teamID string
}

// buildGitHubWorkItemMetricTriplet is deliberately pure. The caller loads the
// shared attribution context once and can reuse it for every UTC backfill day.
// It does not register or activate the composite route.
func buildGitHubWorkItemMetricTriplet(
	claim Claim,
	rows githubWorkItemRows,
	day time.Time,
	computedAt time.Time,
	derived teamattribution.GithubWorkItemDerivationContext,
) (githubWorkItemMetricTriplet, error) {
	return buildWorkItemMetricTripletForProvider(
		"github", claim, rows, day, computedAt, derived,
	)
}

// buildWorkItemMetricTripletForProvider keeps the metric computation identical
// across providers while retaining the provider value on every schema row.
// GitLab and Jira use the same canonical WorkItem fields and resolver, but
// must never be made to look like GitHub merely to reuse this arithmetic.
func buildWorkItemMetricTripletForProvider(
	provider string,
	claim Claim,
	rows githubWorkItemRows,
	day time.Time,
	computedAt time.Time,
	derived teamattribution.GithubWorkItemDerivationContext,
) (githubWorkItemMetricTriplet, error) {
	if claim.Validate() != nil || claim.Provider != provider ||
		!isWorkItemFamilyDataset(claim.Dataset) || day.IsZero() || computedAt.IsZero() {
		return githubWorkItemMetricTriplet{}, ErrInvalidConfiguration
	}
	computedAt = computedAt.UTC()

	// Tenancy is asserted in ONE pass over the rows in their original order,
	// exactly where the pre-CHAOS-4283 inline version asserted it: the check
	// was the first statement of each loop body, before any `continue`, so
	// every row was checked in order regardless of relevance. Hoisting it here
	// preserves both the set of rejected inputs and WHICH row is reported
	// first.
	transitions := make([]workitemmetrics.Transition, 0, len(rows.StatusTransitions))
	for _, transition := range rows.StatusTransitions {
		if transition.OrgID != claim.OrgID {
			return githubWorkItemMetricTriplet{}, ErrInvalidConfiguration
		}
		transitions = append(transitions, workitemmetrics.Transition{
			WorkItemID: transition.WorkItemID,
			OccurredAt: transition.OccurredAt,
			ToStatus:   transition.ToStatus,
		})
	}
	items, err := workItemMetricItems(claim, rows)
	if err != nil {
		return githubWorkItemMetricTriplet{}, err
	}

	triplet := workitemmetrics.ComputeDailyTriplet(
		day, items, transitions,
		workitemmetrics.AssertAligned(len(rows.WorkItems), len(items), workItemMetricResolver(rows, derived)),
	)
	return githubWorkItemMetricTriplet{
		MetricsDaily:     githubWorkItemMetricsDailyRows(triplet.MetricsDaily, computedAt, claim.OrgID),
		UserMetricsDaily: githubWorkItemUserMetricsDailyRows(triplet.UserMetricsDaily, computedAt, claim.OrgID),
		CycleTimes:       githubWorkItemCycleTimeRecords(triplet.CycleTimes, computedAt, claim.OrgID),
	}, nil
}

// workItemMetricItems projects providersync's row shape onto the shared
// compute's input, asserting tenancy in row order (see the caller's comment).
func workItemMetricItems(claim Claim, rows githubWorkItemRows) ([]workitemmetrics.Item, error) {
	items := make([]workitemmetrics.Item, 0, len(rows.WorkItems))
	for _, item := range rows.WorkItems {
		if item.OrgID != claim.OrgID || item.Provider != claim.Provider || item.CreatedAt.IsZero() {
			return nil, ErrInvalidConfiguration
		}
		items = append(items, workitemmetrics.Item{
			WorkItemID:  item.WorkItemID,
			Provider:    item.Provider,
			Type:        item.Type,
			Status:      item.Status,
			Assignee:    workitemmetrics.FirstAssignee(item.Assignees),
			CreatedAt:   item.CreatedAt,
			StartedAt:   item.StartedAt,
			CompletedAt: item.CompletedAt,
			ClosedAt:    item.ClosedAt,
			StoryPoints: item.StoryPoints,
		})
	}
	return items, nil
}

// workItemMetricResolver runs the live teamattribution cascade LAZILY, once per
// item the shared compute actually reaches -- the same call sites, in the same
// order, as the pre-extraction inline version. Resolving eagerly in
// workItemMetricItems would call Resolve for items the relevance filter
// discards, which is a behaviour change even though Resolve is pure.
func workItemMetricResolver(
	rows githubWorkItemRows, derived teamattribution.GithubWorkItemDerivationContext,
) workitemmetrics.Resolver {
	return func(index int) workitemmetrics.Attribution {
		subject := githubWorkItemDerivationSubjectFromRow(rows.WorkItems[index])
		teamID, teamName, _ := derived.Resolve(subject)
		return workitemmetrics.Attribution{
			WorkScopeID: teamattribution.WorkItemDerivationScope(subject),
			TeamID:      workitemmetrics.NormalizeTeamID(teamID),
			TeamName:    workitemmetrics.NormalizeTeamName(teamName),
		}
	}
}

func githubWorkItemMetricsDailyRows(
	computed []workitemmetrics.MetricsDailyRow, computedAt time.Time, orgID string,
) []githubWorkItemMetricsDailyRow {
	if len(computed) == 0 {
		return nil
	}
	result := make([]githubWorkItemMetricsDailyRow, 0, len(computed))
	for _, row := range computed {
		result = append(result, githubWorkItemMetricsDailyRow{
			Day: newGitHubWorkItemMetricDay(row.Day), Provider: row.Provider,
			WorkScopeID: row.WorkScopeID, TeamID: row.TeamID, TeamName: row.TeamName,
			ItemsStarted: row.ItemsStarted, ItemsCompleted: row.ItemsCompleted,
			ItemsStartedUnassigned:   row.ItemsStartedUnassigned,
			ItemsCompletedUnassigned: row.ItemsCompletedUnassigned,
			WIPCountEndOfDay:         row.WIPCountEndOfDay,
			WIPUnassignedEndOfDay:    row.WIPUnassignedEndOfDay,
			CycleTimeP50Hours:        row.CycleTimeP50Hours,
			CycleTimeP90Hours:        row.CycleTimeP90Hours,
			LeadTimeP50Hours:         row.LeadTimeP50Hours,
			LeadTimeP90Hours:         row.LeadTimeP90Hours,
			WIPAgeP50Hours:           row.WIPAgeP50Hours,
			WIPAgeP90Hours:           row.WIPAgeP90Hours,
			BugCompletedRatio:        row.BugCompletedRatio,
			StoryPointsCompleted:     row.StoryPointsCompleted,
			NewBugsCount:             row.NewBugsCount, NewItemsCount: row.NewItemsCount,
			DefectIntroRate: row.DefectIntroRate, WIPCongestionRatio: row.WIPCongestionRatio,
			PredictabilityScore: row.PredictabilityScore,
			ComputedAt:          computedAt, OrgID: orgID,
		})
	}
	return result
}

func githubWorkItemUserMetricsDailyRows(
	computed []workitemmetrics.UserMetricsDailyRow, computedAt time.Time, orgID string,
) []githubWorkItemUserMetricsDailyRow {
	if len(computed) == 0 {
		return nil
	}
	result := make([]githubWorkItemUserMetricsDailyRow, 0, len(computed))
	for _, row := range computed {
		result = append(result, githubWorkItemUserMetricsDailyRow{
			Day: newGitHubWorkItemMetricDay(row.Day), Provider: row.Provider,
			WorkScopeID: row.WorkScopeID, UserIdentity: row.UserIdentity,
			TeamID: row.TeamID, TeamName: row.TeamName,
			ItemsStarted: row.ItemsStarted, ItemsCompleted: row.ItemsCompleted,
			WIPCountEndOfDay:  row.WIPCountEndOfDay,
			CycleTimeP50Hours: row.CycleTimeP50Hours,
			CycleTimeP90Hours: row.CycleTimeP90Hours,
			ComputedAt:        computedAt, OrgID: orgID,
		})
	}
	return result
}

func githubWorkItemCycleTimeRecords(
	computed []workitemmetrics.CycleTimeRecord, computedAt time.Time, orgID string,
) []githubWorkItemCycleTimeRecord {
	if len(computed) == 0 {
		return nil
	}
	result := make([]githubWorkItemCycleTimeRecord, 0, len(computed))
	for _, row := range computed {
		result = append(result, githubWorkItemCycleTimeRecord{
			WorkItemID: row.WorkItemID, Provider: row.Provider,
			Day: newGitHubWorkItemMetricDay(row.Day), WorkScopeID: row.WorkScopeID,
			TeamID: row.TeamID, TeamName: row.TeamName, Assignee: row.Assignee,
			Type: row.Type, Status: row.Status, CreatedAt: row.CreatedAt,
			StartedAt: row.StartedAt, CompletedAt: row.CompletedAt,
			CycleTimeHours: row.CycleTimeHours, LeadTimeHours: row.LeadTimeHours,
			ActiveTimeHours: row.ActiveTimeHours, WaitTimeHours: row.WaitTimeHours,
			FlowEfficiency: row.FlowEfficiency,
			ComputedAt:     computedAt, OrgID: orgID,
		})
	}
	return result
}

// githubWorkItemMetricTripletDestinations is exactly what this lane owns. It is
// a SUBSET of githubWorkItemDerivedDestinations, not a replacement for it: the
// composite route builds all sixteen effects in one canonical-order pass
// through BuildGitHubWorkItemEffects, so this lane must hand its rows to the
// githubWorkItemsDeriver seam rather than mint EffectBatches of its own. A
// second BuildEffectBatch call site for the same destination would be able to
// disagree with the route's about recovery class or row encoding, and nothing
// would compare the two.
var githubWorkItemMetricTripletDestinations = []string{
	githubWorkItemCycleTimesDestination,
	githubWorkItemMetricsDailyDestination,
	githubWorkItemUserMetricsDailyDestination,
}

// derivedRows projects the triplet onto the deriver seam's wire shape. The
// cycle-time entry is the PERSISTENCE projection: Python's ClickHouse sink
// (metrics/sinks/clickhouse/work_graph.py:283-309) names sixteen columns and
// active_time_hours/wait_time_hours/flow_efficiency are not among them, even
// though migration 003 added those columns to the table. Emitting the full
// compute record here would write three columns Python leaves at their
// DEFAULT 0, which readback would then see as a conflict on every re-run.
func (triplet githubWorkItemMetricTriplet) derivedRows() (map[string][]json.RawMessage, error) {
	cycleRows := make([]githubWorkItemCycleTimePersistenceRow, 0, len(triplet.CycleTimes))
	for _, row := range triplet.CycleTimes {
		cycleRows = append(cycleRows, row.persistenceRow())
	}
	values := map[string]any{
		githubWorkItemCycleTimesDestination:       cycleRows,
		githubWorkItemMetricsDailyDestination:     triplet.MetricsDaily,
		githubWorkItemUserMetricsDailyDestination: triplet.UserMetricsDaily,
	}
	result := make(map[string][]json.RawMessage, len(values))
	for _, destination := range githubWorkItemMetricTripletDestinations {
		rows, owned := values[destination]
		if !owned {
			return nil, ErrInvalidConfiguration
		}
		encoded, err := marshalGitHubWorkItemMetricRows(rows)
		if err != nil {
			return nil, err
		}
		result[destination] = encoded
	}
	if len(result) != len(values) {
		return nil, ErrInvalidConfiguration
	}
	return result, nil
}

func marshalGitHubWorkItemMetricRows(rows any) ([]json.RawMessage, error) {
	encoded, err := json.Marshal(rows)
	if err != nil {
		return nil, err
	}
	var result []json.RawMessage
	if err := json.Unmarshal(encoded, &result); err != nil {
		return nil, fmt.Errorf("%w: metric rows", ErrEffectRecoveryUnsafe)
	}
	if result == nil {
		// A nil Go slice marshals to `null`, which round-trips back to nil. The
		// deriver seam distinguishes "evaluated, produced nothing" from "missing
		// destination" by key presence, so the value must be an empty slice.
		result = []json.RawMessage{}
	}
	return result, nil
}

func (row githubWorkItemCycleTimeRecord) persistenceRow() githubWorkItemCycleTimePersistenceRow {
	return githubWorkItemCycleTimePersistenceRow{
		WorkItemID: row.WorkItemID, Provider: row.Provider, Day: row.Day,
		WorkScopeID: row.WorkScopeID, TeamID: row.TeamID, TeamName: row.TeamName,
		Assignee: row.Assignee, Type: row.Type, Status: row.Status,
		CreatedAt: row.CreatedAt, StartedAt: row.StartedAt, CompletedAt: row.CompletedAt,
		CycleTimeHours: row.CycleTimeHours, LeadTimeHours: row.LeadTimeHours,
		ComputedAt: row.ComputedAt, OrgID: row.OrgID,
	}
}

func floatPointer(value float64) *float64 { return &value }
