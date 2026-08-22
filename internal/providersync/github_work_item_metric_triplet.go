package providersync

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
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

var githubWorkItemMetricWaitStatuses = map[string]struct{}{
	"backlog": {}, "todo": {}, "waiting": {}, "blocked": {},
	"review_requested": {}, "waiting_for_review": {},
}

// buildGitHubWorkItemMetricTriplet is deliberately pure. The caller loads the
// shared attribution context once and can reuse it for every UTC backfill day.
// It does not register or activate the composite route.
func buildGitHubWorkItemMetricTriplet(
	claim Claim,
	rows githubWorkItemRows,
	day time.Time,
	computedAt time.Time,
	derived githubWorkItemDerivationContext,
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
	derived githubWorkItemDerivationContext,
) (githubWorkItemMetricTriplet, error) {
	if claim.Validate() != nil || claim.Provider != provider ||
		!isWorkItemFamilyDataset(claim.Dataset) || day.IsZero() || computedAt.IsZero() {
		return githubWorkItemMetricTriplet{}, ErrInvalidConfiguration
	}
	dayUTC := time.Date(day.UTC().Year(), day.UTC().Month(), day.UTC().Day(), 0, 0, 0, 0, time.UTC)
	end := dayUTC.AddDate(0, 0, 1)
	weekStart := end.AddDate(0, 0, -7)
	computedAt = computedAt.UTC()

	transitions := make(map[string][]githubWorkItemTransitionRow)
	for _, transition := range rows.StatusTransitions {
		if transition.OrgID != claim.OrgID {
			return githubWorkItemMetricTriplet{}, ErrInvalidConfiguration
		}
		transitions[transition.WorkItemID] = append(transitions[transition.WorkItemID], transition)
	}
	groups := make(map[githubWorkItemMetricGroupKey]*githubWorkItemMetricGroupBucket)
	users := make(map[githubWorkItemMetricUserKey]*githubWorkItemMetricUserBucket)
	result := githubWorkItemMetricTriplet{}

	for _, item := range rows.WorkItems {
		if item.OrgID != claim.OrgID || item.Provider != claim.Provider || item.CreatedAt.IsZero() {
			return githubWorkItemMetricTriplet{}, ErrInvalidConfiguration
		}
		createdAt := item.CreatedAt.UTC()
		if !createdAt.Before(end) {
			continue
		}
		startedAt := utcTimePointer(item.StartedAt)
		completedAt := utcTimePointer(item.CompletedAt)
		terminalAt := earliestTimePointer(completedAt, utcTimePointer(item.ClosedAt))
		startedToday := inHalfOpenDay(startedAt, dayUTC, end)
		completedToday := inHalfOpenDay(completedAt, dayUTC, end)
		wipEndOfDay := startedAt != nil && startedAt.Before(end) &&
			(terminalAt == nil || !terminalAt.Before(end))
		createdToday := !createdAt.Before(dayUTC) && createdAt.Before(end)
		if !startedToday && !completedToday && !wipEndOfDay && !createdToday {
			continue
		}

		scope := workItemDerivationScope(githubWorkItemDerivationSubjectFromRow(item))
		teamID, teamName, _ := derived.resolve(githubWorkItemDerivationSubjectFromRow(item))
		teamIDValue := normalizeGitHubWorkItemMetricTeamID(teamID)
		teamNameValue := normalizeGitHubWorkItemMetricTeamName(teamName)
		groupKey := githubWorkItemMetricGroupKey{item.Provider, scope, teamIDValue}
		bucket := groups[groupKey]
		if bucket == nil {
			bucket = &githubWorkItemMetricGroupBucket{teamName: teamNameValue}
			groups[groupKey] = bucket
		}

		// compute_work_items.py:872 is `user_identity = assignee or "unassigned"`,
		// so an assignee that is present but EMPTY falls to "unassigned" — an
		// empty string is falsy in Python. The unassigned group counters below
		// deliberately do NOT follow: they test `assignee is None`
		// (compute_work_items.py:906/916/1007), which an empty string passes.
		// One assignee value, two different questions.
		assignee := firstStringPointer(item.Assignees)
		userIdentity := "unassigned"
		if assignee != nil && *assignee != "" {
			userIdentity = *assignee
		}
		userKey := githubWorkItemMetricUserKey{item.Provider, scope, userIdentity, teamIDValue}
		userBucket := users[userKey]
		if userBucket == nil {
			userBucket = &githubWorkItemMetricUserBucket{teamName: teamNameValue}
			users[userKey] = userBucket
		}

		if createdToday {
			bucket.newItems++
			if item.Type == "bug" {
				bucket.newBugs++
			}
		}
		if completedAt != nil && !completedAt.Before(weekStart) && completedAt.Before(end) {
			bucket.weeklyThroughput++
		}
		if startedToday {
			bucket.itemsStarted++
			userBucket.itemsStarted++
			if assignee == nil {
				bucket.itemsStartedUnassigned++
			}
		}
		if completedToday {
			bucket.itemsCompleted++
			userBucket.itemsCompleted++
			if assignee == nil {
				bucket.itemsCompletedUnassigned++
			}
			if item.Type == "bug" {
				bucket.bugCompleted++
			}
			if item.StoryPoints != nil {
				bucket.storyPointsCompleted += *item.StoryPoints
			}
			leadHours := githubWorkItemMetricHours(completedAt.Sub(createdAt))
			bucket.leadHours = append(bucket.leadHours, leadHours)
			var cycleHours, activeHours, waitHours, efficiency *float64
			if startedAt != nil {
				value := githubWorkItemMetricHours(completedAt.Sub(*startedAt))
				cycleHours = &value
				bucket.cycleHours = append(bucket.cycleHours, value)
				userBucket.cycleHours = append(userBucket.cycleHours, value)
				if value > 0 {
					active, wait := calculateGitHubWorkItemFlowBreakdown(*startedAt, *completedAt, transitions[item.WorkItemID])
					if active+wait == 0 {
						active = value
					}
					activeHours, waitHours = floatPointer(active), floatPointer(wait)
					ratio := 0.0
					if active+wait > 0 {
						ratio = active / (active + wait)
					}
					efficiency = &ratio
				}
			}
			result.CycleTimes = append(result.CycleTimes, githubWorkItemCycleTimeRecord{
				WorkItemID: item.WorkItemID, Provider: item.Provider,
				Day: newGitHubWorkItemMetricDay(*completedAt), WorkScopeID: scope,
				TeamID: teamIDValue, TeamName: teamNameValue, Assignee: assignee,
				Type: item.Type, Status: item.Status, CreatedAt: createdAt,
				StartedAt: startedAt, CompletedAt: completedAt, CycleTimeHours: cycleHours,
				LeadTimeHours: floatPointer(leadHours), ActiveTimeHours: activeHours,
				WaitTimeHours: waitHours, FlowEfficiency: efficiency,
				ComputedAt: computedAt, OrgID: claim.OrgID,
			})
		}
		if wipEndOfDay {
			bucket.wipCount++
			userBucket.wipCount++
			if assignee == nil {
				bucket.wipUnassigned++
			}
			bucket.wipAgeHours = append(bucket.wipAgeHours, githubWorkItemMetricHours(end.Sub(*startedAt)))
		}
	}

	groupKeys := make([]githubWorkItemMetricGroupKey, 0, len(groups))
	for key := range groups {
		groupKeys = append(groupKeys, key)
	}
	sort.Slice(groupKeys, func(i, j int) bool {
		left, right := groupKeys[i], groupKeys[j]
		return left.provider < right.provider || left.provider == right.provider &&
			(left.scope < right.scope || left.scope == right.scope && left.teamID < right.teamID)
	})
	for _, key := range groupKeys {
		bucket := groups[key]
		completed := float64(bucket.itemsCompleted)
		bugRatio := 0.0
		if completed > 0 {
			bugRatio = float64(bucket.bugCompleted) / completed
		}
		defectRate := 0.0
		if bucket.newItems > 0 {
			defectRate = float64(bucket.newBugs) / float64(bucket.newItems)
		}
		congestion := float64(bucket.wipCount) / math.Max(1, float64(bucket.weeklyThroughput))
		predictability := 0.0
		if bucket.itemsCompleted+bucket.wipCount > 0 {
			predictability = completed / float64(bucket.itemsCompleted+bucket.wipCount)
		}
		result.MetricsDaily = append(result.MetricsDaily, githubWorkItemMetricsDailyRow{
			Day: newGitHubWorkItemMetricDay(dayUTC), Provider: key.provider,
			WorkScopeID: key.scope, TeamID: key.teamID, TeamName: bucket.teamName,
			ItemsStarted: bucket.itemsStarted, ItemsCompleted: bucket.itemsCompleted,
			ItemsStartedUnassigned:   bucket.itemsStartedUnassigned,
			ItemsCompletedUnassigned: bucket.itemsCompletedUnassigned,
			WIPCountEndOfDay:         bucket.wipCount, WIPUnassignedEndOfDay: bucket.wipUnassigned,
			CycleTimeP50Hours: percentilePointer(bucket.cycleHours, 50),
			CycleTimeP90Hours: percentilePointer(bucket.cycleHours, 90),
			LeadTimeP50Hours:  percentilePointer(bucket.leadHours, 50),
			LeadTimeP90Hours:  percentilePointer(bucket.leadHours, 90),
			WIPAgeP50Hours:    percentilePointer(bucket.wipAgeHours, 50),
			WIPAgeP90Hours:    percentilePointer(bucket.wipAgeHours, 90),
			BugCompletedRatio: bugRatio, StoryPointsCompleted: bucket.storyPointsCompleted,
			NewBugsCount: bucket.newBugs, NewItemsCount: bucket.newItems,
			DefectIntroRate: defectRate, WIPCongestionRatio: congestion,
			PredictabilityScore: predictability, ComputedAt: computedAt, OrgID: claim.OrgID,
		})
	}

	userKeys := make([]githubWorkItemMetricUserKey, 0, len(users))
	for key := range users {
		userKeys = append(userKeys, key)
	}
	sort.Slice(userKeys, func(i, j int) bool {
		left, right := userKeys[i], userKeys[j]
		return left.provider < right.provider || left.provider == right.provider &&
			(left.scope < right.scope || left.scope == right.scope &&
				(left.user < right.user || left.user == right.user && left.teamID < right.teamID))
	})
	for _, key := range userKeys {
		bucket := users[key]
		result.UserMetricsDaily = append(result.UserMetricsDaily, githubWorkItemUserMetricsDailyRow{
			Day: newGitHubWorkItemMetricDay(dayUTC), Provider: key.provider,
			WorkScopeID: key.scope, UserIdentity: key.user, TeamID: key.teamID,
			TeamName: bucket.teamName, ItemsStarted: bucket.itemsStarted,
			ItemsCompleted: bucket.itemsCompleted, WIPCountEndOfDay: bucket.wipCount,
			CycleTimeP50Hours: percentilePointer(bucket.cycleHours, 50),
			CycleTimeP90Hours: percentilePointer(bucket.cycleHours, 90),
			ComputedAt:        computedAt, OrgID: claim.OrgID,
		})
	}
	return result, nil
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

func normalizeGitHubWorkItemMetricTeamID(value *string) string {
	if value == nil || strings.TrimSpace(*value) == "" {
		return "unassigned"
	}
	return strings.TrimSpace(*value)
}

func normalizeGitHubWorkItemMetricTeamName(value *string) string {
	if value == nil || strings.TrimSpace(*value) == "" {
		return "Unassigned"
	}
	return strings.TrimSpace(*value)
}

func firstStringPointer(values []string) *string {
	if len(values) == 0 {
		return nil
	}
	value := values[0]
	return &value
}

func utcTimePointer(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	utc := value.UTC()
	return &utc
}

func earliestTimePointer(values ...*time.Time) *time.Time {
	var result *time.Time
	for _, value := range values {
		if value != nil && (result == nil || value.Before(*result)) {
			copy := *value
			result = &copy
		}
	}
	return result
}

func inHalfOpenDay(value *time.Time, start, end time.Time) bool {
	return value != nil && !value.Before(start) && value.Before(end)
}

func percentilePointer(values []float64, percentile float64) *float64 {
	if len(values) == 0 {
		return nil
	}
	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)
	if percentile <= 0 {
		return floatPointer(sorted[0])
	}
	if percentile >= 100 {
		return floatPointer(sorted[len(sorted)-1])
	}
	// Each intermediate below is bound to its own name and passed through an
	// explicit float64 conversion, and the parenthesisation mirrors
	// compute_work_items.py:48-52 exactly -- `(n-1) * (p/100)`, not `(n-1)*p/100`.
	//
	// This shape is not stylistic. Written as one expression, the Go compiler is
	// free to contract `a*b + c*d` into fused operations that skip the
	// intermediate rounding Python performs. Measured over 6000 percentile
	// evaluations drawn from the real input class (hours derived from
	// microsecond-resolution durations, n = 2..7, p = 50 and 90), the collapsed
	// form landed one ulp away from the live `_percentile` on 319 of them -- 5.3%
	// -- while this form matched on all 6000.
	//
	// One ulp matters because these columns are compared for EQUALITY on
	// readback: a percentile disagreeing in its last bit is an effect that can
	// never be confirmed, on every re-run, forever.
	ratio := float64(percentile / 100)
	rank := float64(float64(len(sorted)-1) * ratio)
	lo := int(rank)
	hi := lo + 1
	if hi >= len(sorted) {
		hi = len(sorted) - 1
	}
	frac := float64(rank - float64(lo))
	low := float64(sorted[lo] * float64(1-frac))
	high := float64(sorted[hi] * frac)
	return floatPointer(low + high)
}

// githubWorkItemMetricSeconds and githubWorkItemMetricHours mirror how PYTHON
// reaches these quantities, which is not how Go's own Duration methods do.
//
// timedelta.total_seconds() is `(whole microseconds) / 10**6` -- one division
// of an exact integer. Duration.Seconds() instead splits into whole seconds
// plus a nanosecond remainder and adds them, and Duration.Hours() splits into
// whole hours plus a nanosecond remainder. Those are different roundings of the
// same interval, and they disagree in the last bit for ordinary values: a
// 12h31m08.107259s cycle is 12.518918683055555 hours through Python's path and
// 12.518918683055556 through Duration.Hours(). Both numbers are "right"; only
// one of them matches the producer, and these columns are compared for equality
// on readback.
//
// Every datetime Python can hold is a whole number of microseconds, so
// Microseconds() loses nothing here.
func githubWorkItemMetricSeconds(value time.Duration) float64 {
	return float64(float64(value.Microseconds()) / 1e6)
}

func githubWorkItemMetricHours(value time.Duration) float64 {
	return float64(githubWorkItemMetricSeconds(value) / 3600)
}

func floatPointer(value float64) *float64 { return &value }

func calculateGitHubWorkItemFlowBreakdown(
	startedAt, completedAt time.Time,
	transitions []githubWorkItemTransitionRow,
) (float64, float64) {
	start, end := startedAt.UTC(), completedAt.UTC()
	if !start.Before(end) {
		return 0, 0
	}
	sorted := append([]githubWorkItemTransitionRow(nil), transitions...)
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].OccurredAt.Before(sorted[j].OccurredAt) })
	currentStatus := "unknown"
	for _, transition := range sorted {
		occurred := transition.OccurredAt.UTC()
		if occurred.After(start) {
			break
		}
		currentStatus = transition.ToStatus
	}
	if currentStatus == "unknown" || currentStatus == "todo" || currentStatus == "backlog" {
		currentStatus = "in_progress"
	}
	last := start
	activeSeconds, waitSeconds := 0.0, 0.0
	for _, transition := range sorted {
		occurred := transition.OccurredAt.UTC()
		if !occurred.After(start) {
			continue
		}
		if !occurred.Before(end) {
			break
		}
		duration := githubWorkItemMetricSeconds(occurred.Sub(last))
		if _, waiting := githubWorkItemMetricWaitStatuses[strings.ToLower(currentStatus)]; waiting {
			waitSeconds = float64(waitSeconds + duration)
		} else {
			activeSeconds = float64(activeSeconds + duration)
		}
		currentStatus, last = transition.ToStatus, occurred
	}
	if duration := githubWorkItemMetricSeconds(end.Sub(last)); duration > 0 {
		if _, waiting := githubWorkItemMetricWaitStatuses[strings.ToLower(currentStatus)]; waiting {
			waitSeconds = float64(waitSeconds + duration)
		} else {
			activeSeconds = float64(activeSeconds + duration)
		}
	}
	return float64(activeSeconds / 3600), float64(waitSeconds / 3600)
}
