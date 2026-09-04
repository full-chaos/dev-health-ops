// Package workitemmetrics holds the SINGLE Go implementation of the three
// work-item daily metric computations Python defines in
// src/dev_health_ops/metrics/compute_work_items.py:
//
//   - compute_work_item_metrics_daily  (:1075) -> ComputeDailyTriplet
//   - compute_estimate_coverage_metrics_daily (:1425) -> ComputeEstimateCoverage
//
// # Why this package exists (CHAOS-4283)
//
// This arithmetic was written and adversarially reviewed once already, for the
// sync-time deriver in internal/providersync, and is covered there by sixteen
// live-Python differential oracle pairs
// (internal/providersync/testdata/oracle_pairs/{github,gitlab,jira,linear}_
// work-items_{metrics-daily,user-metrics-daily,cycle-times,estimate-coverage}.py).
// CHAOS-4283 needs the SAME computation a second time, for the
// metrics.daily_partition native family executors. Copying it would create two
// implementations of one concept that are free to disagree -- and a float-parity
// helper has no safe direction to disagree in (see the fleet's standing
// Python<->Go parity lessons). So the arithmetic moved HERE, verbatim, and
// providersync now calls it through a thin adapter. Those sixteen oracle pairs
// continuing to pass unchanged is the regression proof that the move was
// behaviour-neutral -- they are this package's differential oracle too, not
// merely providersync's.
//
// # What is in scope
//
// ONLY the arithmetic. Team attribution and work-scope resolution are supplied
// by the caller through Resolver, because the two call sites resolve them from
// genuinely different sources: providersync runs the live
// teamattribution cascade over facts it already loaded, while the daily
// executor reads the cascade's already-materialised output from
// work_item_team_attributions (the CHAOS-4278 ruling). Effect-wire concerns --
// destination names, JSON row encoding, readback stamp precision -- stay with
// their callers; this package returns plain values.
package workitemmetrics

import (
	"sort"
	"time"
)

// Item is one work item, reduced to exactly the fields the computations read.
// It deliberately does NOT carry the project/team columns work_scope_id is
// derived from: that derivation is the caller's, via Resolver.
type Item struct {
	WorkItemID string
	Provider   string
	Type       string
	Status     string
	// Assignee is the FIRST assignee, or nil when the item has none.
	//
	// Two different questions are asked of this one value, and they disagree
	// on the empty string. compute_work_items.py:1180 is
	// `user_identity = assignee or "unassigned"`, so a present-but-EMPTY
	// assignee falls to "unassigned" (an empty string is falsy in Python).
	// The unassigned COUNTERS (:1214/:1220/:1315) instead test
	// `assignee is None`, which an empty string passes. Keeping a *string
	// (rather than a string plus a bool) is what preserves both answers.
	Assignee    *string
	CreatedAt   time.Time
	StartedAt   *time.Time
	CompletedAt *time.Time
	ClosedAt    *time.Time
	StoryPoints *float64
}

// Transition is one status transition, reduced to the fields
// _calculate_flow_breakdown reads (compute_work_items.py:969). from_status is
// absent because that function never reads it.
type Transition struct {
	WorkItemID string
	OccurredAt time.Time
	ToStatus   string
}

// Attribution is the caller's resolved answer for one item.
// TeamID/TeamName must ALREADY carry Python's normalize_team_id/
// normalize_team_name defaults ("unassigned"/"Unassigned") -- this package
// never applies them, so a caller that forgets is visible in the oracle rather
// than silently papered over here.
type Attribution struct {
	WorkScopeID string
	TeamID      string
	TeamName    string
}

// Resolver answers for the item at a given INDEX in the Items slice handed to
// the computation -- an index, not a work_item_id, so that two rows sharing an
// id can never collapse into one answer.
//
// It is called at exactly the points Python calls resolve_team_attribution:
// AFTER the relevance filter in ComputeDailyTriplet (compute_work_items.py
// short-circuits ineligible items before resolving), and for every
// window-eligible item in ComputeEstimateCoverage. Preserving those call sites
// keeps the port a move rather than a rewrite.
type Resolver func(index int) Attribution

// MetricsDailyRow mirrors WorkItemMetricsDailyRecord (metrics/schemas.py).
type MetricsDailyRow struct {
	Day                      time.Time
	Provider                 string
	WorkScopeID              string
	TeamID                   string
	TeamName                 string
	ItemsStarted             int
	ItemsCompleted           int
	ItemsStartedUnassigned   int
	ItemsCompletedUnassigned int
	WIPCountEndOfDay         int
	WIPUnassignedEndOfDay    int
	CycleTimeP50Hours        *float64
	CycleTimeP90Hours        *float64
	LeadTimeP50Hours         *float64
	LeadTimeP90Hours         *float64
	WIPAgeP50Hours           *float64
	WIPAgeP90Hours           *float64
	BugCompletedRatio        float64
	StoryPointsCompleted     float64
	NewBugsCount             int
	NewItemsCount            int
	DefectIntroRate          float64
	WIPCongestionRatio       float64
	PredictabilityScore      float64
}

// UserMetricsDailyRow mirrors WorkItemUserMetricsDailyRecord.
type UserMetricsDailyRow struct {
	Day               time.Time
	Provider          string
	WorkScopeID       string
	UserIdentity      string
	TeamID            string
	TeamName          string
	ItemsStarted      int
	ItemsCompleted    int
	WIPCountEndOfDay  int
	CycleTimeP50Hours *float64
	CycleTimeP90Hours *float64
}

// CycleTimeRecord mirrors the COMPLETE Python WorkItemCycleTimeRecord,
// including the three flow fields.
//
// Python's ClickHouse sink (metrics/sinks/clickhouse/work_graph.py:284-309)
// names sixteen columns and ActiveTimeHours/WaitTimeHours/FlowEfficiency are
// NOT among them, even though migration 003_flow_efficiency.sql added those
// columns (Float64 DEFAULT 0) to work_item_cycle_times. That drop belongs to
// the WRITE side, not to the computation: both callers reproduce it when they
// persist. Computing them here and dropping them there is exactly what Python
// does, and keeps the record honest for any future reader that wants them.
type CycleTimeRecord struct {
	WorkItemID      string
	Provider        string
	Day             time.Time
	WorkScopeID     string
	TeamID          string
	TeamName        string
	Assignee        *string
	Type            string
	Status          string
	CreatedAt       time.Time
	StartedAt       *time.Time
	CompletedAt     *time.Time
	CycleTimeHours  *float64
	LeadTimeHours   *float64
	ActiveTimeHours *float64
	WaitTimeHours   *float64
	FlowEfficiency  *float64
}

// Triplet is compute_work_item_metrics_daily's three-tuple return.
type Triplet struct {
	MetricsDaily     []MetricsDailyRow
	UserMetricsDaily []UserMetricsDailyRow
	CycleTimes       []CycleTimeRecord
}

// EstimateCoverageRow mirrors EstimateCoverageMetricsDailyRecord.
type EstimateCoverageRow struct {
	Day              time.Time
	Provider         string
	WorkScopeID      string
	TeamID           string
	TeamName         string
	EstimatedCount   int
	UnestimatedCount int
	BacklogSize      int
	Ratio            *float64
}

// waitStatuses ports WAIT_STATUSES (compute_work_items.py:959).
var waitStatuses = map[string]struct{}{
	"backlog": {}, "todo": {}, "waiting": {}, "blocked": {},
	"review_requested": {}, "waiting_for_review": {},
}

type groupKey struct {
	provider, workScopeID, teamID string
}

type userKey struct {
	provider, workScopeID, user, teamID string
}

type groupBucket struct {
	teamName                                          string
	itemsStarted, itemsCompleted                      int
	itemsStartedUnassigned, itemsCompletedUnassigned  int
	wipCount, wipUnassigned                           int
	wipAgeHours, leadHours, cycleHours                []float64
	bugCompleted, newBugs, newItems, weeklyThroughput int
	storyPointsCompleted                              float64
}

type userBucket struct {
	teamName                     string
	itemsStarted, itemsCompleted int
	wipCount                     int
	cycleHours                   []float64
}

// UTCDay truncates an instant to the UTC midnight its day starts at, the way
// _utc_day_window (compute_work_items.py:33) does.
func UTCDay(value time.Time) time.Time {
	utc := value.UTC()
	return time.Date(utc.Year(), utc.Month(), utc.Day(), 0, 0, 0, 0, time.UTC)
}

// ComputeDailyTriplet ports compute_work_item_metrics_daily
// (compute_work_items.py:1075) for one UTC day.
//
// day is truncated to UTC midnight; the window is the half-open [day, day+24h).
// resolve is consulted only for items that clear the relevance filter, exactly
// as Python does.
func ComputeDailyTriplet(
	day time.Time,
	items []Item,
	transitions []Transition,
	resolve Resolver,
) Triplet {
	dayUTC := UTCDay(day)
	end := dayUTC.AddDate(0, 0, 1)
	weekStart := end.AddDate(0, 0, -7)

	transitionsByItem := make(map[string][]Transition, len(transitions))
	for _, transition := range transitions {
		transitionsByItem[transition.WorkItemID] = append(transitionsByItem[transition.WorkItemID], transition)
	}

	groups := make(map[groupKey]*groupBucket)
	users := make(map[userKey]*userBucket)
	result := Triplet{}

	for index, item := range items {
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

		attribution := resolve(index)
		group := groupKey{provider: item.Provider, workScopeID: attribution.WorkScopeID, teamID: attribution.TeamID}
		bucket := groups[group]
		if bucket == nil {
			bucket = &groupBucket{teamName: attribution.TeamName}
			groups[group] = bucket
		}

		assignee := item.Assignee
		userIdentity := "unassigned"
		if assignee != nil && *assignee != "" {
			userIdentity = *assignee
		}
		uKey := userKey{item.Provider, attribution.WorkScopeID, userIdentity, attribution.TeamID}
		uBucket := users[uKey]
		if uBucket == nil {
			uBucket = &userBucket{teamName: attribution.TeamName}
			users[uKey] = uBucket
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
			uBucket.itemsStarted++
			if assignee == nil {
				bucket.itemsStartedUnassigned++
			}
		}
		if completedToday {
			bucket.itemsCompleted++
			uBucket.itemsCompleted++
			if assignee == nil {
				bucket.itemsCompletedUnassigned++
			}
			if item.Type == "bug" {
				bucket.bugCompleted++
			}
			if item.StoryPoints != nil {
				// Plain sequential accumulation, matching Python's own `+=`
				// (compute_work_items.py:1226). CPython's Neumaier-compensated
				// sum() is NOT in play here -- Python never calls sum() on this
				// quantity -- so a compensated Go accumulation would be the
				// divergence, not the fix.
				bucket.storyPointsCompleted += *item.StoryPoints
			}
			leadHours := Hours(completedAt.Sub(createdAt))
			bucket.leadHours = append(bucket.leadHours, leadHours)
			var cycleHours, activeHours, waitHours, efficiency *float64
			if startedAt != nil {
				value := Hours(completedAt.Sub(*startedAt))
				cycleHours = &value
				bucket.cycleHours = append(bucket.cycleHours, value)
				uBucket.cycleHours = append(uBucket.cycleHours, value)
				if value > 0 {
					active, wait := FlowBreakdown(*startedAt, *completedAt, transitionsByItem[item.WorkItemID])
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
			result.CycleTimes = append(result.CycleTimes, CycleTimeRecord{
				WorkItemID: item.WorkItemID, Provider: item.Provider,
				Day: UTCDay(*completedAt), WorkScopeID: attribution.WorkScopeID,
				TeamID: attribution.TeamID, TeamName: attribution.TeamName, Assignee: assignee,
				Type: item.Type, Status: item.Status, CreatedAt: createdAt,
				StartedAt: startedAt, CompletedAt: completedAt, CycleTimeHours: cycleHours,
				LeadTimeHours: floatPointer(leadHours), ActiveTimeHours: activeHours,
				WaitTimeHours: waitHours, FlowEfficiency: efficiency,
			})
		}
		if wipEndOfDay {
			bucket.wipCount++
			uBucket.wipCount++
			if assignee == nil {
				bucket.wipUnassigned++
			}
			bucket.wipAgeHours = append(bucket.wipAgeHours, Hours(end.Sub(*startedAt)))
		}
	}

	groupKeys := make([]groupKey, 0, len(groups))
	for key := range groups {
		groupKeys = append(groupKeys, key)
	}
	sort.Slice(groupKeys, func(i, j int) bool {
		left, right := groupKeys[i], groupKeys[j]
		return left.provider < right.provider || left.provider == right.provider &&
			(left.workScopeID < right.workScopeID || left.workScopeID == right.workScopeID &&
				left.teamID < right.teamID)
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
		congestion := float64(bucket.wipCount) / maxFloat(1, float64(bucket.weeklyThroughput))
		predictability := 0.0
		if bucket.itemsCompleted+bucket.wipCount > 0 {
			predictability = completed / float64(bucket.itemsCompleted+bucket.wipCount)
		}
		result.MetricsDaily = append(result.MetricsDaily, MetricsDailyRow{
			Day: dayUTC, Provider: key.provider, WorkScopeID: key.workScopeID,
			TeamID: key.teamID, TeamName: bucket.teamName,
			ItemsStarted: bucket.itemsStarted, ItemsCompleted: bucket.itemsCompleted,
			ItemsStartedUnassigned:   bucket.itemsStartedUnassigned,
			ItemsCompletedUnassigned: bucket.itemsCompletedUnassigned,
			WIPCountEndOfDay:         bucket.wipCount, WIPUnassignedEndOfDay: bucket.wipUnassigned,
			CycleTimeP50Hours: Percentile(bucket.cycleHours, 50),
			CycleTimeP90Hours: Percentile(bucket.cycleHours, 90),
			LeadTimeP50Hours:  Percentile(bucket.leadHours, 50),
			LeadTimeP90Hours:  Percentile(bucket.leadHours, 90),
			WIPAgeP50Hours:    Percentile(bucket.wipAgeHours, 50),
			WIPAgeP90Hours:    Percentile(bucket.wipAgeHours, 90),
			BugCompletedRatio: bugRatio, StoryPointsCompleted: bucket.storyPointsCompleted,
			NewBugsCount: bucket.newBugs, NewItemsCount: bucket.newItems,
			DefectIntroRate: defectRate, WIPCongestionRatio: congestion,
			PredictabilityScore: predictability,
		})
	}

	userKeys := make([]userKey, 0, len(users))
	for key := range users {
		userKeys = append(userKeys, key)
	}
	sort.Slice(userKeys, func(i, j int) bool {
		left, right := userKeys[i], userKeys[j]
		return left.provider < right.provider || left.provider == right.provider &&
			(left.workScopeID < right.workScopeID || left.workScopeID == right.workScopeID &&
				(left.user < right.user || left.user == right.user && left.teamID < right.teamID))
	})
	for _, key := range userKeys {
		bucket := users[key]
		result.UserMetricsDaily = append(result.UserMetricsDaily, UserMetricsDailyRow{
			Day: dayUTC, Provider: key.provider, WorkScopeID: key.workScopeID,
			UserIdentity: key.user, TeamID: key.teamID, TeamName: bucket.teamName,
			ItemsStarted: bucket.itemsStarted, ItemsCompleted: bucket.itemsCompleted,
			WIPCountEndOfDay:  bucket.wipCount,
			CycleTimeP50Hours: Percentile(bucket.cycleHours, 50),
			CycleTimeP90Hours: Percentile(bucket.cycleHours, 90),
		})
	}
	return result
}

type estimateKey struct {
	provider, workScopeID, teamID string
}

type estimateBucket struct {
	teamName                         string
	estimatedCount, unestimatedCount int
}

// ComputeEstimateCoverage ports compute_estimate_coverage_metrics_daily
// (compute_work_items.py:1425).
func ComputeEstimateCoverage(day time.Time, items []Item, resolve Resolver) []EstimateCoverageRow {
	dayUTC := UTCDay(day)
	end := dayUTC.AddDate(0, 0, 1)

	buckets := make(map[estimateKey]*estimateBucket)
	order := make([]estimateKey, 0, len(items))

	for index, item := range items {
		createdAt := item.CreatedAt.UTC()
		terminalAt := earliestTimePointer(utcTimePointer(item.CompletedAt), utcTimePointer(item.ClosedAt))
		if !createdAt.Before(end) {
			continue
		}
		attribution := resolve(index)
		key := estimateKey{
			provider:    item.Provider,
			workScopeID: attribution.WorkScopeID,
			teamID:      attribution.TeamID,
		}
		bucket := buckets[key]
		if bucket == nil {
			// Python creates the bucket BEFORE the terminal-item skip below, so
			// an item that reached a terminal state before the window end still
			// materialises an all-zero group (backlog_size 0, ratio None). The
			// team_name recorded is the FIRST contributing item's -- a later
			// item resolving a different name does NOT overwrite it.
			bucket = &estimateBucket{teamName: attribution.TeamName}
			buckets[key] = bucket
			order = append(order, key)
		}
		if terminalAt != nil && terminalAt.Before(end) {
			continue
		}
		if item.StoryPoints == nil {
			bucket.unestimatedCount++
			continue
		}
		bucket.estimatedCount++
	}

	// Python sorts by (provider, work_scope_id, str(team_id or "")). team_id is
	// already normalised to a non-empty string by the caller, so the `or ""`
	// arm is unreachable and sorting on the normalised value matches.
	sort.SliceStable(order, func(left, right int) bool {
		a, b := order[left], order[right]
		if a.provider != b.provider {
			return a.provider < b.provider
		}
		if a.workScopeID != b.workScopeID {
			return a.workScopeID < b.workScopeID
		}
		return a.teamID < b.teamID
	})

	result := make([]EstimateCoverageRow, 0, len(order))
	for _, key := range order {
		bucket := buckets[key]
		backlogSize := bucket.estimatedCount + bucket.unestimatedCount
		var ratio *float64
		if backlogSize != 0 {
			value := float64(bucket.estimatedCount) / float64(backlogSize)
			ratio = &value
		}
		result = append(result, EstimateCoverageRow{
			Day: dayUTC, Provider: key.provider, WorkScopeID: key.workScopeID,
			TeamID: key.teamID, TeamName: bucket.teamName,
			EstimatedCount: bucket.estimatedCount, UnestimatedCount: bucket.unestimatedCount,
			BacklogSize: backlogSize, Ratio: ratio,
		})
	}
	return result
}
