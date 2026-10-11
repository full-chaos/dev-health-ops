package workitemengine

import (
	"math"
	"sort"
	"time"

	"github.com/google/uuid"
)

// WorkItemCompletionDailyRow is one work_item_completion_daily row without the caller's stamp: what was completed
// on one day by one repository and team (CHAOS-9264). TeamID is the empty string for the unassigned team. There is
// no investment area or project stream: the rule classifier that split these rows is gone, and the cycle p50 is
// one value for the whole (repository, team, day).
type WorkItemCompletionDailyRow struct {
	RepoID             *uuid.UUID
	TeamID             string
	DeliveryUnits      int
	WorkItemsCompleted int
	PRsMerged          int
	ChurnLOC           int
	CycleP50Hours      float64
}

type completionKey struct {
	repoID nullableUUIDKey
	teamID string
}

// ComputeWorkItemCompletionDaily counts the items completed in [dayUTC, end) by (repository, team): the same
// items, the same story-point delivery units and the same unassigned rule as ComputeInvestmentDaily, with the rule
// classification left out. Summed over the areas, its counts equal the counts of the rows it replaces; the cycle
// p50 is the p50 of all the day's cycle hours of the key, not a p50 per area.
//
// dayUTC is the UTC midnight of the day and end the next midnight.
func ComputeWorkItemCompletionDaily(
	items []Item,
	dayUTC, end time.Time,
	resolveTeam TeamResolver,
) ([]WorkItemCompletionDailyRow, error) {
	buckets := make(map[completionKey]*investmentMetricBucket)
	order := make([]completionKey, 0, len(items))
	for index, item := range items {
		created := item.CreatedAt.UTC()
		if !created.Before(end) ||
			(item.CompletedAt != nil && item.CompletedAt.UTC().Before(dayUTC)) {
			continue
		}
		if item.CompletedAt == nil {
			continue
		}
		completed := item.CompletedAt.UTC()
		if completed.Before(dayUTC) || !completed.Before(end) {
			continue
		}
		team := normalizeTeamID(resolveTeam(index))
		if team == unassignedTeamID {
			team = ""
		}
		key := completionKey{repoID: newNullableUUIDKey(item.RepoID), teamID: team}
		bucket := buckets[key]
		if bucket == nil {
			bucket = &investmentMetricBucket{}
			buckets[key] = bucket
			order = append(order, key)
		}
		bucket.completed++
		points := 1.0
		if item.StoryPoints != nil && *item.StoryPoints != 0 {
			points = *item.StoryPoints
		}
		if math.IsNaN(points) || math.IsInf(points, 0) ||
			points >= float64(math.MaxInt64) || points < float64(math.MinInt64) {
			return nil, ErrUnrepresentableDeliveryUnits
		}
		bucket.deliveryUnits += int(math.Trunc(points))
		if item.StartedAt != nil {
			cycle := completed.Sub(item.StartedAt.UTC()).Hours()
			if cycle >= 0 {
				bucket.cycleHours = append(bucket.cycleHours, cycle)
			}
		}
	}

	rows := make([]WorkItemCompletionDailyRow, 0, len(order))
	for _, key := range order {
		bucket := buckets[key]
		sort.Float64s(bucket.cycleHours)
		var p50 float64
		if len(bucket.cycleHours) > 0 {
			p50 = bucket.cycleHours[len(bucket.cycleHours)/2]
		}
		rows = append(rows, WorkItemCompletionDailyRow{
			RepoID:             key.repoID.pointer(),
			TeamID:             key.teamID,
			DeliveryUnits:      bucket.deliveryUnits,
			WorkItemsCompleted: bucket.completed,
			ChurnLOC:           bucket.churn,
			CycleP50Hours:      p50,
		})
	}
	return rows, nil
}
