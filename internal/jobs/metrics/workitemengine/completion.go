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

type linkedPRKey struct {
	repoID uuid.UUID
	number uint32
}

type completionKey struct {
	repoID nullableUUIDKey
	teamID string
}

// ComputeWorkItemCompletionDaily counts the items completed in [dayUTC, end) by (repository, team): the items
// active on the day that completed in it, their story-point delivery units, and the unassigned-team rule of the
// rows it replaced (investment_metrics_daily), with no rule classification. Summed over the areas of those rows,
// its counts equal theirs; the cycle p50 is the p50 of all the day's cycle hours of the key, not a p50 per area.
//
// dayUTC is the UTC midnight of the day and end the next midnight.
func ComputeWorkItemCompletionDaily(
	items []Item,
	dayUTC, end time.Time,
	resolveTeam TeamResolver,
) ([]WorkItemCompletionDailyRow, error) {
	buckets := make(map[completionKey]*completionBucket)
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
			bucket = &completionBucket{}
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
		for _, pr := range item.LinkedPRs {
			if bucket.prs == nil {
				bucket.prs = make(map[linkedPRKey]struct{})
			}
			prKey := linkedPRKey{repoID: pr.RepoID, number: pr.Number}
			if _, counted := bucket.prs[prKey]; counted {
				continue
			}
			bucket.prs[prKey] = struct{}{}
			// Each part counts on its own; a pull request with neither adds nothing.
			if pr.Additions != nil {
				bucket.churn += int(*pr.Additions)
			}
			if pr.Deletions != nil {
				bucket.churn += int(*pr.Deletions)
			}
		}
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

// ChurnStats describes the inputs of a day's churn_loc: it explains a churn of 0 on a day with completed items, which
// means no linked pull request (or none with a size), not that no lines changed.
type ChurnStats struct {
	CompletedItems       int
	ItemsWithoutLinkedPR int
	LinksByTier          map[string]int
	LinkedPRsWithoutSize int
}

// ChurnInputs counts, for the items completed in [dayUTC, end), the links by provenance tier and the linked pull
// requests with no stored size. The caller logs it; this package holds no logger.
func ChurnInputs(items []Item, dayUTC, end time.Time) ChurnStats {
	stats := ChurnStats{LinksByTier: map[string]int{}}
	for _, item := range items {
		if item.CompletedAt == nil {
			continue
		}
		done := item.CompletedAt.UTC()
		if done.Before(dayUTC) || !done.Before(end) {
			continue
		}
		stats.CompletedItems++
		if len(item.LinkedPRs) == 0 {
			stats.ItemsWithoutLinkedPR++
		}
		for _, pr := range item.LinkedPRs {
			stats.LinksByTier[pr.Provenance]++
			if pr.Additions == nil && pr.Deletions == nil {
				stats.LinkedPRsWithoutSize++
			}
		}
	}
	return stats
}
