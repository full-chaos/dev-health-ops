package quadrant

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/workitemmetrics"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/linkeditems"
)

const (
	linkedMaxExecutionSeconds = 30
	linkedMaxResultRows       = 200_000
	linkedRepoBinding         = "link_repo_ids"
)

// linkedRepoScope is the work of the named repositories at the team grain: the
// items linked to the repositories' pull requests (work_graph_issue_pr), computed
// into the daily rows by the work_item family's own compute (linkeditems), so a
// repository's number is the daily table's number over that repository's items.
// A named repository is never matched by an item's own repository column or an
// issue-key prefix.
type linkedRepoScope struct {
	repoIDs []string

	once sync.Once
	rows map[time.Time][]workitemmetrics.MetricsDailyRow
	err  error
}

// linkedItemCondition is the condition on the work item id of a read of per-item
// rows (alias wct), and its binding.
func (scope *linkedRepoScope) linkedItemCondition(itemColumn string) extraScope {
	return extraScope{
		SQL:      "\n              AND " + itemColumn + " IN " + linkeditems.Subquery,
		Bindings: []dhclickhouse.Binding{{Name: linkedRepoBinding, Value: scope.repoIDs}},
	}
}

// ownerRepoCondition narrows a user_metrics_daily read (alias m) to the repositories.
func (scope *linkedRepoScope) ownerRepoCondition() extraScope {
	return extraScope{
		SQL:      "\n          AND toString(m.repo_id) IN {scope_repo_ids:Array(String)}",
		Bindings: []dhclickhouse.Binding{{Name: "scope_repo_ids", Value: scope.repoIDs}},
	}
}

func joinScopes(first, second extraScope) extraScope {
	return extraScope{SQL: first.SQL + second.SQL, Bindings: append(append([]dhclickhouse.Binding(nil), first.Bindings...), second.Bindings...)}
}

func (scope *linkedRepoScope) dailyRows(ctx context.Context, client QueryClient, orgID string, startDay, endDay time.Time) (map[time.Time][]workitemmetrics.MetricsDailyRow, error) {
	scope.once.Do(func() {
		loader := linkeditems.Loader{
			Client: client, OrgID: orgID, RepoIDs: scope.repoIDs, WindowStart: startDay, WindowEnd: endDay,
			Budget: fmt.Sprintf("\nSETTINGS max_execution_time = %d, max_result_rows = %d", linkedMaxExecutionSeconds, linkedMaxResultRows),
		}
		versions, attributions, err := loader.Read(ctx)
		if err != nil {
			scope.err = err
			return
		}
		scope.rows = linkeditems.RowsByDay(startDay, endDay, versions, attributions)
	})
	return scope.rows, scope.err
}

// fetchLinkedTeamMetric reads one work-item team metric (throughput, wip) of the
// named repositories' items, aggregated as fetchQuadrantMetricIn aggregates the
// daily table: throughput the sum of items_completed, wip the mean of
// wip_count_end_of_day over the (day, provider, scope, team) rows of a bucket, a
// team without an id skipped, the selected teams kept.
func fetchLinkedTeamMetric(ctx context.Context, client QueryClient, scope *linkedRepoScope, metric string, startDay, endDay time.Time, bucket, orgID string, teams []string) ([]metricRow, error) {
	rowsByDay, err := scope.dailyRows(ctx, client, orgID, startDay, endDay)
	if err != nil {
		return nil, fmt.Errorf("quadrant: linked work items: %w", err)
	}
	selected := map[string]bool{}
	for _, id := range teams {
		selected[id] = true
	}
	type key struct {
		bucket time.Time
		team   string
	}
	type sum struct {
		label string
		total float64
		count int
	}
	sums := map[key]*sum{}
	for day, rows := range rowsByDay {
		if day.Before(startDay) || !day.Before(endDay) {
			continue
		}
		for _, row := range rows {
			if row.TeamID == "" || (len(selected) > 0 && !selected[row.TeamID]) || !measurementRow(row) {
				continue
			}
			k := key{bucketStart(bucket, day, startDay), row.TeamID}
			entry := sums[k]
			if entry == nil {
				entry = &sum{}
				sums[k] = entry
			}
			label := row.TeamName
			if label == "" {
				label = row.TeamID
			}
			if label > entry.label {
				entry.label = label
			}
			switch metric {
			case "throughput":
				entry.total += float64(row.ItemsCompleted)
			case "wip":
				entry.total += float64(row.WIPCountEndOfDay)
			default:
				return nil, fmt.Errorf("quadrant: unsupported linked team metric: %s", metric)
			}
			entry.count++
		}
	}
	out := make([]metricRow, 0, len(sums))
	for k, entry := range sums {
		value := entry.total
		if metric == "wip" {
			value = entry.total / float64(entry.count)
		}
		out = append(out, metricRow{Bucket: k.bucket, EntityID: k.team, EntityLabel: entry.label, Value: value})
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].Bucket.Equal(out[j].Bucket) {
			return out[i].Bucket.Before(out[j].Bucket)
		}
		return out[i].EntityID < out[j].EntityID
	})
	return out, nil
}

// bucketStart is bucketExpr in Go: the window's first day, the Sunday-started week
// (ClickHouse toStartOfWeek) or the first of the month of a day.
func bucketStart(bucket string, day, startDay time.Time) time.Time {
	switch bucket {
	case bucketWindow:
		return startDay
	case "month":
		return time.Date(day.Year(), day.Month(), 1, 0, 0, 0, 0, time.UTC)
	}
	return day.AddDate(0, 0, -int(day.Weekday()))
}

// measurementRow is the daily table's live-row rule (teamkeytables.WorkItemMetricsDaily
// LiveRow): a row with every measure 0 and every nullable measure NULL is a
// retraction, not a sample of a mean.
func measurementRow(row workitemmetrics.MetricsDailyRow) bool {
	return row.ItemsStarted != 0 || row.ItemsCompleted != 0 ||
		row.ItemsStartedUnassigned != 0 || row.ItemsCompletedUnassigned != 0 ||
		row.WIPCountEndOfDay != 0 || row.WIPUnassignedEndOfDay != 0 ||
		row.BugCompletedRatio != 0 || row.StoryPointsCompleted != 0 ||
		row.NewBugsCount != 0 || row.NewItemsCount != 0 ||
		row.DefectIntroRate != 0 || row.WIPCongestionRatio != 0 || row.PredictabilityScore != 0 ||
		row.CycleTimeP50Hours != nil || row.CycleTimeP90Hours != nil ||
		row.LeadTimeP50Hours != nil || row.LeadTimeP90Hours != nil ||
		row.WIPAgeP50Hours != nil || row.WIPAgeP90Hours != nil
}
