package people

import (
	"context"
	"sort"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/workitemmetrics"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/linkeditems"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/linkscope"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/teamscope"
)

// readScope is what a request reads under (CHAOS-13087): the repository ceiling of its decision, and the per-person
// daily rows recomputed from the items linked to the ceiling's repositories. The zero value reads every repository and
// every row: today's statements, byte for byte.
type readScope struct {
	ceiling teamscope.Ceiling
	user    *linkeditems.UserDaily
}

func (s readScope) restricted() bool { return s.ceiling.Restricted() }

// newReadScope is the scope of a request whose windows span [from, to): the user-grain recompute reads the linked
// items of that span once.
func newReadScope(client QueryClient, orgID string, ceiling teamscope.Ceiling, from, to time.Time) readScope {
	scope := readScope{ceiling: ceiling}
	if ceiling.Restricted() {
		scope.user = linkeditems.NewUserDaily(linkeditems.Loader{
			Client: client, OrgID: orgID, RepoIDs: ceiling.IDs(),
			WindowStart: utcDay(from), WindowEnd: utcDay(to), Budget: linkeditems.UserBudget,
		})
	}
	return scope
}

func utcDay(t time.Time) time.Time {
	t = t.UTC()
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
}

// itemTerm holds a read of work_item_cycle_times to the items linked to the ceiling's repositories.
func (s readScope) itemTerm() (string, []dhclickhouse.Binding) {
	return linkscope.ItemCondition(s.ceiling, "work_item_id")
}

// repoTerm holds a read of a table keyed by repository (column) to the ceiling.
func (s readScope) repoTerm(column string) (string, []dhclickhouse.Binding) {
	return s.ceiling.RepoTerm(column)
}

// termFor is the condition a read of table takes under the ceiling. work_item_user_metrics_daily has no item and no
// repository: a restricted read never reaches it (userRowsOf recomputes it).
func (s readScope) termFor(table string) (string, []dhclickhouse.Binding) {
	switch table {
	case "work_item_cycle_times":
		return s.itemTerm()
	case "user_metrics_daily":
		return s.repoTerm("toString(user_metrics_daily.repo_id)")
	}
	return "", nil
}

// recomputed reports a table that a restricted read replaces with the recompute.
func (s readScope) recomputed(table string) bool {
	return s.restricted() && table == "work_item_user_metrics_daily"
}

// userRow is one recomputed row of one of the person's identities.
func (s readScope) userRows(ctx context.Context, identities []string, startDay, endDay time.Time) ([]workitemmetrics.UserMetricsDailyRow, error) {
	byDay, err := s.user.Rows(ctx)
	if err != nil {
		return nil, err
	}
	wanted := make(map[string]bool, len(identities))
	for _, identity := range identities {
		wanted[identity] = true
	}
	start, end := utcDay(startDay), utcDay(endDay)
	var days []time.Time
	for day := range byDay {
		if !day.Before(start) && day.Before(end) {
			days = append(days, day)
		}
	}
	sort.Slice(days, func(i, j int) bool { return days[i].Before(days[j]) })
	var out []workitemmetrics.UserMetricsDailyRow
	for _, day := range days {
		for _, row := range byDay[day] {
			if wanted[row.UserIdentity] {
				out = append(out, row)
			}
		}
	}
	return out, nil
}

// userColumn is the value of a work_item_user_metrics_daily column of a recomputed row, and whether the row holds one
// (a NULL column holds none).
func userColumn(row workitemmetrics.UserMetricsDailyRow, column string) (float64, bool) {
	switch column {
	case "cycle_time_p50_hours":
		if row.CycleTimeP50Hours == nil {
			return 0, false
		}
		return *row.CycleTimeP50Hours, true
	case "items_completed":
		return float64(row.ItemsCompleted), true
	case "items_started":
		return float64(row.ItemsStarted), true
	case "wip_count_end_of_day":
		return float64(row.WIPCountEndOfDay), true
	}
	return 0, false
}

func aggregate(aggregator string, values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	total := 0.0
	for _, v := range values {
		total += v
	}
	if aggregator == "avg" {
		return total / float64(len(values))
	}
	return total
}

// recomputedValue is fetchPersonMetricValue over the recompute: the aggregate of the column over the person's rows of
// the window, and whether one row holds a value.
func (s readScope) recomputedValue(ctx context.Context, column, aggregator string, identities []string, startDay, endDay time.Time) (float64, bool, error) {
	rows, err := s.userRows(ctx, identities, startDay, endDay)
	if err != nil {
		return 0, false, err
	}
	var values []float64
	for _, row := range rows {
		if v, known := userColumn(row, column); known {
			values = append(values, v)
		}
	}
	return aggregate(aggregator, values), len(values) > 0, nil
}

// recomputedSeries is fetchPersonMetricSeries over the recompute: the aggregate of each day that has a value.
func (s readScope) recomputedSeries(ctx context.Context, column, aggregator string, identities []string, startDay, endDay time.Time) ([]personMetricTimeseriesRow, error) {
	rows, err := s.userRows(ctx, identities, startDay, endDay)
	if err != nil {
		return nil, err
	}
	perDay := map[time.Time][]float64{}
	var days []time.Time
	for _, row := range rows {
		v, known := userColumn(row, column)
		if !known {
			continue
		}
		if _, seen := perDay[row.Day]; !seen {
			days = append(days, row.Day)
		}
		perDay[row.Day] = append(perDay[row.Day], v)
	}
	sort.Slice(days, func(i, j int) bool { return days[i].Before(days[j]) })
	out := make([]personMetricTimeseriesRow, 0, len(days))
	for _, day := range days {
		out = append(out, personMetricTimeseriesRow{Day: day, Value: aggregate(aggregator, perDay[day])})
	}
	return out, nil
}
