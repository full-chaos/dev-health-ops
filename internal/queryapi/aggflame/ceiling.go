package aggflame

import (
	"context"
	"sort"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/linkeditems"
)

// linkedCycleBreakdown is fetchCycleBreakdown over the state-duration rows recomputed from the items linked to the
// ceiling's repositories (CHAOS-13088): work_item_state_durations_daily holds no item and no repository, so a request
// held to a ceiling never reads it. The rows are filtered by the request's team, provider and work scope, as the
// statement filters them, and summed by status, the most hours first (a tie in status order).
func linkedCycleBreakdown(ctx context.Context, client QueryClient, orgID string, params Params) ([]cycleBreakdownRow, error) {
	ids := params.Ceiling.IDs()
	if len(ids) == 0 {
		return nil, nil
	}
	start, end := utcDay(params.StartDay), utcDay(params.EndDay)
	byDay, err := linkeditems.NewStateDaily(linkeditems.Loader{
		Client: client, OrgID: orgID, RepoIDs: ids, WindowStart: start, WindowEnd: end, Budget: linkeditems.StateBudget,
	}, params.Now).Rows(ctx)
	if err != nil {
		return nil, err
	}
	totals := map[string]float64{}
	for day, rows := range byDay {
		if day.Before(start) || !day.Before(end) {
			continue
		}
		for _, row := range rows {
			if (params.TeamID != "" && row.TeamID != params.TeamID) || (params.Provider != "" && row.Provider != params.Provider) ||
				(params.WorkScopeID != "" && row.WorkScopeID != params.WorkScopeID) || row.DurationHours == 0 {
				continue
			}
			totals[row.Status] += row.DurationHours
		}
	}
	out := make([]cycleBreakdownRow, 0, len(totals))
	for status, hours := range totals {
		out = append(out, cycleBreakdownRow{Status: status, TotalHours: hours})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].TotalHours != out[j].TotalHours {
			return out[i].TotalHours > out[j].TotalHours
		}
		return out[i].Status < out[j].Status
	})
	return out, nil
}

func utcDay(t time.Time) time.Time {
	year, month, day := t.Date()
	return time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
}
