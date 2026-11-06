package sankey

import (
	"context"
	"sort"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/linkeditems"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/teamscope"
)

// The flows of a request held to a repository ceiling (CHAOS-13088). work_item_metrics_daily and
// work_item_state_durations_daily hold no item and no repository, so under a ceiling the expense and state flows read
// the rows recomputed from the items linked to the allowed repositories (linkeditems) where they read the tables, and
// work_item_cycle_times is held to those items (linkscope). The investment and hotspot flows are keyed by repository and
// take the ceiling on that column. With no ceiling nothing here runs: the statements are today's.

// linkRepoIDs are the repositories the work-item reads of a restricted request go through: the ceiling's, held to
// the repositories the request names when it names any (a named repository outside the ceiling leaves nothing).
func linkRepoIDs(ctx context.Context, client QueryClient, ceiling teamscope.Ceiling, scopeLevel string, scopeIDs, whatRepos []string, orgID string) ([]string, error) {
	if len(teamscope.NamedRepoRefs(scopeLevel, scopeIDs, whatRepos)) == 0 {
		return ceiling.IDs(), nil
	}
	named, err := resolveRepoFilterIDs(ctx, client, scopeLevel, scopeIDs, whatRepos, orgID)
	if err != nil {
		return nil, err
	}
	allowed := map[string]bool{}
	for _, id := range ceiling.IDs() {
		allowed[id] = true
	}
	var ids []string
	for _, id := range named {
		if allowed[id] {
			ids = append(ids, id)
		}
	}
	return ids, nil
}

func utcDay(t time.Time) time.Time {
	year, month, day := t.Date()
	return time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
}

func linkedLoader(client QueryClient, orgID string, ids []string, startDay, endDay time.Time) linkeditems.Loader {
	return linkeditems.Loader{
		Client: client, OrgID: orgID, RepoIDs: ids,
		WindowStart: utcDay(startDay), WindowEnd: utcDay(endDay), Budget: linkeditems.StateBudget,
	}
}

// rowFilter is the team and work-scope filter of the expense and state flows (teamScopeFilter, workScopeFilter) over
// a recomputed row.
type rowFilter struct {
	teams, scopes map[string]bool
}

func newRowFilter(scopeLevel string, scopeIDs, whatRepos []string) rowFilter {
	var filter rowFilter
	if scopeLevel == "team" && len(scopeIDs) > 0 {
		filter.teams = setOf(scopeIDs)
	}
	var scopes []string
	if scopeLevel == "repo" {
		scopes = append(scopes, scopeIDs...)
	}
	for _, id := range append(scopes, whatRepos...) {
		if id != "" {
			if filter.scopes == nil {
				filter.scopes = map[string]bool{}
			}
			filter.scopes[id] = true
		}
	}
	return filter
}

func setOf(values []string) map[string]bool {
	out := make(map[string]bool, len(values))
	for _, value := range values {
		out[value] = true
	}
	return out
}

// keeps is the SQL filter of the two flows: the team id, an empty one read as unassigned, IN the team ids, and
// work_scope_id IN the work scope ids.
func (f rowFilter) keeps(teamID, workScopeID string) bool {
	if f.teams != nil {
		team := teamID
		if team == "" {
			team = "unassigned"
		}
		if !f.teams[team] {
			return false
		}
	}
	return f.scopes == nil || f.scopes[workScopeID]
}

// inWindow reports a recomputed day inside [startDay, endDay), the window of the table reads.
func inWindow(day, startDay, endDay time.Time) bool {
	return !day.Before(utcDay(startDay)) && day.Before(utcDay(endDay))
}

// linkedExpenseCounts is fetchExpenseCounts over the rows recomputed from the linked items: one row, as the SQL
// aggregate always answers one.
func linkedExpenseCounts(ctx context.Context, client QueryClient, orgID string, ids []string, startDay, endDay time.Time, filter rowFilter) ([]expenseCountsRow, error) {
	row := expenseCountsRow{}
	if len(ids) == 0 {
		return []expenseCountsRow{row}, nil
	}
	loader := linkedLoader(client, orgID, ids, startDay, endDay)
	versions, attributions, err := loader.Read(ctx)
	if err != nil {
		return nil, err
	}
	byDay := linkeditems.RowsByDay(loader.WindowStart, loader.WindowEnd, versions, attributions)
	for _, day := range sortedDays(byDay) {
		if !inWindow(day, startDay, endDay) {
			continue
		}
		for _, metrics := range byDay[day] {
			if !filter.keeps(metrics.TeamID, metrics.WorkScopeID) {
				continue
			}
			row.NewBugs += float64(metrics.NewBugsCount)
			row.BugCompletedEstimate += float64(metrics.ItemsCompleted) * metrics.BugCompletedRatio
		}
	}
	return []expenseCountsRow{row}, nil
}

// linkedStateStatusCounts is fetchStateStatusCounts over the rows recomputed from the linked items: items_touched
// summed by status, the largest first (a tie in status order, so the answer does not depend on map order).
func linkedStateStatusCounts(ctx context.Context, client QueryClient, orgID string, ids []string, startDay, endDay, computedAt time.Time, filter rowFilter) ([]stateStatusCountRow, error) {
	if len(ids) == 0 {
		return []stateStatusCountRow{}, nil
	}
	loader := linkedLoader(client, orgID, ids, startDay, endDay)
	byDay, err := linkeditems.NewStateDaily(loader, computedAt).Rows(ctx)
	if err != nil {
		return nil, err
	}
	totals := map[string]float64{}
	for _, day := range sortedDays(byDay) {
		if !inWindow(day, startDay, endDay) {
			continue
		}
		for _, state := range byDay[day] {
			if state.ItemsTouched == 0 || !filter.keeps(state.TeamID, state.WorkScopeID) {
				continue
			}
			totals[state.Status] += float64(state.ItemsTouched)
		}
	}
	out := make([]stateStatusCountRow, 0, len(totals))
	for status, items := range totals {
		out = append(out, stateStatusCountRow{Status: status, ItemsTouched: items})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].ItemsTouched != out[j].ItemsTouched {
			return out[i].ItemsTouched > out[j].ItemsTouched
		}
		return out[i].Status < out[j].Status
	})
	return out, nil
}

func sortedDays[Row any](byDay map[time.Time][]Row) []time.Time {
	days := make([]time.Time, 0, len(byDay))
	for day := range byDay {
		days = append(days, day)
	}
	sort.Slice(days, func(i, j int) bool { return days[i].Before(days[j]) })
	return days
}
