package explain

import (
	"context"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/linkeditems"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/teamscope"
)

// The explain of a request held to a repository ceiling (CHAOS-13088). A metric stored per team reads
// work_item_metrics_daily or work_item_state_durations_daily, which hold no item and no repository: under a ceiling
// their rows are recomputed from the items linked to the allowed repositories (linkeditems, the daily job's own
// compute) and the value, drivers, contributors and source are taken from those rows with the statements' own rules.
// A metric stored per repository takes the ceiling on its repository column. With no ceiling nothing here runs.

// teamRow is one recomputed row of a metric stored per team: its day, team, provider and the metric column (nil for a
// NULL column).
type teamRow struct {
	day      time.Time
	teamID   string
	provider string
	value    *float64
}

func utcDay(t time.Time) time.Time {
	year, month, day := t.Date()
	return time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
}

// linkRepoIDs are the ceiling's repositories, held to the repositories the request names when it names any.
func (reader *Reader) linkRepoIDs(ctx context.Context, params Params, orgID string) ([]string, error) {
	refs := teamscope.NamedRepoRefs(params.ScopeLevel, params.ScopeIDs, params.WhatRepos)
	if len(refs) == 0 {
		return params.Ceiling.IDs(), nil
	}
	named, err := reader.resolveRepoIDs(ctx, refs, orgID)
	if err != nil {
		return nil, err
	}
	allowed := params.Ceiling.IDs()
	var ids []string
	for _, id := range named {
		if slices.Contains(allowed, id) {
			ids = append(ids, id)
		}
	}
	return ids, nil
}

// linkedTeamRows recomputes the rows of config's table over both windows of the request, held to the request's team
// scope and to config's status, as the statements' scope filter holds them.
func (reader *Reader) linkedTeamRows(ctx context.Context, config metricConfig, params Params, orgID string) ([]teamRow, error) {
	ids, err := reader.linkRepoIDs(ctx, params, orgID)
	if err != nil || len(ids) == 0 {
		return nil, err
	}
	from, to := utcDay(params.StartDay), utcDay(params.EndDay)
	if start := utcDay(params.CompareStart); start.Before(from) {
		from = start
	}
	if end := utcDay(params.CompareEnd); end.After(to) {
		to = end
	}
	loader := linkeditems.Loader{Client: reader.client, OrgID: orgID, RepoIDs: ids, WindowStart: from, WindowEnd: to, Budget: linkeditems.StateBudget}
	var teams map[string]bool
	if config.Scope == "team" && params.ScopeLevel == "team" && len(params.ScopeIDs) > 0 {
		teams = map[string]bool{}
		for _, id := range params.ScopeIDs {
			teams[id] = true
		}
	}
	var out []teamRow
	keep := func(teamID string) bool { return teams == nil || teams[teamID] }
	switch config.Table {
	case "work_item_state_durations_daily":
		byDay, err := linkeditems.NewStateDaily(loader, params.Now).Rows(ctx)
		if err != nil {
			return nil, err
		}
		for day, rows := range byDay {
			for _, row := range rows {
				if keep(row.TeamID) && (config.StatusFilter == "" || row.Status == config.StatusFilter) && config.Column == "duration_hours" {
					hours := row.DurationHours
					out = append(out, teamRow{day, row.TeamID, row.Provider, &hours})
				}
			}
		}
	case "work_item_metrics_daily":
		versions, attributions, err := loader.Read(ctx)
		if err != nil {
			return nil, err
		}
		for day, rows := range linkeditems.RowsByDay(from, to, versions, attributions) {
			for _, row := range rows {
				if !keep(row.TeamID) {
					continue
				}
				var value *float64
				switch config.Column {
				case "cycle_time_p50_hours":
					value = row.CycleTimeP50Hours
				case "items_completed":
					completed := float64(row.ItemsCompleted)
					value = &completed
				case "wip_congestion_ratio":
					ratio := row.WIPCongestionRatio
					value = &ratio
				}
				out = append(out, teamRow{day, row.TeamID, row.Provider, value})
			}
		}
	}
	return out, nil
}

// aggregate is the statements' aggregate over the non-NULL values (avg or sum): nil when none is stored.
func aggregate(aggregator string, values []*float64) *float64 {
	total, count := 0.0, 0
	for _, value := range values {
		if value != nil {
			total += *value
			count++
		}
	}
	if count == 0 {
		return nil
	}
	if aggregator == "avg" {
		total /= float64(count)
	}
	return &total
}

func inWindow(day, start, end time.Time) bool {
	return !day.Before(utcDay(start)) && day.Before(utcDay(end))
}

// windowValue is fetchMetricValue over the rows: the aggregate of the window and whether it holds a stored value.
func windowValue(rows []teamRow, aggregator string, start, end time.Time) (float64, bool) {
	var values []*float64
	for _, row := range rows {
		if inWindow(row.day, start, end) {
			values = append(values, row.value)
		}
	}
	value := aggregate(aggregator, values)
	return floatOrZero(value), value != nil
}

// byTeam groups the window's values by team: a team with a row and only NULL values is present with a nil value.
func byTeam(rows []teamRow, aggregator string, start, end time.Time) map[string]*float64 {
	values := map[string][]*float64{}
	for _, row := range rows {
		if inWindow(row.day, start, end) {
			values[row.teamID] = append(values[row.teamID], row.value)
		}
	}
	out := make(map[string]*float64, len(values))
	for team, teamValues := range values {
		out[team] = aggregate(aggregator, teamValues)
	}
	return out
}

// driverPercent is deltarule.DriverPercentSQL in Go.
func driverPercent(current *float64, previous *float64, previousPresent bool) *float64 {
	if current == nil || !previousPresent || previous == nil {
		return nil
	}
	switch {
	case *previous == 0 && *current == 0:
		zero := 0.0
		return &zero
	case *previous == 0:
		return nil
	}
	pct := (*current - *previous) / *previous * 100
	return &pct
}

// linkedDrivers is fetchMetricDriverDelta over the rows: the teams stored in either window, the largest delta first
// (no delta last), three of them.
func linkedDrivers(rows []teamRow, aggregator string, params Params) []metricRow {
	current := byTeam(rows, aggregator, params.StartDay, params.EndDay)
	previous := byTeam(rows, aggregator, params.CompareStart, params.CompareEnd)
	teams := map[string]bool{}
	for team := range current {
		teams[team] = true
	}
	for team := range previous {
		teams[team] = true
	}
	out := make([]metricRow, 0, len(teams))
	for team := range teams {
		currentValue, currentPresent := current[team]
		previousValue, previousPresent := previous[team]
		currentStored, previousStored := currentPresent && currentValue != nil, previousPresent && previousValue != nil
		if !currentStored && !previousStored {
			continue
		}
		row := metricRow{ID: team, Value: floatOrZero(currentValue), HasData: currentStored, HasPriorData: previousStored}
		if delta := driverPercent(currentValue, previousValue, previousPresent); row.HasData && row.HasPriorData && delta != nil {
			safe := safeFloat(*delta)
			row.DeltaPct = &safe
		}
		out = append(out, row)
	}
	sort.Slice(out, func(i, j int) bool {
		a, b := out[i].DeltaPct, out[j].DeltaPct
		switch {
		case a != nil && b != nil && *a != *b:
			return *a > *b
		case (a == nil) != (b == nil):
			return a != nil
		}
		return out[i].ID < out[j].ID
	})
	if len(out) > 3 {
		out = out[:3]
	}
	return out
}

// linkedContributors is fetchMetricContributors over the rows: the teams with a stored value in the current window,
// the largest first, six of them.
func linkedContributors(rows []teamRow, aggregator string, params Params) []metricRow {
	out := []metricRow{}
	for team, value := range byTeam(rows, aggregator, params.StartDay, params.EndDay) {
		if value != nil {
			out = append(out, metricRow{ID: team, Value: *value, HasData: true})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Value != out[j].Value {
			return out[i].Value > out[j].Value
		}
		return out[i].ID < out[j].ID
	})
	if len(out) > 6 {
		out = out[:6]
	}
	return out
}

// linkedProviders is fetchWorkItemProviders over the rows of the current window.
func linkedProviders(rows []teamRow, params Params) []string {
	var out []string
	for _, row := range rows {
		provider := strings.TrimSpace(row.provider)
		if inWindow(row.day, params.StartDay, params.EndDay) && provider != "" && !strings.EqualFold(provider, "unknown") {
			out = append(out, provider)
		}
	}
	return out
}
