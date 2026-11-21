package home

import (
	"context"
	"log"
	"strings"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
)

// driverCaptionLimit is the number of drivers a caption names.
const driverCaptionLimit = 3

// driverCaption is the one-line caption under a metric signal (CHAOS-8573):
// where the move appears largest. It is built from the metric label (never
// the hedged Title) and the display names of the drivers, with ONE hedge word.
// nil when no driver has a name: there is no placeholder.
func driverCaption(label string, names []string) *string {
	if label == "" || len(names) == 0 {
		return nil
	}
	var list string
	switch len(names) {
	case 1:
		list = names[0]
	default:
		list = strings.Join(names[:len(names)-1], ", ") + " and " + names[len(names)-1]
	}
	s := label + " appears to have moved most in " + list + "."
	return &s
}

// statesMove says whether the value of a metric states a move with a driver to name (CHAOS-14379, CHAOS-14441): a
// percent between two measured values that is not exactly 0 %, or a rise from a measured 0, which is stated in absolute
// values and has no percent. Not a steady value, not a value missing in a window, and not a work-item metric under a
// repository filter: its value is the repo-linked calculation, while the team daily rows that name drivers have no
// repository dimension (CHAOS-14425). The summary sentence and the signal captions take this one predicate, in every
// form.
func statesMove(d MetricDelta) bool {
	if d.RepoFilterApplied != nil && *d.RepoFilterApplied && metricScope(d.Metric) == "team" {
		return false
	}
	if d.FromZero() {
		return true
	}
	_, ok := d.Percent()
	return ok && d.HasData && namesDrivers(d)
}

// oneTeamDriverRows is the driver of a metric of a one-team request (CHAOS-14379): the team itself when the metric's
// value states a move (statesMove), so the driver has the value's own source, predicates and reducer; none
// otherwise.
func oneTeamDriverRows(d MetricDelta, f Filters) []driverRow {
	if !statesMove(d) {
		return nil
	}
	return []driverRow{{ID: f.Scope.IDs[0]}}
}

// attachDriverCaptions sets Signal.Driver on the metric signals whose delta
// states a move, from ONE batched driver read per request. A failed driver
// read fails the request (like the summary sentence's); a failed name read
// leaves the caption out (logged). Risk and recommendation signals carry none.
func attachDriverCaptions(ctx context.Context, client QueryClient, f Filters, orgID string, now, startDay, endDay, compareStart, compareEnd time.Time, deltas []MetricDelta, signals []Signal) error {
	var batch []driverBatchMetric
	oneTeamRows := map[string][]driverRow{}
	labels := map[string]string{}
	var scopeFilters = map[string]string{}
	var bindings []dhclickhouse.Binding
	for _, d := range deltas {
		scope := metricScope(d.Metric)
		if oneTeamScope(scope, f) {
			if rows := oneTeamDriverRows(d, f); rows != nil {
				oneTeamRows[d.Metric] = rows
				labels[d.Metric] = d.Label
			}
			continue
		}
		if !statesMove(d) {
			continue
		}
		filter, ok := scopeFilters[scope]
		if !ok {
			var b []dhclickhouse.Binding
			var err error
			filter, b, err = scopeFilterForMetric(ctx, client, scope, f, orgID, "team_id", "repo_id", now)
			if err != nil {
				return err
			}
			if scope == "repo" {
				// The repository variant may bind the same names as the team
				// variant (scope_ids) with other values: its names get a prefix.
				for _, binding := range b {
					filter = strings.ReplaceAll(filter, "{"+binding.Name+":", "{r_"+binding.Name+":")
					binding.Name = "r_" + binding.Name
					bindings = append(bindings, binding)
				}
			} else {
				bindings = append(bindings, b...)
			}
			scopeFilters[scope] = filter
		}
		batch = append(batch, driverBatchMetric{Metric: d.Metric, ScopeFilter: filter, FromZero: d.FromZero()})
		labels[d.Metric] = d.Label
	}
	if len(batch) == 0 && len(oneTeamRows) == 0 {
		return nil
	}
	rowsByMetric := map[string][]driverRow{}
	if len(batch) > 0 {
		var err error
		if rowsByMetric, err = fetchMetricDriversBatch(ctx, client, batch, startDay, endDay, compareStart, compareEnd, bindings, orgID, driverCaptionLimit); err != nil {
			return err
		}
	}
	for metric, rows := range oneTeamRows {
		rowsByMetric[metric] = rows
	}
	names := driverNamesByMetric(ctx, client, orgID, rowsByMetric)
	for i := range signals {
		if signals[i].Metric == "" || !strings.HasPrefix(signals[i].ID, "metric:") {
			continue
		}
		signals[i].Driver = driverCaption(labels[signals[i].Metric], names[signals[i].Metric])
	}
	return nil
}

// driverNamesByMetric is driverNames for the batch: the ids of every metric go
// through the shared name lookup once per kind (team, repository), with the
// same rules (an id with no name is left out; a team that is not active is not
// a driver; a failed read leaves the names out), then each metric keeps its
// own order and lists a name once.
func driverNamesByMetric(ctx context.Context, client QueryClient, orgID string, rowsByMetric map[string][]driverRow) map[string][]string {
	idsByKind := map[string][]string{}
	for metric, rows := range rowsByMetric {
		kind := "team"
		if metricGroup(metric) == "repo_id" {
			kind = "repo"
		}
		for _, row := range rows {
			idsByKind[kind] = append(idsByKind[kind], row.ID)
		}
	}
	nameByKind := map[string]map[string]string{}
	for kind, ids := range idsByKind {
		ids = uniqueIDs(ids)
		names := resolveScopeNames(ctx, client, orgID, kind, ids, "home signal drivers")
		if kind == "team" {
			active, err := activeTeamIDs(ctx, client, orgID, ids)
			if err != nil {
				log.Printf("home signal drivers: could not read the active teams: %v", err)
				continue
			}
			for id := range names {
				if !active[id] {
					delete(names, id)
				}
			}
		}
		nameByKind[kind] = names
	}
	out := map[string][]string{}
	for metric, rows := range rowsByMetric {
		kind := "team"
		if metricGroup(metric) == "repo_id" {
			kind = "repo"
		}
		seen := map[string]bool{}
		for _, row := range rows {
			name, ok := nameByKind[kind][row.ID]
			name = strings.Join(strings.Fields(name), " ")
			if !ok || name == "" || seen[name] {
				continue
			}
			seen[name] = true
			out[metric] = append(out[metric], name)
		}
	}
	return out
}
