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

// statesDriverMove says whether a delta is a stated move with a driver to
// name: a percent between two measured values that is not exactly 0 %.
func statesDriverMove(d MetricDelta) bool {
	_, ok := d.Percent()
	return ok && d.HasData && namesDrivers(d)
}

// attachDriverCaptions sets Signal.Driver on the metric signals whose delta
// states a move, from ONE batched driver read per request. A failed driver
// read fails the request (like the summary sentence's); a failed name read
// leaves the caption out (logged). Risk and recommendation signals carry none.
func attachDriverCaptions(ctx context.Context, client QueryClient, f Filters, orgID string, now, startDay, endDay, compareStart, compareEnd time.Time, deltas []MetricDelta, signals []Signal) error {
	var batch []driverBatchMetric
	labels := map[string]string{}
	var scopeFilters = map[string]string{}
	var bindings []dhclickhouse.Binding
	for _, d := range deltas {
		if !statesDriverMove(d) {
			continue
		}
		scope := metricScope(d.Metric)
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
		batch = append(batch, driverBatchMetric{Metric: d.Metric, ScopeFilter: filter})
		labels[d.Metric] = d.Label
	}
	if len(batch) == 0 {
		return nil
	}
	rowsByMetric, err := fetchMetricDriversBatch(ctx, client, batch, startDay, endDay, compareStart, compareEnd, bindings, orgID, driverCaptionLimit)
	if err != nil {
		return err
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
			if !ok || seen[name] {
				continue
			}
			seen[name] = true
			out[metric] = append(out[metric], name)
		}
	}
	return out
}
