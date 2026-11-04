package chartgates_test

import (
	"encoding/json"
	"fmt"
	"os"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// The auth service SLO dashboard (deploy/signoz/dashboards/auth-slo.json, CHAOS-11221). The dashboard
// carries panels only: the alert rules belong to the SLO table change, and a signal with no set number
// has no alert. The guard pins what the dashboard may read: metrics that the service really emits, only
// the bounded labels the allowlist (labels.tsv) lists for that metric, no identity label, no shadow
// series (the shadow dashboard owns them), and every panel placed once in the layout.
// Every field of the file is bound (authSLODashboardIsTheInventory): the panel set, each panel's fields, each
// query's kind, signal, metric, aggregations, filter clauses (a set: order free, no clause or route added or
// removed), group-by labels (a set), legend and limits; an unknown or missing field refuses. A panel that draws an
// alert line takes its query, unit, threshold value and threshold label from the alert rule (sloRules).
// The inventory (panel ids, titles, layout places, query names, legends) is pinned to exact values by design
// (static-text class): a dashboard change must change the inventory in the same commit, so the guard fails closed.

const (
	authSLODashboardRel = "deploy/signoz/dashboards/auth-slo.json"
	authSLOLabelsRel    = "internal/platform/otlpmetrics/labels.tsv"
	authSLOLineColor    = "#e5484d"
	authSLOEnvF         = "deployment.environment = '" + sloEnv + "'"
	authSLOSvcF         = authSLOEnvF + " AND service.name = '" + sloService + "'"
	authSLOHTTPF        = authSLOSvcF + " AND listener = 'auth-api-http'"
	authSLORequests     = "dev_health_api_http_requests_total"
)

var (
	// resource keys any panel may filter on; every other key must be a label of that query's metric.
	authSLOResourceKeys = map[string]bool{"deployment.environment": true, "service.name": true}
	// identity and unbounded labels: banned even where labels.tsv lists them for the metric (subject and
	// audience are listed for auth.workload.exchanges).
	authSLOBannedKeys = append([]string{"subject", "audience", "organization", "org_id", "user", "user_id", "credential", "token", "decision_id", "instance_id", "args"}, prohibitedLabels...)
)

type authSLOWantQuery struct {
	metric, timeAgg, spaceAgg, filter, legend string
	groupBy                                   []string
}

// authSLOWantPanel is one panel of the inventory, in layout order. An alert panel lists no query: its one query
// comes from the rule (authSLOAlertQuery).
type authSLOWantPanel struct {
	id, title, alert string
	queries          []authSLOWantQuery
}

var authSLOPanels = []authSLOWantPanel{
	{"request-rate", "Requests by status class", "", []authSLOWantQuery{{authSLORequests, "rate", "sum", authSLOHTTPF, "{{status_class}}", []string{"status_class"}}}},
	{"error-5xx", "5xx requests", "", []authSLOWantQuery{{authSLORequests, "increase", "sum", authSLOHTTPF + " AND status_class = '5xx'", "", nil}}},
	{"authorize-p95", "Authorize p95", "AuthzLatencyP95High", nil},
	{"authorize-p99", "Authorize p99", "AuthzLatencyP99High", nil},
	{"token-p95", "Token issue and refresh p95", "AuthTokenLatencyP95High", nil},
	{"jwks", "JWKS rate and p95", "", []authSLOWantQuery{
		{authSLORequests, "rate", "sum", authSLOHTTPF + " AND route IN ('/.well-known/jwks.json')", "rate", nil},
		{sloDuration, "rate", "p95", authSLOHTTPF + " AND route IN ('/.well-known/jwks.json')", "p95", nil},
	}},
	{"exchange-latency", "Exchange rate and p95", "", []authSLOWantQuery{
		{authSLORequests, "rate", "sum", authSLOHTTPF + " AND route IN ('/internal/v1/token/exchange')", "rate", nil},
		{sloDuration, "rate", "p95", authSLOHTTPF + " AND route IN ('/internal/v1/token/exchange')", "p95", nil},
	}},
	{"exchange-outcomes", "Exchanges by outcome and reason", "", []authSLOWantQuery{{"auth.workload.exchanges", "rate", "sum", authSLOSvcF, "{{outcome}}{{reason}}", []string{"outcome", "reason"}}}},
	{"refresh-exchanges", "Refresh exchanges by reason", "", []authSLOWantQuery{{"auth.session.refresh_exchanges", "rate", "sum", authSLOSvcF, "{{reason_code}}", []string{"reason_code"}}}},
	{"token-refused", "Access tokens refused by reason", "", []authSLOWantQuery{{"devhealth_auth_access_token_refused_total", "rate", "sum", authSLOEnvF, "{{reason}}{{profile}}", []string{"reason", "profile"}}}},
	{"decision-cache", "Decision cache lookups by result", "", []authSLOWantQuery{{"devhealth_auth_decision_cache_lookups_total", "rate", "sum", authSLOSvcF, "{{result}}", []string{"result"}}}},
	{"outbox-age", "Outbox oldest pending age", "AuthOutboxOldestPendingHigh", nil},
	{"outbox-delivery", "Outbox delivered and failures", "", []authSLOWantQuery{
		{"devhealth_auth_outbox_delivered_total", "rate", "sum", authSLOSvcF, "delivered", nil},
		{"devhealth_auth_outbox_delivery_failures_total", "increase", "sum", authSLOSvcF, "{{reason}}", []string{"reason"}},
	}},
	{"outbox-publisher", "Outbox publisher enabled", "", []authSLOWantQuery{{"devhealth_auth_outbox_publisher_enabled", "latest", "max", authSLOSvcF, "", nil}}},
	{"revfeed-events", "Revision feed events by disposition", "", []authSLOWantQuery{{"devhealth_auth_revision_feed_events_total", "rate", "sum", authSLOEnvF, "{{disposition}}", []string{"disposition"}}}},
	{"revfeed-resync", "Revision feed resyncs by cause", "", []authSLOWantQuery{{"devhealth_auth_revision_feed_resync_total", "increase", "sum", authSLOEnvF, "{{cause}}", []string{"cause"}}}},
	{"revfeed-up", "Revision feed subscription up", "", []authSLOWantQuery{{"devhealth_auth_revision_feed_subscription_up", "latest", "min", authSLOEnvF, "", nil}}},
	{"push-lag", "Push projection lag, parked and final rows", "", []authSLOWantQuery{
		{"devhealth_auth_push_projection_lag", "latest", "max", authSLOEnvF, "lag", nil},
		{"devhealth_auth_push_projection_parked_rows", "latest", "max", authSLOEnvF, "parked", nil},
		{"devhealth_auth_push_projection_final_rows", "latest", "max", authSLOEnvF, "final", nil},
	}},
	{"ingest-audit", "Legacy ingest audit failures by reason", "", []authSLOWantQuery{{"dev_health_api_ingest_auth_audit_failures_total", "increase", "sum", authSLOEnvF, "{{reason}}", []string{"reason"}}}},
}

// authSLOTags is the exact tag set (order free).
var authSLOTags = []string{"environment=production", "component=auth-slo", "ticket=CHAOS-11221"} // gitleaks:allow

// authSLOInert are the only keys whose value the guard does not bind; each says why.
var authSLOInert = map[string]string{
	"spec.display.description": "hover text; an alert panel must state its line and alert, other panels no SLO line (authSLODescriptionStatesTheLine)",
}

func authSLORuleNamed(alert string) *sloRule {
	for i := range sloRules {
		if sloRules[i].alert == alert {
			return &sloRules[i]
		}
	}
	return nil
}

// authSLOAlertQuery is the one query of an alert panel: the metric, time and space aggregation and filter of the rule.
func authSLOAlertQuery(r sloRule) authSLOWantQuery {
	space := r.quantile
	if space == "" {
		space = "max"
	}
	f := authSLOSvcF
	if r.routes != "" {
		f = authSLOHTTPF + " AND route IN ('" + strings.Join(strings.Split(r.routes, "|"), "', '") + "')"
	}
	return authSLOWantQuery{metric: r.metric, timeAgg: r.timeAgg, spaceAgg: space, filter: f}
}

// authSLOLineLabel is the visible label of the rule's line: "25 ms" for a latency target, "300 s" otherwise.
func authSLOLineLabel(r sloRule) string {
	if r.quantile != "" {
		return sloMs(r)
	}
	return r.target + " " + sloUnit
}

func authSLOPanelWant(p authSLOWantPanel, r *sloRule) map[string]string {
	const s = "spec.plugin.spec."
	w := map[string]string{
		"kind":                                  sloJSON("Panel"),
		"spec.display.name":                     sloJSON(p.title),
		"spec.links":                            "null",
		"spec.plugin.kind":                      sloJSON("signoz/TimeSeriesPanel"),
		s + "axes.isLogScale":                   sloJSON(false),
		s + "axes.softMax":                      "null",
		s + "axes.softMin":                      "null",
		s + "chartAppearance.fillMode":          sloJSON("none"),
		s + "chartAppearance.lineInterpolation": sloJSON("spline"),
		s + "chartAppearance.lineStyle":         sloJSON("solid"),
		s + "chartAppearance.showPoints":        sloJSON(false),
		s + "chartAppearance.spanGaps":          sloJSON(false),
		s + "formatting.decimalPrecision":       sloJSON("2"),
		s + "formatting.unit":                   sloJSON(""),
		s + "legend.customColors":               "null",
		s + "legend.mode":                       sloJSON("list"),
		s + "legend.position":                   sloJSON("bottom"),
		s + "thresholds":                        "null",
		s + "visualization.fillSpans":           sloJSON(false),
		s + "visualization.timePreference":      sloJSON("global_time"),
	}
	if r != nil {
		target, _ := strconv.ParseFloat(r.target, 64)
		delete(w, s+"thresholds")
		w[s+"formatting.unit"] = sloJSON(sloUnit)
		w[s+"thresholds[0].color"] = sloJSON(authSLOLineColor)
		w[s+"thresholds[0].label"] = sloJSON(authSLOLineLabel(*r))
		w[s+"thresholds[0].unit"] = sloJSON(sloUnit)
		w[s+"thresholds[0].value"] = sloJSON(target)
	}
	return w
}

const (
	authSLOFilterKey  = "spec.plugin.spec.filter.expression"
	authSLOGroupByKey = "spec.plugin.spec.groupBy"
)

func authSLOQueryWant(name string, q authSLOWantQuery) map[string]string {
	const s = "spec.plugin.spec."
	return map[string]string{
		"kind":                                 sloJSON("time_series"),
		"spec.name":                            sloJSON(name),
		"spec.plugin.kind":                     sloJSON("signoz/BuilderQuery"),
		s + "aggregations[0].metricName":       sloJSON(q.metric),
		s + "aggregations[0].timeAggregation":  sloJSON(q.timeAgg),
		s + "aggregations[0].spaceAggregation": sloJSON(q.spaceAgg),
		s + "disabled":                         sloJSON(false),
		authSLOFilterKey:                       "",
		s + "legend":                           sloJSON(q.legend),
		s + "limit":                            sloJSON(100),
		s + "name":                             sloJSON(name),
		s + "order":                            "[]",
		s + "signal":                           sloJSON("metrics"),
		s + "source":                           sloJSON(""),
	}
}

// authSLOBind compares every leaf of v with want: a changed value, a missing key and an unknown key each refuse.
// A want of "" is a key checked by its own rule; keys under a skip prefix are checked by the caller.
func authSLOBind(g *guard, what, why string, v any, want map[string]string, skip ...string) {
	got := map[string]string{}
	for _, k := range sloFlatten(v, "", got) {
		g.failf("%s key %q holds a path character (. [ ]), refused%s", what, k, why)
	}
	under := func(k string) bool {
		for _, p := range skip {
			if k == p || strings.HasPrefix(k, p+".") || strings.HasPrefix(k, p+"[") {
				return true
			}
		}
		return false
	}
	for _, k := range authSLOSorted(got) {
		wv, bound := want[k]
		_, inert := authSLOInert[k]
		switch {
		case under(k) || inert:
		case !bound:
			g.failf("%s key %s is not a known field (value %s), refused%s", what, k, got[k], why)
		case wv != "" && got[k] != wv:
			g.failf("%s key %s = %s, want %s%s", what, k, got[k], wv, why)
		}
	}
	for _, k := range authSLOSorted(want) {
		if _, ok := got[k]; !ok {
			g.failf("%s key %s is missing, want %s%s", what, k, want[k], why)
		}
	}
}

func authSLOSorted(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// authSLOSameSet refuses a missing item, an extra item and a repeated item.
func authSLOSameSet(g *guard, what string, got, want []string) {
	n := map[string]int{}
	for _, x := range got {
		n[x]++
	}
	for _, w := range want {
		g.expect(n[w] > 0, "%s lack %q (want exactly %v)", what, w, want)
	}
	for _, x := range authSLOSortedSet(got) {
		in := false
		for _, w := range want {
			in = in || w == x
		}
		g.expect(in, "%s hold %q, which the inventory does not list (want exactly %v)", what, x, want)
		g.expect(n[x] <= 1, "%s hold %q %d times (want exactly %v)", what, x, n[x], want)
	}
}

func authSLOSortedSet(items []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, x := range items {
		if !seen[x] {
			seen[x] = true
			out = append(out, x)
		}
	}
	sort.Strings(out)
	return out
}

func authSLOPanelIDs() []string {
	var ids []string
	for _, p := range authSLOPanels {
		ids = append(ids, p.id)
	}
	return ids
}

// authSLODashboardIsTheInventory binds every field of the dashboard to the inventory and the alert rules.
func authSLODashboardIsTheInventory(g *guard) {
	root := authSLODoc(g)
	if root == nil {
		return
	}
	authSLOBind(g, "dashboard", "", root, map[string]string{
		"name":                 sloJSON("auth-slo"),
		"schemaVersion":        sloJSON("v6"),
		"spec.display.name":    sloJSON("Dev Health - Auth Service SLO (production)"),
		"spec.duration":        sloJSON("24h"),
		"spec.refreshInterval": sloJSON(""),
		"spec.variables":       "[]",
		"spec.layouts[0].kind": sloJSON("Grid"),
	}, "spec.panels", "spec.layouts[0].spec.items", "tags")
	panels := mp(mp(root["spec"])["panels"])
	var ids []string
	for id := range panels {
		ids = append(ids, id)
	}
	authSLOSameSet(g, "dashboard panels", ids, authSLOPanelIDs())
	for _, p := range authSLOPanels {
		pv, ok := panels[p.id]
		if !ok {
			continue
		}
		what, why, qs := "panel "+p.id, "", p.queries
		var rule *sloRule
		if p.alert != "" {
			if rule = authSLORuleNamed(p.alert); rule == nil {
				g.failf("panel %s draws the line of %s, which is not an SLO rule", p.id, p.alert)
				continue
			}
			why, qs = " (bound to "+p.alert+")", []authSLOWantQuery{authSLOAlertQuery(*rule)}
		}
		authSLOBind(g, what, why, pv, authSLOPanelWant(p, rule), "spec.queries")
		authSLODescriptionStatesTheLine(g, p.id, str(mp(mp(mp(pv)["spec"])["display"])["description"]), rule)
		authSLOQueriesAreBound(g, what, why, mp(mp(pv)["spec"])["queries"], qs)
	}
}

// authSLODescriptionStatesTheLine: the hover text of an alert panel names its line once, as its label, and its
// alert; a panel without an alert states no SLO line and names no SLO alert.
func authSLODescriptionStatesTheLine(g *guard, id, desc string, r *sloRule) {
	if r != nil {
		g.expect(strings.Count(desc, "SLO line") == 1 && strings.Contains(desc, "SLO line "+authSLOLineLabel(*r)+" ") && strings.Contains(desc, "alert "+r.alert),
			"panel %s description %q must state the line once as %q and name the alert %s", id, desc, "SLO line "+authSLOLineLabel(*r), r.alert)
		return
	}
	g.expect(!strings.Contains(desc, "SLO line"), "panel %s description %q states an SLO line, but the panel draws no alert line", id, desc)
	for _, w := range sloRules {
		g.expect(!strings.Contains(desc, w.alert), "panel %s description %q names the SLO alert %s, but the panel draws no alert line", id, desc, w.alert)
	}
}

// authSLOQueriesAreBound matches the queries by name (A, B, C; list order free) and binds each.
func authSLOQueriesAreBound(g *guard, what, why string, v any, want []authSLOWantQuery) {
	qs, ok := v.([]any)
	if !ok {
		g.failf("%s queries are %s, want a list%s", what, sloJSON(v), why)
		return
	}
	byName := map[string]any{}
	var names, wantNames []string
	for _, q := range qs {
		n := str(mp(mp(q)["spec"])["name"])
		byName[n] = q
		names = append(names, n)
	}
	for i := range want {
		wantNames = append(wantNames, string(rune('A'+i)))
	}
	authSLOSameSet(g, what+" queries", names, wantNames)
	for i, w := range want {
		n := wantNames[i]
		q, ok := byName[n]
		if !ok {
			continue
		}
		qw := what + " query " + n
		authSLOBind(g, qw, why, q, authSLOQueryWant(n, w), authSLOGroupByKey)
		spec := mp(mp(mp(mp(q)["spec"])["plugin"])["spec"])
		fe := str(mp(spec["filter"])["expression"])
		wantC, werr := sloFilterClauses(w.filter)
		g.expect(werr == "", "inventory filter of %s does not parse: %s", qw, werr)
		got, err := sloFilterClauses(fe)
		g.expect(err == "", "%s filter is %q: %s%s", qw, fe, err, why)
		if err == "" && werr == "" {
			g.expect(sloClausesEqual(got, wantC), "%s filter is %q, want exactly the clauses %v (order free; no clause or route added or removed)%s", qw, fe, wantC, why)
		}
		gb, ok := spec["groupBy"].([]any)
		g.expect(ok, "%s groupBy is %s, want a list%s", qw, sloJSON(spec["groupBy"]), why)
		var labels []string
		for j, e := range gb {
			authSLOBind(g, fmt.Sprintf("%s groupBy[%d]", qw, j), why, e, map[string]string{
				"fieldContext": sloJSON("attribute"), "fieldDataType": sloJSON("string"), "signal": sloJSON("metrics"), "name": "",
			})
			labels = append(labels, str(mp(e)["name"]))
		}
		authSLOSameSet(g, qw+" group-by labels", labels, w.groupBy)
	}
}

type authSLOQuery struct {
	panel, metric, filter, filterErr string
	clauses                          map[string]string
	groupBy                          []string
}

func authSLODoc(g *guard) map[string]any {
	var root map[string]any
	if err := json.Unmarshal([]byte(g.read(authSLODashboardRel)), &root); err != nil {
		g.failf("%s does not parse: %v", authSLODashboardRel, err)
		return nil
	}
	return root
}

func authSLOQueries(g *guard) []authSLOQuery {
	var out []authSLOQuery
	root := authSLODoc(g)
	if root == nil {
		return nil
	}
	for id, p := range mp(mp(root["spec"])["panels"]) {
		qs := list(mp(mp(p)["spec"])["queries"])
		g.expect(len(qs) > 0, "dashboard panel %s has no query", id)
		for _, q := range qs {
			spec := mp(mp(mp(mp(q)["spec"])["plugin"])["spec"])
			agg := list(spec["aggregations"])
			if len(agg) != 1 {
				g.failf("dashboard panel %s has a query with %d aggregations, want 1", id, len(agg))
				continue
			}
			aq := authSLOQuery{panel: id, metric: str(mp(agg[0])["metricName"]), filter: str(mp(spec["filter"])["expression"])}
			aq.clauses, aq.filterErr = sloFilterClauses(aq.filter)
			for _, gb := range list(spec["groupBy"]) {
				aq.groupBy = append(aq.groupBy, str(mp(gb)["name"]))
			}
			out = append(out, aq)
		}
	}
	return out
}

// authSLOMetricRows maps a metric to its allowed labels and the file labels.tsv names as its code.
func authSLOMetricRows(g *guard) (labels map[string][]string, code map[string]string) {
	labels, code = map[string][]string{}, map[string]string{}
	for _, line := range strings.Split(g.read(authSLOLabelsRel), "\n") {
		f := strings.Split(line, "\t")
		if len(f) < 3 || strings.HasPrefix(f[0], "#") {
			continue
		}
		if f[1] != "" {
			labels[f[0]] = strings.Split(f[1], ",")
		} else {
			labels[f[0]] = nil
		}
		code[f[0]] = strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(f[2]), "code:"))
	}
	return labels, code
}

func authSLOMetricsAreEmitted(g *guard) {
	labels, code := authSLOMetricRows(g)
	seen := map[string]bool{}
	for _, q := range authSLOQueries(g) {
		g.expect(!strings.HasPrefix(q.metric, "dev_health_api_shadow_"), "dashboard panel %s reads %s: the shadow series belong to the shadow dashboard", q.panel, q.metric)
		if _, ok := labels[q.metric]; !ok {
			g.failf("dashboard panel %s reads %s, which %s does not list", q.panel, q.metric, authSLOLabelsRel)
			continue
		}
		if seen[q.metric] {
			continue
		}
		seen[q.metric] = true
		// the emit site: any non-test Go file of the directory labels.tsv names (the name constant may sit in a sibling file).
		dir := path.Dir(code[q.metric])
		found := false
		for _, f := range authSLOGoFiles(g, dir) {
			if strings.Contains(g.read(f), `"`+q.metric+`"`) {
				found = true
				break
			}
		}
		g.expect(found, "dashboard panel %s reads %s, which no Go file under %s emits", q.panel, q.metric, dir)
	}
}

func authSLOGoFiles(g *guard, dir string) []string {
	var out []string
	for _, e := range authSLOReadDir(g, dir) {
		if strings.HasSuffix(e, ".go") && !strings.HasSuffix(e, "_test.go") {
			out = append(out, dir+"/"+e)
		}
	}
	sort.Strings(out)
	return out
}

// authSLOLabelsAreBounded reads each filter as exact clauses (sloFilterClauses: any other operator or form refuses).
func authSLOLabelsAreBounded(g *guard) {
	labels, _ := authSLOMetricRows(g)
	for _, q := range authSLOQueries(g) {
		if q.filterErr != "" {
			g.failf("dashboard panel %s filter is %q: %s", q.panel, q.filter, q.filterErr)
			continue
		}
		keys := append([]string{}, q.groupBy...)
		for k := range q.clauses {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		allowed := map[string]bool{}
		for _, l := range labels[q.metric] {
			allowed[l] = true
		}
		for _, k := range keys {
			for _, b := range authSLOBannedKeys {
				g.expect(k != b, "dashboard panel %s uses the prohibited label %q", q.panel, k)
			}
			g.expect(allowed[k] || authSLOResourceKeys[k], "dashboard panel %s uses the key %q, which is neither a label of %s in %s nor a resource key", q.panel, k, q.metric, authSLOLabelsRel)
		}
		g.expect(q.clauses["deployment.environment"] == "= '"+sloEnv+"'", "dashboard panel %s does not filter on deployment.environment = 'production'", q.panel)
	}
}

// authSLOPanelsArePlaced: the exact tag set, and each inventory panel placed once at its layout place (two per row,
// 6 wide and 7 high, in inventory order; the item list order is free).
func authSLOPanelsArePlaced(g *guard) {
	root := authSLODoc(g)
	if root == nil {
		return
	}
	var tags []string
	for i, t := range list(root["tags"]) {
		authSLOBind(g, fmt.Sprintf("dashboard tags[%d]", i), "", t, map[string]string{"key": "", "value": ""})
		tags = append(tags, str(mp(t)["key"])+"="+str(mp(t)["value"]))
	}
	authSLOSameSet(g, "dashboard tags", tags, authSLOTags)
	place := map[string]int{}
	for i, p := range authSLOPanels {
		place["#/spec/panels/"+p.id] = i
	}
	seen := map[string]bool{}
	var refs []string
	for _, l := range list(mp(root["spec"])["layouts"]) {
		for _, it := range list(mp(mp(l)["spec"])["items"]) {
			ref := str(mp(mp(it)["content"])["$ref"])
			g.expect(!seen[ref], "layout references %s twice", ref)
			seen[ref] = true
			refs = append(refs, ref)
			i, ok := place[ref]
			if !ok {
				g.failf("layout references %s, which is not a panel of the inventory", ref)
				continue
			}
			authSLOBind(g, "layout item "+ref, "", it, map[string]string{
				"content.$ref": sloJSON(ref), "x": sloJSON(i % 2 * 6), "y": sloJSON(i / 2 * 7), "width": sloJSON(6), "height": sloJSON(7),
			})
		}
	}
	var want []string
	for _, p := range authSLOPanels {
		want = append(want, "#/spec/panels/"+p.id)
	}
	authSLOSameSet(g, "layout items", refs, want)
}

var authSLOEdges = regexp.MustCompile(`var durationBuckets = \[\]float64\{([^}]*)\}`)

// authSLOLinesMatchTheAlerts: each SLO alert has exactly one line panel, and each latency line is an edge of the
// duration histogram (a percentile resolves to a bucket edge, so a line between two edges would never be crossed
// exactly). The line's query, unit, value and label are bound by authSLODashboardIsTheInventory.
func authSLOLinesMatchTheAlerts(g *guard) {
	edges := map[string]bool{}
	if m := authSLOEdges.FindStringSubmatch(g.read(sloDurationSrc)); m != nil {
		for _, e := range strings.Split(m[1], ",") {
			edges[strings.TrimSpace(e)] = true
		}
	} else {
		g.failf("%s has no durationBuckets literal", sloDurationSrc)
	}
	var lines, alerts []string
	for _, p := range authSLOPanels {
		if p.alert != "" {
			lines = append(lines, p.alert)
		}
	}
	for _, w := range sloRules {
		alerts = append(alerts, w.alert)
	}
	authSLOSameSet(g, "dashboard alert lines", lines, alerts)
	for _, p := range authSLOPanels {
		if r := authSLORuleNamed(p.alert); r != nil && r.routes != "" {
			g.expect(edges[r.target], "panel %s line %s is not a bucket edge of %s durationBuckets", p.id, r.target, sloDurationSrc)
		}
	}
}

// authSLODashboardAll is every real-tree check of the dashboard; the cells run it.
func authSLODashboardAll(g *guard) {
	authSLOMetricsAreEmitted(g)
	authSLOLabelsAreBounded(g)
	authSLOPanelsArePlaced(g)
	authSLOLinesMatchTheAlerts(g)
	authSLODashboardIsTheInventory(g)
}

func TestAuthSLODashboardGuardsRealTree(t *testing.T) {
	runReal(t,
		check{"authslo-metrics-are-emitted", authSLOMetricsAreEmitted},
		check{"authslo-labels-are-bounded", authSLOLabelsAreBounded},
		check{"authslo-panels-are-placed", authSLOPanelsArePlaced},
		check{"authslo-lines-match-the-alerts", authSLOLinesMatchTheAlerts},
		check{"authslo-dashboard-is-the-inventory", authSLODashboardIsTheInventory},
	)
}

func TestAuthSLODashboardPlantedDefects(t *testing.T) {
	d := authSLOCellCopy
	runPlanted(t, []planted{
		{"identity-label-in-group-by", d, mut(authSLODashboardRel, `"name": "reason_code"`, `"name": "subject"`), authSLOLabelsAreBounded, `prohibited label "subject"`},
		{"label-not-of-the-metric", d, mut(authSLODashboardRel, `"name": "disposition"`, `"name": "route"`), authSLOLabelsAreBounded, `neither a label of`},
		{"metric-renamed", d, mut(authSLODashboardRel, `"metricName": "devhealth_auth_outbox_publisher_enabled"`, `"metricName": "devhealth_auth_outbox_publisher_on"`), authSLOMetricsAreEmitted, "does not list"},
		{"shadow-series-added", d, mut(authSLODashboardRel, `"metricName": "devhealth_auth_outbox_publisher_enabled"`, `"metricName": "dev_health_api_shadow_mismatch_total"`), authSLOMetricsAreEmitted, "shadow dashboard"},
		{"environment-filter-dropped", d, mut(authSLODashboardRel, `deployment.environment = 'production' AND service.name = 'auth-service' AND listener = 'auth-api-http' AND route IN ('/v1/authorize', '/v1/authorize:batch')`, `service.name = 'auth-service' AND listener = 'auth-api-http' AND route IN ('/v1/authorize', '/v1/authorize:batch')`), authSLOLabelsAreBounded, "deployment.environment"},
		{"environment-filter-negated", d, mut(authSLODashboardRel, `deployment.environment = 'production' AND service.name = 'auth-service' AND listener = 'auth-api-http' AND route IN ('/v1/authorize', '/v1/authorize:batch')`, `NOT deployment.environment = 'production' AND service.name = 'auth-service' AND listener = 'auth-api-http' AND route IN ('/v1/authorize', '/v1/authorize:batch')`), authSLOLabelsAreBounded, "is neither key = 'v'"},
		{"panel-not-placed", d, mut(authSLODashboardRel, `"$ref": "#/spec/panels/ingest-audit"`, `"$ref": "#/spec/panels/request-rate"`), authSLOPanelsArePlaced, "twice"},
		{"threshold-line-flipped", d, mut(authSLODashboardRel, `"value": 0.025`, `"value": 0.05`), authSLODashboardIsTheInventory, "panel authorize-p95 key spec.plugin.spec.thresholds[0].value = 0.05, want 0.025 (bound to AuthzLatencyP95High)"},
		{"line-not-a-bucket-edge", d, mut(sloDurationSrc, "0.05, 0.075, 0.1,", "0.05, 0.1,"), authSLOLinesMatchTheAlerts, "not a bucket edge"},
		{"route-set-narrowed", d, mut(authSLODashboardRel, `route IN ('/internal/v1/sessions', '/v1/token/refresh')`, `route IN ('/v1/token/refresh')`), authSLODashboardIsTheInventory, "(bound to AuthTokenLatencyP95High)"},
		{"percentile-source-metric-swapped", d, mut(authSLODashboardRel, `"metricName": "dev_health_api_http_request_duration_seconds"`, `"metricName": "dev_health_api_http_requests_total"`), authSLODashboardIsTheInventory, "aggregations[0].metricName = \"dev_health_api_http_requests_total\", want \"dev_health_api_http_request_duration_seconds\" (bound to AuthzLatencyP95High)"},
	})
}

func authSLOReadDir(g *guard, dir string) []string {
	es, err := os.ReadDir(g.path(dir))
	if err != nil {
		g.failf("cannot read %s: %v", dir, err)
		return nil
	}
	var out []string
	for _, e := range es {
		if !e.IsDir() {
			out = append(out, e.Name())
		}
	}
	return out
}
