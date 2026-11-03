package chartgates_test

import (
	"encoding/json"
	"regexp"
	"slices"
	"strings"
	"testing"
)

// The auth shadow mismatch alert (alerts/rules.yml, group auth_shadow), its SigNoz builder form in
// docs/operate/observe/dashboards-and-alerts.md and the dashboard source in deploy/signoz/dashboards.
// The rule text decides the metric, the window and the threshold; the builder JSON and the dashboard must
// read the same metrics, and only the bounded labels of the instruments.

const (
	shadowGroup        = "auth_shadow"
	shadowAlert        = "AuthShadowMismatchUnresolved"
	shadowMismatch     = "dev_health_api_shadow_mismatch_total"
	shadowResults      = "dev_health_api_shadow_results_total"
	shadowEnabled      = "dev_health_api_shadow_enabled"
	shadowConfigBad    = "dev_health_api_shadow_config_invalid"
	shadowDashboardRel = "deploy/signoz/dashboards/auth-shadow-mismatch.json"
	shadowCountersRel  = "internal/api/policy/shadowcounters.go"
	shadowRel          = "internal/api/policy/shadow.go"
	shadowLabelsRel    = "internal/platform/otlpmetrics/labels.tsv"
)

var (
	shadowAllowedKeys = map[string]bool{"namespace": true, "caller": true, "class": true, "result": true, "service.name": true, "deployment.environment": true}
	shadowBannedKeys  = append([]string{"organization", "user", "route", "action", "decision_id", "reason_code", "reason", "instance_id"}, prohibitedLabels...)
	shadowFilterKey   = regexp.MustCompile(`([A-Za-z_][A-Za-z0-9_.]*)\s*(?:!=|=~|=|\bNOT IN\b|\bIN\b|\bNOT LIKE\b|\bLIKE\b|\bEXISTS\b)`)
	shadowQuoted      = regexp.MustCompile(`'[^']*'|"[^"]*"`)
	shadowPromMatcher = regexp.MustCompile(`\{([^}]*)\}`)
	shadowPromBy      = regexp.MustCompile(`\b(?:by|without)\s*\(([^)]*)\)`)
	shadowPromKey     = regexp.MustCompile(`([A-Za-z_][A-Za-z0-9_]*)\s*(?:=~|!=|=)`)
)

func shadowAlertsAreTheGroup(g *guard) {
	var names []string
	for _, r := range groupRules(g, shadowGroup) {
		names = append(names, str(r["alert"]))
	}
	g.expect(strings.Join(names, ",") == shadowAlert, "group %s holds %v, want exactly %s", shadowGroup, names, shadowAlert)
}

func shadowRuleText(g *guard) {
	a := alertNamed(g, shadowAlert)
	want := `sum(increase(` + shadowMismatch + `{class="unresolved"}[1h])) > 0`
	g.expect(oneLine(str(a["expr"])) == want, "%s expr is %q, want %q", shadowAlert, oneLine(str(a["expr"])), want)
	_, hasFor := a["for"]
	g.expect(!hasFor, "%s has a `for`: one unresolved mismatch must alert", shadowAlert)
	labels := mp(a["labels"])
	g.expect(str(labels["severity"]) == "warning" && str(labels["team"]) == "platform", "%s labels are %v, want severity warning and team platform", shadowAlert, labels)
}

func shadowBuilderMatchesTheRule(g *guard) {
	root := orgActivityBuilder(g, shadowAlert)
	if root == nil {
		return
	}
	cond := mp(root["condition"])
	q := list(mp(cond["compositeQuery"])["queries"])
	g.expect(len(q) == 1, "%s has %d queries, want 1", shadowAlert, len(q))
	if len(q) == 1 {
		spec := mp(mp(q[0])["spec"])
		agg := list(spec["aggregations"])
		g.expect(len(agg) == 1 && str(mp(agg[0])["metricName"]) == shadowMismatch && str(mp(agg[0])["timeAggregation"]) == "increase" && str(mp(agg[0])["spaceAggregation"]) == "sum",
			"%s aggregation is %v, want sum(increase(%s))", shadowAlert, agg, shadowMismatch)
		filter := str(mp(spec["filter"])["expression"])
		g.expect(strings.Contains(filter, "class = 'unresolved'"), "%s filter is %q, want class = 'unresolved'", shadowAlert, filter)
		g.expect(len(list(spec["groupBy"])) == 0, "%s groups by %v, want no group-by (one alert)", shadowAlert, spec["groupBy"])
	}
	th := list(mp(cond["thresholds"])["spec"])
	g.expect(len(th) == 1 && str(mp(th[0])["op"]) == "above" && str(mp(th[0])["matchType"]) == "at_least_once" && jsonNumber(mp(th[0])["target"]) == "0",
		"%s threshold is %v, want above 0 at_least_once", shadowAlert, th)
	if len(th) == 1 {
		channels := list(mp(th[0])["channels"])
		g.expect(len(channels) == 1 && str(channels[0]) == orgActivityChannel, "%s channels are %v, want [%s]", shadowAlert, channels, orgActivityChannel)
	}
	ev := mp(mp(root["evaluation"])["spec"])
	g.expect(str(ev["evalWindow"]) == "1h" && str(ev["frequency"]) == "5m", "%s evaluation is %v, want evalWindow 1h and frequency 5m", shadowAlert, ev)
	ns := mp(root["notificationSettings"])
	g.expect(ns["usePolicy"] == false, "%s must name its channel and not use the notification policy", shadowAlert)
	_, renotify := ns["renotify"]
	g.expect(!renotify, "%s must not renotify", shadowAlert)
	labels := mp(root["labels"])
	g.expect(str(labels["severity"]) == "warning" && str(labels["team"]) == "platform", "%s labels are %v", shadowAlert, labels)
}

func shadowMetricNamesAreEmitted(g *guard) {
	src := g.read(shadowCountersRel)
	for _, name := range []string{shadowMismatch, shadowResults, shadowEnabled, shadowConfigBad} {
		g.expect(strings.Contains(src, `"`+name+`"`), "%s does not emit %s", shadowCountersRel, name)
	}
	for _, name := range shadowDashboardMetrics(g) {
		g.expect(strings.Contains(src, `"`+name+`"`), "the dashboard reads %s, which %s does not emit", name, shadowCountersRel)
	}
	rule := str(alertNamed(g, shadowAlert)["expr"])
	for _, m := range regexp.MustCompile(`dev_health_[a-z_]+`).FindAllString(rule, -1) {
		g.expect(strings.Contains(src, `"`+m+`"`), "the rule reads %s, which %s does not emit", m, shadowCountersRel)
	}
}

func shadowCheckKeys(g *guard, where string, keys []string) {
	for _, k := range keys {
		g.expect(shadowAllowedKeys[k], "%s uses the label %q, outside the allowlist {namespace, caller, class, result, service.name, deployment.environment}", where, k)
		for _, b := range shadowBannedKeys {
			g.expect(k != b, "%s uses the prohibited label %q", where, k)
		}
	}
}

func shadowFilterKeys(expr string) []string {
	var out []string
	for _, m := range shadowFilterKey.FindAllStringSubmatch(shadowQuoted.ReplaceAllString(expr, "''"), -1) {
		out = append(out, m[1])
	}
	return out
}

func shadowLabelsStayInAllowlist(g *guard) {
	rows := map[string]string{}
	for _, line := range strings.Split(g.read(shadowLabelsRel), "\n") {
		f := strings.Split(line, "\t")
		if len(f) >= 2 {
			rows[f[0]] = f[1]
		}
	}
	g.expect(rows[shadowMismatch] == "caller,class,namespace", "%s row for %s is %q, want caller,class,namespace", shadowLabelsRel, shadowMismatch, rows[shadowMismatch])
	g.expect(rows[shadowResults] == "result", "%s row for %s is %q, want result", shadowLabelsRel, shadowResults, rows[shadowResults])

	expr := str(alertNamed(g, shadowAlert)["expr"])
	var keys []string
	for _, m := range shadowPromMatcher.FindAllStringSubmatch(expr, -1) {
		for _, k := range shadowPromKey.FindAllStringSubmatch(m[1], -1) {
			keys = append(keys, k[1])
		}
	}
	for _, m := range shadowPromBy.FindAllStringSubmatch(expr, -1) {
		for _, k := range strings.Split(m[1], ",") {
			keys = append(keys, strings.TrimSpace(k))
		}
	}
	shadowCheckKeys(g, "the rule "+shadowAlert, keys)

	if root := orgActivityBuilder(g, shadowAlert); root != nil {
		for _, q := range list(mp(mp(root["condition"])["compositeQuery"])["queries"]) {
			spec := mp(mp(q)["spec"])
			keys := shadowFilterKeys(str(mp(spec["filter"])["expression"]))
			for _, gb := range list(spec["groupBy"]) {
				keys = append(keys, str(mp(gb)["name"]))
			}
			shadowCheckKeys(g, "the builder JSON of "+shadowAlert, keys)
		}
	}
	for id, p := range shadowPanels(g) {
		for _, q := range p.queries {
			keys := shadowFilterKeys(str(mp(q["filter"])["expression"]))
			for _, gb := range list(q["groupBy"]) {
				keys = append(keys, str(mp(gb)["name"]))
			}
			shadowCheckKeys(g, "dashboard panel "+id, keys)
		}
	}
}

func shadowClassValueIsInTheClosedSet(g *guard) {
	g.expect(strings.Contains(g.read(shadowRel), `classUnresolved = "unresolved"`), "%s no longer names the class `unresolved`, which the rule filters on", shadowRel)
}

type shadowPanel struct {
	queries []map[string]any
}

func shadowDashboard(g *guard) map[string]any {
	var root map[string]any
	if err := json.Unmarshal([]byte(g.read(shadowDashboardRel)), &root); err != nil {
		g.failf("%s does not parse: %v", shadowDashboardRel, err)
		return nil
	}
	return root
}

func shadowPanels(g *guard) map[string]shadowPanel {
	out := map[string]shadowPanel{}
	root := shadowDashboard(g)
	if root == nil {
		return out
	}
	for id, p := range mp(mp(root["spec"])["panels"]) {
		var sp shadowPanel
		for _, q := range list(mp(mp(p)["spec"])["queries"]) {
			sp.queries = append(sp.queries, mp(mp(mp(mp(q)["spec"])["plugin"])["spec"]))
		}
		out[id] = sp
	}
	return out
}

func shadowDashboardMetrics(g *guard) []string {
	var out []string
	for _, p := range shadowPanels(g) {
		for _, q := range p.queries {
			for _, a := range list(q["aggregations"]) {
				out = append(out, str(mp(a)["metricName"]))
			}
		}
	}
	return out
}

func shadowDashboardPanels(g *guard) {
	root := shadowDashboard(g)
	if root == nil {
		return
	}
	tags := map[string]string{}
	for _, t := range list(root["tags"]) {
		tags[str(mp(t)["key"])] = str(mp(t)["value"])
	}
	g.expect(tags["environment"] == "production" && tags["component"] == "auth-shadow" && tags["ticket"] == "CHAOS-10849", "dashboard tags are %v, want environment=production, component=auth-shadow, ticket=CHAOS-10849", tags) // gitleaks:allow
	panels := shadowPanels(g)
	g.expect(len(panels) >= 7, "dashboard has %d panels, want at least 7", len(panels))
	byMetric := map[string]map[string]bool{}
	for _, p := range panels {
		for _, q := range p.queries {
			agg := list(q["aggregations"])
			if len(agg) == 0 {
				g.failf("a dashboard panel query has no aggregation")
				continue
			}
			metric := str(mp(agg[0])["metricName"])
			g.expect(strings.HasPrefix(metric, "dev_health_api_shadow_"), "a dashboard panel reads %q, want only dev_health_api_shadow_*", metric)
			var gb []string
			for _, k := range list(q["groupBy"]) {
				gb = append(gb, str(mp(k)["name"]))
			}
			if byMetric[metric] == nil {
				byMetric[metric] = map[string]bool{}
			}
			byMetric[metric][strings.Join(gb, ",")] = true
		}
	}
	g.expect(byMetric[shadowMismatch]["namespace"], "no dashboard panel groups %s by namespace", shadowMismatch)
	g.expect(byMetric[shadowMismatch]["caller"], "no dashboard panel groups %s by caller", shadowMismatch)
	g.expect(byMetric[shadowMismatch]["class"], "no dashboard panel groups %s by class", shadowMismatch)
	g.expect(byMetric[shadowResults]["result"], "no dashboard panel groups %s by result (coverage)", shadowResults)
	g.expect(len(byMetric[shadowEnabled]) > 0 && len(byMetric[shadowConfigBad]) > 0, "no dashboard panel shows %s and %s", shadowEnabled, shadowConfigBad)
	seen := map[string]bool{}
	for _, l := range list(mp(root["spec"])["layouts"]) {
		for _, it := range list(mp(mp(l)["spec"])["items"]) {
			ref := str(mp(mp(it)["content"])["$ref"])
			g.expect(!seen[ref], "layout references %s twice", ref)
			seen[ref] = true
			g.expect(strings.HasPrefix(ref, "#/spec/panels/") && panels[strings.TrimPrefix(ref, "#/spec/panels/")].queries != nil, "layout references %s, which is not a panel", ref)
		}
	}
	g.expect(len(seen) == len(panels), "layout places %d panels of %d", len(seen), len(panels))
}

// Allowed panel plugin fields per SigNoz v6 panel kind; source: schema-notes-10849d-v1.md (live prod dashboards and the SigNoz Number panel doc).
// "*" marks a free-form map, a path absent from a kind is not allowed.
var shadowPanelFields = map[string]map[string][]string{
	"signoz/NumberPanel": {
		"": {"formatting", "thresholds", "visualization"}, "formatting": {"decimalPrecision", "unit"},
		"thresholds[]": {"color", "unit", "value"}, "visualization": {"timePreference"},
	},
	"signoz/TablePanel": {
		"": {"formatting", "thresholds", "visualization"}, "formatting": {"columnUnits", "decimalPrecision"},
		"formatting.columnUnits": {"*"}, "visualization": {"timePreference"},
	},
	"signoz/BarChartPanel": {
		"": {"axes", "formatting", "legend", "thresholds", "visualization"}, "axes": {"isLogScale", "softMax", "softMin"},
		"formatting": {"decimalPrecision", "unit"}, "legend": {"customColors", "mode", "position"},
		"thresholds[]": {"color", "label", "unit", "value"}, "visualization": {"fillSpans", "stackedBarChart", "timePreference"},
	},
	"signoz/TimeSeriesPanel": {
		"": {"axes", "chartAppearance", "formatting", "legend", "thresholds", "visualization"}, "axes": {"isLogScale", "softMax", "softMin"},
		"chartAppearance":          {"fillMode", "lineInterpolation", "lineStyle", "showPoints", "spanGaps"},
		"chartAppearance.spanGaps": {"fillLessThan", "fillOnlyBelow"}, "formatting": {"decimalPrecision", "unit"},
		"legend": {"customColors", "mode", "position"}, "thresholds[]": {"color", "label", "unit", "value"},
		"visualization": {"fillSpans", "timePreference"},
	},
}

func shadowCheckPanelFields(g *guard, where, path string, v any, allowed map[string][]string) {
	keys, ok := allowed[path]
	if !ok {
		g.failf("%s: object at %q is not in the allowed set for its panel kind", where, path)
		return
	}
	m := mp(v)
	for k, child := range m {
		if len(keys) == 1 && keys[0] == "*" {
			continue
		}
		if !slices.Contains(keys, k) {
			g.failf("%s: key %q at %q is not in the SigNoz v6 field set %v", where, k, path, keys)
			continue
		}
		sub := k
		if path != "" {
			sub = path + "." + k
		}
		switch c := child.(type) {
		case map[string]any:
			shadowCheckPanelFields(g, where, sub, c, allowed)
		case []any:
			for _, it := range c {
				if _, isObj := it.(map[string]any); isObj {
					shadowCheckPanelFields(g, where, sub+"[]", it, allowed)
				}
			}
		}
	}
}

func shadowPanelFieldsAreV6(g *guard) {
	root := shadowDashboard(g)
	if root == nil {
		return
	}
	for id, p := range mp(mp(root["spec"])["panels"]) {
		plugin := mp(mp(mp(p)["spec"])["plugin"])
		kind := str(plugin["kind"])
		allowed, ok := shadowPanelFields[kind]
		if !ok {
			g.failf("panel %s has kind %q, which is not in the allowed panel kinds", id, kind)
			continue
		}
		shadowCheckPanelFields(g, "panel "+id, "", plugin["spec"], allowed)
	}
}

// Scalar panels issue a scalar request, so each metric aggregation needs reduceTo; chart panels do not. Source: schema-notes-10849d-v2.md.
var (
	shadowScalarKinds = []string{"signoz/NumberPanel", "signoz/TablePanel", "signoz/PiePanel"}
	shadowReduceTo    = []string{"sum", "count", "avg", "min", "max", "last", "median"}
)

func shadowScalarPanelsReduce(g *guard) {
	root := shadowDashboard(g)
	if root == nil {
		return
	}
	for id, p := range mp(mp(root["spec"])["panels"]) {
		spec := mp(mp(p)["spec"])
		scalar := slices.Contains(shadowScalarKinds, str(mp(spec["plugin"])["kind"]))
		for _, q := range list(spec["queries"]) {
			for _, a := range list(mp(mp(mp(mp(q)["spec"])["plugin"])["spec"])["aggregations"]) {
				if str(mp(a)["metricName"]) == "" {
					continue
				}
				r, has := mp(a)["reduceTo"]
				if scalar {
					g.expect(has && slices.Contains(shadowReduceTo, str(r)), "panel %s is scalar: metric %s has reduceTo %v, want one of %v", id, str(mp(a)["metricName"]), r, shadowReduceTo)
				} else {
					g.expect(!has, "panel %s is a chart panel: metric %s must not set reduceTo", id, str(mp(a)["metricName"]))
				}
			}
		}
	}
}

func TestShadowAlertGuardsRealTree(t *testing.T) {
	runReal(t,
		check{"shadow-alerts-are-the-group", shadowAlertsAreTheGroup},
		check{"shadow-rule-text", shadowRuleText},
		check{"shadow-builder-matches-the-rule", shadowBuilderMatchesTheRule},
		check{"shadow-metric-names-are-emitted", shadowMetricNamesAreEmitted},
		check{"shadow-labels-stay-in-allowlist", shadowLabelsStayInAllowlist},
		check{"shadow-class-value-is-in-the-closed-set", shadowClassValueIsInTheClosedSet},
		check{"shadow-dashboard-panels", shadowDashboardPanels},
		check{"shadow-panel-fields-are-v6", shadowPanelFieldsAreV6},
		check{"shadow-scalar-panels-reduce", shadowScalarPanelsReduce},
	)
}
