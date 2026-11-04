package chartgates_test

import (
	"encoding/json"
	"os"
	"path"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// The auth service SLO dashboard (deploy/signoz/dashboards/auth-slo.json, CHAOS-11221). The dashboard
// carries panels only: the alert rules belong to the SLO table change, and a signal with no set number
// has no alert. The guard pins what the dashboard may read: metrics that the service really emits, only
// the bounded labels the allowlist (labels.tsv) lists for that metric, no identity label, no shadow
// series (the shadow dashboard owns them), and every panel placed once in the layout.

const (
	authSLODashboardRel = "deploy/signoz/dashboards/auth-slo.json"
	authSLOLabelsRel    = "internal/platform/otlpmetrics/labels.tsv"
)

var (
	// resource keys any panel may filter on; every other key must be a label of that query's metric.
	authSLOResourceKeys = map[string]bool{"deployment.environment": true, "service.name": true}
	// identity and unbounded labels: banned even where labels.tsv lists them for the metric (subject and
	// audience are listed for auth.workload.exchanges).
	authSLOBannedKeys = append([]string{"subject", "audience", "organization", "org_id", "user", "user_id", "credential", "token", "decision_id", "instance_id", "args"}, prohibitedLabels...)
	authSLOKey        = regexp.MustCompile(`([A-Za-z_][A-Za-z0-9_.]*)\s*(?:!=|=~|=|\bNOT IN\b|\bIN\b|\bNOT LIKE\b|\bLIKE\b|\bEXISTS\b)`)
	authSLOQuote      = regexp.MustCompile(`'[^']*'|"[^"]*"`)
)

type authSLOQuery struct {
	panel, metric, filter string
	groupBy               []string
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

func authSLOLabelsAreBounded(g *guard) {
	labels, _ := authSLOMetricRows(g)
	for _, q := range authSLOQueries(g) {
		keys := append([]string{}, q.groupBy...)
		for _, m := range authSLOKey.FindAllStringSubmatch(authSLOQuote.ReplaceAllString(q.filter, "''"), -1) {
			keys = append(keys, m[1])
		}
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
		g.expect(strings.Contains(q.filter, "deployment.environment = 'production'"), "dashboard panel %s does not filter on deployment.environment = 'production'", q.panel)
	}
}

func authSLOPanelsArePlaced(g *guard) {
	root := authSLODoc(g)
	if root == nil {
		return
	}
	tags := map[string]string{}
	for _, t := range list(root["tags"]) {
		tags[str(mp(t)["key"])] = str(mp(t)["value"])
	}
	g.expect(tags["environment"] == "production" && tags["component"] == "auth-slo" && tags["ticket"] == "CHAOS-11221", "dashboard tags are %v, want environment=production, component=auth-slo, ticket=CHAOS-11221", tags) // gitleaks:allow
	panels := mp(mp(root["spec"])["panels"])
	g.expect(len(panels) >= 15, "dashboard has %d panels, want at least 15", len(panels))
	seen := map[string]bool{}
	for _, l := range list(mp(root["spec"])["layouts"]) {
		for _, it := range list(mp(mp(l)["spec"])["items"]) {
			ref := str(mp(mp(it)["content"])["$ref"])
			g.expect(!seen[ref], "layout references %s twice", ref)
			seen[ref] = true
			_, ok := panels[strings.TrimPrefix(ref, "#/spec/panels/")]
			g.expect(strings.HasPrefix(ref, "#/spec/panels/") && ok, "layout references %s, which is not a panel", ref)
		}
	}
	g.expect(len(seen) == len(panels), "layout places %d panels of %d", len(seen), len(panels))
}

// authSLOLineAlert names the SLO alert whose threshold each dashboard line draws. The alert rules
// (sloRules, pinned to alerts/rules.yml and the builder JSON by the SLO alert guard) own the numbers: the
// dashboard holds no copy of a rule, it only draws its line.
var authSLOLineAlert = map[string]string{
	"authorize-p95": "AuthzLatencyP95High",
	"authorize-p99": "AuthzLatencyP99High",
	"token-p95":     "AuthTokenLatencyP95High",
	"outbox-age":    "AuthOutboxOldestPendingHigh",
}

var authSLOEdges = regexp.MustCompile(`var durationBuckets = \[\]float64\{([^}]*)\}`)

// authSLOLinesMatchTheAlerts pins the line of each latency and outbox panel to the threshold of its alert,
// its route set and percentile to the alert's, and each latency line to an edge of the duration histogram
// (a percentile resolves to a bucket edge, so a line between two edges would never be crossed exactly).
func authSLOLinesMatchTheAlerts(g *guard) {
	root := authSLODoc(g)
	if root == nil {
		return
	}
	edges := map[string]bool{}
	if m := authSLOEdges.FindStringSubmatch(g.read(sloDurationSrc)); m != nil {
		for _, e := range strings.Split(m[1], ",") {
			edges[strings.TrimSpace(e)] = true
		}
	} else {
		g.failf("%s has no durationBuckets literal", sloDurationSrc)
	}
	panels := mp(mp(root["spec"])["panels"])
	queries := map[string][]authSLOQuery{}
	for _, q := range authSLOQueries(g) {
		queries[q.panel] = append(queries[q.panel], q)
	}
	for id, alert := range authSLOLineAlert {
		var rule *sloRule
		for i := range sloRules {
			if sloRules[i].alert == alert {
				rule = &sloRules[i]
			}
		}
		p, ok := panels[id]
		if rule == nil || !ok {
			g.failf("panel %s or the rule %s is missing", id, alert)
			continue
		}
		th := list(mp(mp(mp(mp(p)["spec"])["plugin"])["spec"])["thresholds"])
		g.expect(len(th) == 1 && jsonNumber(mp(th[0])["value"]) == rule.target, "panel %s threshold line is %v, want %s (the threshold of %s)", id, th, rule.target, alert)
		g.expect(len(queries[id]) == 1, "panel %s has %d queries, want 1", id, len(queries[id]))
		if len(queries[id]) != 1 {
			continue
		}
		q := queries[id][0]
		g.expect(q.metric == rule.metric, "panel %s reads %s, want %s (the metric of %s)", id, q.metric, rule.metric, alert)
		if rule.routes != "" {
			var in []string
			for _, r := range strings.Split(rule.routes, "|") {
				in = append(in, "'"+r+"'")
			}
			g.expect(strings.Contains(q.filter, "route IN ("+strings.Join(in, ", ")+")"), "panel %s filter is %q, want route IN (%s), the routes of %s", id, q.filter, strings.Join(in, ", "), alert)
			g.expect(edges[rule.target], "panel %s line %s is not a bucket edge of %s durationBuckets", id, rule.target, sloDurationSrc)
		}
		g.expect(strings.Contains(q.filter, "service.name = 'auth-service'"), "panel %s filter %q lacks the service", id, q.filter)
		g.expect(rule.routes == "" || strings.Contains(q.filter, "listener = 'auth-api-http'"), "panel %s filter %q lacks the listener", id, q.filter)
	}
}

func TestAuthSLODashboardGuardsRealTree(t *testing.T) {
	runReal(t,
		check{"authslo-metrics-are-emitted", authSLOMetricsAreEmitted},
		check{"authslo-labels-are-bounded", authSLOLabelsAreBounded},
		check{"authslo-panels-are-placed", authSLOPanelsArePlaced},
		check{"authslo-lines-match-the-alerts", authSLOLinesMatchTheAlerts},
	)
}

func TestAuthSLODashboardPlantedDefects(t *testing.T) {
	d := []string{authSLODashboardRel, authSLOLabelsRel, sloDurationSrc, "internal/auth", "internal/audit", "internal/api/externalingest"}
	runPlanted(t, []planted{
		{"identity-label-in-group-by", d, mut(authSLODashboardRel, `"name": "reason_code"`, `"name": "subject"`), authSLOLabelsAreBounded, `prohibited label "subject"`},
		{"label-not-of-the-metric", d, mut(authSLODashboardRel, `"name": "disposition"`, `"name": "route"`), authSLOLabelsAreBounded, `neither a label of`},
		{"metric-renamed", d, mut(authSLODashboardRel, `"metricName": "devhealth_auth_outbox_publisher_enabled"`, `"metricName": "devhealth_auth_outbox_publisher_on"`), authSLOMetricsAreEmitted, "does not list"},
		{"shadow-series-added", d, mut(authSLODashboardRel, `"metricName": "devhealth_auth_outbox_publisher_enabled"`, `"metricName": "dev_health_api_shadow_mismatch_total"`), authSLOMetricsAreEmitted, "shadow dashboard"},
		{"environment-filter-dropped", d, mut(authSLODashboardRel, `deployment.environment = 'production' AND service.name = 'auth-service' AND listener = 'auth-api-http' AND route IN ('/v1/authorize', '/v1/authorize:batch')`, `service.name = 'auth-service' AND listener = 'auth-api-http' AND route IN ('/v1/authorize', '/v1/authorize:batch')`), authSLOLabelsAreBounded, "deployment.environment"},
		{"panel-not-placed", d, mut(authSLODashboardRel, `"$ref": "#/spec/panels/ingest-audit"`, `"$ref": "#/spec/panels/request-rate"`), authSLOPanelsArePlaced, "twice"},
		{"threshold-line-flipped", d, mut(authSLODashboardRel, `"value": 0.025`, `"value": 0.05`), authSLOLinesMatchTheAlerts, "authorize-p95"},
		{"line-not-a-bucket-edge", d, mut(sloDurationSrc, "0.05, 0.075, 0.1,", "0.05, 0.1,"), authSLOLinesMatchTheAlerts, "not a bucket edge"},
		{"route-set-narrowed", d, mut(authSLODashboardRel, `route IN ('/internal/v1/sessions', '/v1/token/refresh')`, `route IN ('/v1/token/refresh')`), authSLOLinesMatchTheAlerts, "the routes of AuthTokenLatencyP95High"},
		{"percentile-source-metric-swapped", d, mut(authSLODashboardRel, `"metricName": "dev_health_api_http_request_duration_seconds"`, `"metricName": "dev_health_api_http_requests_total"`), authSLOLinesMatchTheAlerts, "the metric of"},
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
