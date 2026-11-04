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
	authSLODurationName = "dev_health_api_http_request_duration_seconds"
)

var (
	// resource keys any panel may filter on; every other key must be a label of that query's metric.
	authSLOResourceKeys = map[string]bool{"deployment.environment": true, "service.name": true}
	// identity and unbounded labels: banned even where labels.tsv lists them for the metric (subject and
	// audience are listed for auth.workload.exchanges).
	authSLOBannedKeys = append([]string{"subject", "audience", "organization", "org_id", "user", "user_id", "credential", "token", "decision_id", "instance_id", "args"}, prohibitedLabels...)
	// the SLO lines drawn on the latency and outbox panels; each is a number of the SLO table.
	authSLOLines = map[string]float64{"authorize-p95": 0.025, "authorize-p99": 0.075, "token-p95": 0.2, "outbox-age": 300}
	authSLOKey   = regexp.MustCompile(`([A-Za-z_][A-Za-z0-9_.]*)\s*(?:!=|=~|=|\bNOT IN\b|\bIN\b|\bNOT LIKE\b|\bLIKE\b|\bEXISTS\b)`)
	authSLOQuote = regexp.MustCompile(`'[^']*'|"[^"]*"`)
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

// authSLOLinesMatchTheTable pins the SLO line of each latency and outbox panel to the SLO table number and
// the percentile of the panel to the one its name says. A panel line the table does not carry is wrong.
func authSLOLinesMatchTheTable(g *guard) {
	root := authSLODoc(g)
	if root == nil {
		return
	}
	panels := mp(mp(root["spec"])["panels"])
	for id, want := range authSLOLines {
		p, ok := panels[id]
		if !ok {
			g.failf("dashboard has no panel %s", id)
			continue
		}
		th := list(mp(mp(mp(mp(p)["spec"])["plugin"])["spec"])["thresholds"])
		g.expect(len(th) == 1 && jsonNumber(mp(th[0])["value"]) == jsonNumber(want), "panel %s threshold line is %v, want %v (the SLO table)", id, th, want)
	}
	for _, q := range authSLOQueries(g) {
		if q.panel == "authorize-p95" || q.panel == "token-p95" || q.panel == "authorize-p99" {
			g.expect(q.metric == authSLODurationName, "panel %s reads %s, want %s", q.panel, q.metric, authSLODurationName)
			g.expect(strings.Contains(q.filter, "listener = 'auth-api-http'") && strings.Contains(q.filter, "route = '"), "panel %s filter %q lacks the listener and route", q.panel, q.filter)
		}
	}
}

func TestAuthSLODashboardGuardsRealTree(t *testing.T) {
	runReal(t,
		check{"authslo-metrics-are-emitted", authSLOMetricsAreEmitted},
		check{"authslo-labels-are-bounded", authSLOLabelsAreBounded},
		check{"authslo-panels-are-placed", authSLOPanelsArePlaced},
		check{"authslo-lines-match-the-table", authSLOLinesMatchTheTable},
	)
}

func TestAuthSLODashboardPlantedDefects(t *testing.T) {
	d := []string{authSLODashboardRel, authSLOLabelsRel, "internal/auth", "internal/audit", "internal/api/externalingest"}
	runPlanted(t, []planted{
		{"identity-label-in-group-by", d, mut(authSLODashboardRel, `"name": "reason_code"`, `"name": "subject"`), authSLOLabelsAreBounded, `prohibited label "subject"`},
		{"label-not-of-the-metric", d, mut(authSLODashboardRel, `"name": "disposition"`, `"name": "route"`), authSLOLabelsAreBounded, `neither a label of`},
		{"metric-renamed", d, mut(authSLODashboardRel, `"metricName": "devhealth_auth_outbox_publisher_enabled"`, `"metricName": "devhealth_auth_outbox_publisher_on"`), authSLOMetricsAreEmitted, "does not list"},
		{"shadow-series-added", d, mut(authSLODashboardRel, `"metricName": "devhealth_auth_outbox_publisher_enabled"`, `"metricName": "dev_health_api_shadow_mismatch_total"`), authSLOMetricsAreEmitted, "shadow dashboard"},
		{"environment-filter-dropped", d, mut(authSLODashboardRel, `deployment.environment = 'production' AND service.name = 'auth-service' AND listener = 'auth-api-http' AND route = 'POST /v1/authorize'`, `service.name = 'auth-service' AND listener = 'auth-api-http' AND route = 'POST /v1/authorize'`), authSLOLabelsAreBounded, "deployment.environment"},
		{"panel-not-placed", d, mut(authSLODashboardRel, `"$ref": "#/spec/panels/ingest-audit"`, `"$ref": "#/spec/panels/request-rate"`), authSLOPanelsArePlaced, "twice"},
		{"threshold-line-flipped", d, mut(authSLODashboardRel, `"value": 0.025`, `"value": 0.05`), authSLOLinesMatchTheTable, "authorize-p95"},
		{"route-filter-dropped", d, mut(authSLODashboardRel, ` AND route = 'POST /v1/token/refresh'`, ``), authSLOLinesMatchTheTable, "lacks the listener and route"},
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
