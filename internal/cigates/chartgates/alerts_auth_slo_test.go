package chartgates_test

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// The auth SLO alerts (alerts/rules.yml, group auth_slo), their SigNoz builder form in
// docs/operate/observe/dashboards-and-alerts.md and the SLO table in docs/reference/auth/readiness-and-slo.md.
// The rule text decides the metric, the route, the window and the threshold; the builder JSON and the table must
// say the same, and the p99/p95 targets must be bucket edges of the duration histogram.

const (
	sloGroup       = "auth_slo"
	sloDurationSrc = "internal/auth/httpapi/accesslog.go"
	sloOutboxSrc   = "internal/audit/outboxloop.go"
	sloTableRel    = "docs/reference/auth/readiness-and-slo.md"
	sloDuration    = "dev_health_api_http_request_duration_seconds"
	sloOutboxAge   = "devhealth_auth_outbox_oldest_pending_age_seconds"
	sloService     = "auth-service"
	sloEnv         = "production"
	sloUnit        = "s"
	sloMatchType   = "all_the_times"
)

type sloRule struct {
	alert, expr, window, forDur, severity, metric, quantile, routes, target, timeAgg string
}

var sloRules = []sloRule{
	{"AuthzLatencyP95High", `histogram_quantile(0.95, sum by (le) (rate(` + sloDuration + `_bucket{job="auth-service",route=~"/v1/authorize|/v1/authorize:batch"}[5m]))) > 0.025`, "5m", "5m", "warning", sloDuration, "p95", "/v1/authorize|/v1/authorize:batch", "0.025", "rate"},
	{"AuthzLatencyP99High", `histogram_quantile(0.99, sum by (le) (rate(` + sloDuration + `_bucket{job="auth-service",route=~"/v1/authorize|/v1/authorize:batch"}[5m]))) > 0.075`, "5m", "5m", "warning", sloDuration, "p99", "/v1/authorize|/v1/authorize:batch", "0.075", "rate"},
	{"AuthTokenLatencyP95High", `histogram_quantile(0.95, sum by (le) (rate(` + sloDuration + `_bucket{job="auth-service",route=~"/internal/v1/sessions|/v1/token/refresh"}[5m]))) > 0.2`, "5m", "5m", "warning", sloDuration, "p95", "/internal/v1/sessions|/v1/token/refresh", "0.2", "rate"},
	{"AuthOutboxOldestPendingHigh", `max(` + sloOutboxAge + `{job="auth-service"}) > 300`, "5m", "5m", "warning", sloOutboxAge, "", "", "300", "latest"},
}

func sloAlertsAreTheGroup(g *guard) {
	var got, want []string
	for _, r := range groupRules(g, sloGroup) {
		got = append(got, str(r["alert"]))
	}
	for _, w := range sloRules {
		want = append(want, w.alert)
	}
	g.expect(strings.Join(got, ",") == strings.Join(want, ","), "group %s holds %v, want exactly %v", sloGroup, got, want)
}

func sloRuleText(g *guard) {
	for _, w := range sloRules {
		a := alertNamed(g, w.alert)
		g.expect(oneLine(str(a["expr"])) == w.expr, "%s expr is %q, want %q", w.alert, oneLine(str(a["expr"])), w.expr)
		g.expect(str(a["for"]) == w.forDur, "%s for is %q, want %s", w.alert, str(a["for"]), w.forDur)
		labels := mp(a["labels"])
		g.expect(str(labels["severity"]) == w.severity && str(labels["team"]) == "platform", "%s labels are %v", w.alert, labels)
	}
}

func sloBuilderMatchesTheRule(g *guard) {
	for _, w := range sloRules {
		root := orgActivityBuilder(g, w.alert)
		if root == nil {
			continue
		}
		cond := mp(root["condition"])
		q := list(mp(cond["compositeQuery"])["queries"])
		g.expect(len(q) == 1, "%s has %d queries, want 1", w.alert, len(q))
		if len(q) == 1 {
			spec := mp(mp(q[0])["spec"])
			agg := list(spec["aggregations"])
			space := w.quantile
			if space == "" {
				space = "max"
			}
			g.expect(len(agg) == 1 && str(mp(agg[0])["metricName"]) == w.metric && str(mp(agg[0])["timeAggregation"]) == w.timeAgg && str(mp(agg[0])["spaceAggregation"]) == space,
				"%s aggregation is %v, want %s of %s(%s)", w.alert, agg, space, w.timeAgg, w.metric)
			filter := str(mp(spec["filter"])["expression"])
			want := map[string]string{"service.name": "= '" + sloService + "'", "deployment.environment": "= '" + sloEnv + "'"}
			if w.routes != "" {
				want["route"] = "IN " + sloSortedSet(strings.Split(w.routes, "|"))
			}
			got, err := sloFilterClauses(filter)
			g.expect(err == "", "%s filter is %q: %s", w.alert, filter, err)
			if err == "" {
				g.expect(sloClausesEqual(got, want), "%s filter is %q, want exactly the clauses %v (no clause added or removed, route set exact)", w.alert, filter, want)
			}
			g.expect(len(list(spec["groupBy"])) == 0, "%s groups by %v, want no group-by", w.alert, spec["groupBy"])
		}
		th := list(mp(cond["thresholds"])["spec"])
		g.expect(len(th) == 1 && str(mp(th[0])["op"]) == "above" && jsonNumber(mp(th[0])["target"]) == w.target &&
			str(mp(th[0])["targetUnit"]) == sloUnit && str(mp(th[0])["matchType"]) == sloMatchType && str(mp(th[0])["name"]) == w.severity,
			"%s threshold is %v, want above %s with targetUnit %s, matchType %s and name %s", w.alert, th, w.target, sloUnit, sloMatchType, w.severity)
		g.expect(str(mp(cond["compositeQuery"])["unit"]) == sloUnit, "%s compositeQuery unit is %q, want %s", w.alert, str(mp(cond["compositeQuery"])["unit"]), sloUnit)
		if len(th) == 1 {
			channels := list(mp(th[0])["channels"])
			g.expect(len(channels) == 1 && str(channels[0]) == orgActivityChannel, "%s channels are %v, want [%s]", w.alert, channels, orgActivityChannel)
		}
		ev := mp(mp(root["evaluation"])["spec"])
		g.expect(str(ev["evalWindow"]) == w.window, "%s evalWindow is %v, want %s", w.alert, ev["evalWindow"], w.window)
		g.expect(mp(root["notificationSettings"])["usePolicy"] == false, "%s must name its channel and not use the notification policy", w.alert)
		labels := mp(root["labels"])
		g.expect(str(labels["severity"]) == w.severity && str(labels["team"]) == "platform" && str(labels["service"]) == "auth-service", "%s labels are %v", w.alert, labels)
	}
}

func sloMetricsAreEmittedAndBounded(g *guard) {
	g.expect(strings.Contains(g.read(sloDurationSrc), `"`+sloDuration+`"`), "%s does not emit %s", sloDurationSrc, sloDuration)
	g.expect(strings.Contains(g.read(sloOutboxSrc), `"`+sloOutboxAge+`"`), "%s does not emit %s", sloOutboxSrc, sloOutboxAge)
	for _, w := range sloRules {
		for _, m := range regexp.MustCompile(`dev_health_[a-z_]+|devhealth_[a-z_]+`).FindAllString(w.expr, -1) {
			g.expect(strings.TrimSuffix(m, "_bucket") == sloDuration || m == sloOutboxAge, "%s reads %s, outside the SLO metrics", w.alert, m)
		}
		g.expect(!strings.Contains(w.expr, "by (le, "), "%s groups by a label other than le", w.alert)
	}
	for _, r := range []string{"/v1/authorize", "/v1/authorize:batch", "/internal/v1/sessions", "/v1/token/refresh"} {
		g.expect(strings.Contains(g.read("internal/auth/authapi/authorize.go")+g.read("internal/auth/authapi/issue.go")+g.read("internal/auth/authapi/routes.go"), `"`+r+`"`), "no route constant for %s in internal/auth/authapi", r)
	}
}

// The 75 ms and 200 ms targets must be edges of the histogram, or the quantile is only known to the next edge.
func sloTargetsAreBucketEdges(g *guard) {
	src := g.read(sloDurationSrc)
	for _, e := range []string{"0.025", "0.075", "0.2"} {
		g.expect(regexp.MustCompile(`durationBuckets = \[\]float64\{[^}]*\b`+regexp.QuoteMeta(e)+`\b[^}]*\}`).MatchString(src), "durationBuckets has no edge %s: a target between edges is read at the next edge", e)
	}
}

func TestAuthSLOAlertGuardsRealTree(t *testing.T) {
	runReal(t,
		check{"slo-alerts-are-the-group", sloAlertsAreTheGroup},
		check{"slo-rule-text", sloRuleText},
		check{"slo-builder-matches-the-rule", sloBuilderMatchesTheRule},
		check{"slo-metrics-are-emitted-and-bounded", sloMetricsAreEmittedAndBounded},
		check{"slo-targets-are-bucket-edges", sloTargetsAreBucketEdges},
		check{"slo-table-rows-bind-to-the-rules", sloTableRowsBindToTheRules},
	)
}

var (
	sloClauseEq = regexp.MustCompile(`^([A-Za-z_.]+) = '([^']*)'$`)
	sloClauseIn = regexp.MustCompile(`^([A-Za-z_.]+) IN \(('[^']*'(?:, '[^']*')*)\)$`)
)

// sloFilterClauses reads a builder filter as AND-joined `key = 'v'` and `key IN ('a', 'b')` clauses only; any other
// operator, a repeated key or a clause it cannot read is an error, so a widened or narrowed filter cannot pass.
func sloFilterClauses(expr string) (map[string]string, string) {
	out := map[string]string{}
	for _, c := range strings.Split(expr, " AND ") {
		var key, val string
		if m := sloClauseEq.FindStringSubmatch(c); m != nil {
			key, val = m[1], "= '"+m[2]+"'"
		} else if m := sloClauseIn.FindStringSubmatch(c); m != nil {
			key = m[1]
			var items []string
			for _, it := range strings.Split(m[2], ", ") {
				items = append(items, strings.Trim(it, "'"))
			}
			val = "IN " + sloSortedSet(items)
		} else {
			return nil, fmt.Sprintf("clause %q is neither key = 'v' nor key IN ('a', 'b')", c)
		}
		if _, dup := out[key]; dup {
			return nil, fmt.Sprintf("clause key %s appears twice", key)
		}
		out[key] = val
	}
	return out, ""
}

func sloSortedSet(items []string) string {
	s := append([]string(nil), items...)
	sort.Strings(s)
	return "{" + strings.Join(s, ",") + "}"
}

func sloClausesEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

func sloRuleNamed(alert string) sloRule {
	for _, w := range sloRules {
		if w.alert == alert {
			return w
		}
	}
	panic("no SLO rule " + alert)
}

// sloMs renders a seconds threshold as the table writes it: 0.025 -> "25 ms".
func sloMs(w sloRule) string {
	f, _ := strconv.ParseFloat(w.target, 64)
	return strconv.FormatFloat(math.Round(f*1e6)/1e3, 'f', -1, 64) + " ms"
}

func sloMinutes(window string) string {
	return strings.TrimSuffix(window, "m") + " minutes"
}

var (
	sloRouteTok = regexp.MustCompile("`(?:[A-Z]+ )?(/[^`]*)`")
	sloAlertTok = regexp.MustCompile("`(Auth[A-Za-z0-9]+)`")
)

// sloRow is one expected row of the SLO table. Every cell is bound: label (first-cell prefix), routes (the
// backticked paths of the first cell), SLI substrings, target, window and source (exact cells), and the alerts the
// status cell names. A row without a target says exactly "target not set".
type sloRow struct {
	label, sli, target, window, source string
	routes, alerts, status             []string
	noAlert                            bool
}

func sloTableRows(g *guard) (intro string, rows [][]string) {
	page := g.read(sloTableRel)
	i := strings.Index(page, "\n## Service level objectives\n")
	if i < 0 {
		g.failf("%s has no \"## Service level objectives\" section", sloTableRel)
		return "", nil
	}
	sec := page[i+1:]
	if j := strings.Index(sec[3:], "\n## "); j >= 0 {
		sec = sec[:j+3]
	}
	k := strings.Index(sec, "| Service and path |")
	if k < 0 {
		g.failf("%s SLO section has no table headed \"Service and path\"", sloTableRel)
		return "", nil
	}
	intro = sec[:k]
	for n, line := range strings.Split(sec[k:], "\n") {
		if !strings.HasPrefix(line, "|") {
			break
		}
		if n < 2 {
			continue
		}
		cells := strings.Split(strings.TrimSuffix(strings.TrimPrefix(line, "| "), " |"), " | ")
		g.expect(len(cells) == 6, "%s SLO row %d has %d cells, want 6: %.60s", sloTableRel, n-1, len(cells), line)
		rows = append(rows, cells)
	}
	return intro, rows
}

func sloExpectedRows() []sloRow {
	p95, p99 := sloRuleNamed("AuthzLatencyP95High"), sloRuleNamed("AuthzLatencyP99High")
	tok, outbox := sloRuleNamed("AuthTokenLatencyP95High"), sloRuleNamed("AuthOutboxOldestPendingHigh")
	outMin := func() string { f, _ := strconv.Atoi(outbox.target); return strconv.Itoa(f/60) + " minutes" }()
	return []sloRow{
		{label: "auth-service, all routes", sli: "`dev_health_api_http_requests_total`", target: "99.95%", window: "30 days", source: "CHAOS-11220",
			alerts: []string{"AuthService5xx", "AuthServiceHighServerErrorRate"}},
		{label: "PDP", sli: "p95, p99 latency; error rate", target: "p95 < " + sloMs(p95) + ", p99 < " + sloMs(p99) + ", errors 0",
			window: "Load leg (`ci/pdp_load_k3s.sh`); alert window " + sloMinutes(p95.window), source: "CHAOS-10854, CHAOS-12705 (\"do not loosen the SLO\")",
			routes: []string{"/v1/authorize", "/v1/authorize:batch"}, alerts: []string{p95.alert, p99.alert}},
		{label: "Session issue", sli: "p95 latency", target: "< " + sloMs(tok), window: sloMinutes(tok.window), source: "CHAOS-11220",
			routes: []string{"/internal/v1/sessions"}, alerts: []string{tok.alert}},
		{label: "Refresh", sli: "p95 latency", target: "< " + sloMs(tok), window: sloMinutes(tok.window), source: "CHAOS-11220",
			routes: []string{"/v1/token/refresh"}, alerts: []string{tok.alert}},
		{label: "JWKS", sli: "p95 latency", target: "target not set", window: "-", source: "no ticket or page states one", routes: []string{"/.well-known/jwks.json"}, noAlert: true},
		{label: "Exchange", sli: "latency, outcome", target: "target not set", window: "-", source: "no ticket or page states one", routes: []string{"/internal/v1/token/exchange"}, noAlert: true},
		{label: "Audit outbox", sli: "`" + outbox.metric + "`", target: "under " + outMin, window: sloMinutes(outbox.window), source: "this page, before CHAOS-11220",
			alerts: []string{outbox.alert}, status: []string{"(above " + outbox.target + " seconds)"}},
		{label: "Revocation propagation", sli: "`auth.session.revocation_lag`", target: "target not set", window: "-", source: "no ticket or page states one", noAlert: true},
		{label: "Readiness", sli: "`dev_health_runtime_ready == 1`", target: "99.9%", window: "30 days", source: "this page, kept"},
		{label: "Readiness", sli: "Time to ready after a start", target: "under 60 s", window: "per start", source: "this page, kept"},
	}
}

func sloTableRowsBindToTheRules(g *guard) {
	intro, rows := sloTableRows(g)
	if rows == nil {
		return
	}
	g.expect(strings.Contains(intro, "`"+sloDuration+"` histogram on the auth-service API listener, filtered by `route`"), "%s SLO intro does not name the latency metric %s filtered by route", sloTableRel, sloDuration)
	p95, p99, tok := sloRuleNamed("AuthzLatencyP95High"), sloRuleNamed("AuthzLatencyP99High"), sloRuleNamed("AuthTokenLatencyP95High")
	g.expect(strings.Contains(intro, "its edges include "+sloMs(p95)+", "+sloMs(p99)+" and "+sloMs(tok)), "%s SLO intro does not list the bucket edges %s, %s and %s", sloTableRel, sloMs(p95), sloMs(p99), sloMs(tok))
	g.expect(p95.window == p99.window, "AuthzLatencyP95High and AuthzLatencyP99High disagree on the window: %s vs %s", p95.window, p99.window)
	want := sloExpectedRows()
	g.expect(len(rows) == len(want), "%s SLO table has %d rows, want %d", sloTableRel, len(rows), len(want))
	if len(rows) != len(want) {
		return
	}
	unionRoutes := map[string][]string{}
	for i, w := range want {
		c := rows[i]
		at := fmt.Sprintf("%s SLO row %d (%s)", sloTableRel, i+1, w.label)
		g.expect(strings.HasPrefix(c[0], w.label), "%s: first cell is %q", at, c[0])
		var gotRoutes []string
		for _, m := range sloRouteTok.FindAllStringSubmatch(c[0], -1) {
			gotRoutes = append(gotRoutes, m[1])
		}
		g.expect(sloSortedSet(gotRoutes) == sloSortedSet(w.routes), "%s: route set is %v, want %v", at, gotRoutes, w.routes)
		g.expect(strings.Contains(c[1], w.sli), "%s: SLI cell %q does not say %q", at, c[1], w.sli)
		g.expect(c[2] == w.target, "%s: target is %q, want %q", at, c[2], w.target)
		g.expect(c[3] == w.window, "%s: window is %q, want %q", at, c[3], w.window)
		g.expect(c[4] == w.source, "%s: source is %q, want %q", at, c[4], w.source)
		var gotAlerts []string
		for _, m := range sloAlertTok.FindAllStringSubmatch(c[5], -1) {
			gotAlerts = append(gotAlerts, m[1])
		}
		g.expect(sloSortedSet(gotAlerts) == sloSortedSet(w.alerts), "%s: status names alerts %v, want %v", at, gotAlerts, w.alerts)
		if w.noAlert {
			g.expect(strings.Contains(c[5], "No alert"), "%s: status does not say \"No alert\": %q", at, c[5])
		}
		for _, s := range w.status {
			g.expect(strings.Contains(c[5], s), "%s: status does not say %q", at, s)
		}
		for _, a := range gotAlerts {
			if len(w.routes) > 0 {
				unionRoutes[a] = append(unionRoutes[a], gotRoutes...)
			}
		}
	}
	for _, r := range sloRules {
		if r.routes != "" {
			g.expect(sloSortedSet(unionRoutes[r.alert]) == sloSortedSet(strings.Split(r.routes, "|")),
				"%s: the rows naming %s cover routes %v, the rule reads %s", sloTableRel, r.alert, unionRoutes[r.alert], r.routes)
		}
	}
}

// sloBlockMut changes the first occurrence of old after the builder block of one alert.
func sloBlockMut(alert, old, repl string) func(*testing.T, string) {
	return func(t *testing.T, root string) {
		t.Helper()
		p := filepath.Join(root, filepath.FromSlash(orgActivityDocRel))
		b, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		doc := string(b)
		i := strings.Index(doc, `"alert": "`+alert+`"`)
		if i < 0 {
			t.Fatalf("no builder block for %s", alert)
		}
		j := strings.Index(doc[i:], old)
		if j < 0 {
			t.Fatalf("block %s has no %q, the defect did not land", alert, old)
		}
		out := doc[:i+j] + repl + doc[i+j+len(old):]
		if err := os.WriteFile(p, []byte(out), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

func TestAuthSLOTableGuardPlantedDefects(t *testing.T) {
	const intro = "`" + sloDuration + "` histogram on the auth-service API listener"
	const sessionRow = "| < 200 ms | 5 minutes | CHAOS-11220 | Measured, one pod"
	const refreshRow = "| < 200 ms | 5 minutes | CHAOS-11220 | Measured: p95 about"
	const none = "| target not set | - | no ticket or page states one | "
	runPlanted(t, []planted{
		{"latency-metric-statement-changed", []string{sloTableRel}, mut(sloTableRel, intro, "`dev_health_api_http_requests_total` histogram on the auth-service API listener"), sloTableRowsBindToTheRules, "SLO intro does not name the latency metric"},
		{"bucket-edges-statement-changed", []string{sloTableRel}, mut(sloTableRel, "75 ms and 200 ms (`internal", "75 ms and 250 ms (`internal"), sloTableRowsBindToTheRules, "does not list the bucket edges"},
		{"pdp-authorize-route-changed", []string{sloTableRel}, mut(sloTableRel, "`POST /v1/authorize` and", "`POST /v1/authorise` and"), sloTableRowsBindToTheRules, "(PDP): route set"},
		{"pdp-batch-route-changed", []string{sloTableRel}, mut(sloTableRel, "`/v1/authorize:batch` | p95, p99", "`/v1/authorize:batches` | p95, p99"), sloTableRowsBindToTheRules, "(PDP): route set"},
		{"session-route-changed", []string{sloTableRel}, mut(sloTableRel, "| Session issue `/internal/v1/sessions` |", "| Session issue `/internal/v1/session` |"), sloTableRowsBindToTheRules, "(Session issue): route set"},
		{"refresh-route-changed", []string{sloTableRel}, mut(sloTableRel, "| Refresh `/v1/token/refresh` |", "| Refresh `/v1/token/refresh2` |"), sloTableRowsBindToTheRules, "(Refresh): route set"},
		{"refresh-route-dropped-from-the-rule-union", []string{sloTableRel}, mut(sloTableRel, "| Refresh `/v1/token/refresh` |", "| Refresh |"), sloTableRowsBindToTheRules, "the rows naming AuthTokenLatencyP95High cover routes"},
		{"pdp-window-changed", []string{sloTableRel}, mut(sloTableRel, "alert window 5 minutes", "alert window 10 minutes"), sloTableRowsBindToTheRules, "(PDP): window is"},
		{"session-window-changed", []string{sloTableRel}, mut(sloTableRel, sessionRow, "| < 200 ms | 10 minutes | CHAOS-11220 | Measured, one pod"), sloTableRowsBindToTheRules, "(Session issue): window is"},
		{"refresh-window-changed", []string{sloTableRel}, mut(sloTableRel, refreshRow, "| < 200 ms | 10 minutes | CHAOS-11220 | Measured: p95 about"), sloTableRowsBindToTheRules, "(Refresh): window is"},
		{"session-target-changed", []string{sloTableRel}, mut(sloTableRel, sessionRow, "| < 25 ms | 5 minutes | CHAOS-11220 | Measured, one pod"), sloTableRowsBindToTheRules, "(Session issue): target is"},
		{"refresh-target-changed", []string{sloTableRel}, mut(sloTableRel, refreshRow, "| < 25 ms | 5 minutes | CHAOS-11220 | Measured: p95 about"), sloTableRowsBindToTheRules, "(Refresh): target is"},
		{"pdp-target-changed", []string{sloTableRel}, mut(sloTableRel, "p99 < 75 ms, errors 0", "p99 < 70 ms, errors 0"), sloTableRowsBindToTheRules, "(PDP): target is"},
		{"pdp-source-removed", []string{sloTableRel}, mut(sloTableRel, "| CHAOS-10854, CHAOS-12705 (\"do not loosen the SLO\") |", "| - |"), sloTableRowsBindToTheRules, "(PDP): source is"},
		{"session-source-removed", []string{sloTableRel}, mut(sloTableRel, sessionRow, "| < 200 ms | 5 minutes | - | Measured, one pod"), sloTableRowsBindToTheRules, "(Session issue): source is"},
		{"refresh-source-removed", []string{sloTableRel}, mut(sloTableRel, refreshRow, "| < 200 ms | 5 minutes | - | Measured: p95 about"), sloTableRowsBindToTheRules, "(Refresh): source is"},
		{"outbox-metric-changed", []string{sloTableRel}, mut(sloTableRel, "pending event (`devhealth_auth_outbox_oldest_pending_age_seconds`)", "pending event (`devhealth_auth_outbox_pending`)"), sloTableRowsBindToTheRules, "(Audit outbox): SLI cell"},
		{"outbox-target-changed", []string{sloTableRel}, mut(sloTableRel, "| under 5 minutes | 5 minutes |", "| under 4 minutes | 5 minutes |"), sloTableRowsBindToTheRules, "(Audit outbox): target is"},
		{"outbox-window-changed", []string{sloTableRel}, mut(sloTableRel, "| under 5 minutes | 5 minutes |", "| under 5 minutes | 10 minutes |"), sloTableRowsBindToTheRules, "(Audit outbox): window is"},
		{"outbox-source-removed", []string{sloTableRel}, mut(sloTableRel, "| this page, before CHAOS-11220 |", "| - |"), sloTableRowsBindToTheRules, "(Audit outbox): source is"},
		{"outbox-threshold-status-changed", []string{sloTableRel}, mut(sloTableRel, "(above 300 seconds)", "(above 240 seconds)"), sloTableRowsBindToTheRules, "(Audit outbox): status does not say"},
		{"jwks-target-invented", []string{sloTableRel}, mut(sloTableRel, none+"Measured baseline", "| target 30 ms | - | no ticket or page states one | Measured baseline"), sloTableRowsBindToTheRules, "(JWKS): target is"},
		{"exchange-target-invented", []string{sloTableRel}, mut(sloTableRel, none+"No measurement", "| target 30 ms | - | no ticket or page states one | No measurement"), sloTableRowsBindToTheRules, "(Exchange): target is"},
		{"revocation-target-invented", []string{sloTableRel}, mut(sloTableRel, none+"The SLI is named", "| target 30 ms | - | no ticket or page states one | The SLI is named"), sloTableRowsBindToTheRules, "(Revocation propagation): target is"},
		{"no-target-row-gains-a-window", []string{sloTableRel}, mut(sloTableRel, none+"Measured baseline", "| target not set | 5 minutes | no ticket or page states one | Measured baseline"), sloTableRowsBindToTheRules, "(JWKS): window is"},
		{"no-target-row-gains-an-alert", []string{sloTableRel}, mut(sloTableRel, "up to 1600 requests/s per pod. No alert.", "up to 1600 requests/s per pod. Alert: `AuthJwksSlow`."), sloTableRowsBindToTheRules, "(JWKS): status names alerts"},
		{"session-row-names-the-pdp-alert", []string{sloTableRel}, mut(sloTableRel, "Alert: `AuthTokenLatencyP95High`. |\n| Refresh", "Alert: `AuthzLatencyP95High`. |\n| Refresh"), sloTableRowsBindToTheRules, "(Session issue): status names alerts"},
		{"row-label-changed", []string{sloTableRel}, mut(sloTableRel, "| Readiness | Time to ready after a start |", "| Availability | Time to ready after a start |"), sloTableRowsBindToTheRules, "(Readiness): first cell"},
	})
}

func TestAuthSLOBuilderGuardPlantedDefects(t *testing.T) {
	var cases []planted
	add := func(name string, m func(*testing.T, string), want string) {
		cases = append(cases, planted{name, []string{orgActivityDocRel}, m, sloBuilderMatchesTheRule, want})
	}
	env := "AND deployment.environment = 'production'\""
	for _, w := range sloRules {
		a := w.alert
		add(a+"-filter-clause-added", sloBlockMut(a, env, "AND status_code != '500' "+env), a+" filter is")
		add(a+"-filter-clause-removed", sloBlockMut(a, " AND deployment.environment = 'production'", ""), a+" filter is")
		add(a+"-service-clause-changed", sloBlockMut(a, "service.name = 'auth-service'", "service.name != 'auth-service'"), a+" filter is")
		add(a+"-filter-or-clause", sloBlockMut(a, env, "OR service.name = 'go-api' "+env), a+" filter is")
		add(a+"-target-unit-changed", sloBlockMut(a, `"targetUnit": "s"`, `"targetUnit": "ms"`), a+" threshold is")
		add(a+"-match-type-changed", sloBlockMut(a, `"matchType": "all_the_times"`, `"matchType": "at_least_once"`), a+" threshold is")
		add(a+"-composite-unit-changed", sloBlockMut(a, `"unit": "s"`, `"unit": "ms"`), a+" compositeQuery unit")
		if w.routes == "" {
			add(a+"-route-filter-added", sloBlockMut(a, env, "AND route IN ('/v1/authorize') "+env), a+" filter is")
			continue
		}
		add(a+"-route-added", sloBlockMut(a, "') AND deployment", "', '/healthz') AND deployment"), a+" filter is")
		add(a+"-route-clause-removed", sloBlockMut(a, " AND route IN ('"+strings.ReplaceAll(w.routes, "|", "', '")+"')", ""), a+" filter is")
		add(a+"-route-narrowed", sloBlockMut(a, "route IN ('"+strings.ReplaceAll(w.routes, "|", "', '")+"')", "route IN ('"+strings.Split(w.routes, "|")[0]+"')"), a+" filter is")
		add(a+"-route-clause-excluding", sloBlockMut(a, env, "AND route NOT IN ('/healthz') "+env), a+" filter is")
	}
	runPlanted(t, cases)
}

func TestAuthSLOTableGuardRowCount(t *testing.T) {
	runPlanted(t, []planted{
		{"row-appended", []string{sloTableRel}, mut(sloTableRel, "| Readiness | Time to ready after a start | under 60 s | per start | this page, kept | Proposed. Read from `dev_health_runtime_uptime_seconds` at first ready. |\n", "| Readiness | Time to ready after a start | under 60 s | per start | this page, kept | Proposed. Read from `dev_health_runtime_uptime_seconds` at first ready. |\n| Extra | x | target 1 ms | 1 minute | none | none |\n"), sloTableRowsBindToTheRules, "SLO table has 11 rows, want 10"},
	})
}
