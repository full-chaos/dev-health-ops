package chartgates_test

import (
	"encoding/json"
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
// The status cells are pinned to exact text by design (static-text class): a wording change in the table must
// change the pin in the same commit, so the guard fails closed instead of accepting unreviewed status text.

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
		n := 0
		for _, m := range orgActivityBlock.FindAllStringSubmatch(g.read(orgActivityDocRel), -1) {
			var r map[string]any
			if json.Unmarshal([]byte(m[1]), &r) == nil && r["alert"] == w.alert {
				n++
			}
		}
		g.expect(n == 1, "%s has %d builder JSON blocks in %s, want 1", w.alert, n, orgActivityDocRel)
		root := orgActivityBuilder(g, w.alert)
		if root == nil {
			continue
		}
		sloBuilderKeysAreBound(g, w, root)
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

// sloBuilderInert are the only builder keys that do not change what the rule evaluates; each says why.
var sloBuilderInert = map[string]string{
	"annotations.summary":     "notification title text only",
	"annotations.description": "notification body text only",
}

// sloBuilderFilterKey is bound by the order-free clause check in sloBuilderMatchesTheRule, not by its exact text.
const sloBuilderFilterKey = "condition.compositeQuery.queries[0].spec.filter.expression"

func sloJSON(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}

// sloBuilderWant is every evaluated key of one builder block with its JSON value. Labels and channels route the
// alert, so they are bound too.
func sloBuilderWant(w sloRule) map[string]string {
	const q, cq = "condition.compositeQuery.queries[0].", "condition.compositeQuery."
	const s, a, th = q + "spec.", q + "spec.aggregations[0].", "condition.thresholds.spec[0]."
	space := w.quantile
	if space == "" {
		space = "max"
	}
	target, _ := strconv.ParseFloat(w.target, 64)
	return map[string]string{
		"alert":                          sloJSON(w.alert),
		"alertType":                      sloJSON("METRIC_BASED_ALERT"),
		"ruleType":                       sloJSON("threshold_rule"),
		"schemaVersion":                  sloJSON("v2alpha1"),
		cq + "queryType":                 sloJSON("builder"),
		cq + "panelType":                 sloJSON("graph"),
		cq + "unit":                      sloJSON(sloUnit),
		q + "type":                       sloJSON("builder_query"),
		s + "name":                       sloJSON("A"),
		s + "signal":                     sloJSON("metrics"),
		s + "stepInterval":               sloJSON(60),
		s + "disabled":                   sloJSON(false),
		a + "metricName":                 sloJSON(w.metric),
		a + "timeAggregation":            sloJSON(w.timeAgg),
		a + "spaceAggregation":           sloJSON(space),
		sloBuilderFilterKey:              "",
		"condition.selectedQueryName":    sloJSON("A"),
		"condition.thresholds.kind":      sloJSON("basic"),
		th + "name":                      sloJSON(w.severity),
		th + "target":                    sloJSON(target),
		th + "targetUnit":                sloJSON(sloUnit),
		th + "op":                        sloJSON("above"),
		th + "matchType":                 sloJSON(sloMatchType),
		th + "channels[0]":               sloJSON(orgActivityChannel),
		"evaluation.kind":                sloJSON("rolling"),
		"evaluation.spec.evalWindow":     sloJSON(w.window),
		"evaluation.spec.frequency":      sloJSON("1m"),
		"notificationSettings.usePolicy": sloJSON(false),
		"labels.severity":                sloJSON(w.severity),
		"labels.team":                    sloJSON("platform"),
		"labels.environment":             sloJSON(sloEnv),
		"labels.service":                 sloJSON(sloService),
	}
}

// sloFlatten writes each leaf of v as path -> JSON value (an empty object or list is a leaf too) and returns the
// keys that hold a path character, which could forge the path of another key.
func sloFlatten(v any, path string, out map[string]string) (bad []string) {
	switch x := v.(type) {
	case map[string]any:
		if len(x) == 0 {
			out[path] = "{}"
		}
		for k, e := range x {
			if strings.ContainsAny(k, ".[]") {
				bad = append(bad, path+"/"+k)
				continue
			}
			p := k
			if path != "" {
				p = path + "." + k
			}
			bad = append(bad, sloFlatten(e, p, out)...)
		}
	case []any:
		if len(x) == 0 {
			out[path] = "[]"
		}
		for i, e := range x {
			bad = append(bad, sloFlatten(e, fmt.Sprintf("%s[%d]", path, i), out)...)
		}
	default:
		out[path] = sloJSON(x)
	}
	return bad
}

// sloBuilderKeysAreBound refuses a block unless every key is bound to the rule or named inert: a changed value, a
// missing key and an unknown key each refuse.
func sloBuilderKeysAreBound(g *guard, w sloRule, root map[string]any) {
	got := map[string]string{}
	for _, k := range sloFlatten(root, "", got) {
		g.failf("%s builder key %q holds a path character (. [ ]), refused", w.alert, k)
	}
	want := sloBuilderWant(w)
	keys := make([]string, 0, len(got))
	for k := range got {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		wv, bound := want[k]
		_, inert := sloBuilderInert[k]
		switch {
		case inert || k == sloBuilderFilterKey:
		case !bound:
			g.failf("%s builder key %s is not a known field (value %s), refused", w.alert, k, got[k])
		case got[k] != wv:
			g.failf("%s builder key %s = %s, want %s", w.alert, k, got[k], wv)
		}
	}
	keys = keys[:0]
	for k := range want {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if _, ok := got[k]; !ok {
			g.failf("%s builder key %s is missing, want %s", w.alert, k, want[k])
		}
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
// backticked paths of the first cell), SLI substrings, target, window and source (exact cells), the alerts the
// status cell names and the exact status cell (pin). A row without a target says exactly "target not set".
type sloRow struct {
	label, sli, target, window, source, pin string
	routes, alerts, status                  []string
	noAlert                                 bool
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
		cells := strings.Split(strings.TrimSuffix(strings.TrimPrefix(strings.TrimSpace(line), "|"), "|"), "|")
		for i := range cells {
			cells[i] = strings.TrimSpace(cells[i])
		}
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
		{label: "auth-service, all routes", sli: "`dev_health_api_http_requests_total`", target: "99.95%", window: "30 days", source: "this page",
			alerts: []string{"AuthService5xx", "AuthServiceHighServerErrorRate"},
			pin:    "Target only; no production measurement. Alerts: `AuthService5xx` (any 5xx in 5 minutes, live in SigNoz) and `AuthServiceHighServerErrorRate` (5xx ratio above 5% for 10 minutes, in `alerts/rules.yml`, not yet created in SigNoz). No burn-rate rule."},
		{label: "PDP", sli: "p95, p99 latency; error rate", target: "p95 < " + sloMs(p95) + ", p99 < " + sloMs(p99) + ", errors 0",
			window: "Load leg (`ci/pdp_load_k3s.sh`); alert window " + sloMinutes(p95.window), source: "this page",
			routes: []string{"/v1/authorize", "/v1/authorize:batch"}, alerts: []string{p95.alert, p99.alert},
			pin: "Not yet met at peak. At normal load it holds: low p95 7.1 ms, high 10.2, deny 7.6, batch-10 16.4. At 200 requests/s (client concurrency 64): authorize-high p95 2075 ms with 6.9% dropped, authorize-deny p95 32.6 ms and p99 120 ms, authorize-batch-10 p50 2.6 s with 38% dropped; batch-100 is recorded, not gated (82% dropped). Alerts: `" + p95.alert + "`, `" + p99.alert + "`."},
		{label: "Session issue", sli: "p95 latency", target: "< " + sloMs(tok), window: sloMinutes(tok.window), source: "this page",
			routes: []string{"/internal/v1/sessions"}, alerts: []string{tok.alert},
			pin: "Measured, one pod of 1 CPU: p95 under 10 ms up to 400 requests/s; 161 ms at 800 requests/s ([capacity](../../operate/plan/auth-service-capacity.md)). Alert: `" + tok.alert + "`."},
		{label: "Refresh", sli: "p95 latency", target: "< " + sloMs(tok), window: sloMinutes(tok.window), source: "this page",
			routes: []string{"/v1/token/refresh"}, alerts: []string{tok.alert},
			pin: "Measured: p95 about 7 ms at 300 requests/s; 6.8 ms with 6 times the refresh rate (10-minute token lifetime). Alert: `" + tok.alert + "`."},
		{label: "JWKS", sli: "p95 latency", target: "target not set", window: "-", source: "no ticket or page states one", routes: []string{"/.well-known/jwks.json"}, noAlert: true,
			pin: "Measured baseline: p95 under 2 ms up to 1600 requests/s per pod. No alert."},
		{label: "Exchange", sli: "latency, outcome", target: "target not set", window: "-", source: "no ticket or page states one", routes: []string{"/internal/v1/token/exchange"}, noAlert: true,
			pin: "No measurement: the load harness covers issue, refresh and JWKS. No alert."},
		{label: "Audit outbox", sli: "`" + outbox.metric + "`", target: "under " + outMin, window: sloMinutes(outbox.window), source: "this page",
			alerts: []string{outbox.alert}, status: []string{"(above " + outbox.target + " seconds)"},
			pin: "Metric exists; not measured under load. Alert: `" + outbox.alert + "` (above " + outbox.target + " seconds)."},
		{label: "Revocation propagation", sli: "`auth.session.revocation_lag`", target: "target not set", window: "-", source: "no ticket or page states one", noAlert: true,
			pin: "The SLI is named; the metric is not on main yet. No alert."},
		{label: "Signing-key compromise", sli: "Seconds from the decision to drop a signing key", target: "go-api and query-api without a restart: 330 s; auth-service: 20 s", window: "per compromise", source: "this page: the verifiers' JWKS cache (5 min) and refresh floor (30 s); auth-service's key-state refresh (15 s)", noAlert: true,
			pin: "Measured in the compose key-compromise drill, not in production: two runs, go-api 277 s and 274 s, query-api 277 s and 274 s, auth-service 18 s and 3 s ([emergency rotation](../../operate/runbooks/auth-signing-key-emergency-rotation-and-rollback.md#the-key-compromise-drill-compose-2026-11-10)). A restart of go-api and query-api closes their window at once. No alert."},
		{label: "Readiness", sli: "`dev_health_runtime_ready == 1`", target: "99.9%", window: "30 days", source: "this page, kept",
			pin: "Proposed, not measured in production."},
		{label: "Readiness", sli: "Time to ready after a start", target: "under 60 s", window: "per start", source: "this page, kept",
			pin: "Proposed. Read from `dev_health_runtime_uptime_seconds` at first ready."},
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
	used := make([]bool, len(rows))
	sameLabel := map[string]int{}
	for _, w := range want {
		sameLabel[w.label]++
	}
	for _, w := range want {
		at := fmt.Sprintf("%s SLO row (%s)", sloTableRel, w.label)
		found := -1
		for i, r := range rows {
			if used[i] || !strings.HasPrefix(r[0], w.label) {
				continue
			}
			if sameLabel[w.label] == 1 || (len(r) > 1 && strings.Contains(r[1], w.sli)) {
				found = i
				break
			}
		}
		if found < 0 {
			g.failf("%s: first cell: no unmatched row starts with %q", at, w.label)
			continue
		}
		used[found] = true
		c := rows[found]
		if len(c) != 6 {
			continue // sloTableRows recorded the cell count; the cells of this row cannot be read by position
		}
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
		g.expect(c[5] == w.pin, "%s: status is %q, want exactly %q", at, c[5], w.pin)
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
	const sessionRow = "| < 200 ms | 5 minutes | this page | Measured, one pod"
	const refreshRow = "| < 200 ms | 5 minutes | this page | Measured: p95 about"
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
		{"session-window-changed", []string{sloTableRel}, mut(sloTableRel, sessionRow, "| < 200 ms | 10 minutes | this page | Measured, one pod"), sloTableRowsBindToTheRules, "(Session issue): window is"},
		{"refresh-window-changed", []string{sloTableRel}, mut(sloTableRel, refreshRow, "| < 200 ms | 10 minutes | this page | Measured: p95 about"), sloTableRowsBindToTheRules, "(Refresh): window is"},
		{"session-target-changed", []string{sloTableRel}, mut(sloTableRel, sessionRow, "| < 25 ms | 5 minutes | this page | Measured, one pod"), sloTableRowsBindToTheRules, "(Session issue): target is"},
		{"refresh-target-changed", []string{sloTableRel}, mut(sloTableRel, refreshRow, "| < 25 ms | 5 minutes | this page | Measured: p95 about"), sloTableRowsBindToTheRules, "(Refresh): target is"},
		{"pdp-target-changed", []string{sloTableRel}, mut(sloTableRel, "p99 < 75 ms, errors 0", "p99 < 70 ms, errors 0"), sloTableRowsBindToTheRules, "(PDP): target is"},
		{"pdp-source-removed", []string{sloTableRel}, mut(sloTableRel, "alert window 5 minutes | this page |", "alert window 5 minutes | - |"), sloTableRowsBindToTheRules, "(PDP): source is"},
		{"session-source-removed", []string{sloTableRel}, mut(sloTableRel, sessionRow, "| < 200 ms | 5 minutes | - | Measured, one pod"), sloTableRowsBindToTheRules, "(Session issue): source is"},
		{"refresh-source-removed", []string{sloTableRel}, mut(sloTableRel, refreshRow, "| < 200 ms | 5 minutes | - | Measured: p95 about"), sloTableRowsBindToTheRules, "(Refresh): source is"},
		{"outbox-metric-changed", []string{sloTableRel}, mut(sloTableRel, "pending event (`devhealth_auth_outbox_oldest_pending_age_seconds`)", "pending event (`devhealth_auth_outbox_pending`)"), sloTableRowsBindToTheRules, "(Audit outbox): SLI cell"},
		{"outbox-target-changed", []string{sloTableRel}, mut(sloTableRel, "| under 5 minutes | 5 minutes |", "| under 4 minutes | 5 minutes |"), sloTableRowsBindToTheRules, "(Audit outbox): target is"},
		{"outbox-window-changed", []string{sloTableRel}, mut(sloTableRel, "| under 5 minutes | 5 minutes |", "| under 5 minutes | 10 minutes |"), sloTableRowsBindToTheRules, "(Audit outbox): window is"},
		{"outbox-source-removed", []string{sloTableRel}, mut(sloTableRel, "| under 5 minutes | 5 minutes | this page |", "| under 5 minutes | 5 minutes | - |"), sloTableRowsBindToTheRules, "(Audit outbox): source is"},
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
		{"row-appended", []string{sloTableRel}, mut(sloTableRel, "| Readiness | Time to ready after a start | under 60 s | per start | this page, kept | Proposed. Read from `dev_health_runtime_uptime_seconds` at first ready. |\n", "| Readiness | Time to ready after a start | under 60 s | per start | this page, kept | Proposed. Read from `dev_health_runtime_uptime_seconds` at first ready. |\n| Extra | x | target 1 ms | 1 minute | none | none |\n"), sloTableRowsBindToTheRules, "SLO table has 12 rows, want 11"},
	})
}

func sloRowLine(t *testing.T, doc, prefix string) string {
	t.Helper()
	for _, l := range strings.Split(doc, "\n") {
		if strings.HasPrefix(l, "| "+prefix) {
			return l
		}
	}
	t.Fatalf("no SLO row starting %q", prefix)
	return ""
}

func sloRowsMut(f func(t *testing.T, doc string) string) func(*testing.T, string) {
	return func(t *testing.T, root string) {
		t.Helper()
		p := rootPath(root, sloTableRel)
		if err := os.WriteFile(p, []byte(f(t, mustRead(t, p))), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

func TestAuthSLOTableGuardAcceptsEquivalentLayouts(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*testing.T, string)
	}{
		{"padding-space-in-a-cell", mut(sloTableRel, "| Session issue `/internal/v1/sessions` |", "|  Session issue `/internal/v1/sessions`  |")},
		{"padding-in-every-cell-of-a-row", sloRowsMut(func(t *testing.T, doc string) string {
			l := sloRowLine(t, doc, "Refresh ")
			return strings.Replace(doc, l, strings.ReplaceAll(l, " | ", "   |   "), 1)
		})},
		{"two-rows-swapped", sloRowsMut(func(t *testing.T, doc string) string {
			a, b := sloRowLine(t, doc, "PDP "), sloRowLine(t, doc, "Session issue ")
			return strings.Replace(strings.Replace(strings.Replace(doc, a, "@@A@@", 1), b, a, 1), "@@A@@", b, 1)
		})},
		{"readiness-rows-swapped", sloRowsMut(func(t *testing.T, doc string) string {
			a, b := sloRowLine(t, doc, "Readiness | Fraction"), sloRowLine(t, doc, "Readiness | Time to ready")
			return strings.Replace(strings.Replace(strings.Replace(doc, a, "@@A@@", 1), b, a, 1), "@@A@@", b, 1)
		})},
	}
	real := realRoot(t)
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			root := t.TempDir()
			copyPath(t, real, root, sloTableRel)
			copyPath(t, real, root, orgActivityDocRel)
			c.mutate(t, root)
			g := &guard{root: root, tmp: t.TempDir()}
			sloTableRowsBindToTheRules(g)
			if len(g.v) != 0 {
				t.Fatalf("an equivalent Markdown layout was refused: %v", g.v)
			}
		})
	}
}

func TestAuthSLOTableGuardRowSetDefects(t *testing.T) {
	runPlanted(t, []planted{
		{"row-dropped", []string{sloTableRel}, sloRowsMut(func(t *testing.T, doc string) string {
			return strings.Replace(doc, sloRowLine(t, doc, "Exchange ")+"\n", "", 1)
		}), sloTableRowsBindToTheRules, "SLO table has 10 rows, want 11"},
		{"row-replaced-by-a-duplicate", []string{sloTableRel}, sloRowsMut(func(t *testing.T, doc string) string {
			return strings.Replace(doc, sloRowLine(t, doc, "Refresh "), sloRowLine(t, doc, "Session issue "), 1)
		}), sloTableRowsBindToTheRules, "(Refresh): first cell: no unmatched row"},
	})
}

// sloBuilderEdit decodes the builder block of one alert, lets edit change it and writes it back as JSON.
func sloBuilderEdit(alert string, edit func(t *testing.T, root map[string]any)) func(*testing.T, string) {
	return func(t *testing.T, dir string) {
		t.Helper()
		p := rootPath(dir, orgActivityDocRel)
		doc := mustRead(t, p)
		for _, m := range orgActivityBlock.FindAllStringSubmatchIndex(doc, -1) {
			var r map[string]any
			if json.Unmarshal([]byte(doc[m[2]:m[3]]), &r) != nil || r["alert"] != alert {
				continue
			}
			edit(t, r)
			b, err := json.MarshalIndent(r, "", "  ")
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(p, []byte(doc[:m[2]]+string(b)+doc[m[3]:]), 0o644); err != nil {
				t.Fatal(err)
			}
			return
		}
		t.Fatalf("no builder block for %s", alert)
	}
}

// sloPathEdit applies f to the value at a flattened path (a.b[0].c); f returns the new value, or keep=false to
// delete it. A path that does not reach its parent fails the test: the defect did not land.
func sloPathEdit(t *testing.T, root map[string]any, path string, f func(old any, ok bool) (v any, keep bool)) {
	t.Helper()
	var steps []any
	for _, seg := range strings.Split(path, ".") {
		name, rest, _ := strings.Cut(seg, "[")
		steps = append(steps, name)
		for rest != "" {
			n, tail, _ := strings.Cut(rest, "]")
			i, err := strconv.Atoi(n)
			if err != nil {
				t.Fatalf("bad path %s", path)
			}
			steps = append(steps, i)
			rest = strings.TrimPrefix(tail, "[")
		}
	}
	var cur any = root
	var holder map[string]any
	var hkey string
	for _, s := range steps[:len(steps)-1] {
		switch k := s.(type) {
		case string:
			holder, hkey = mp(cur), k
			cur = holder[k]
		case int:
			if k >= len(list(cur)) {
				t.Fatalf("path %s does not exist, the defect did not land", path)
			}
			cur = list(cur)[k]
		}
		if cur == nil {
			t.Fatalf("path %s does not exist, the defect did not land", path)
		}
	}
	switch k := steps[len(steps)-1].(type) {
	case string:
		m := mp(cur)
		old, ok := m[k]
		if v, keep := f(old, ok); keep {
			m[k] = v
		} else if ok {
			delete(m, k)
		} else {
			t.Fatalf("path %s does not exist, the defect did not land", path)
		}
	case int:
		l := list(cur)
		if k >= len(l) {
			t.Fatalf("path %s does not exist, the defect did not land", path)
		}
		if v, keep := f(l[k], true); keep {
			l[k] = v
		} else {
			holder[hkey] = append(append([]any{}, l[:k]...), l[k+1:]...)
		}
	}
}

func sloSet(path string, v any) func(*testing.T, map[string]any) {
	return func(t *testing.T, r map[string]any) {
		sloPathEdit(t, r, path, func(any, bool) (any, bool) { return v, true })
	}
}

func sloOther(v any) any {
	switch x := v.(type) {
	case string:
		return x + "-changed"
	case float64:
		return x + 1
	case bool:
		return !x
	}
	return "changed"
}

func TestAuthSLOBuilderGuardEveryKey(t *testing.T) {
	var cases []planted
	add := func(name, alert string, edit func(*testing.T, map[string]any), want string) {
		cases = append(cases, planted{name, []string{orgActivityDocRel}, sloBuilderEdit(alert, edit), sloBuilderMatchesTheRule, want})
	}
	const s = "condition.compositeQuery.queries[0].spec."
	for _, w := range sloRules {
		a := w.alert
		want := sloBuilderWant(w)
		keys := make([]string, 0, len(want))
		for k := range want {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			changed, removed := a+" builder key "+k+" = ", a+" builder key "+k+" is missing"
			if k == "alert" {
				changed, removed = "has no builder JSON block for "+a, "has no builder JSON block for "+a
			}
			if k == sloBuilderFilterKey {
				changed = a + " filter is"
			}
			add(a+"/"+k+"/changed", a, func(t *testing.T, r map[string]any) {
				sloPathEdit(t, r, k, func(old any, ok bool) (any, bool) {
					if !ok {
						t.Fatalf("%s has no key %s, the defect did not land", a, k)
					}
					return sloOther(old), true
				})
			}, changed)
			add(a+"/"+k+"/removed", a, func(t *testing.T, r map[string]any) {
				sloPathEdit(t, r, k, func(any, bool) (any, bool) { return nil, false })
			}, removed)
		}
		for _, k := range []struct {
			name, path string
			v          any
			leaf       string
		}{
			{"root-disabled", "disabled", true, "disabled"},
			{"group-by", s + "groupBy", []any{map[string]any{"name": "route"}}, s + "groupBy[0].name"},
			{"empty-group-by", s + "groupBy", []any{}, s + "groupBy"},
			{"having", s + "having", map[string]any{"expression": "count() > 1"}, s + "having.expression"},
			{"functions", s + "functions", []any{map[string]any{"name": "cutOffMin"}}, s + "functions[0].name"},
			{"reduce-to", s + "aggregations[0].reduceTo", "avg", s + "aggregations[0].reduceTo"},
			{"recovery-target", "condition.thresholds.spec[0].recoveryTarget", 0, "condition.thresholds.spec[0].recoveryTarget"},
		} {
			add(a+"/unknown/"+k.name, a, sloSet(k.path, k.v), a+" builder key "+k.leaf+" is not a known field")
		}
		add(a+"/unknown/second-query", a, func(t *testing.T, r map[string]any) {
			q := mp(mp(r["condition"])["compositeQuery"])
			q["queries"] = append(list(q["queries"]), map[string]any{"type": "builder_query", "spec": map[string]any{"name": "B"}})
		}, a+" builder key condition.compositeQuery.queries[1].spec.name is not a known field")
		add(a+"/forged-path-key", a, func(t *testing.T, r map[string]any) {
			sloPathEdit(t, r, "condition.selectedQueryName", func(any, bool) (any, bool) { return nil, false })
			r["condition.selectedQueryName"] = "A"
		}, a+" builder key \"/condition.selectedQueryName\" holds a path character")
		add(a+"/r1b-signal-logs", a, sloSet(s+"signal", "logs"), a+" builder key "+s+"signal = ")
		add(a+"/r1b-query-type-promql", a, sloSet("condition.compositeQuery.queryType", "promql"), a+" builder key condition.compositeQuery.queryType = ")
		add(a+"/r1b-selected-query-b", a, sloSet("condition.selectedQueryName", "B"), a+" builder key condition.selectedQueryName = ")
		add(a+"/r1b-disabled", a, sloSet(s+"disabled", true), a+" builder key "+s+"disabled = ")
		cases = append(cases, planted{a + "/second-block", []string{orgActivityDocRel}, func(t *testing.T, dir string) {
			p := rootPath(dir, orgActivityDocRel)
			doc := mustRead(t, p)
			for _, m := range orgActivityBlock.FindAllStringSubmatch(doc, -1) {
				if strings.Contains(m[1], `"alert": "`+a+`"`) {
					if err := os.WriteFile(p, []byte(doc+"\n"+m[0]+"\n"), 0o644); err != nil {
						t.Fatal(err)
					}
					return
				}
			}
			t.Fatalf("no builder block for %s", a)
		}, sloBuilderMatchesTheRule, a + " has 2 builder JSON blocks"})
	}
	runPlanted(t, cases)
}

func TestAuthSLOBuilderGuardAcceptsInertKeys(t *testing.T) {
	real := realRoot(t)
	for _, w := range sloRules {
		for k := range sloBuilderInert {
			t.Run(w.alert+"/"+k, func(t *testing.T) {
				root := t.TempDir()
				copyPath(t, real, root, orgActivityDocRel)
				sloBuilderEdit(w.alert, sloSet(k, "other text"))(t, root)
				g := &guard{root: root, tmp: t.TempDir()}
				sloBuilderMatchesTheRule(g)
				if len(g.v) != 0 {
					t.Fatalf("an inert key change was refused: %v", g.v)
				}
			})
		}
	}
}

// sloRowPrefixes name each SLO table row by a unique line prefix and the label the guard reports.
var sloRowPrefixes = []struct{ prefix, label string }{
	{"auth-service, all routes ", "auth-service, all routes"},
	{"PDP ", "PDP"},
	{"Session issue ", "Session issue"},
	{"Refresh ", "Refresh"},
	{"JWKS ", "JWKS"},
	{"Exchange ", "Exchange"},
	{"Audit outbox ", "Audit outbox"},
	{"Revocation propagation ", "Revocation propagation"},
	{"Signing-key compromise ", "Signing-key compromise"},
	{"Readiness | Fraction", "Readiness"},
	{"Readiness | Time to ready", "Readiness"},
}

func sloRowEdit(prefix string, f func(line string) string) func(*testing.T, string) {
	return sloRowsMut(func(t *testing.T, doc string) string {
		l := sloRowLine(t, doc, prefix)
		return strings.Replace(doc, l, f(l), 1)
	})
}

func TestAuthSLOTableGuardStatusPins(t *testing.T) {
	var cases []planted
	for _, r := range sloRowPrefixes {
		cases = append(cases, planted{"status-changed/" + r.prefix, []string{sloTableRel}, sloRowEdit(r.prefix, func(l string) string {
			return strings.TrimSuffix(l, " |") + " Measured: p95 5 ms. |"
		}), sloTableRowsBindToTheRules, "(" + r.label + "): status is"})
	}
	cases = append(cases,
		planted{"r1b-jwks-baseline-changed", []string{sloTableRel}, mut(sloTableRel, "p95 under 2 ms up to 1600", "p95 under 22 ms up to 1600"), sloTableRowsBindToTheRules, "(JWKS): status is"},
		planted{"r1b-exchange-invented-baseline", []string{sloTableRel}, mut(sloTableRel, "| No measurement: the load harness covers issue, refresh and JWKS. No alert. |", "| Measured baseline: p95 17 ms. No alert. |"), sloTableRowsBindToTheRules, "(Exchange): status is"},
		planted{"r1b-revocation-invented-baseline", []string{sloTableRel}, mut(sloTableRel, "the metric is not on main yet. No alert. |", "the metric is not on main yet. Measured baseline: p95 17 ms. No alert. |"), sloTableRowsBindToTheRules, "(Revocation propagation): status is"},
		planted{"no-target-row-names-another-alert", []string{sloTableRel}, mut(sloTableRel, "covers issue, refresh and JWKS. No alert. |", "covers issue, refresh and JWKS. No alert; see AuthTokenLatencyP95High. |"), sloTableRowsBindToTheRules, "(Exchange): status is"},
	)
	runPlanted(t, cases)
}

func TestAuthSLOTableGuardCellCount(t *testing.T) {
	var cases []planted
	for _, r := range sloRowPrefixes {
		cases = append(cases, planted{"status-cell-dropped/" + r.prefix, []string{sloTableRel}, sloRowEdit(r.prefix, func(l string) string {
			body := strings.TrimSuffix(l, " |")
			return body[:strings.LastIndex(body, " | ")] + " |"
		}), sloTableRowsBindToTheRules, "has 5 cells, want 6: | " + r.prefix})
	}
	cases = append(cases,
		planted{"extra-cell", []string{sloTableRel}, sloRowEdit("JWKS ", func(l string) string { return l + " extra |" }), sloTableRowsBindToTheRules, "has 7 cells, want 6: | JWKS "},
		planted{"one-cell-row", []string{sloTableRel}, sloRowEdit("Readiness | Time to ready", func(string) string { return "| Readiness |" }), sloTableRowsBindToTheRules, "has 1 cells, want 6: | Readiness |"},
	)
	runPlanted(t, cases)
}
