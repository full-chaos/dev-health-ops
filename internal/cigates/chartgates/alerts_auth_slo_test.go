package chartgates_test

import (
	"regexp"
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
)

type sloRule struct {
	alert, expr, window, forDur, severity, metric, quantile, routes, target, timeAgg string
}

var sloRules = []sloRule{
	{"AuthPDPLatencyP95High", `histogram_quantile(0.95, sum by (le) (rate(` + sloDuration + `_bucket{job="auth-service",route=~"/v1/authorize|/v1/authorize:batch"}[5m]))) > 0.025`, "5m", "5m", "warning", sloDuration, "p95", "/v1/authorize|/v1/authorize:batch", "0.025", "rate"},
	{"AuthPDPLatencyP99High", `histogram_quantile(0.99, sum by (le) (rate(` + sloDuration + `_bucket{job="auth-service",route=~"/v1/authorize|/v1/authorize:batch"}[5m]))) > 0.075`, "5m", "5m", "warning", sloDuration, "p99", "/v1/authorize|/v1/authorize:batch", "0.075", "rate"},
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
			g.expect(strings.Contains(filter, "service.name = 'auth-service'"), "%s filter is %q, want service.name = 'auth-service'", w.alert, filter)
			if w.routes != "" {
				var in []string
				for _, r := range strings.Split(w.routes, "|") {
					in = append(in, "'"+r+"'")
				}
				g.expect(strings.Contains(filter, "route IN ("+strings.Join(in, ", ")+")"), "%s filter is %q, want route IN (%s)", w.alert, filter, strings.Join(in, ", "))
			}
			g.expect(len(list(spec["groupBy"])) == 0, "%s groups by %v, want no group-by", w.alert, spec["groupBy"])
		}
		th := list(mp(cond["thresholds"])["spec"])
		g.expect(len(th) == 1 && str(mp(th[0])["op"]) == "above" && jsonNumber(mp(th[0])["target"]) == w.target,
			"%s threshold is %v, want above %s", w.alert, th, w.target)
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

func sloTableNamesEveryRule(g *guard) {
	table := g.read(sloTableRel)
	for _, w := range sloRules {
		g.expect(strings.Contains(table, w.alert), "%s does not name %s", sloTableRel, w.alert)
	}
	for _, s := range []string{"99.95%", "p95 < 25 ms", "p99 < 75 ms", "CHAOS-12705", "target not set", "CHAOS-12917", "AuthService5xx", "AuthServiceHighServerErrorRate"} {
		g.expect(strings.Contains(table, s), "%s does not say %q", sloTableRel, s)
	}
}

func TestAuthSLOAlertGuardsRealTree(t *testing.T) {
	runReal(t,
		check{"slo-alerts-are-the-group", sloAlertsAreTheGroup},
		check{"slo-rule-text", sloRuleText},
		check{"slo-builder-matches-the-rule", sloBuilderMatchesTheRule},
		check{"slo-metrics-are-emitted-and-bounded", sloMetricsAreEmittedAndBounded},
		check{"slo-targets-are-bucket-edges", sloTargetsAreBucketEdges},
		check{"slo-table-names-every-rule", sloTableNamesEveryRule},
	)
}
