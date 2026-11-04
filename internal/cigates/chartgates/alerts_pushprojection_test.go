package chartgates_test

import (
	"strings"
	"testing"
)

// The push-projection alerts (alerts/rules.yml, group auth_push_projection) and their SigNoz builder form in
// docs/operate/observe/dashboards-and-alerts.md. The rule text decides the metric, the window and the threshold; the
// builder JSON must say the same, the metric names must be the ones internal/auth/pushprojection/reader.go emits,
// and the 15 minute window must keep its source: 3 x the 5 minute retry-backoff cap (reader.go maxDelay).

const (
	pushProjGroup      = "auth_push_projection"
	pushProjParked     = "AuthPushProjectionParkedStuck"
	pushProjFinal      = "AuthPushProjectionFinalRows"
	pushProjParkedM    = "devhealth_auth_push_projection_parked_rows"
	pushProjFinalM     = "devhealth_auth_push_projection_final_rows"
	pushProjLagM       = "devhealth_auth_push_projection_lag"
	pushProjReaderRel  = "internal/auth/pushprojection/reader.go"
	pushProjCapSource  = "3 x the 5 min retry-backoff cap (reader.go:48-49)"
	pushProjParkedExpr = "min_over_time(" + pushProjParkedM + "[15m]) > 0"
	pushProjFinalExpr  = pushProjFinalM + " > 0"
)

func pushProjAlertsAreTheGroup(g *guard) {
	var names []string
	for _, r := range groupRules(g, pushProjGroup) {
		names = append(names, str(r["alert"]))
	}
	g.expect(strings.Join(names, ",") == pushProjParked+","+pushProjFinal,
		"group %s holds %v, want exactly %s and %s (no lag rule)", pushProjGroup, names, pushProjParked, pushProjFinal)
}

func pushProjRuleText(g *guard) {
	parked := alertNamed(g, pushProjParked)
	g.expect(oneLine(str(parked["expr"])) == pushProjParkedExpr, "%s expr is %q, want %q", pushProjParked, oneLine(str(parked["expr"])), pushProjParkedExpr)
	final := alertNamed(g, pushProjFinal)
	g.expect(oneLine(str(final["expr"])) == pushProjFinalExpr, "%s expr is %q, want %q", pushProjFinal, oneLine(str(final["expr"])), pushProjFinalExpr)
	for _, a := range []doc{parked, final} {
		_, hasFor := a["for"]
		g.expect(!hasFor, "%s has a `for`: the window is in the expression or the row is final", str(a["alert"]))
		labels := mp(a["labels"])
		g.expect(str(labels["severity"]) == "warning" && str(labels["team"]) == "platform", "%s labels are %v, want severity warning and team platform", str(a["alert"]), labels)
	}
	g.expect(strings.Contains(oneLine(str(mp(parked["annotations"])["description"])), pushProjCapSource),
		"%s description does not cite its window source %q", pushProjParked, pushProjCapSource)
	g.expect(strings.Contains(str(mp(final["annotations"])["description"]), "drift alert"), "%s description does not name itself the drift alert", pushProjFinal)
}

func pushProjBuilderMatchesTheRule(g *guard) {
	type want struct {
		alert, metric, timeAgg, window, matchType, source string
	}
	for _, w := range []want{
		{pushProjParked, pushProjParkedM, "min", "15m", "all_the_times", pushProjCapSource},
		{pushProjFinal, pushProjFinalM, "latest", "5m", "at_least_once", "drift alert"},
	} {
		root := orgActivityBuilder(g, w.alert)
		if root == nil {
			continue
		}
		cond := mp(root["condition"])
		q := list(mp(cond["compositeQuery"])["queries"])
		g.expect(len(q) == 1, "%s has %d queries, want 1", w.alert, len(q))
		if len(q) == 1 {
			agg := list(mp(mp(q[0])["spec"])["aggregations"])
			g.expect(len(agg) == 1 && str(mp(agg[0])["metricName"]) == w.metric && str(mp(agg[0])["timeAggregation"]) == w.timeAgg,
				"%s aggregation is %v, want metric %s with %s", w.alert, agg, w.metric, w.timeAgg)
		}
		th := list(mp(cond["thresholds"])["spec"])
		g.expect(len(th) == 1 && str(mp(th[0])["op"]) == "above" && str(mp(th[0])["matchType"]) == w.matchType && jsonNumber(mp(th[0])["target"]) == "0",
			"%s threshold is %v, want above 0 %s", w.alert, th, w.matchType)
		if len(th) == 1 {
			channels := list(mp(th[0])["channels"])
			g.expect(len(channels) == 1 && str(channels[0]) == orgActivityChannel, "%s channels are %v, want [%s]", w.alert, channels, orgActivityChannel)
		}
		ev := mp(mp(root["evaluation"])["spec"])
		g.expect(str(ev["evalWindow"]) == w.window && str(ev["frequency"]) == "5m", "%s evaluation is %v, want evalWindow %s and frequency 5m", w.alert, ev, w.window)
		g.expect(mp(root["notificationSettings"])["usePolicy"] == false, "%s must name its channel and not use the notification policy", w.alert)
		labels := mp(root["labels"])
		g.expect(str(labels["severity"]) == "warning" && str(labels["team"]) == "platform", "%s labels are %v", w.alert, labels)
		g.expect(strings.Contains(str(mp(root["annotations"])["description"]), w.source), "%s description does not cite %q", w.alert, w.source)
	}
}

func pushProjSourceLinesHold(g *guard) {
	lines := strings.Split(g.read(pushProjReaderRel), "\n")
	g.expect(len(lines) >= 49 && strings.Contains(lines[47], "baseDelay") && strings.Contains(lines[48], "maxDelay") && strings.Contains(lines[48], "5 * time.Minute"),
		"%s lines 48-49 are no longer baseDelay and maxDelay = 5 * time.Minute, the source of the 15m window", pushProjReaderRel)
}

func pushProjMetricNamesAreEmitted(g *guard) {
	src := g.read(pushProjReaderRel)
	for _, name := range []string{pushProjParkedM, pushProjFinalM, pushProjLagM} {
		g.expect(strings.Contains(src, `"`+name+`"`), "%s does not emit %s", pushProjReaderRel, name)
	}
	g.expect(strings.Contains(src, "the drift alert owns them"), "%s no longer names the drift alert that owns final rows", pushProjReaderRel)
	for _, a := range []string{pushProjParked, pushProjFinal} {
		expr := str(alertNamed(g, a)["expr"])
		g.expect(!strings.Contains(expr, pushProjLagM), "%s reads the lag gauge, which counts outbox ids and stays above 0 for up to 1 h after a decided event", a)
	}
}

func TestPushProjectionAlertGuardsRealTree(t *testing.T) {
	runReal(t,
		check{"pushproj-alerts-are-the-group", pushProjAlertsAreTheGroup},
		check{"pushproj-rule-text", pushProjRuleText},
		check{"pushproj-builder-matches-the-rule", pushProjBuilderMatchesTheRule},
		check{"pushproj-source-lines-hold", pushProjSourceLinesHold},
		check{"pushproj-metric-names-are-emitted", pushProjMetricNamesAreEmitted},
	)
}
