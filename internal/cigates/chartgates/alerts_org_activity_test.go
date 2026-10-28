package chartgates_test

import (
	"encoding/json"
	"regexp"
	"strings"
	"testing"
)

// The org_activity alerts (alerts/rules.yml, group org_activity) and their SigNoz builder form in
// docs/operate/observe/dashboards-and-alerts.md. The rule text decides the metric, the window and the threshold; the builder JSON must
// say the same, and the metric names must be the ones the worker's code emits.

const (
	orgActivityGroup        = "org_activity"
	orgActivityFailedAlert  = "OrgActivityRefreshFailed"
	orgActivityStaleAlert   = "OrgActivityStale"
	orgActivityFailedMetric = "daily_finalize_org_activity_refresh_failed_total"
	orgActivityAgeMetric    = "dev_health_org_activity_refresh_age_seconds"
	orgActivityReadFailed   = "dev_health_org_activity_refresh_age_read_failed_total"
	orgActivityStaleSeconds = "172800"
	orgActivityDocRel       = "docs/operate/observe/dashboards-and-alerts.md"
)

var orgActivityBlock = regexp.MustCompile("(?s)```json\n(.*?)\n```")

func orgActivityAlertsAreTheGroup(g *guard) {
	var names []string
	for _, r := range groupRules(g, orgActivityGroup) {
		names = append(names, str(r["alert"]))
	}
	g.expect(strings.Join(names, ",") == orgActivityFailedAlert+","+orgActivityStaleAlert,
		"group %s holds %v, want exactly %s and %s", orgActivityGroup, names, orgActivityFailedAlert, orgActivityStaleAlert)
}

func orgActivityRuleText(g *guard) {
	failed := alertNamed(g, orgActivityFailedAlert)
	want := "sum(increase(" + orgActivityFailedMetric + "[24h])) > 0"
	g.expect(oneLine(str(failed["expr"])) == want, "%s expr is %q, want %q", orgActivityFailedAlert, oneLine(str(failed["expr"])), want)
	_, hasFor := failed["for"]
	g.expect(!hasFor, "%s has a `for`: one failure in the window must alert", orgActivityFailedAlert)
	stale := alertNamed(g, orgActivityStaleAlert)
	want = "max(" + orgActivityAgeMetric + ") > " + orgActivityStaleSeconds
	g.expect(oneLine(str(stale["expr"])) == want, "%s expr is %q, want %q", orgActivityStaleAlert, oneLine(str(stale["expr"])), want)
	g.expect(str(stale["for"]) == "15m", "%s `for` is %q, want 15m", orgActivityStaleAlert, str(stale["for"]))
	for _, a := range []doc{failed, stale} {
		labels := mp(a["labels"])
		g.expect(str(labels["severity"]) == "warning" && str(labels["team"]) == "platform", "%s labels are %v, want severity warning and team platform", str(a["alert"]), labels)
	}
}

// orgActivityBuilder is the decoded import JSON of one alert in the docs page.
func orgActivityBuilder(g *guard, alert string) map[string]any {
	for _, m := range orgActivityBlock.FindAllStringSubmatch(g.read(orgActivityDocRel), -1) {
		var root map[string]any
		if err := json.Unmarshal([]byte(m[1]), &root); err != nil {
			g.failf("a JSON block of %s does not parse: %v", orgActivityDocRel, err)
			continue
		}
		if root["alert"] == alert {
			return root
		}
	}
	g.failf("%s has no builder JSON block for %s", orgActivityDocRel, alert)
	return nil
}

func orgActivityBuilderMatchesTheRule(g *guard) {
	type want struct {
		alert, metric, timeAgg, window, op, matchType, target string
	}
	for _, w := range []want{
		{orgActivityFailedAlert, orgActivityFailedMetric, "increase", "24h", "above", "at_least_once", "0"},
		{orgActivityStaleAlert, orgActivityAgeMetric, "latest", "15m", "above", "all_the_times", orgActivityStaleSeconds},
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
		g.expect(len(th) == 1 && str(mp(th[0])["op"]) == w.op && str(mp(th[0])["matchType"]) == w.matchType && jsonNumber(mp(th[0])["target"]) == w.target,
			"%s threshold is %v, want %s %s %s", w.alert, th, w.op, w.target, w.matchType)
		g.expect(str(mp(mp(root["evaluation"])["spec"])["evalWindow"]) == w.window, "%s evalWindow is %q, want %s", w.alert, str(mp(mp(root["evaluation"])["spec"])["evalWindow"]), w.window)
		g.expect(mp(root["notificationSettings"])["usePolicy"] == true, "%s must route through the notification policy", w.alert)
		labels := mp(root["labels"])
		g.expect(str(labels["severity"]) == "warning" && str(labels["team"]) == "platform", "%s labels are %v", w.alert, labels)
	}
}

func jsonNumber(v any) string {
	f, ok := v.(float64)
	if !ok {
		return "?"
	}
	b, _ := json.Marshal(f)
	return string(b)
}

func orgActivityMetricNamesAreEmitted(g *guard) {
	age := g.read("internal/orgactivity/refresh/age.go")
	for _, name := range []string{orgActivityAgeMetric, orgActivityReadFailed} {
		g.expect(strings.Contains(age, `"`+name+`"`), "internal/orgactivity/refresh/age.go does not emit %s", name)
	}
	g.expect(strings.Contains(g.read("internal/jobs/metrics/daily/daily.go"), `"`+orgActivityFailedMetric+`"`), "daily.go does not emit %s", orgActivityFailedMetric)
}

func TestOrgActivityAlertGuardsRealTree(t *testing.T) {
	runReal(t,
		check{"org-activity-alerts-are-the-group", orgActivityAlertsAreTheGroup},
		check{"org-activity-rule-text", orgActivityRuleText},
		check{"org-activity-builder-matches-the-rule", orgActivityBuilderMatchesTheRule},
		check{"org-activity-metric-names-are-emitted", orgActivityMetricNamesAreEmitted},
	)
}
