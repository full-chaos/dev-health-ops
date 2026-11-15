package chartgates_test

import (
	"strings"
	"testing"
)

// The daily metrics blocked-run rules (alerts/rules.yml, group daily_metrics_blocked_runs). The counter
// names are checked against the worker's telemetry file, so a rename there fails here instead of leaving a
// rule on a series nobody emits.

const (
	dailyBlockedGroup       = "daily_metrics_blocked_runs"
	dailyBlockedAlert       = "DailyMetricsRunBlocked"
	dailyBlockedReconcile   = "DailyMetricsBlockedReconcileFailing"
	dailyBlockedMarked      = "dev_health_daily_metrics_runs_blocked_marked_total"
	dailyBlockedReconcileMn = "dev_health_daily_metrics_runs_blocked_reconcile_failed_total"
	dailyBlockedGoRel       = "internal/jobruntime/telemetry.go"
	dailyBlockedDocs        = "docs/operate/observe/dashboards-and-alerts.md"
)

func dailyBlockedAlertsAreTheGroup(g *guard) {
	var names []string
	for _, r := range groupRules(g, dailyBlockedGroup) {
		names = append(names, str(r["alert"]))
	}
	want := dailyBlockedAlert + "," + dailyBlockedReconcile
	g.expect(strings.Join(names, ",") == want, "group %s holds %v, want exactly %s", dailyBlockedGroup, names, want)
}

func dailyBlockedRuleText(g *guard) {
	for alert, metric := range map[string]string{
		dailyBlockedAlert:     dailyBlockedMarked,
		dailyBlockedReconcile: dailyBlockedReconcileMn,
	} {
		a := alertNamed(g, alert)
		want := "sum(increase(" + metric + "[24h])) > 0"
		g.expect(oneLine(str(a["expr"])) == want, "%s expr is %q, want %q", alert, oneLine(str(a["expr"])), want)
		_, hasFor := a["for"]
		g.expect(!hasFor, "%s has a `for`: one increase in the window must alert", alert)
		labels := mp(a["labels"])
		g.expect(str(labels["severity"]) == "warning" && str(labels["team"]) == "platform", "%s labels are %v, want severity warning and team platform", alert, labels)
	}
}

func dailyBlockedMetricsAreEmitted(g *guard) {
	src := g.read(dailyBlockedGoRel)
	for _, metric := range []string{dailyBlockedMarked, dailyBlockedReconcileMn} {
		g.expect(strings.Contains(src, `"`+metric+`"`), "%s no longer emits %s", dailyBlockedGoRel, metric)
	}
}

func dailyBlockedDocsNameTheRules(g *guard) {
	for _, alert := range []string{dailyBlockedAlert, dailyBlockedReconcile} {
		g.expect(strings.Contains(g.read(dailyBlockedDocs), alert), "%s does not name %s", dailyBlockedDocs, alert)
	}
}

func TestDailyBlockedRunAlertsGuardRealTree(t *testing.T) {
	runReal(t,
		check{"daily-blocked-alerts-are-the-group", dailyBlockedAlertsAreTheGroup},
		check{"daily-blocked-rule-text", dailyBlockedRuleText},
		check{"daily-blocked-metrics-are-emitted", dailyBlockedMetricsAreEmitted},
		check{"daily-blocked-docs-name-the-rules", dailyBlockedDocsNameTheRules},
	)
}

func TestDailyBlockedRunAlertsGuardPlantedDefects(t *testing.T) {
	rules := []string{rulesRel, dailyBlockedGoRel}
	const marked = "sum(increase(" + dailyBlockedMarked + "[24h])) > 0"
	const reconcile = "sum(increase(" + dailyBlockedReconcileMn + "[24h])) > 0"
	runPlanted(t, []planted{
		{"daily-blocked-rule-reads-the-cleared-counter", rules, mut(rulesRel, marked, strings.Replace(marked, "marked", "cleared", 1)), dailyBlockedRuleText, "want"},
		{"daily-blocked-threshold-raised", rules, mut(rulesRel, marked, strings.Replace(marked, "> 0", "> 5", 1)), dailyBlockedRuleText, "want"},
		{"daily-blocked-window-shortened", rules, mut(rulesRel, marked, strings.Replace(marked, "[24h]", "[5m]", 1)), dailyBlockedRuleText, "want"},
		{"daily-blocked-reconcile-rule-reads-the-marked-counter", rules, mut(rulesRel, reconcile, strings.Replace(reconcile, "reconcile_failed", "marked", 1)), dailyBlockedRuleText, "want"},
		{"daily-blocked-for-added", rules, mut(rulesRel, "- alert: "+dailyBlockedAlert+"\n        expr: |\n          "+marked+"\n", "- alert: "+dailyBlockedAlert+"\n        expr: |\n          "+marked+"\n        for: 30m\n"), dailyBlockedRuleText, "has a `for`"},
		{"daily-blocked-rule-renamed", rules, mut(rulesRel, "- alert: "+dailyBlockedAlert, "- alert: "+dailyBlockedAlert+"X"), dailyBlockedAlertsAreTheGroup, "want exactly"},
		{"daily-blocked-counter-renamed-in-go", rules, mutAll(dailyBlockedGoRel, `"`+dailyBlockedMarked+`"`, `"dev_health_daily_metrics_runs_blocked_marked_renamed_total"`), dailyBlockedMetricsAreEmitted, "no longer emits"},
		{"daily-blocked-docs-lose-a-rule", []string{dailyBlockedDocs}, mutAll(dailyBlockedDocs, dailyBlockedReconcile, "TheRule"), dailyBlockedDocsNameTheRules, "does not name"},
	})
}
