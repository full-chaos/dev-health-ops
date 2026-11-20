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
	dailyBlockedReasonsRel  = "internal/jobs/metrics/daily/blocked.go"
	dailyBlockedFinalize    = "finalize_failed"
	dailyBlockedRerun       = "daily-start --rerun-tag"
	dailyBlockedRedrive     = "daily-redrive"
	dailyBlockedClearRow    = "row clears from `daily-blocked`"
	dailyBlockedWindow      = "Recompute window"
	dailyBlockedOperatorRel = "docs/operate/runbooks/operator-commands.md"
	dailyBlockedOperatorRow = "metrics daily-blocked --org"
	dailyBlockedPartition   = "partition reason"
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

// dailyBlockedUnits splits text at ". " and "; " outside parentheses, so a clause that names a reason keeps
// the action written for it.
func dailyBlockedUnits(text string) []string {
	var units []string
	depth, start := 0, 0
	for i := 0; i < len(text); i++ {
		switch text[i] {
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		case '.', ';':
			if depth == 0 && i+1 < len(text) && text[i+1] == ' ' {
				units = append(units, text[start:i])
				start = i + 2
			}
		}
	}
	return append(units, text[start:])
}

// dailyBlockedPairs holds the action to the reason it is written for: no unit may pair a reason with the
// other reason's action, and one unit must pair each reason with its own.
func dailyBlockedPairs(g *guard, where, text string) {
	var partitionOK, finalizeOK bool
	for _, u := range dailyBlockedUnits(oneLine(text)) {
		isPartition := strings.Contains(u, dailyBlockedPartition)
		isFinalize := strings.Contains(u, dailyBlockedFinalize)
		g.expect(!(isPartition && isFinalize), "%s: one clause names both reasons: %q", where, u)
		g.expect(!(isPartition && strings.Contains(u, dailyBlockedRerun)), "%s: a partition reason is paired with %q: %q", where, dailyBlockedRerun, u)
		g.expect(!(isFinalize && strings.Contains(u, dailyBlockedRedrive)), "%s: %s is paired with %q: %q", where, dailyBlockedFinalize, dailyBlockedRedrive, u)
		partitionOK = partitionOK || (isPartition && strings.Contains(u, dailyBlockedRedrive))
		finalizeOK = finalizeOK || (isFinalize && strings.Contains(u, dailyBlockedRerun))
	}
	g.expect(partitionOK, "%s: no clause pairs a partition reason with %q", where, dailyBlockedRedrive)
	g.expect(finalizeOK, "%s: no clause pairs %s with %q", where, dailyBlockedFinalize, dailyBlockedRerun)
}

// A finalize_failed run has status failed, which no redrive selects, so the alert and the runbook must name
// the rerun for that reason and keep the redrive for the partition reasons.
func dailyBlockedActionPerReason(g *guard) {
	g.expect(strings.Contains(g.read(dailyBlockedReasonsRel), `BlockedReasonFinalizeFailed = "`+dailyBlockedFinalize+`"`), "%s no longer defines the reason %s", dailyBlockedReasonsRel, dailyBlockedFinalize)
	text := oneLine(str(mp(alertNamed(g, dailyBlockedAlert)["annotations"])["description"]))
	for _, want := range []string{dailyBlockedFinalize, dailyBlockedRerun, dailyBlockedRedrive} {
		g.expect(strings.Contains(text, want), "%s description does not name %q", dailyBlockedAlert, want)
	}
	dailyBlockedPairs(g, dailyBlockedAlert+" description", text)
	docs := oneLine(g.read(dailyBlockedDocs))
	for _, want := range []string{dailyBlockedFinalize, dailyBlockedRerun, dailyBlockedRedrive, dailyBlockedClearRow, dailyBlockedWindow} {
		g.expect(strings.Contains(docs, want), "%s does not name %q", dailyBlockedDocs, want)
	}
	dailyBlockedPairs(g, dailyBlockedDocs, docs)
	row := ""
	for _, line := range strings.Split(g.read(dailyBlockedOperatorRel), "\n") {
		if strings.Contains(line, dailyBlockedOperatorRow) {
			row = line
		}
	}
	g.expect(row != "", "%s has no row for %q", dailyBlockedOperatorRel, dailyBlockedOperatorRow)
	for _, want := range []string{dailyBlockedFinalize, dailyBlockedRerun, dailyBlockedRedrive} {
		g.expect(strings.Contains(row, want), "%s row does not name %q", dailyBlockedOperatorRel, want)
	}
	dailyBlockedPairs(g, dailyBlockedOperatorRel, row)
}

// swapActions exchanges the two actions in one file.
func swapActions(rel string) func(*testing.T, string) {
	return func(t *testing.T, root string) {
		t.Helper()
		b := mustRead(t, rootPath(root, rel))
		if !strings.Contains(b, dailyBlockedRerun) || !strings.Contains(b, dailyBlockedRedrive) {
			t.Fatalf("plant %s: an action is not in the file, the swap did not land", rel)
		}
		const tmp = "\x00swap\x00"
		b = strings.ReplaceAll(b, dailyBlockedRedrive, tmp)
		b = strings.ReplaceAll(b, dailyBlockedRerun, dailyBlockedRedrive)
		writeFile(t, root, rel, strings.ReplaceAll(b, tmp, dailyBlockedRerun))
	}
}

func TestDailyBlockedRunAlertsGuardRealTree(t *testing.T) {
	runReal(t,
		check{"daily-blocked-alerts-are-the-group", dailyBlockedAlertsAreTheGroup},
		check{"daily-blocked-rule-text", dailyBlockedRuleText},
		check{"daily-blocked-metrics-are-emitted", dailyBlockedMetricsAreEmitted},
		check{"daily-blocked-docs-name-the-rules", dailyBlockedDocsNameTheRules},
		check{"daily-blocked-action-per-reason", dailyBlockedActionPerReason},
	)
}

func TestDailyBlockedRunAlertsGuardPlantedDefects(t *testing.T) {
	rules := []string{rulesRel, dailyBlockedGoRel}
	const marked = "sum(increase(" + dailyBlockedMarked + "[24h])) > 0"
	const reconcile = "sum(increase(" + dailyBlockedReconcileMn + "[24h])) > 0"
	perReason := []string{rulesRel, dailyBlockedDocs, dailyBlockedReasonsRel, dailyBlockedOperatorRel}
	runPlanted(t, []planted{
		{"daily-blocked-rule-reads-the-cleared-counter", rules, mut(rulesRel, marked, strings.Replace(marked, "marked", "cleared", 1)), dailyBlockedRuleText, "want"},
		{"daily-blocked-threshold-raised", rules, mut(rulesRel, marked, strings.Replace(marked, "> 0", "> 5", 1)), dailyBlockedRuleText, "want"},
		{"daily-blocked-window-shortened", rules, mut(rulesRel, marked, strings.Replace(marked, "[24h]", "[5m]", 1)), dailyBlockedRuleText, "want"},
		{"daily-blocked-reconcile-rule-reads-the-marked-counter", rules, mut(rulesRel, reconcile, strings.Replace(reconcile, "reconcile_failed", "marked", 1)), dailyBlockedRuleText, "want"},
		{"daily-blocked-for-added", rules, mut(rulesRel, "- alert: "+dailyBlockedAlert+"\n        expr: |\n          "+marked+"\n", "- alert: "+dailyBlockedAlert+"\n        expr: |\n          "+marked+"\n        for: 30m\n"), dailyBlockedRuleText, "has a `for`"},
		{"daily-blocked-rule-renamed", rules, mut(rulesRel, "- alert: "+dailyBlockedAlert, "- alert: "+dailyBlockedAlert+"X"), dailyBlockedAlertsAreTheGroup, "want exactly"},
		{"daily-blocked-counter-renamed-in-go", rules, mutAll(dailyBlockedGoRel, `"`+dailyBlockedMarked+`"`, `"dev_health_daily_metrics_runs_blocked_marked_renamed_total"`), dailyBlockedMetricsAreEmitted, "no longer emits"},
		{"daily-blocked-docs-lose-a-rule", []string{dailyBlockedDocs}, mutAll(dailyBlockedDocs, dailyBlockedReconcile, "TheRule"), dailyBlockedDocsNameTheRules, "does not name"},
		{"daily-blocked-alert-drops-the-rerun", perReason, mutAll(rulesRel, dailyBlockedRerun, "daily-start"), dailyBlockedActionPerReason, "description does not name"},
		{"daily-blocked-alert-drops-the-finalize-reason", perReason, mutAll(rulesRel, dailyBlockedFinalize, "finalize"), dailyBlockedActionPerReason, "description does not name"},
		{"daily-blocked-docs-drop-the-rerun", perReason, mutAll(dailyBlockedDocs, dailyBlockedRerun, "daily-start"), dailyBlockedActionPerReason, dailyBlockedDocs + " does not name"},
		{"daily-blocked-docs-drop-the-clear-row", perReason, mutAll(dailyBlockedDocs, dailyBlockedClearRow, "row stays listed by `daily-blocked`"), dailyBlockedActionPerReason, dailyBlockedDocs + " does not name"},
		{"daily-blocked-docs-drop-the-recompute-window", perReason, mutAll(dailyBlockedDocs, dailyBlockedWindow, "Window"), dailyBlockedActionPerReason, dailyBlockedDocs + " does not name"},
		{"daily-blocked-alert-swaps-the-actions", perReason, swapActions(rulesRel), dailyBlockedActionPerReason, "is paired with"},
		{"daily-blocked-docs-swap-the-actions", perReason, swapActions(dailyBlockedDocs), dailyBlockedActionPerReason, dailyBlockedDocs + ": "},
		{"daily-blocked-operator-commands-swap-the-actions", perReason, swapActions(dailyBlockedOperatorRel), dailyBlockedActionPerReason, dailyBlockedOperatorRel + ": "},
		{"daily-blocked-operator-commands-drop-the-rerun", perReason, mutAll(dailyBlockedOperatorRel, dailyBlockedRerun, "daily-start"), dailyBlockedActionPerReason, dailyBlockedOperatorRel + " row does not name"},
		{"daily-blocked-operator-commands-drop-the-redrive", perReason, mutAll(dailyBlockedOperatorRel, dailyBlockedRedrive, "redrive"), dailyBlockedActionPerReason, dailyBlockedOperatorRel + " row does not name"},
		{"daily-blocked-finalize-reason-renamed-in-go", perReason, mutAll(dailyBlockedReasonsRel, `"`+dailyBlockedFinalize+`"`, `"finalize_failed_terminal"`), dailyBlockedActionPerReason, "no longer defines the reason"},
	})
}
