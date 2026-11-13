package chartgates_test

import (
	"regexp"
	"strings"
	"testing"
)

// The sync plan executed-proof alert (alerts/rules.yml, group sync_plan_gate). The metric name and the
// outcome value are read from the Go telemetry file, so a rename there fails here instead of leaving a rule
// on a series nobody emits. The rule is scoped to the work-items claim, so pairs without proof yet (PagerDuty
// datasets) do not hold it red.

const (
	syncPlanGateGroup = "sync_plan_gate"
	syncPlanGateAlert = "SyncPlanExecutedProofUnsatisfied"
	syncPlanGateGoRel = "internal/scheduler/sync/plan_gate_telemetry.go"
	syncPlanGateDocs  = "docs/operate/observe/dashboards-and-alerts.md"
)

var (
	syncPlanGateMetricRe  = regexp.MustCompile(`# TYPE ([a-z_]+) counter`)
	syncPlanGateOutcomeRe = regexp.MustCompile(`planGateOutcomeExecutedProofUnsatisfied\s*=\s*"([a-z_]+)"`)
)

func syncPlanGateWantExpr(g *guard) string {
	src := g.read(syncPlanGateGoRel)
	metric := syncPlanGateMetricRe.FindStringSubmatch(src)
	outcome := syncPlanGateOutcomeRe.FindStringSubmatch(src)
	if metric == nil || outcome == nil {
		g.failf("%s has no counter TYPE line or no planGateOutcomeExecutedProofUnsatisfied constant", syncPlanGateGoRel)
		return ""
	}
	return `sum by (provider) (increase(` + metric[1] + `{dataset="work-items",outcome="` + outcome[1] + `"}[2h])) > 0`
}

func syncPlanGateAlertIsTheGroup(g *guard) {
	var names []string
	for _, r := range groupRules(g, syncPlanGateGroup) {
		names = append(names, str(r["alert"]))
	}
	g.expect(strings.Join(names, ",") == syncPlanGateAlert, "group %s holds %v, want exactly %s", syncPlanGateGroup, names, syncPlanGateAlert)
}

func syncPlanGateRuleText(g *guard) {
	a := alertNamed(g, syncPlanGateAlert)
	want := syncPlanGateWantExpr(g)
	g.expect(oneLine(str(a["expr"])) == want, "%s expr is %q, want %q", syncPlanGateAlert, oneLine(str(a["expr"])), want)
	_, hasFor := a["for"]
	g.expect(!hasFor, "%s has a `for`: one refusal in the window must alert", syncPlanGateAlert)
	labels := mp(a["labels"])
	g.expect(str(labels["severity"]) == "warning" && str(labels["team"]) == "platform", "%s labels are %v, want severity warning and team platform", syncPlanGateAlert, labels)
}

func syncPlanGateDocsNameTheRule(g *guard) {
	g.expect(strings.Contains(g.read(syncPlanGateDocs), syncPlanGateAlert), "%s does not name %s", syncPlanGateDocs, syncPlanGateAlert)
}

func TestSyncPlanGateAlertGuardsRealTree(t *testing.T) {
	runReal(t,
		check{"sync-plan-gate-alert-is-the-group", syncPlanGateAlertIsTheGroup},
		check{"sync-plan-gate-rule-text", syncPlanGateRuleText},
		check{"sync-plan-gate-docs-name-the-rule", syncPlanGateDocsNameTheRule},
	)
}

func TestSyncPlanGateAlertGuardsPlantedDefects(t *testing.T) {
	rules := []string{rulesRel, syncPlanGateGoRel}
	const expr = `sum by (provider) (increase(sync_plan_gate_total{dataset="work-items",outcome="executed_proof_unsatisfied"}[2h])) > 0`
	runPlanted(t, []planted{
		{"sync-plan-gate-rule-reads-another-outcome", rules, mut(rulesRel, expr, strings.Replace(expr, "executed_proof_unsatisfied", "planned", 1)), syncPlanGateRuleText, "want"},
		{"sync-plan-gate-rule-loses-work-items-scope", rules, mut(rulesRel, expr, strings.Replace(expr, `dataset="work-items",`, "", 1)), syncPlanGateRuleText, "want"},
		{"sync-plan-gate-threshold-raised", rules, mut(rulesRel, expr, strings.Replace(expr, "> 0", "> 5", 1)), syncPlanGateRuleText, "want"},
		{"sync-plan-gate-window-shortened", rules, mut(rulesRel, expr, strings.Replace(expr, "[2h]", "[5m]", 1)), syncPlanGateRuleText, "want"},
		{"sync-plan-gate-outcome-renamed-in-go", rules, mut(syncPlanGateGoRel, `planGateOutcomeExecutedProofUnsatisfied = "executed_proof_unsatisfied"`, `planGateOutcomeExecutedProofUnsatisfied = "executed_proof_missing"`), syncPlanGateRuleText, "want"},
		{"sync-plan-gate-rule-renamed", rules, mut(rulesRel, "- alert: "+syncPlanGateAlert, "- alert: "+syncPlanGateAlert+"X"), syncPlanGateAlertIsTheGroup, "want exactly"},
		{"sync-plan-gate-docs-lose-the-rule", []string{syncPlanGateDocs}, mutAll(syncPlanGateDocs, syncPlanGateAlert, "TheRule"), syncPlanGateDocsNameTheRule, "does not name"},
	})
}
