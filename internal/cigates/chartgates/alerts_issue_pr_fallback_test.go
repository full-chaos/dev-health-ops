package chartgates_test

import (
	"regexp"
	"strings"
	"testing"
)

// The issue-PR fallback invariant alert (alerts/rules.yml, group issue_pr_fallback_invariant). The
// metric name is read from the Go constant, so a rename of the counter fails here instead of leaving a
// rule on a name nobody emits.

const (
	issuePRFallbackGroup = "issue_pr_fallback_invariant"
	issuePRFallbackAlert = "IssuePRFallbackInvariantBreached"
	issuePRFallbackGoRel = "internal/jobs/workgraph/issuepredges/invariant.go"
	issuePRFallbackDocs  = "docs/contribute/architecture/data-and-storage.md"
)

var issuePRFallbackConst = regexp.MustCompile(`FallbackBesideNativeCounter\s*=\s*"([a-z_]+)"`)

func issuePRFallbackMetric(g *guard) string {
	m := issuePRFallbackConst.FindStringSubmatch(g.read(issuePRFallbackGoRel))
	if m == nil {
		g.failf("%s has no FallbackBesideNativeCounter constant", issuePRFallbackGoRel)
		return ""
	}
	return m[1]
}

func issuePRFallbackAlertIsTheGroup(g *guard) {
	var names []string
	for _, r := range groupRules(g, issuePRFallbackGroup) {
		names = append(names, str(r["alert"]))
	}
	g.expect(strings.Join(names, ",") == issuePRFallbackAlert, "group %s holds %v, want exactly %s", issuePRFallbackGroup, names, issuePRFallbackAlert)
}

func issuePRFallbackRuleText(g *guard) {
	a := alertNamed(g, issuePRFallbackAlert)
	want := "sum(increase(" + issuePRFallbackMetric(g) + "[24h])) > 0"
	g.expect(oneLine(str(a["expr"])) == want, "%s expr is %q, want %q", issuePRFallbackAlert, oneLine(str(a["expr"])), want)
	_, hasFor := a["for"]
	g.expect(!hasFor, "%s has a `for`: one breach in the window must alert", issuePRFallbackAlert)
	labels := mp(a["labels"])
	g.expect(str(labels["severity"]) == "warning" && str(labels["team"]) == "platform", "%s labels are %v, want severity warning and team platform", issuePRFallbackAlert, labels)
}

func issuePRFallbackDocsNameTheRule(g *guard) {
	g.expect(strings.Contains(g.read(issuePRFallbackDocs), issuePRFallbackAlert), "%s does not name %s", issuePRFallbackDocs, issuePRFallbackAlert)
	g.expect(strings.Contains(g.read(orgActivityDocRel), issuePRFallbackAlert), "%s does not name %s", orgActivityDocRel, issuePRFallbackAlert)
}

func TestIssuePRFallbackAlertGuardsRealTree(t *testing.T) {
	runReal(t,
		check{"issue-pr-fallback-alert-is-the-group", issuePRFallbackAlertIsTheGroup},
		check{"issue-pr-fallback-rule-text", issuePRFallbackRuleText},
		check{"issue-pr-fallback-docs-name-the-rule", issuePRFallbackDocsNameTheRule},
	)
}

func TestIssuePRFallbackAlertGuardsPlantedDefects(t *testing.T) {
	rules := []string{rulesRel, issuePRFallbackGoRel}
	const expr = "sum(increase(workgraph_issue_pr_fallback_beside_native_rows_total[24h])) > 0"
	runPlanted(t, []planted{
		{"issue-pr-fallback-rule-reads-another-metric", rules, mut(rulesRel, expr, strings.Replace(expr, "beside_native_rows", "beside_native_issue_rows", 1)), issuePRFallbackRuleText, "want"},
		{"issue-pr-fallback-threshold-raised", rules, mut(rulesRel, expr, strings.Replace(expr, "> 0", "> 5", 1)), issuePRFallbackRuleText, "want"},
		{"issue-pr-fallback-window-shortened", rules, mut(rulesRel, expr, strings.Replace(expr, "[24h]", "[1h]", 1)), issuePRFallbackRuleText, "want"},
		{"issue-pr-fallback-rule-renamed", rules, mut(rulesRel, "- alert: "+issuePRFallbackAlert, "- alert: "+issuePRFallbackAlert+"X"), issuePRFallbackAlertIsTheGroup, "want exactly"},
		{"issue-pr-fallback-docs-lose-the-rule", []string{issuePRFallbackDocs, orgActivityDocRel}, mutAll(issuePRFallbackDocs, issuePRFallbackAlert, "TheRule"), issuePRFallbackDocsNameTheRule, "does not name"},
	})
}
