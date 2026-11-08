package chartgates_test

import (
	"regexp"
	"strings"
	"testing"
)

// The query-api PDP gate alerts (alerts/rules.yml, group query_api_pdp). The metrics, the result values and the
// labels the rules read must exist in the emitting source and in the label census.

const (
	qapiGroup       = "query_api_pdp"
	qapiInvalid     = "QueryAPIPDPConfigInvalid"
	qapiRefusing    = "QueryAPIPDPRefusing"
	qapiPDPRel      = "internal/queryapi/pdp/pdp.go"
	qapiResults     = "dev_health_query_api_pdp_results_total"
	qapiConfigBad   = "dev_health_query_api_pdp_config_invalid"
	qapiEnabled     = "dev_health_query_api_pdp_enabled"
	qapiRefuseRegex = `result=~"unavailable|no_decision_point|not_carried_out|not_configured"`
)

func qapiAlertsAreTheGroup(g *guard) {
	var names []string
	for _, r := range groupRules(g, qapiGroup) {
		names = append(names, str(r["alert"]))
	}
	g.expect(strings.Join(names, ",") == qapiInvalid+","+qapiRefusing, "group %s holds %v, want %s and %s", qapiGroup, names, qapiInvalid, qapiRefusing)
}

func qapiRuleText(g *guard) {
	a := alertNamed(g, qapiInvalid)
	g.expect(oneLine(str(a["expr"])) == "max("+qapiConfigBad+") > 0", "%s expr is %q", qapiInvalid, oneLine(str(a["expr"])))
	b := alertNamed(g, qapiRefusing)
	want := `sum by (result) (increase(` + qapiResults + `{` + qapiRefuseRegex + `}[15m])) > 0`
	g.expect(oneLine(str(b["expr"])) == want, "%s expr is %q, want %q", qapiRefusing, oneLine(str(b["expr"])), want)
	for _, r := range []doc{a, b} {
		g.expect(str(r["for"]) == "5m", "%s `for` is %q, want a 5m noise window (no wall-clock gate on a flip)", str(r["alert"]), str(r["for"]))
		g.expect(str(mp(r["labels"])["team"]) == "platform", "%s labels are %v, want team platform", str(r["alert"]), r["labels"])
	}
}

func qapiRuleNamesEachReason(g *guard) {
	for name, reasons := range map[string][]string{
		qapiInvalid:  {"QUERY_API_PDP_MODE", "AUTH_PROXY_URL", "QUERY_API_AUTHZ_ENFORCE", "QUERY_API_HEADERS_AUTHZ_ENFORCE"},
		qapiRefusing: {"unavailable", "no_decision_point", "not_carried_out", "not_configured"},
	} {
		desc := str(mp(alertNamed(g, name)["annotations"])["description"])
		for _, reason := range reasons {
			g.expect(strings.Contains(desc, reason), "%s description does not name %q", name, reason)
		}
	}
}

func qapiMetricsAreEmitted(g *guard) {
	src := g.read(qapiPDPRel)
	for _, m := range []string{qapiResults, qapiConfigBad, qapiEnabled} {
		g.expect(strings.Contains(src, `"`+m+`"`), "%s does not emit %s", qapiPDPRel, m)
	}
	for _, v := range []string{"unavailable", "no_decision_point", "not_carried_out", "not_configured"} {
		g.expect(strings.Contains(src, `= "`+v+`"`), "%s has no result value %q, which the rule reads", qapiPDPRel, v)
	}
	for _, name := range []string{qapiInvalid, qapiRefusing} {
		for _, m := range regexp.MustCompile(`dev_health_[a-z_]+`).FindAllString(str(alertNamed(g, name)["expr"]), -1) {
			g.expect(strings.Contains(src, `"`+m+`"`), "%s reads %s, which %s does not emit", name, m, qapiPDPRel)
		}
	}
}

func qapiLabelCensus(g *guard) {
	rows := map[string]string{}
	for _, line := range strings.Split(g.read(shadowLabelsRel), "\n") {
		f := strings.Split(line, "\t")
		if len(f) >= 2 {
			rows[f[0]] = f[1]
		}
	}
	g.expect(rows[qapiResults] == "action,mode,result", "%s row for %s is %q, want action,mode,result", shadowLabelsRel, qapiResults, rows[qapiResults])
	g.expect(rows[qapiConfigBad] == "" && rows[qapiEnabled] == "", "%s: %s and %s must carry no labels", shadowLabelsRel, qapiConfigBad, qapiEnabled)
}

func TestQueryAPIPDPAlertGuardsRealTree(t *testing.T) {
	runReal(t,
		check{"qapi-alerts-are-the-group", qapiAlertsAreTheGroup},
		check{"qapi-rule-text", qapiRuleText},
		check{"qapi-rule-names-each-reason", qapiRuleNamesEachReason},
		check{"qapi-metrics-are-emitted", qapiMetricsAreEmitted},
		check{"qapi-label-census", qapiLabelCensus},
	)
}

func TestQueryAPIPDPAlertGuardsPlantedDefects(t *testing.T) {
	rules := []string{rulesRel}
	both := []string{rulesRel, qapiPDPRel}
	runPlanted(t, []planted{
		{"invalid-for-lengthened", rules, mut(rulesRel, "max(dev_health_query_api_pdp_config_invalid) > 0\n        for: 5m", "max(dev_health_query_api_pdp_config_invalid) > 0\n        for: 24h"), qapiRuleText, "noise window"},
		{"refusing-drops-a-result", rules, mut(rulesRel, "unavailable|no_decision_point|not_carried_out|not_configured", "unavailable|no_decision_point|not_carried_out"), qapiRuleText, "want"},
		{"refusing-groups-by-action", rules, mut(rulesRel, "sum by (result) (increase(dev_health_query_api_pdp_results_total", "sum by (result, action) (increase(dev_health_query_api_pdp_results_total"), qapiRuleText, "want"},
		{"description-loses-a-reason", rules, mutAll(rulesRel, "not_carried_out", "not_done"), qapiRuleNamesEachReason, "not_carried_out"},
		{"metric-renamed-in-source", both, mutAll(qapiPDPRel, `"dev_health_query_api_pdp_config_invalid"`, `"dev_health_query_api_pdp_cfg_invalid"`), qapiMetricsAreEmitted, "does not emit"},
	})
}
