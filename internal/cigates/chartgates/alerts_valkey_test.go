package chartgates_test

import (
	"encoding/json"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"
)

// The alerts on the in-chart Valkey (alerts/rules.yml, group valkey). The
// series come from the collector's redis receiver, which names them with dots
// (redis.keys.evicted, redis.memory.used) and writes no underscore form, so the
// rules select them by __name__.

const (
	valkeyGroup         = "valkey"
	valkeyEvictedAlert  = "ValkeyKeysEvicted"
	valkeyMemoryAlert   = "ValkeyMemoryNearMaxmemory"
	valkeyEvictedMetric = "redis.keys.evicted"
	valkeyMemoryMetric  = "redis.memory.used"
	// valkeyMaxmemory is 192mb in Valkey units (1mb = 1048576 bytes): the
	// maxmemory the S-V1 roll step sets for prod.
	valkeyMaxmemory        = 192 * 1048576
	valkeyMemoryThreshold  = 161061274
	valkeyThresholdComment = "0.8 x maxmemory 192mb set by roll step S-V1 (CHAOS-11630); change both together"
)

func oneLine(s string) string { return strings.Join(strings.Fields(s), " ") }

func valkeyAlertsAreTheGroup(g *guard) {
	groups := 0
	for _, grp := range ruleGroups(g) {
		if str(grp["name"]) == valkeyGroup {
			groups++
		}
	}
	g.expect(groups == 1, "%d groups are named %s, want exactly one", groups, valkeyGroup)
	var names []string
	for _, r := range groupRules(g, valkeyGroup) {
		names = append(names, str(r["alert"]))
	}
	g.expect(strings.Join(names, ",") == valkeyEvictedAlert+","+valkeyMemoryAlert, "group %s holds %v, want exactly %s and %s", valkeyGroup, names, valkeyEvictedAlert, valkeyMemoryAlert)
}

func valkeyEvictedAlertShape(g *guard) {
	a := alertNamed(g, valkeyEvictedAlert)
	want := `sum(increase({__name__="` + valkeyEvictedMetric + `"}[10m])) > 0`
	g.expect(oneLine(str(a["expr"])) == want, "%s expr is %q, want %q", valkeyEvictedAlert, oneLine(str(a["expr"])), want)
	_, hasFor := a["for"]
	g.expect(!hasFor, "%s has a `for`: one eviction in the window must page", valkeyEvictedAlert)
}

func valkeyMemoryAlertShape(g *guard) {
	a := alertNamed(g, valkeyMemoryAlert)
	want := `max({__name__="` + valkeyMemoryMetric + `"}) > ` + strconv.Itoa(valkeyMemoryThreshold)
	g.expect(oneLine(str(a["expr"])) == want, "%s expr is %q, want %q", valkeyMemoryAlert, oneLine(str(a["expr"])), want)
	g.expect(str(a["for"]) == "10m", "%s `for` is %q, want 10m", valkeyMemoryAlert, str(a["for"]))
}

// The threshold is 80% of the maxmemory, rounded up to a whole byte.
func valkeyMemoryThresholdIs80PercentOfMaxmemory(g *guard) {
	g.expect(valkeyMemoryThreshold*10 >= valkeyMaxmemory*8 && (valkeyMemoryThreshold-1)*10 < valkeyMaxmemory*8,
		"threshold %d is not 0.8 x %d rounded up", valkeyMemoryThreshold, valkeyMaxmemory)
}

// The comment that ties the threshold to the roll step sits on the memory rule.
var valkeyMemoryRuleWithComment = regexp.MustCompile(`(?m)^\s*# ` + regexp.QuoteMeta(valkeyThresholdComment) + `\n\s*- alert: ` + valkeyMemoryAlert + `\n`)

func valkeyMemoryThresholdCommentPinned(g *guard) {
	g.expect(valkeyMemoryRuleWithComment.MatchString(g.read(rulesRel)), "the line before `- alert: %s` is not the comment %q", valkeyMemoryAlert, valkeyThresholdComment)
}

func valkeyAlertsRouted(g *guard) {
	for _, name := range []string{valkeyEvictedAlert, valkeyMemoryAlert} {
		a := alertNamed(g, name)
		labels := mp(a["labels"])
		g.expect(str(labels["severity"]) == "warning", "%s severity is %q, want warning", name, str(labels["severity"]))
		g.expect(str(labels["team"]) == "platform", "%s team is %q, want platform", name, str(labels["team"]))
		g.expect(str(mp(a["annotations"])["summary"]) != "", "%s has no summary", name)
		g.expect(strings.Contains(str(mp(a["annotations"])["description"]), "auth-service-scrape-and-alerts.md"), "%s description does not point at the import runbook", name)
	}
}

const valkeyRunbook = "docs/operate/runbooks/auth-service-scrape-and-alerts.md"

var (
	runbookJSONBlock = regexp.MustCompile("(?s)```json\n(.*?)\n```")
	exprThreshold    = regexp.MustCompile(`> (\d+)$`)
	exprRange        = regexp.MustCompile(`\[(\d+[smh])\]`)
	exprMetricName   = regexp.MustCompile(`__name__="([^"]+)"`)
	exprOuterFunc    = regexp.MustCompile(`^(\w+)\(`)
)

// importRule is the part of a SigNoz builder threshold rule JSON the import
// must keep equal to the rule text.
type importRule struct {
	Alert     string `json:"alert"`
	RuleType  string `json:"ruleType"`
	Condition struct {
		CompositeQuery struct {
			QueryType string `json:"queryType"`
			Queries   []struct {
				Spec struct {
					Aggregations []struct {
						MetricName       string `json:"metricName"`
						TimeAggregation  string `json:"timeAggregation"`
						SpaceAggregation string `json:"spaceAggregation"`
					} `json:"aggregations"`
				} `json:"spec"`
			} `json:"queries"`
		} `json:"compositeQuery"`
		Thresholds struct {
			Spec []struct {
				Target    float64 `json:"target"`
				Op        string  `json:"op"`
				MatchType string  `json:"matchType"`
			} `json:"spec"`
		} `json:"thresholds"`
	} `json:"condition"`
	Evaluation struct {
		Spec struct {
			EvalWindow string `json:"evalWindow"`
		} `json:"spec"`
	} `json:"evaluation"`
	Labels map[string]string `json:"labels"`
}

// valkeyRunbookSection is the "Valkey memory and eviction rules" section only.
// The other rules in the runbook (Step 2) keep their own form.
func valkeyRunbookSection(g *guard) (string, bool) {
	section := g.read(valkeyRunbook)
	i := strings.Index(section, "## Valkey memory and eviction rules")
	if i < 0 {
		g.failf("%s has no section \"Valkey memory and eviction rules\"", valkeyRunbook)
		return "", false
	}
	section = section[i:]
	if j := strings.Index(section[1:], "\n## "); j >= 0 {
		section = section[:j+1]
	}
	return section, true
}

// The import JSON in the runbook is the builder threshold rule saved in
// SigNoz (PromQL rules do not evaluate there): its metric, aggregations,
// threshold, operator, window, match type and labels must equal the rule text.
// The window is the rule's `for` (match "all the times") or, without a `for`,
// the range of its increase (match "at least once").
func valkeyRunbookImportMatchesRules(g *guard) {
	section, ok := valkeyRunbookSection(g)
	if !ok {
		return
	}
	imports := map[string]importRule{}
	for _, m := range runbookJSONBlock.FindAllStringSubmatch(section, -1) {
		var r importRule
		if err := json.Unmarshal([]byte(m[1]), &r); err != nil {
			g.failf("an import JSON block in %s does not parse: %v", valkeyRunbook, err)
			continue
		}
		if _, dup := imports[r.Alert]; dup {
			g.failf("the runbook imports %s twice", r.Alert)
		}
		imports[r.Alert] = r
	}
	g.expect(len(imports) == 2, "the runbook section imports %d rules, want 2", len(imports))
	for _, name := range []string{valkeyEvictedAlert, valkeyMemoryAlert} {
		rule, imp := alertNamed(g, name), imports[name]
		if imp.Alert == "" {
			g.failf("the runbook section has no import JSON for %s", name)
			continue
		}
		expr := oneLine(str(rule["expr"]))
		g.expect(imp.RuleType == "threshold_rule" && imp.Condition.CompositeQuery.QueryType == "builder", "%s import is ruleType %q queryType %q, want threshold_rule and builder", name, imp.RuleType, imp.Condition.CompositeQuery.QueryType)
		metric, outer := "", ""
		if m := exprMetricName.FindStringSubmatch(expr); m != nil {
			metric = m[1]
		}
		if m := exprOuterFunc.FindStringSubmatch(expr); m != nil {
			outer = m[1]
		}
		timeAgg := outer
		if strings.Contains(expr, "increase(") {
			timeAgg = "increase"
		}
		qs := imp.Condition.CompositeQuery.Queries
		if len(qs) != 1 || len(qs[0].Spec.Aggregations) != 1 {
			g.failf("%s import has %d builder queries, want 1 with 1 aggregation", name, len(qs))
		} else {
			a := qs[0].Spec.Aggregations[0]
			g.expect(a.MetricName == metric, "%s import metricName %q, want the rule metric %q", name, a.MetricName, metric)
			g.expect(a.TimeAggregation == timeAgg, "%s import timeAggregation %q, want %q", name, a.TimeAggregation, timeAgg)
			g.expect(a.SpaceAggregation == outer, "%s import spaceAggregation %q, want %q", name, a.SpaceAggregation, outer)
		}
		window, match := str(rule["for"]), "all_the_times"
		if window == "" {
			match = "at_least_once"
			if r := exprRange.FindStringSubmatch(expr); r != nil {
				window = r[1]
			}
		}
		d, err := time.ParseDuration(window)
		if err != nil {
			g.failf("%s has no window to compare (for %q): %v", name, str(rule["for"]), err)
		} else if got, perr := time.ParseDuration(imp.Evaluation.Spec.EvalWindow); perr != nil || got != d {
			g.failf("%s import evalWindow %q, want %q", name, imp.Evaluation.Spec.EvalWindow, window)
		}
		th := exprThreshold.FindStringSubmatch(expr)
		if th == nil {
			g.failf("%s expr %q has no `> N` threshold", name, expr)
		}
		specs := imp.Condition.Thresholds.Spec
		if len(specs) != 1 {
			g.failf("%s import has %d thresholds, want 1", name, len(specs))
		} else {
			if th != nil {
				want, _ := strconv.ParseFloat(th[1], 64)
				g.expect(specs[0].Target == want, "%s import target %v, want the rule threshold %v", name, specs[0].Target, want)
			}
			g.expect(specs[0].Op == "above", "%s import op %q, want above", name, specs[0].Op)
			g.expect(specs[0].MatchType == match, "%s import matchType %q, want %q", name, specs[0].MatchType, match)
		}
		labels := mp(rule["labels"])
		g.expect(len(imp.Labels) == len(labels), "%s import labels %v, want %v", name, imp.Labels, labels)
		for k, v := range labels {
			g.expect(imp.Labels[k] == str(v), "%s import label %s=%q, want %q", name, k, imp.Labels[k], str(v))
		}
	}
}

// Scope: the Valkey section ONLY. Step 2 of the runbook still holds PromQL
// blocks, so this guard must not read the whole file.
func valkeySectionHasNoPromQLRule(g *guard) {
	section, ok := valkeyRunbookSection(g)
	if !ok {
		return
	}
	for _, banned := range []string{"promql_rule", "promQueries", `"queryType": "promql"`} {
		g.expect(!strings.Contains(section, banned), "the Valkey runbook section contains %q: PromQL rules do not evaluate on this SigNoz", banned)
	}
}

func TestValkeyAlertGuardsRealTree(t *testing.T) {
	runReal(t,
		check{"valkey-alerts-are-the-group", valkeyAlertsAreTheGroup},
		check{"valkey-evicted-alert-shape", valkeyEvictedAlertShape},
		check{"valkey-memory-alert-shape", valkeyMemoryAlertShape},
		check{"valkey-memory-threshold-80-percent", valkeyMemoryThresholdIs80PercentOfMaxmemory},
		check{"valkey-memory-threshold-comment", valkeyMemoryThresholdCommentPinned},
		check{"valkey-alerts-routed", valkeyAlertsRouted},
		check{"valkey-runbook-import-matches-rules", valkeyRunbookImportMatchesRules},
		check{"valkey-runbook-section-has-no-promql-rule", valkeySectionHasNoPromQLRule},
	)
}

func TestValkeyAlertGuardsPlantedDefects(t *testing.T) {
	rules := []string{rulesRel}
	runPlanted(t, []planted{
		{"evicted-alert-reads-the-underscore-name", rules, mut(rulesRel, `{__name__="redis.keys.evicted"}`, `redis_keys_evicted`), valkeyEvictedAlertShape, "expr is"},
		{"evicted-alert-gets-a-for", rules, mut(rulesRel, "[10m])) > 0\n", "[10m])) > 0\n        for: 5m\n"), valkeyEvictedAlertShape, "has a `for`"},
		{"memory-threshold-moved", rules, mut(rulesRel, "> 161061274", "> 201326592"), valkeyMemoryAlertShape, "expr is"},
		{"memory-for-shortened", rules, mut(rulesRel, "> 161061274\n        for: 10m", "> 161061274\n        for: 5m"), valkeyMemoryAlertShape, "`for` is"},
		{"threshold-comment-dropped", rules, mut(rulesRel, "# "+valkeyThresholdComment+"\n", ""), valkeyMemoryThresholdCommentPinned, "is not the comment"},
		{"memory-alert-paged-critical", rules, mut(rulesRel, "> 161061274\n        for: 10m\n        labels:\n          severity: warning", "> 161061274\n        for: 10m\n        labels:\n          severity: critical"), valkeyAlertsRouted, "severity is"},
		{"third-rule-in-the-group", rules, appendText(rulesRel, "      - alert: ValkeyExtra\n        expr: up == 0\n"), valkeyAlertsAreTheGroup, "want exactly"},
		{"second-valkey-group", rules, appendText(rulesRel, "  - name: valkey\n    rules:\n      - alert: ValkeyUnnoticedExtra\n        expr: up == 0\n"), valkeyAlertsAreTheGroup, "groups are named valkey"},
		{"runbook-target-drift", []string{rulesRel, valkeyRunbook}, mut(valkeyRunbook, `"target": 161061274,`, `"target": 999999999,`), valkeyRunbookImportMatchesRules, "import target"},
		{"runbook-metric-drift", []string{rulesRel, valkeyRunbook}, mut(valkeyRunbook, `"metricName": "redis.keys.evicted"`, `"metricName": "redis.keys.evicted_total"`), valkeyRunbookImportMatchesRules, "import metricName"},
		{"runbook-time-aggregation-drift", []string{rulesRel, valkeyRunbook}, mut(valkeyRunbook, `"timeAggregation": "increase"`, `"timeAggregation": "rate"`), valkeyRunbookImportMatchesRules, "import timeAggregation"},
		{"runbook-space-aggregation-drift", []string{rulesRel, valkeyRunbook}, mut(valkeyRunbook, `"spaceAggregation": "sum"`, `"spaceAggregation": "avg"`), valkeyRunbookImportMatchesRules, "import spaceAggregation"},
		{"runbook-window-drift", []string{rulesRel, valkeyRunbook}, mut(valkeyRunbook, `"evalWindow": "10m"`, `"evalWindow": "5m"`), valkeyRunbookImportMatchesRules, "import evalWindow"},
		{"runbook-match-drift", []string{rulesRel, valkeyRunbook}, mut(valkeyRunbook, `"matchType": "all_the_times"`, `"matchType": "at_least_once"`), valkeyRunbookImportMatchesRules, "import matchType"},
		{"runbook-op-drift", []string{rulesRel, valkeyRunbook}, mut(valkeyRunbook, `"op": "above"`, `"op": "below"`), valkeyRunbookImportMatchesRules, "import op"},
		{"runbook-label-drift", []string{rulesRel, valkeyRunbook}, mutAll(valkeyRunbook, `"severity": "warning"`, `"severity": "critical"`), valkeyRunbookImportMatchesRules, "import label severity"},
		{"runbook-promql-queries-added", []string{rulesRel, valkeyRunbook}, mut(valkeyRunbook, `"alert": "ValkeyKeysEvicted",`, `"alert": "ValkeyKeysEvicted", "promQueries": {},`), valkeySectionHasNoPromQLRule, "promQueries"},
		{"runbook-rule-type-back-to-promql", []string{rulesRel, valkeyRunbook}, mut(valkeyRunbook, `"ruleType": "threshold_rule"`, `"ruleType": "promql_rule"`), valkeySectionHasNoPromQLRule, "promql_rule"},
	})
}
