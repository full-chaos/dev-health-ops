package chartgates_test

import (
	"bytes"
	"encoding/json"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"
)

// The SigNoz import JSON in the auth-service scrape and alerts runbook (the
// whole file: Step 2 and the Valkey section). PromQL rules do not evaluate on
// this SigNoz, so every block is a builder threshold rule. Each block is
// parsed with no unknown field allowed, and every field is pinned: derived
// from the rule text in alerts/rules.yml where the rule text decides it, and
// from the table below where the builder form decides it.

type rbRule struct {
	Alert         string `json:"alert"`
	AlertType     string `json:"alertType"`
	RuleType      string `json:"ruleType"`
	SchemaVersion string `json:"schemaVersion"`
	Condition     struct {
		CompositeQuery struct {
			QueryType string `json:"queryType"`
			PanelType string `json:"panelType"`
			Unit      string `json:"unit"`
			Queries   []struct {
				Type string `json:"type"`
				Spec struct {
					Name         string `json:"name"`
					Signal       string `json:"signal"`
					StepInterval int    `json:"stepInterval"`
					Disabled     bool   `json:"disabled"`
					Aggregations []struct {
						MetricName       string `json:"metricName"`
						TimeAggregation  string `json:"timeAggregation"`
						SpaceAggregation string `json:"spaceAggregation"`
					} `json:"aggregations"`
					Filter *struct {
						Expression string `json:"expression"`
					} `json:"filter"`
					GroupBy []struct {
						Name string `json:"name"`
					} `json:"groupBy"`
				} `json:"spec"`
			} `json:"queries"`
		} `json:"compositeQuery"`
		SelectedQueryName string `json:"selectedQueryName"`
		Thresholds        struct {
			Kind string `json:"kind"`
			Spec []struct {
				Name      string  `json:"name"`
				Target    float64 `json:"target"`
				Op        string  `json:"op"`
				MatchType string  `json:"matchType"`
			} `json:"spec"`
		} `json:"thresholds"`
	} `json:"condition"`
	Evaluation struct {
		Kind string `json:"kind"`
		Spec struct {
			EvalWindow string `json:"evalWindow"`
			Frequency  string `json:"frequency"`
		} `json:"spec"`
	} `json:"evaluation"`
	NotificationSettings struct {
		UsePolicy bool `json:"usePolicy"`
	} `json:"notificationSettings"`
	Labels      map[string]string `json:"labels"`
	Annotations map[string]string `json:"annotations"`
}

// rbWant is what the builder form decides for one rule: the pieces that
// alerts/rules.yml cannot say (unit, filter text, group-by key, annotations).
// ruleLabel is the `by (...)` label of the rule expr; groupBy is the builder
// key that carries it (the scrape's `instance` label is the resource attribute
// service.instance.id in SigNoz).
type rbWant struct {
	alert       string
	unit        string
	filter      string
	ruleLabel   string
	groupBy     string
	summary     string
	description string
}

var rbWants = []rbWant{
	{
		alert:       "AuthServiceRateLimitStoreErrors",
		filter:      "limit REGEXP 'auth_svc_.*'",
		ruleLabel:   "limit",
		groupBy:     "limit",
		summary:     "auth-service rate-limit store errors on {{$labels.limit}}",
		description: "The shared rate-limit store failed for limiter {{$labels.limit}}. Limited auth routes answer a closed 500 while it fails, by design. Next: Rate-limit store down in docs/reference/auth/readiness-and-slo.md.",
	},
	{
		alert:       "AuthServiceRateLimitStoreErrorsHigh",
		filter:      "limit REGEXP 'auth_svc_.*'",
		ruleLabel:   "limit",
		groupBy:     "limit",
		summary:     "Auth rate-limit store is failing at volume",
		description: "Limit {{$labels.limit}}: more than 20 store errors in ten minutes; callers on that route are being refused.",
	},
	{
		alert:       "AuthServiceRateLimitStoreNotReady",
		filter:      "check = 'valkey' AND service.name = 'auth-service'",
		ruleLabel:   "instance",
		groupBy:     "service.instance.id",
		summary:     "auth-service cannot reach its rate-limit store",
		description: "The required readiness check valkey has failed for five minutes on a replica: /readyz answers 503 and limited auth routes answer a closed 500. The alert is grouped by service.instance.id. Next: Rate-limit store down in docs/reference/auth/readiness-and-slo.md.",
	},
	{
		alert:       valkeyEvictedAlert,
		summary:     "Valkey evicted keys at maxmemory",
		description: "The in-chart Valkey evicted keys because it reached maxmemory. Next: redis.memory.used and redis.db.keys, then valkey.maxmemory.",
	},
	{
		alert:       valkeyMemoryAlert,
		unit:        "bytes",
		summary:     "Valkey used memory above 80% of maxmemory for ten minutes",
		description: "redis.memory.used is above 161061274 bytes (80% of maxmemory 192mb) for ten minutes. Next: redis.db.keys and redis.keys.evicted, then valkey.maxmemory (container limit 256Mi).",
	},
}

var (
	exprRange       = regexp.MustCompile(`\[(\d+[smh])\]`)
	exprMetricName  = regexp.MustCompile(`__name__="([^"]+)"`)
	rbExprHead      = regexp.MustCompile(`^(\w+)(?: by \(([\w, ]+)\))? ?\(`)
	rbExprThreshold = regexp.MustCompile(`(>|==) (\d+)$`)
	rbExprMatcher   = regexp.MustCompile(`(\w+)(=~|=)"([^"]*)"`)
	rbExprMetric    = regexp.MustCompile(`dev_health_\w+`)
	rbBlockHead     = regexp.MustCompile("(?s)```json\n(.*?)\n```")
)

func rbRuleAndInterval(g *guard, name string) (doc, string) {
	for _, grp := range ruleGroups(g) {
		for _, r := range list(grp["rules"]) {
			if str(mp(r)["alert"]) == name {
				return mp(r), str(grp["interval"])
			}
		}
	}
	g.failf("alerts/rules.yml has no rule %s", name)
	return nil, ""
}

func rbPin(g *guard, name, field string, got, want any) {
	g.expect(got == want, "%s import %s is %v, want %v", name, field, got, want)
}

func rbDuration(g *guard, name, field, got, want string) {
	a, errA := time.ParseDuration(got)
	b, errB := time.ParseDuration(want)
	g.expect(errA == nil && errB == nil && a == b, "%s import %s is %q, want %q", name, field, got, want)
}

func runbookBuilderRulesMatchRules(g *guard) {
	text := g.read(valkeyRunbook)
	imports := map[string]rbRule{}
	for _, m := range rbBlockHead.FindAllStringSubmatch(text, -1) {
		var r rbRule
		dec := json.NewDecoder(bytes.NewReader([]byte(m[1])))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&r); err != nil {
			g.failf("an import JSON block in %s does not parse (no unknown field is allowed): %v", valkeyRunbook, err)
			continue
		}
		if _, dup := imports[r.Alert]; dup {
			g.failf("the runbook imports %s twice", r.Alert)
		}
		imports[r.Alert] = r
	}
	g.expect(len(imports) == len(rbWants), "the runbook imports %d rules, want %d", len(imports), len(rbWants))
	for _, want := range rbWants {
		name, imp := want.alert, imports[want.alert]
		if imp.Alert == "" {
			g.failf("the runbook has no import JSON for %s", name)
			continue
		}
		rule, interval := rbRuleAndInterval(g, name)
		if rule == nil {
			continue
		}
		expr := oneLine(str(rule["expr"]))

		rbPin(g, name, "alertType", imp.AlertType, "METRIC_BASED_ALERT")
		rbPin(g, name, "ruleType", imp.RuleType, "threshold_rule")
		rbPin(g, name, "schemaVersion", imp.SchemaVersion, "v2alpha1")
		cq := imp.Condition.CompositeQuery
		rbPin(g, name, "queryType", cq.QueryType, "builder")
		rbPin(g, name, "panelType", cq.PanelType, "graph")
		rbPin(g, name, "unit", cq.Unit, want.unit)
		rbPin(g, name, "selectedQueryName", imp.Condition.SelectedQueryName, "A")
		rbPin(g, name, "evaluation kind", imp.Evaluation.Kind, "rolling")
		rbPin(g, name, "usePolicy", imp.NotificationSettings.UsePolicy, true)
		rbDuration(g, name, "frequency", imp.Evaluation.Spec.Frequency, interval)
		rbPin(g, name, "summary annotation", imp.Annotations["summary"], want.summary)
		rbPin(g, name, "description annotation", imp.Annotations["description"], want.description)
		g.expect(len(imp.Annotations) == 2, "%s import has %d annotations, want summary and description", name, len(imp.Annotations))

		head := rbExprHead.FindStringSubmatch(expr)
		if head == nil {
			g.failf("%s expr %q has no leading function", name, expr)
			continue
		}
		outer := head[1]
		timeAgg := outer
		switch {
		case strings.Contains(expr, "increase("):
			timeAgg = "increase"
		case strings.Contains(expr, "rate("):
			timeAgg = "rate"
		}
		metric := ""
		if m := exprMetricName.FindStringSubmatch(expr); m != nil {
			metric = m[1]
		} else {
			metric = rbExprMetric.FindString(expr)
		}

		qs := cq.Queries
		if len(qs) != 1 || len(qs[0].Spec.Aggregations) != 1 {
			g.failf("%s import has %d builder queries, want 1 with 1 aggregation", name, len(qs))
		} else {
			q, a := qs[0], qs[0].Spec.Aggregations[0]
			rbPin(g, name, "query type", q.Type, "builder_query")
			rbPin(g, name, "query name", q.Spec.Name, "A")
			rbPin(g, name, "signal", q.Spec.Signal, "metrics")
			rbPin(g, name, "stepInterval", q.Spec.StepInterval, 60)
			rbPin(g, name, "disabled", q.Spec.Disabled, false)
			rbPin(g, name, "metricName", a.MetricName, metric)
			rbPin(g, name, "timeAggregation", a.TimeAggregation, timeAgg)
			rbPin(g, name, "spaceAggregation", a.SpaceAggregation, outer)
			gotFilter := ""
			if q.Spec.Filter != nil {
				gotFilter = q.Spec.Filter.Expression
			}
			rbPin(g, name, "filter", gotFilter, want.filter)
			gotGroup := ""
			if len(q.Spec.GroupBy) == 1 {
				gotGroup = q.Spec.GroupBy[0].Name
			} else if len(q.Spec.GroupBy) > 1 {
				gotGroup = "(several keys)"
			}
			rbPin(g, name, "groupBy", gotGroup, want.groupBy)
			if want.ruleLabel != "" {
				g.expect(strings.TrimSpace(head[2]) == want.ruleLabel, "%s expr groups by %q, the table says %q", name, head[2], want.ruleLabel)
			} else {
				g.expect(head[2] == "", "%s expr groups by %q, the table says no grouping", name, head[2])
			}
			for _, m := range rbExprMatcher.FindAllStringSubmatch(expr, -1) {
				if m[1] != "__name__" {
					g.expect(strings.Contains(gotFilter, m[3]), "%s import filter %q does not carry the rule matcher value %q", name, gotFilter, m[3])
				}
			}
		}

		th := rbExprThreshold.FindStringSubmatch(expr)
		if th == nil {
			g.failf("%s expr %q has no `> N` or `== N` threshold", name, expr)
		}
		window, match := "", "at_least_once"
		if r := exprRange.FindStringSubmatch(expr); r != nil {
			window = r[1]
		}
		if f := str(rule["for"]); f != "" {
			match = "all_the_times"
			a, errA := time.ParseDuration(f)
			b, errB := time.ParseDuration(window)
			if window == "" || errB != nil || (errA == nil && a >= b) {
				window = f
			}
		}
		rbDuration(g, name, "evalWindow", imp.Evaluation.Spec.EvalWindow, window)
		specs := imp.Condition.Thresholds.Spec
		rbPin(g, name, "threshold kind", imp.Condition.Thresholds.Kind, "basic")
		if len(specs) != 1 {
			g.failf("%s import has %d thresholds, want 1", name, len(specs))
		} else {
			if th != nil {
				n, _ := strconv.ParseFloat(th[2], 64)
				op := map[string]string{">": "above", "==": "equal"}[th[1]]
				rbPin(g, name, "target", specs[0].Target, n)
				rbPin(g, name, "op", specs[0].Op, op)
			}
			rbPin(g, name, "matchType", specs[0].MatchType, match)
			rbPin(g, name, "threshold name", specs[0].Name, str(mp(rule["labels"])["severity"]))
		}
		labels := mp(rule["labels"])
		g.expect(len(imp.Labels) == len(labels), "%s import labels %v, want %v", name, imp.Labels, labels)
		for k, v := range labels {
			g.expect(imp.Labels[k] == str(v), "%s import label %s=%q, want %q", name, k, imp.Labels[k], str(v))
		}
	}
}

// PromQL rules do not evaluate on this SigNoz: no block of the whole runbook
// may be a PromQL rule.
func runbookHasNoPromQLRule(g *guard) {
	text := g.read(valkeyRunbook)
	for _, banned := range []string{"promql_rule", "promQueries", `"queryType": "promql"`} {
		g.expect(!strings.Contains(text, banned), "the runbook contains %q: PromQL rules do not evaluate on this SigNoz", banned)
	}
}

// "No data" must not be read as a metric-name problem alone: a scrape or a
// collector gap gives the same result. Step 3 and Check 1 of the Valkey
// section both say so, and Step 2 says the three AuthService blocks are not
// yet imported.
func runbookNoDataNamesScrapeAndCollector(g *guard) {
	text := g.read(valkeyRunbook)
	for _, anchor := range []string{"## Step 3", "## Valkey memory and eviction rules"} {
		i := strings.Index(text, anchor)
		if i < 0 {
			g.failf("%s has no section %q", valkeyRunbook, anchor)
			continue
		}
		section := text[i:]
		if j := strings.Index(section[1:], "\n## "); j >= 0 {
			section = section[:j+1]
		}
		found := false
		for _, line := range strings.Split(section, "\n") {
			if strings.Contains(line, `"no data"`) {
				found = true
				g.expect(strings.Contains(line, "scrape") && strings.Contains(line, "collector"), "the \"no data\" line of %q does not name a scrape and a collector gap: %s", anchor, line)
			}
		}
		g.expect(found, "section %q has no \"no data\" line", anchor)
	}
	i := strings.Index(text, "## Step 2")
	j := strings.Index(text, "## Step 3")
	g.expect(i >= 0 && j > i && strings.Contains(text[i:j], "not yet imported"), "Step 2 does not say the AuthService blocks are derived from alerts/rules.yml and not yet imported")
}

// mutBlock edits ONE import JSON block (found by its alert name) and demands
// that the old text occurs exactly once in that block: a plant never lands in
// another rule or section by accident.
func mutBlock(alert, old, repl string) func(*testing.T, string) {
	return func(t *testing.T, root string) {
		t.Helper()
		p := rootPath(root, valkeyRunbook)
		b := mustRead(t, p)
		for _, loc := range rbBlockHead.FindAllStringSubmatchIndex(b, -1) {
			block := b[loc[2]:loc[3]]
			if !strings.Contains(block, `"alert": "`+alert+`"`) {
				continue
			}
			if n := strings.Count(block, old); n != 1 {
				t.Fatalf("plant in %s: %q occurs %d times in the block, want exactly 1", alert, old, n)
			}
			writeFile(t, root, valkeyRunbook, b[:loc[2]]+strings.Replace(block, old, repl, 1)+b[loc[3]:])
			return
		}
		t.Fatalf("plant: no import block for %s", alert)
	}
}

func TestRunbookBuilderGuardsRealTree(t *testing.T) {
	runReal(t,
		check{"runbook-builder-rules-match-rules", runbookBuilderRulesMatchRules},
		check{"runbook-has-no-promql-rule", runbookHasNoPromQLRule},
		check{"runbook-no-data-names-scrape-and-collector", runbookNoDataNamesScrapeAndCollector},
	)
}

func TestRunbookBuilderGuardsPlantedDefects(t *testing.T) {
	both := []string{rulesRel, valkeyRunbook}
	const (
		storeErrors = "AuthServiceRateLimitStoreErrors"
		errorsHigh  = "AuthServiceRateLimitStoreErrorsHigh"
		notReady    = "AuthServiceRateLimitStoreNotReady"
	)
	plant := func(name, alert, old, repl, want string) planted {
		return planted{name, both, mutBlock(alert, old, repl), runbookBuilderRulesMatchRules, want}
	}
	runPlanted(t, []planted{
		plant("alert-type", valkeyEvictedAlert, `"alertType": "METRIC_BASED_ALERT"`, `"alertType": "LOGS_BASED_ALERT"`, "alertType"),
		plant("schema-version", storeErrors, `"schemaVersion": "v2alpha1"`, `"schemaVersion": "v1"`, "schemaVersion"),
		plant("rule-type", errorsHigh, `"ruleType": "threshold_rule"`, `"ruleType": "anomaly_rule"`, "ruleType"),
		plant("panel-type", notReady, `"panelType": "graph"`, `"panelType": "table"`, "panelType"),
		plant("unit", valkeyMemoryAlert, `"unit": "bytes"`, `"unit": ""`, "import unit"),
		plant("query-type", valkeyEvictedAlert, `"queryType": "builder"`, `"queryType": "clickhouse_sql"`, "queryType"),
		plant("step-interval", storeErrors, `"stepInterval": 60`, `"stepInterval": 300`, "stepInterval"),
		plant("signal", errorsHigh, `"signal": "metrics"`, `"signal": "logs"`, "signal"),
		plant("query-name", notReady, `"name": "A"`, `"name": "B"`, "query name"),
		plant("query-disabled", valkeyMemoryAlert, `"disabled": false`, `"disabled": true`, "disabled"),
		plant("selected-query", storeErrors, `"selectedQueryName": "A"`, `"selectedQueryName": "B"`, "selectedQueryName"),
		plant("threshold-name", errorsHigh, `"name": "critical"`, `"name": "warning"`, "threshold name"),
		plant("threshold-kind", notReady, `"kind": "basic"`, `"kind": "advanced"`, "threshold kind"),
		plant("target", valkeyMemoryAlert, `"target": 161061274`, `"target": 999999999`, "import target"),
		plant("target-high", errorsHigh, `"target": 20`, `"target": 200`, "import target"),
		plant("op", storeErrors, `"op": "above"`, `"op": "below"`, "import op"),
		plant("op-equal", notReady, `"op": "equal"`, `"op": "above"`, "import op"),
		plant("match-type", valkeyMemoryAlert, `"matchType": "all_the_times"`, `"matchType": "at_least_once"`, "import matchType"),
		plant("eval-window", errorsHigh, `"evalWindow": "10m"`, `"evalWindow": "5m"`, "evalWindow"),
		plant("eval-kind", valkeyEvictedAlert, `"kind": "rolling"`, `"kind": "cumulative"`, "evaluation kind"),
		plant("frequency", storeErrors, `"frequency": "30s"`, `"frequency": "5m"`, "frequency"),
		plant("frequency-valkey", valkeyMemoryAlert, `"frequency": "1m"`, `"frequency": "10m"`, "frequency"),
		plant("use-policy", notReady, `"usePolicy": true`, `"usePolicy": false`, "usePolicy"),
		plant("label-severity", valkeyMemoryAlert, `"severity": "warning"`, `"severity": "critical"`, "import label severity"),
		plant("label-team", errorsHigh, `"team": "platform"`, `"team": "growth"`, "import label team"),
		plant("summary", valkeyEvictedAlert, `"summary": "Valkey evicted keys at maxmemory"`, `"summary": "Valkey evicted"`, "summary annotation"),
		plant("description", notReady, `Next: Rate-limit store down`, `Next: nothing`, "description annotation"),
		plant("metric-name", valkeyEvictedAlert, `"metricName": "redis.keys.evicted"`, `"metricName": "redis.keys.evicted_total"`, "metricName"),
		plant("time-aggregation", storeErrors, `"timeAggregation": "rate"`, `"timeAggregation": "increase"`, "timeAggregation"),
		plant("space-aggregation", errorsHigh, `"spaceAggregation": "sum"`, `"spaceAggregation": "avg"`, "spaceAggregation"),
		plant("filter", storeErrors, `limit REGEXP 'auth_svc_.*'`, `limit REGEXP '.*'`, "import filter"),
		plant("filter-dropped-service", notReady, ` AND service.name = 'auth-service'`, ``, "import filter"),
		plant("group-by", notReady, `"name": "service.instance.id"`, `"name": "k8s.pod.name"`, "groupBy"),
		plant("unknown-field", valkeyEvictedAlert, `"alert": "ValkeyKeysEvicted",`, `"alert": "ValkeyKeysEvicted", "version": "v4",`, "does not parse"),
		planted{"promql-block-in-step-2", both, mutBlock(storeErrors, `"ruleType": "threshold_rule"`, `"ruleType": "promql_rule"`), runbookHasNoPromQLRule, "promql_rule"},
		planted{"promql-queries-in-valkey", both, mutBlock(valkeyEvictedAlert, `"alert": "ValkeyKeysEvicted",`, `"alert": "ValkeyKeysEvicted", "promQueries": {},`), runbookHasNoPromQLRule, "promQueries"},
		planted{"step-3-no-data-line-loses-collector", both, mut(valkeyRunbook, "or the collector is down", "or something else"), runbookNoDataNamesScrapeAndCollector, "scrape and a collector gap"},
		planted{"valkey-check-1-loses-scrape", both, mut(valkeyRunbook, "the metric is not scraped", "the metric is gone"), runbookNoDataNamesScrapeAndCollector, "scrape and a collector gap"},
		planted{"step-2-claims-live", both, mut(valkeyRunbook, "not yet imported", "imported"), runbookNoDataNamesScrapeAndCollector, "not yet imported"},
	})
}
