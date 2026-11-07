package chartgates_test

import (
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// The push credential projection alerts (alerts/rules.yml, group
// auth_push_projection) and their SigNoz builder form in the runbook page
// pushRunbook. CHAOS-10897 plan 6.1 and 6.2: the rule text decides the series,
// the window, the threshold and the severity; the runbook JSON says the same;
// every rule names the channel accounts-email with renotify and does not use
// the notification policy (the live form, as every other prod rule), ParkedGap
// matches all the times on space max, NotRunning matches all the times and
// alerts on absent for 180 minutes, and ONLY the status and scopes rule is
// critical.

const (
	pushGroup   = "auth_push_projection"
	pushRunbook = "docs/operate/runbooks/auth-push-projection-drift-alerts.md"

	pushDriftMetric  = "devhealth_auth_push_reconcile_drift"
	pushRunsMetric   = "devhealth_auth_push_reconcile_runs_total"
	pushParkedMetric = "devhealth_auth_push_projection_parked_rows"
	pushFinalMetric  = "devhealth_auth_push_projection_final_rows"

	pushDrift      = "PushCredentialProjectionDrift"
	pushDriftLive  = "PushCredentialProjectionDriftLive"
	pushParkedGap  = "PushProjectionParkedGap"
	pushFinalRows  = "PushProjectionFinalRows"
	pushNotRunning = "PushReconcileNotRunning"
)

// pushRule is the text of record for one rule: the expression, the `for`, the
// severity. Order is the order of the group in the file.
type pushRule struct{ alert, expr, forDur, severity string }

var pushRules = []pushRule{
	{pushDrift, "max by (class) (" + pushDriftMetric + ") > 0", "10m", "warning"},
	{pushDriftLive, "max by (class) (" + pushDriftMetric + `{class=~"status|scopes"}) > 0`, "5m", "critical"},
	{pushParkedGap, "min_over_time(" + pushParkedMetric + "[1h]) > 0", "", "warning"},
	{pushFinalRows, "max(" + pushFinalMetric + ") > 0", "5m", "warning"},
	{pushNotRunning, "sum(increase(" + pushRunsMetric + `{outcome="ok"}[3h])) == 0`, "", "warning"},
}

var pushWants = []rbWant{
	{alert: pushDrift, ruleLabel: "class", groupBy: "class",
		summary:     "Push credential projection differs from the domain",
		description: "Class {{$labels.class}}: two confirmed reconcile passes ten minutes apart saw the push projection differ from the credential domain. Next: Step 3 in docs/operate/runbooks/auth-push-projection-drift-alerts.md.", routed: true},
	{alert: pushDriftLive, filter: "class REGEXP 'status|scopes'", ruleLabel: "class", groupBy: "class",
		summary:     "Push projection holds a credential live or wider than the domain",
		description: "Class {{$labels.class}}: the projection reads a credential as live that the domain ended, or with wider scopes. Next: Step 3 in docs/operate/runbooks/auth-push-projection-drift-alerts.md.", routed: true},
	{alert: pushParkedGap, routed: true, match: "all_the_times", space: "max",
		summary:     "Push projection rows stayed parked for an hour",
		description: "Parked projection rows did not clear for an hour, longer than the retry backoff allows. Next: Step 4 in docs/operate/runbooks/auth-push-projection-drift-alerts.md."},
	{alert: pushFinalRows, routed: true,
		summary:     "Push projection has final rows that need a person",
		description: "Final projection rows (malformed, unknown organization or conflict) stay for five minutes and are never retried. Next: Step 4 in docs/operate/runbooks/auth-push-projection-drift-alerts.md."},
	{alert: pushNotRunning, filter: "outcome = 'ok'", routed: true, match: "all_the_times", absentFor: 180,
		summary:     "Push projection reconcile has not completed for three hours",
		description: "No reconcile pass finished ok in three hours, so the drift rules cannot fire. Next: Step 5 in docs/operate/runbooks/auth-push-projection-drift-alerts.md."},
}

func pushAlertsAreTheGroup(g *guard) {
	rules := groupRules(g, pushGroup)
	g.expect(len(rules) == len(pushRules), "group %s holds %d rules, want %d", pushGroup, len(rules), len(pushRules))
	for i, want := range pushRules {
		if i >= len(rules) {
			break
		}
		r := rules[i]
		g.expect(str(r["alert"]) == want.alert, "group %s rule %d is %q, want %q", pushGroup, i+1, str(r["alert"]), want.alert)
		g.expect(oneLine(str(r["expr"])) == want.expr, "%s expr is %q, want %q", want.alert, oneLine(str(r["expr"])), want.expr)
		g.expect(str(r["for"]) == want.forDur, "%s `for` is %q, want %q", want.alert, str(r["for"]), want.forDur)
		labels := mp(r["labels"])
		g.expect(str(labels["severity"]) == want.severity, "%s severity is %q, want %q", want.alert, str(labels["severity"]), want.severity)
		g.expect(str(labels["team"]) == "platform" && len(labels) == 2, "%s labels are %v, want severity and team platform", want.alert, labels)
		d := str(mp(r["annotations"])["description"])
		g.expect(strings.Contains(d, pushRunbook), "%s description does not name the runbook page", want.alert)
		g.expect(!regexp.MustCompile(`(?i)token|org(anization)?[_ ]?id|secret`).MatchString(str(mp(r["annotations"])["summary"])), "%s summary carries a token or an organization", want.alert)
	}
	critical := 0
	for _, r := range rules {
		if str(mp(r["labels"])["severity"]) == "critical" {
			critical++
		}
	}
	g.expect(critical == 1, "group %s has %d critical rules, want 1 (the status and scopes rule)", pushGroup, critical)
}

func pushRunbookMatchesTheRules(g *guard) {
	runbookBuilderRulesMatch(g, pushRunbook, pushWants)
}

func pushRunbookHasNoPromQLRule(g *guard) {
	text := g.read(pushRunbook)
	for _, banned := range []string{"promql_rule", "promQueries", `"queryType": "promql"`} {
		g.expect(!strings.Contains(text, banned), "the runbook contains %q: PromQL does not evaluate on this SigNoz", banned)
	}
}

// Step 2 must accept the healthy no-drift state: an absent drift gauge is
// expected while the run counter advances, and only a series that must exist
// (or an absent drift gauge with a stalled counter) is a stop.
func pushRunbookStep2AllowsAbsentDrift(g *guard) {
	text := g.read(pushRunbook)
	start := strings.Index(text, "## Step 2")
	if start < 0 {
		g.failf("the runbook has no Step 2 section")
		return
	}
	sec := text[start:]
	if end := strings.Index(sec[len("## Step 2"):], "\n## "); end >= 0 {
		sec = sec[:len("## Step 2")+end]
	}
	stops, allows := 0, 0
	for _, line := range strings.Split(sec, "\n") {
		if strings.Contains(line, "Stop and report") {
			stops++
			g.expect(strings.Contains(line, "run counter is not advancing"), "Step 2 stop rule %q does not limit the absent drift gauge to a run counter that is not advancing: the healthy no-drift state has no drift sample", strings.TrimSpace(line))
		}
		if strings.Contains(line, "absent drift gauge") && strings.Contains(line, "run counter is advancing") {
			allows++
		}
	}
	g.expect(stops > 0, "Step 2 has no stop rule")
	g.expect(allows > 0, "Step 2 does not say that an absent drift gauge is expected while the run counter is advancing")
}

// C50b: the series of rules 3 and 4 are literal instrument names in the reader
// code today. Rules 1, 2 and 5 name the series of the reconcile probe (plan
// 6.1); their names are pinned here by pushRules, and the probe's own test
// pins the literals in code when it lands.
func pushMetricNamesAreEmitted(g *guard) {
	reader := g.read("internal/auth/pushprojection/reader.go")
	for _, name := range []string{pushParkedMetric, pushFinalMetric} {
		g.expect(strings.Contains(reader, `"`+name+`"`), "internal/auth/pushprojection/reader.go does not emit %s", name)
	}
}

// --- C51: a small evaluator over synthetic series ---

type pushPoint struct {
	min int
	v   float64
}
type pushSeries struct {
	labels map[string]string
	pts    []pushPoint
}

var (
	pushCmp    = regexp.MustCompile(`^(.*) (>|==) (\d+)$`)
	pushGroupE = regexp.MustCompile(`^(max|min|sum) by \((\w+)\) \((\w+)(?:\{(.*)\})?\)$`)
	pushPlainE = regexp.MustCompile(`^(max|min|sum)\((\w+)(?:\{(.*)\})?\)$`)
	pushOverE  = regexp.MustCompile(`^(min|max)_over_time\((\w+)(?:\{(.*)\})?\[(\d+)([mh])\]\)$`)
	pushIncrE  = regexp.MustCompile(`^sum\(increase\((\w+)(?:\{(.*)\})?\[(\d+)([mh])\]\)\)$`)
)

func pushMatches(sel string, labels map[string]string) bool {
	for _, m := range rbExprMatcher.FindAllStringSubmatch(sel, -1) {
		if m[2] == "=" && labels[m[1]] != m[3] {
			return false
		}
		if m[2] == "=~" && !regexp.MustCompile("^(?:"+m[3]+")$").MatchString(labels[m[1]]) {
			return false
		}
	}
	return true
}

func pushWindow(n, unit string) int {
	v, _ := strconv.Atoi(n)
	if unit == "h" {
		return v * 60
	}
	return v
}

// pushEval returns, per output group, the value of the left side of the
// comparison at minute t; a group is absent when no series answers.
func pushEval(g *guard, expr string, set map[string][]pushSeries, t int) map[string]float64 {
	m := pushCmp.FindStringSubmatch(expr)
	if m == nil {
		g.failf("expr %q has no comparison", expr)
		return nil
	}
	body := m[1]
	latest := func(s pushSeries) (float64, bool) { // staleness 5 minutes
		for i := len(s.pts) - 1; i >= 0; i-- {
			if s.pts[i].min <= t && t-s.pts[i].min < 5 {
				return s.pts[i].v, true
			}
		}
		return 0, false
	}
	agg := func(op string, vals []float64) float64 {
		r := vals[0]
		for _, v := range vals[1:] {
			switch op {
			case "max":
				r = max(r, v)
			case "min":
				r = min(r, v)
			case "sum":
				r += v
			}
		}
		return r
	}
	out := map[string]float64{}
	switch {
	case pushGroupE.MatchString(body):
		p := pushGroupE.FindStringSubmatch(body)
		by := map[string][]float64{}
		for _, s := range set[p[3]] {
			if v, ok := latest(s); ok && pushMatches(p[4], s.labels) {
				by[s.labels[p[2]]] = append(by[s.labels[p[2]]], v)
			}
		}
		for k, vs := range by {
			out[k] = agg(p[1], vs)
		}
	case pushPlainE.MatchString(body):
		p := pushPlainE.FindStringSubmatch(body)
		var vs []float64
		for _, s := range set[p[2]] {
			if v, ok := latest(s); ok && pushMatches(p[3], s.labels) {
				vs = append(vs, v)
			}
		}
		if len(vs) > 0 {
			out[""] = agg(p[1], vs)
		}
	case pushOverE.MatchString(body):
		p := pushOverE.FindStringSubmatch(body)
		w := pushWindow(p[4], p[5])
		var vs []float64
		for _, s := range set[p[2]] {
			if !pushMatches(p[3], s.labels) {
				continue
			}
			var in []float64
			for _, pt := range s.pts {
				if pt.min <= t && pt.min > t-w {
					in = append(in, pt.v)
				}
			}
			if len(in) > 0 {
				vs = append(vs, agg(p[1], in))
			}
		}
		if len(vs) > 0 {
			out[""] = agg(p[1], vs)
		}
	case pushIncrE.MatchString(body):
		p := pushIncrE.FindStringSubmatch(body)
		w := pushWindow(p[3], p[4])
		sum, any := 0.0, false
		for _, s := range set[p[1]] {
			if !pushMatches(p[2], s.labels) {
				continue
			}
			var first, last *pushPoint
			for i, pt := range s.pts {
				if pt.min <= t && pt.min > t-w {
					if first == nil {
						first = &s.pts[i]
					}
					last = &s.pts[i]
				}
			}
			if first != nil {
				sum, any = sum+last.v-first.v, true
			}
		}
		if any {
			out[""] = sum
		}
	default:
		g.failf("expr %q is not a shape the evaluator knows", expr)
		return nil
	}
	th, _ := strconv.ParseFloat(m[3], 64)
	for k, v := range out {
		if (m[2] == ">" && !(v > th)) || (m[2] == "==" && v != th) {
			delete(out, k)
		}
	}
	return out
}

// pushFires runs the rule minute by minute over [0, end] and reports whether
// any group held the condition for its whole `for` (and the window had data).
func pushFires(g *guard, r pushRule, set map[string][]pushSeries, from, end int) bool {
	forMin := 0
	if r.forDur != "" {
		forMin = pushWindow(strings.TrimRight(r.forDur, "mh"), r.forDur[len(r.forDur)-1:])
	}
	since := map[string]int{}
	for t := from; t <= end; t++ {
		got := pushEval(g, r.expr, set, t)
		for k := range since {
			if _, ok := got[k]; !ok {
				delete(since, k)
			}
		}
		for k := range got {
			if _, ok := since[k]; !ok {
				since[k] = t
			}
			if t-since[k] >= forMin {
				return true
			}
		}
	}
	return false
}

func pushRuleFromFile(g *guard, name string) pushRule {
	a := alertNamed(g, name)
	return pushRule{name, oneLine(str(a["expr"])), str(a["for"]), str(mp(a["labels"])["severity"])}
}

func pushConst(class string, v float64, from, to int) pushSeries {
	s := pushSeries{labels: map[string]string{"class": class}}
	for t := from; t <= to; t++ {
		s.pts = append(s.pts, pushPoint{t, v})
	}
	return s
}

func pushAlertsFireOnSeededSeries(g *guard) {
	drift := func(class string, v float64, to int) map[string][]pushSeries {
		return map[string][]pushSeries{pushDriftMetric: {pushConst(class, v, 0, to)}}
	}
	r1, r2 := pushRuleFromFile(g, pushDrift), pushRuleFromFile(g, pushDriftLive)
	g.expect(pushFires(g, r1, drift("status", 1, 11), 0, 11), "rule 1 is silent on drift{class=status}=1 for ten minutes")
	g.expect(!pushFires(g, r1, drift("status", 1, 8), 0, 9), "rule 1 fires before ten minutes of drift")
	g.expect(!pushFires(g, r1, drift("status", 0, 30), 0, 30), "rule 1 fires on drift 0")
	g.expect(!pushFires(g, r1, map[string][]pushSeries{}, 0, 30), "rule 1 fires with no sample")
	g.expect(pushFires(g, r2, drift("status", 1, 6), 0, 6), "rule 2 is silent on class status for five minutes")
	g.expect(pushFires(g, r2, drift("scopes", 1, 6), 0, 6), "rule 2 is silent on class scopes for five minutes")
	for _, class := range []string{"missing", "extra", "expiry", "binding", "unwireable"} {
		g.expect(!pushFires(g, r2, drift(class, 1, 30), 0, 30), "rule 2 (critical) fires on class %s, which is not live or wider", class)
		g.expect(pushFires(g, r1, drift(class, 1, 11), 0, 11), "rule 1 is silent on class %s", class)
	}

	r3 := pushRuleFromFile(g, pushParkedGap)
	parked := func(dip bool) map[string][]pushSeries {
		s := pushSeries{labels: map[string]string{}}
		for t := 0; t <= 90; t++ {
			v := 1.0
			if dip && t == 30 {
				v = 0
			}
			s.pts = append(s.pts, pushPoint{t, v})
		}
		return map[string][]pushSeries{pushParkedMetric: {s}}
	}
	g.expect(pushFires(g, r3, parked(false), 61, 90), "rule 3 is silent on rows parked for the whole hour")
	g.expect(!pushFires(g, r3, parked(true), 61, 89), "rule 3 fires although the parked rows cleared once inside the hour")

	r4 := pushRuleFromFile(g, pushFinalRows)
	final := func(v float64) map[string][]pushSeries {
		s := pushSeries{labels: map[string]string{}}
		for t := 0; t <= 6; t++ {
			s.pts = append(s.pts, pushPoint{t, v})
		}
		return map[string][]pushSeries{pushFinalMetric: {s}}
	}
	g.expect(pushFires(g, r4, final(1), 0, 6), "rule 4 is silent on one final row for five minutes")
	g.expect(!pushFires(g, r4, final(0), 0, 6), "rule 4 fires on zero final rows")

	r5 := pushRuleFromFile(g, pushNotRunning)
	runs := func(okAt int) map[string][]pushSeries {
		ok := pushSeries{labels: map[string]string{"outcome": "ok"}}
		failed := pushSeries{labels: map[string]string{"outcome": "failed"}}
		for t := 0; t <= 190; t++ {
			v := 0.0
			if okAt >= 0 && t >= okAt {
				v = 1
			}
			ok.pts = append(ok.pts, pushPoint{t, v})
			failed.pts = append(failed.pts, pushPoint{t, float64(t)}) // failed runs do not count
		}
		return map[string][]pushSeries{pushRunsMetric: {ok, failed}}
	}
	g.expect(pushFires(g, r5, runs(-1), 190, 190), "rule 5 is silent although no run finished ok for three hours")
	g.expect(!pushFires(g, r5, runs(100), 190, 190), "rule 5 fires although one run finished ok inside the three hours")
}

func TestPushProjectionAlertGuardsRealTree(t *testing.T) {
	runReal(t,
		check{"push-alerts-are-the-group", pushAlertsAreTheGroup},
		check{"push-runbook-matches-the-rules", pushRunbookMatchesTheRules},
		check{"push-runbook-has-no-promql-rule", pushRunbookHasNoPromQLRule},
		check{"push-runbook-step2-allows-absent-drift", pushRunbookStep2AllowsAbsentDrift},
		check{"push-metric-names-are-emitted", pushMetricNamesAreEmitted},
		check{"push-alerts-fire-on-seeded-series", pushAlertsFireOnSeededSeries},
	)
}

func TestPushProjectionAlertGuardsPlantedDefects(t *testing.T) {
	both := []string{rulesRel, pushRunbook}
	pb := func(name, alert, old, repl, want string) planted {
		return planted{name, both, mutBlockIn(pushRunbook, alert, old, repl), pushRunbookMatchesTheRules, want}
	}
	live := "max by (class) (" + pushDriftMetric + `{class=~"status|scopes"}) > 0`
	runPlanted(t, []planted{
		{"live-rule-drops-status-and-scopes", both, mut(rulesRel, live, "max by (class) ("+pushDriftMetric+") > 0"), pushAlertsFireOnSeededSeries, "rule 2 (critical) fires on class"},
		{"live-rule-drops-status-and-scopes-text", both, mut(rulesRel, live, "max by (class) ("+pushDriftMetric+") > 0"), pushAlertsAreTheGroup, "PushCredentialProjectionDriftLive expr"},
		{"drift-rule-made-critical", both, mut(rulesRel, "severity: warning\n          team: platform\n        annotations:\n          summary: \"Push credential projection differs", "severity: critical\n          team: platform\n        annotations:\n          summary: \"Push credential projection differs"), pushAlertsAreTheGroup, "critical"},
		pb("runbook-for-differs", pushDrift, `"evalWindow": "10m"`, `"evalWindow": "5m"`, "evalWindow"),
		pb("runbook-severity-differs", pushDriftLive, `"severity": "critical"`, `"severity": "warning"`, "import label severity"),
		pb("runbook-uses-policy", pushDrift, `"usePolicy": false`, `"usePolicy": true`, "usePolicy"),
		pb("runbook-drops-the-channel", pushDrift, `"accounts-email"`, ``, "import channels"),
		pb("runbook-names-another-channel", pushFinalRows, `"accounts-email"`, `"other-channel"`, "import channels"),
		pb("runbook-stops-renotify", pushDriftLive, `"enabled": true`, `"enabled": false`, "renotify"),
		pb("runbook-renotify-interval", pushFinalRows, `"interval": "4h"`, `"interval": "1h"`, "renotify"),
		pb("parked-gap-at-least-once", pushParkedGap, `"matchType": "all_the_times"`, `"matchType": "at_least_once"`, "matchType"),
		pb("parked-gap-space-min", pushParkedGap, `"spaceAggregation": "max"`, `"spaceAggregation": "min"`, "spaceAggregation"),
		pb("not-running-at-least-once", pushNotRunning, `"matchType": "all_the_times"`, `"matchType": "at_least_once"`, "matchType"),
		pb("not-running-no-absent", pushNotRunning, `"alertOnAbsent": true`, `"alertOnAbsent": false`, "alertOnAbsent"),
		pb("not-running-absent-for-differs", pushNotRunning, `"absentFor": 180`, `"absentFor": 60`, "absentFor"),
		pb("runbook-metric-differs", pushFinalRows, `"metricName": "`+pushFinalMetric+`"`, `"metricName": "devhealth_auth_push_projection_final"`, "metricName"),
		{"runbook-step2-stops-on-any-no-data", both, mut(pushRunbook, "No data for the run counter, the parked rows or the final rows, or an absent drift gauge while the run counter is not advancing, means", `A query that returns "no data" means`), pushRunbookStep2AllowsAbsentDrift, "Step 2 stop rule"},
		{"parked-rule-uses-max", both, mut(rulesRel, "min_over_time("+pushParkedMetric, "max_over_time("+pushParkedMetric), pushAlertsFireOnSeededSeries, "rule 3 fires although"},
		{"parked-rule-window-shortened", both, mut(rulesRel, pushParkedMetric+"[1h]", pushParkedMetric+"[5m]"), pushAlertsFireOnSeededSeries, "rule 3 fires although"},
		{"not-running-counts-failed-runs", both, mut(rulesRel, `{outcome="ok"}[3h]`, `[3h]`), pushAlertsFireOnSeededSeries, "rule 5 is silent"},
		{"drift-metric-renamed", both, mut(rulesRel, "max by (class) ("+pushDriftMetric+") > 0", "max by (class) (devhealth_auth_push_drift) > 0"), pushAlertsAreTheGroup, "PushCredentialProjectionDrift expr"},
	})
}

// mutBlockIn is mutBlock for any runbook page.
func mutBlockIn(rel, alert, old, repl string) func(*testing.T, string) {
	return func(t *testing.T, root string) {
		t.Helper()
		b := mustRead(t, rootPath(root, rel))
		for _, loc := range rbBlockHead.FindAllStringSubmatchIndex(b, -1) {
			block := b[loc[2]:loc[3]]
			if !strings.Contains(block, `"alert": "`+alert+`"`) {
				continue
			}
			if n := strings.Count(block, old); n != 1 {
				t.Fatalf("plant in %s: %q occurs %d times in the block, want exactly 1", alert, old, n)
			}
			writeFile(t, root, rel, b[:loc[2]]+strings.Replace(block, old, repl, 1)+b[loc[3]:])
			return
		}
		t.Fatalf("plant: no import block for %s", alert)
	}
}
