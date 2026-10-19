package chartgates_test

import (
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// Ported from tests/workers/test_go_worker_alerts.py, test_provider_artifact_skip_alert.py (every test
// except the tier-floor one, which only guards Python code and is deleted),
// test_unreclaimable_sweep_alerts.py, test_go_worker_dashboard.py, tests/test_deploy_alternates_removed.py,
// tests/test_unreclaimable_sweep_deploy_default.py and tests/test_worker_otel_deployments.py.

const rulesRel = "alerts/rules.yml"

func ruleGroups(g *guard) []doc {
	var out []doc
	for _, grp := range list(g.yamlFile(rulesRel)["groups"]) {
		out = append(out, mp(grp))
	}
	return out
}

func groupRules(g *guard, group string) []doc {
	for _, grp := range ruleGroups(g) {
		if str(grp["name"]) == group {
			var out []doc
			for _, r := range list(grp["rules"]) {
				out = append(out, mp(r))
			}
			return out
		}
	}
	g.failf("group %q not found in %s", group, rulesRel)
	return nil
}

func allAlerts(g *guard) []doc {
	var out []doc
	for _, grp := range ruleGroups(g) {
		for _, r := range list(grp["rules"]) {
			if _, ok := mp(r)["alert"]; ok {
				out = append(out, mp(r))
			}
		}
	}
	return out
}

func alertNamed(g *guard, n string) doc {
	var found []doc
	for _, a := range allAlerts(g) {
		if str(a["alert"]) == n {
			found = append(found, a)
		}
	}
	if len(found) != 1 {
		g.failf("expected exactly one %s rule, got %d", n, len(found))
		if len(found) == 0 {
			return doc{}
		}
	}
	return found[0]
}

func goWorkerRules(g *guard) map[string]string {
	for _, grp := range ruleGroups(g) {
		if str(grp["name"]) != "go_workers" {
			continue
		}
		g.expect(str(grp["interval"]) == "30s", "go_workers interval is %q, want 30s", str(grp["interval"]))
		out := map[string]string{}
		for _, r := range list(grp["rules"]) {
			out[str(mp(r)["alert"])] = str(mp(r)["expr"])
		}
		return out
	}
	g.failf("group go_workers not found in %s", rulesRel)
	return nil
}

func serialized(v any) string {
	b, _ := yaml.Marshal(v)
	return string(b)
}

func goWorkerGroupSerialized(g *guard) string {
	for _, grp := range ruleGroups(g) {
		if str(grp["name"]) == "go_workers" {
			return serialized(grp["rules"])
		}
	}
	return ""
}

func workerAlertsCoverSignals(g *guard) {
	alerts := goWorkerRules(g)
	want := map[string]string{
		"GoWorkerOldestAvailableJobHigh":                      "worker_job_oldest_age_seconds",
		"GoWorkerExecutionSaturated":                          "worker_execution_saturation_ratio",
		"GoWorkerAttemptFailureRateHigh":                      "worker_job_attempts_total",
		"SyncDispatchExhaustedDeliveryRecoveryLoop":           "sync_dispatch_exhausted_delivery_recoveries_total",
		"GoWorkerDomainStateMismatch":                         "worker_domain_state_mismatch_total",
		"GoWorkerStreamLagHigh":                               "worker_stream_lag",
		"GoWorkerStreamPendingTooOld":                         "worker_stream_oldest_pending_seconds",
		"GoWorkerQueueControlPoolPressure":                    "worker_database_pool_saturation_ratio",
		"GoWorkerTelemetryTargetDown":                         "dev-health-go-",
		"SyncDispatchUnreclaimableCandidatesNotTerminalizing": "sync_dispatch_unreclaimable_candidates",
		"SyncDispatchUnreclaimableSweepFailing":               "sync_dispatch_unreclaimable_sweep_failures_total",
		"ProviderUnitStrandRearmsClimbing":                    "worker_outbox_reconciler_provider_unit_strands_rearmed_total",
	}
	var got, exp []string
	for k := range alerts {
		got = append(got, k)
	}
	for k := range want {
		exp = append(exp, k)
	}
	sort.Strings(got)
	sort.Strings(exp)
	g.expect(equal(got, exp), "go_workers alert set differs: got %v want %v", got, exp)
	for a, metric := range want {
		g.expect(strings.Contains(alerts[a], metric), "alert %s does not read %s", a, metric)
	}
}

func workerAlertsUseRuntimeDimensions(g *guard) {
	alerts := goWorkerRules(g)
	for a, sub := range map[string]string{
		"GoWorkerOldestAvailableJobHigh": "max by (queue) (worker_job_oldest_age_seconds)",
		"GoWorkerExecutionSaturated":     "avg by (queue) (worker_execution_saturation_ratio)",
		"GoWorkerStreamLagHigh":          "max by (job, instance) (worker_stream_lag)",
		"GoWorkerStreamPendingTooOld":    "max by (job, instance) (worker_stream_oldest_pending_seconds)",
	} {
		g.expect(strings.Contains(alerts[a], sub), "alert %s must use %q (worker_execution_saturation_ratio is per process: the fleet is the AVERAGE, never max, CHAOS-3867)", a, sub)
	}
}

func workerAlertsNoFixedProfileLabel(g *guard) {
	s := strings.ToLower(goWorkerGroupSerialized(g))
	g.expect(s != "", "go_workers rules did not serialise")
	g.expect(!strings.Contains(s, "profile"), "go_workers alerts carry a fixed worker profile label")
	g.expect(!strings.Contains(s, "worker_group"), "go_workers alerts carry a worker_group label")
}

var prohibitedLabels = []string{"organization_id", "repository", "job_id", "correlation_id", "encoded_args", "payload", "error_text"}

func workerAlertsLowCardinality(g *guard) {
	s := goWorkerGroupSerialized(g)
	g.expect(s != "", "go_workers rules did not serialise")
	for _, l := range prohibitedLabels {
		g.expect(!strings.Contains(s, l), "go_workers alerts mention the high-cardinality or payload label %q", l)
	}
}

func alertNamesUnique(g *guard) {
	seen := map[string]int{}
	for _, a := range allAlerts(g) {
		seen[str(a["alert"])]++
	}
	g.expect(len(seen) > 0, "no alerts parsed")
	for n, c := range seen {
		g.expect(c == 1, "alert name %s is used %d times: alert names must be unique across the rule file", n, c)
	}
}

func noCeleryAlerts(g *guard) {
	for _, grp := range ruleGroups(g) {
		g.expect(str(grp["name"]) != "celery", "a celery alert group ships")
	}
	for _, a := range allAlerts(g) {
		g.expect(!strings.HasPrefix(str(a["alert"]), "Celery"), "a Celery alert ships: %s", str(a["alert"]))
	}
	g.expect(!strings.Contains(g.read(rulesRel), "devhealth_celery_"), "rules.yml still references a devhealth_celery_ metric")
}

// --- provider artifact skip alert ---

const (
	skipAlert = "ProviderArtifactSkipsSustained"
	skipGroup = "provider_artifact_integrity"
	budgetGo  = "internal/providerfoundation/budget.go"
)

var (
	funcStart  = regexp.MustCompile(`(?m)^func \(m \*Metrics\) WritePrometheus\(`)
	nextFunc   = regexp.MustCompile(`(?m)^func `)
	counterRef = regexp.MustCompile(`(?s)writeProviderDatasetReasonCounter\(\s*writer,\s*"([^"]+)",\s*"([^"]+)",\s*"([^"]+)",\s*m\.artifactSkipped,`)
)

func skipMetricFromGoSource(g *guard) (metric, label string) {
	text := g.read(budgetGo)
	loc := funcStart.FindStringIndex(text)
	if loc == nil {
		g.failf("could not find func (m *Metrics) WritePrometheus( in budget.go")
		return "", ""
	}
	rest := text[loc[1]:]
	end := len(text)
	if n := nextFunc.FindStringIndex(rest); n != nil {
		end = loc[1] + n[0]
	}
	m := counterRef.FindStringSubmatch(text[loc[0]:end])
	if m == nil {
		g.failf("could not find the writeProviderDatasetReasonCounter(...) call for m.artifactSkipped inside WritePrometheus's own body in budget.go: has the wiring changed shape or left the live scrape path?")
		return "", ""
	}
	return m[1], m[3]
}

func skipAlertDoc(g *guard) doc {
	for _, r := range groupRules(g, skipGroup) {
		if str(r["alert"]) == skipAlert {
			return r
		}
	}
	g.failf("alert %s not found in group %s", skipAlert, skipGroup)
	return doc{}
}

func skipMetricNamePinned(g *guard) {
	metric, _ := skipMetricFromGoSource(g)
	g.expect(metric == "dev_health_provider_artifact_skipped_total", "the artifact-skipped metric name is %q in budget.go, want dev_health_provider_artifact_skipped_total", metric)
}

func skipAlertTargetsRealMetric(g *guard) {
	metric, reason := skipMetricFromGoSource(g)
	expr := str(skipAlertDoc(g)["expr"])
	g.expect(metric != "" && strings.Contains(expr, metric), "%s expr does not read the real metric %q: %q", skipAlert, metric, expr)
	g.expect(strings.Contains(expr, "by (provider, dataset)"), "%s must group by the metric's bounded labels (provider, dataset): %q", skipAlert, expr)
	g.expect(reason == "reason", "the metric's third label is %q, want reason", reason)
}

var (
	lookback = regexp.MustCompile(`increase\(\s*dev_health_provider_artifact_skipped_total\[(\d+)([hm])\]\)`)
	forDur   = regexp.MustCompile(`^(\d+)([hm])$`)
)

func minutes(n, unit string) int {
	v, _ := strconv.Atoi(n)
	if unit == "h" {
		return v * 60
	}
	return v
}

func skipAlertForOutlastsWindow(g *guard) {
	a := skipAlertDoc(g)
	w := lookback.FindStringSubmatch(str(a["expr"]))
	if w == nil {
		g.failf("expected an increase(...[<N>h]) lookback window in %q", str(a["expr"]))
		return
	}
	f := forDur.FindStringSubmatch(str(a["for"]))
	if f == nil {
		g.failf("expected `for` as e.g. '3h', got %q", str(a["for"]))
		return
	}
	g.expect(minutes(f[1], f[2]) > minutes(w[1], w[2]), "`for` (%s) must be strictly longer than the lookback window (%s) or a single isolated skip can satisfy `for` before its sample ages out of the window", str(a["for"]), w[0])
}

var thresholdZero = regexp.MustCompile(`\)\s*>\s*0\s*$`)

func skipAlertThresholdIsAnyNonzero(g *guard) {
	expr := strings.TrimSpace(str(skipAlertDoc(g)["expr"]))
	g.expect(thresholdZero.MatchString(expr), "the threshold must be the simple '> 0'; the `for > window` property does the 'not a single event' work: %q", expr)
}

func skipAlertAnnotationExplainsLocalization(g *guard) {
	d := str(at(skipAlertDoc(g), "annotations", "description"))
	for _, w := range []string{"repository", "run", "unit"} {
		g.expect(strings.Contains(d, w), "the annotation must say where localization comes from; it lacks %q", w)
	}
}

func skipAlertLabelsBounded(g *guard) {
	a := skipAlertDoc(g)
	g.expect(!strings.Contains(str(a["expr"]), "repository"), "the alert expr grew a repository dimension")
	labels := mp(a["labels"])
	for k := range labels {
		g.expect(!strings.Contains(k, "repository"), "the alert labels grew a repository key")
	}
	g.expect(str(labels["team"]) == "platform", "team label is %q, want platform", str(labels["team"]))
	g.expect(str(labels["severity"]) == "warning" || str(labels["severity"]) == "critical", "severity label is %q", str(labels["severity"]))
}

func skipAlertNameUniqueInFile(g *guard) {
	n := 0
	for _, a := range allAlerts(g) {
		if str(a["alert"]) == skipAlert {
			n++
		}
	}
	g.expect(n == 1, "%s must appear exactly once in the rule file, found %d", skipAlert, n)
}

// --- unreclaimable sweep alerts ---

const (
	reconcilerLoop = "internal/syncreconciler/loop.go"
	outboxLoop     = "internal/joboutbox/loop.go"
	strandAlert    = "SyncDispatchUnreclaimableCandidatesNotTerminalizing"
	failingAlert   = "SyncDispatchUnreclaimableSweepFailing"
	rearmAlert     = "ProviderUnitStrandRearmsClimbing"
)

var (
	goLineComment  = regexp.MustCompile(`//[^\n]*`)
	helpOrType     = regexp.MustCompile(`#\s*(HELP|TYPE)\s+[a-zA-Z_][a-zA-Z0-9_]*`)
	escapedNewline = regexp.MustCompile(`\\n`)
	writeProm      = regexp.MustCompile(`(?m)^func \([^)]+\) WritePrometheus\(`)
	durationLit    = regexp.MustCompile(`\[\s*\d+[smhdwy]\s*\]`)
	identTok       = regexp.MustCompile(`[a-zA-Z_][a-zA-Z0-9_]*`)
	promWords      = map[string]bool{"sum": true, "max": true, "min": true, "avg": true, "count": true, "increase": true, "rate": true, "min_over_time": true, "max_over_time": true, "avg_over_time": true, "and": true, "or": true, "unless": true, "by": true, "without": true, "on": true, "ignoring": true, "group_left": true, "group_right": true, "instance": true, "job": true}
)

func writePrometheusBody(g *guard, rel string) string {
	text := g.read(rel)
	loc := writeProm.FindStringIndex(text)
	if loc == nil {
		g.failf("no WritePrometheus function in %s", rel)
		return ""
	}
	rest := text[loc[1]:]
	end := len(text)
	if n := nextFunc.FindStringIndex(rest); n != nil {
		end = loc[1] + n[0]
	}
	body := goLineComment.ReplaceAllString(text[loc[0]:end], " ")
	body = escapedNewline.ReplaceAllString(body, " ")
	return helpOrType.ReplaceAllString(body, " ")
}

func metricNames(expr string) map[string]bool {
	out := map[string]bool{}
	for _, tok := range identTok.FindAllString(durationLit.ReplaceAllString(expr, " "), -1) {
		if !promWords[tok] && strings.Contains(tok, "_") {
			out[tok] = true
		}
	}
	return out
}

func sweepReaderIgnoresComments(g *guard) {
	body := writePrometheusBody(g, reconcilerLoop)
	g.expect(strings.Contains(body, "sync_dispatch_unreclaimable_candidates"), "sync_dispatch_unreclaimable_candidates is not written as a sample by WritePrometheus")
	g.expect(!strings.Contains(body, "//"), "comments survived the strip")
	g.expect(!strings.Contains(body, "# HELP sync_dispatch_unreclaimable_candidates") && !strings.Contains(body, "# TYPE sync_dispatch_unreclaimable_candidates"), "HELP/TYPE metadata survived the strip")
}

func sweepMetricParserNotVacuous(g *guard) {
	got := metricNames("min_over_time(some_metric_total[15m]) > 0 and sum by (instance) (increase(other_metric_total[1h])) == 0")
	g.expect(equal(got, map[string]bool{"some_metric_total": true, "other_metric_total": true}), "the PromQL metric-name parser extracted %v", got)
	g.expect(!got["m"] && !got["h"], "duration units were extracted as metric names")
	g.expect(len(metricNames("up == 0")) == 0, "a rule that reads no metric must give an empty set")
}

func sweepEveryAlertMetricEmitted(g *guard) {
	emitted := writePrometheusBody(g, reconcilerLoop) + writePrometheusBody(g, outboxLoop)
	checked := 0
	for _, n := range []string{strandAlert, failingAlert, rearmAlert} {
		for metric := range metricNames(str(alertNamed(g, n)["expr"])) {
			checked++
			g.expect(regexp.MustCompile(`\b`+regexp.QuoteMeta(metric)+`\b`).MatchString(emitted), "%s reads %q, which no WritePrometheus emits: an alert on a metric nobody publishes never fires", n, metric)
		}
	}
	g.expect(checked == 4, "checked %d metric reads across the three sweep rules, want 4 (a parser that returns nothing must not pass)", checked)
}

func sweepStrandAlertReadsThePair(g *guard) {
	expr := str(alertNamed(g, strandAlert)["expr"])
	g.expect(strings.Contains(expr, "sync_dispatch_unreclaimable_candidates"), "the strand alert does not read the candidate gauge")
	g.expect(strings.Contains(expr, "sync_dispatch_unreclaimable_terminalized_total"), "the strand alert does not read the terminalized counter")
	g.expect(regexp.MustCompile(`increase\(\s*sync_dispatch_unreclaimable_terminalized_total`).MatchString(expr), "the terminalized half must be an increase()")
	g.expect(strings.Contains(expr, "== 0"), "the terminalized half must assert NO movement (== 0); without it this alert fires on a sweep that is working")
}

func sweepFailureAlertIsSeparate(g *guard) {
	g.expect(strings.Contains(str(alertNamed(g, failingAlert)["expr"]), "sync_dispatch_unreclaimable_sweep_failures_total"), "the failure alert does not read the failures counter")
	g.expect(!strings.Contains(str(alertNamed(g, strandAlert)["expr"]), "sync_dispatch_unreclaimable_sweep_failures_total"),
		"the strand alert and the sweep-failure alert must stay separate: a failed pass reads zero on the candidate gauge, so one rule cannot express both halves")
}

func sweepStrandForOutlastsPass(g *guard) {
	f := forDur.FindStringSubmatch(str(alertNamed(g, strandAlert)["for"]))
	if f == nil {
		g.failf("unexpected `for` format: %q", str(alertNamed(g, strandAlert)["for"]))
		return
	}
	g.expect(minutes(f[1], f[2]) >= 15, "`for` is %s, below 15m: a shorter `for` pages on one slow pass", str(alertNamed(g, strandAlert)["for"]))
}

func sweepRearmThresholdExceedsOneUnitBudget(g *guard) {
	reg := g.jsonFile("contracts/jobs/v1/registry.json")
	budget := -1
	for _, j := range list(reg["jobs"]) {
		if str(at(j, "kind")) == "sync.provider_unit" {
			budget, _ = at(j, "max_attempts").(int)
		}
	}
	g.expect(budget > 0, "sync.provider_unit has no max_attempts in contracts/jobs/v1/registry.json")
	expr := str(alertNamed(g, rearmAlert)["expr"])
	m := regexp.MustCompile(`>\s*(\d+)`).FindStringSubmatch(expr)
	if m == nil {
		g.failf("no threshold in %q", expr)
		return
	}
	th, _ := strconv.Atoi(m[1])
	g.expect(th > budget, "threshold %d does not exceed one unit's whole delivery budget (%d): a single permanently broken unit would page", th, budget)
}

func sweepAlertsCarryRoutingAndEvidence(g *guard) {
	for n, want := range map[string]string{
		strandAlert:  "unreclaimable_sweep_mode_resolved",
		failingAlert: "queue-role",
		rearmAlert:   "dispatch_sync_run.publish_hit_terminal_delivery",
	} {
		a := alertNamed(g, n)
		labels := mp(a["labels"])
		g.expect(str(labels["team"]) == "platform", "%s team label is %q", n, str(labels["team"]))
		g.expect(str(labels["severity"]) == "warning" || str(labels["severity"]) == "critical", "%s severity label is %q", n, str(labels["severity"]))
		g.expect(strings.Contains(str(at(a, "annotations", "description")), want), "%s description must name the log line %q that carries the evidence", n, want)
	}
}

// --- dashboard ---

const dashboardRel = "deploy/grafana/dashboards/go-workers.json"

func dashboardExprs(g *guard) (panels []any, exprs []string) {
	d := g.jsonFile(dashboardRel)
	panels = list(d["panels"])
	for _, p := range panels {
		for _, tg := range list(mp(p)["targets"]) {
			exprs = append(exprs, str(mp(tg)["expr"]))
		}
	}
	return
}

func dashboardCoversRuntimeSignals(g *guard) {
	panels, exprs := dashboardExprs(g)
	all := strings.Join(exprs, "\n")
	for _, m := range []string{"worker_job_oldest_age_seconds", "worker_jobs_available", "worker_jobs_running", "worker_execution_saturation_ratio", "worker_job_attempts_total", "worker_job_duration_seconds_bucket", "worker_domain_state_mismatch_total", "worker_stream_lag", "worker_stream_pending", "worker_database_pool_saturation_ratio"} {
		g.expect(strings.Contains(all, m), "the dashboard reads no required metric %s", m)
	}
	ids := map[any]bool{}
	for _, p := range panels {
		id := at(p, "id")
		g.expect(!ids[id], "panel id %v is used twice", id)
		ids[id] = true
	}
	g.expect(str(g.jsonFile(dashboardRel)["refresh"]) == "30s", "dashboard refresh is %q, want 30s", str(g.jsonFile(dashboardRel)["refresh"]))
	var pool doc
	for _, p := range panels {
		if at(p, "id") == 8 {
			pool = mp(p)
		}
	}
	g.expect(pool != nil && str(at(pool, "targets", 0, "legendFormat")) == "{{pool}}", "panel 8 must legend by {{pool}}")
}

func dashboardUsesRuntimeDimensions(g *guard) {
	_, exprs := dashboardExprs(g)
	have := map[string]bool{}
	for _, e := range exprs {
		have[e] = true
	}
	for _, want := range []string{
		"max by (queue) (worker_job_oldest_age_seconds)",
		"sum by (queue) (worker_jobs_available)",
		"sum by (queue) (worker_jobs_running)",
		"max by (queue) (worker_execution_saturation_ratio)",
		"histogram_quantile(0.95, sum by (le, queue, kind) (rate(worker_job_duration_seconds_bucket[10m])))",
		"max by (job, instance) (worker_stream_lag)",
		"max by (job, instance) (worker_stream_pending)",
	} {
		g.expect(have[want], "the dashboard has no panel expression %q", want)
	}
}

func dashboardNoFixedProfileLabel(g *guard) {
	s := strings.ToLower(g.read(dashboardRel))
	g.expect(s != "", "dashboard unreadable")
	g.expect(!strings.Contains(s, "profile"), "the dashboard carries a fixed worker profile label")
	g.expect(!strings.Contains(s, "worker_group"), "the dashboard carries a worker_group label")
}

func dashboardLowCardinality(g *guard) {
	s := g.read(dashboardRel)
	g.expect(s != "", "dashboard unreadable")
	for _, l := range prohibitedLabels {
		g.expect(!strings.Contains(s, l), "the dashboard mentions the high-cardinality or payload label %q", l)
	}
}

// --- deploy alternates, sweep default, otel ---

var pythonImage = regexp.MustCompile(`dev-hops-(?:api|runner)|your-org/dev-health-ops|dev-health-ops:(?:latest|local)`)

func deployAlternatesStayDeleted(g *guard) {
	for _, n := range []string{"docker-compose", "docker-swarm", "kubernetes"} {
		g.expect(!exists(g.path("deploy/"+n)), "deploy/%s was deleted (CHAOS-6950): Helm is the one supported production artefact; a new alternate needs its own supported statement", n)
	}
}

func deployNoArtefactNamesPythonImage(g *guard) {
	n := 0
	for _, f := range walkFiles(g.path("deploy")) {
		rel := strings.TrimPrefix(strings.TrimPrefix(f, g.path("deploy")), "/")
		if strings.HasPrefix(rel, "helm/") || strings.HasSuffix(rel, ".json") || strings.HasSuffix(rel, ".png") {
			continue
		}
		n++
		if pythonImage.MatchString(g.read("deploy/" + rel)) {
			g.failf("deploy/%s references the Python api image", rel)
		}
	}
	g.expect(n > 0, "no deploy artefact scanned; the guard would pass vacuously")
}

const sweepKey = "SYNC_UNRECLAIMABLE_SWEEP"

func sweepComposeFlag(g *guard) {
	svc := mp(at(g.yamlFile("compose.yml"), "services", "go-reconciler"))
	g.expect(svc != nil, "compose.yml has no go-reconciler service")
	want := "--unreclaimable-sweep=${" + sweepKey + ":-active}"
	found := false
	for _, a := range list(svc["command"]) {
		found = found || str(a) == want
	}
	g.expect(found, "go-reconciler command lacks %s: the sweep must ship ACTIVE", want)
	env := svc["environment"]
	switch e := env.(type) {
	case map[string]any:
		_, in := e[sweepKey]
		g.expect(!in, "the sweep mode must be a flag, not an environment entry")
	case []any:
		for _, x := range e {
			g.expect(!strings.HasPrefix(str(x), sweepKey), "the sweep mode must be a flag, not an environment entry")
		}
	}
}

func sweepHelmValue(g *guard) {
	g.expect(str(at(g.yamlFile(chartFile("values.yaml")), "config", sweepKey)) == "active", "helm values config.%s is %q, want active", sweepKey, str(at(g.yamlFile(chartFile("values.yaml")), "config", sweepKey)))
}

func sweepEnvExample(g *guard) {
	want := sweepKey + `="active"`
	found := false
	for _, l := range strings.Split(g.read(".env.example"), "\n") {
		found = found || l == want
	}
	g.expect(found, ".env.example must carry the shipped default %s", want)
}

func otelHelmConfig(g *guard) {
	cfg := mp(g.yamlFile(chartFile("values.yaml"))["config"])
	for _, k := range []string{"OTEL_ENABLED", "OTEL_EXPORTER_OTLP_ENDPOINT", "OTEL_SERVICE_NAME", "OTEL_METRIC_EXPORT_INTERVAL"} {
		_, ok := cfg[k]
		g.expect(ok, "helm values config lacks %s: workers must receive the OTel metric configuration", k)
	}
}

func TestAlertGuardsRealTree(t *testing.T) {
	runReal(t,
		check{"worker-alerts-cover-signals", workerAlertsCoverSignals},
		check{"worker-alerts-runtime-dimensions", workerAlertsUseRuntimeDimensions},
		check{"worker-alerts-no-fixed-profile-label", workerAlertsNoFixedProfileLabel},
		check{"worker-alerts-low-cardinality", workerAlertsLowCardinality},
		check{"alert-names-unique", alertNamesUnique},
		check{"no-celery-alerts", noCeleryAlerts},
		check{"skip-metric-name-pinned", skipMetricNamePinned},
		check{"skip-alert-targets-real-metric", skipAlertTargetsRealMetric},
		check{"skip-alert-for-outlasts-window", skipAlertForOutlastsWindow},
		check{"skip-alert-threshold-any-nonzero", skipAlertThresholdIsAnyNonzero},
		check{"skip-alert-annotation-localization", skipAlertAnnotationExplainsLocalization},
		check{"skip-alert-labels-bounded", skipAlertLabelsBounded},
		check{"skip-alert-name-unique", skipAlertNameUniqueInFile},
		check{"sweep-reader-ignores-comments", sweepReaderIgnoresComments},
		check{"sweep-metric-parser-not-vacuous", sweepMetricParserNotVacuous},
		check{"sweep-every-alert-metric-emitted", sweepEveryAlertMetricEmitted},
		check{"sweep-strand-alert-reads-the-pair", sweepStrandAlertReadsThePair},
		check{"sweep-failure-alert-separate", sweepFailureAlertIsSeparate},
		check{"sweep-strand-for-outlasts-pass", sweepStrandForOutlastsPass},
		check{"sweep-rearm-threshold-exceeds-budget", sweepRearmThresholdExceedsOneUnitBudget},
		check{"sweep-alerts-routing-and-evidence", sweepAlertsCarryRoutingAndEvidence},
		check{"dashboard-covers-runtime-signals", dashboardCoversRuntimeSignals},
		check{"dashboard-runtime-dimensions", dashboardUsesRuntimeDimensions},
		check{"dashboard-no-fixed-profile-label", dashboardNoFixedProfileLabel},
		check{"dashboard-low-cardinality", dashboardLowCardinality},
		check{"deploy-alternates-stay-deleted", deployAlternatesStayDeleted},
		check{"deploy-no-artefact-names-python-image", deployNoArtefactNamesPythonImage},
		check{"sweep-compose-flag", sweepComposeFlag},
		check{"sweep-helm-value", sweepHelmValue},
		check{"sweep-env-example", sweepEnvExample},
		check{"otel-helm-config", otelHelmConfig},
	)
}

func appendText(rel, text string) func(*testing.T, string) {
	return func(t *testing.T, root string) {
		t.Helper()
		writeFile(t, root, rel, mustRead(t, rootPath(root, rel))+text)
	}
}

func TestAlertGuardsPlantedDefects(t *testing.T) {
	rules := []string{rulesRel}
	withGo := []string{rulesRel, budgetGo}
	sweepSrc := []string{rulesRel, reconcilerLoop, outboxLoop, "contracts/jobs/v1/registry.json"}
	dash := []string{dashboardRel}
	const lagPanel = `"expr": "max by (job, instance) (worker_stream_lag)"`
	const skipExpr = "sum by (provider, dataset) (\n            increase(dev_health_provider_artifact_skipped_total[25h])\n          ) > 0\n        for: 26h\n"
	runPlanted(t, []planted{
		{"alert-removed-from-the-go-workers-group", rules, mut(rulesRel, "- alert: GoWorkerStreamLagHigh", "- alert: GoWorkerStreamLagHighX"), workerAlertsCoverSignals, "alert set differs"},
		{"alert-reads-another-metric", rules, mut(rulesRel, "max by (job, instance) (worker_stream_lag)", "max by (job, instance) (worker_stream_depth)"), workerAlertsCoverSignals, "does not read worker_stream_lag"},
		{"saturation-aggregated-by-max", rules, mut(rulesRel, "avg by (queue) (worker_execution_saturation_ratio)", "max by (queue) (worker_execution_saturation_ratio)"), workerAlertsUseRuntimeDimensions, "AVERAGE"},
		{"worker-group-label-added", rules, mut(rulesRel, "max by (queue) (worker_job_oldest_age_seconds)", "max by (queue, worker_group) (worker_job_oldest_age_seconds)"), workerAlertsNoFixedProfileLabel, "worker_group label"},
		{"job-id-label-added", rules, mut(rulesRel, "max by (queue) (worker_job_oldest_age_seconds)", "max by (queue, job_id) (worker_job_oldest_age_seconds)"), workerAlertsLowCardinality, "job_id"},
		{"duplicate-alert-name", rules, mut(rulesRel, "- alert: GoWorkerStreamLagHigh", "- alert: GoWorkerStreamPendingTooOld"), alertNamesUnique, "must be unique"},
		{"celery-group-returns", rules, appendText(rulesRel, "\n  - name: celery\n    rules:\n      - alert: CeleryWorkerDown\n        expr: up == 0\n"), noCeleryAlerts, "celery"},
		{"skip-metric-renamed-in-go", withGo, mut(budgetGo, `writer, "dev_health_provider_artifact_skipped_total",`, `writer, "dev_health_provider_artifact_skipped_total_v2",`), skipMetricNamePinned, "artifact-skipped metric name"},
		{"skip-alert-reads-another-metric", withGo, mut(rulesRel, "increase(dev_health_provider_artifact_skipped_total[25h])", "increase(dev_health_provider_artifact_skips_total[25h])"), skipAlertTargetsRealMetric, "does not read the real metric"},
		{"skip-alert-for-shortened", rules, mut(rulesRel, skipExpr, strings.Replace(skipExpr, "for: 26h", "for: 24h", 1)), skipAlertForOutlastsWindow, "strictly longer"},
		{"skip-alert-threshold-raised", rules, mut(rulesRel, skipExpr, strings.Replace(skipExpr, ") > 0", ") > 5", 1)), skipAlertThresholdIsAnyNonzero, "simple '> 0'"},
		{"skip-alert-annotation-loses-localization", rules, mutAll(rulesRel, "repository", "repo"), skipAlertAnnotationExplainsLocalization, "localization"},
		{"skip-alert-grows-a-repository-dimension", rules, mut(rulesRel, skipExpr, strings.Replace(skipExpr, "(provider, dataset)", "(provider, dataset, repository)", 1)), skipAlertLabelsBounded, "repository"},
		{"skip-alert-routed-to-nobody", rules, mut(rulesRel, "          team: platform\n        annotations:\n          summary: \"Provider {{ $labels.provider }}/{{ $labels.dataset }} is sustaining", "          team: nobody\n        annotations:\n          summary: \"Provider {{ $labels.provider }}/{{ $labels.dataset }} is sustaining"), skipAlertLabelsBounded, "team label"},
		{"skip-alert-name-duplicated", rules, mut(rulesRel, "- alert: GoWorkerStreamLagHigh", "- alert: ProviderArtifactSkipsSustained"), skipAlertNameUniqueInFile, "exactly once"},
		{"sweep-emission-deleted-comment-survives", sweepSrc, mut(reconcilerLoop, `# TYPE sync_dispatch_unreclaimable_candidates gauge\nsync_dispatch_unreclaimable_candidates %d\n"`, `# TYPE sync_dispatch_unreclaimable_candidates gauge\n"`), sweepEveryAlertMetricEmitted, "which no WritePrometheus emits"},
		{"sweep-strand-alert-stops-requiring-no-movement", sweepSrc, mut(rulesRel, "sum by (instance) (increase(sync_dispatch_unreclaimable_terminalized_total[15m])) == 0", "sum by (instance) (increase(sync_dispatch_unreclaimable_terminalized_total[15m])) >= 0"), sweepStrandAlertReadsThePair, "NO movement"},
		{"sweep-failure-folded-into-the-strand-alert", sweepSrc, mut(rulesRel, "sum by (instance) (increase(sync_dispatch_unreclaimable_terminalized_total[15m])) == 0", "sum by (instance) (increase(sync_dispatch_unreclaimable_terminalized_total[15m])) == 0 and on (instance) sum by (instance) (sync_dispatch_unreclaimable_sweep_failures_total) == 0"), sweepFailureAlertIsSeparate, "must stay separate"},
		{"sweep-strand-for-shortened", sweepSrc, mut(rulesRel, "        for: 15m\n        labels:\n          severity: warning\n          team: platform\n        annotations:\n          summary: \"Unreclaimable sync units are selected", "        for: 5m\n        labels:\n          severity: warning\n          team: platform\n        annotations:\n          summary: \"Unreclaimable sync units are selected"), sweepStrandForOutlastsPass, "below 15m"},
		{"sweep-rearm-threshold-below-one-unit-budget", sweepSrc, mut(rulesRel, "provider_unit_strands_rearmed_total[1h])) > 20", "provider_unit_strands_rearmed_total[1h])) > 3"), sweepRearmThresholdExceedsOneUnitBudget, "does not exceed"},
		{"sweep-description-loses-the-evidence-pointer", sweepSrc, mutAll(rulesRel, "unreclaimable_sweep_mode_resolved", "mode"), sweepAlertsCarryRoutingAndEvidence, "must name the log line"},
		{"dashboard-drops-a-required-metric", dash, mutAll(dashboardRel, "worker_stream_lag", "worker_stream_x"), dashboardCoversRuntimeSignals, "worker_stream_lag"},
		{"dashboard-duplicates-a-panel-id", dash, mut(dashboardRel, `"id": 2,`, `"id": 1,`), dashboardCoversRuntimeSignals, "used twice"},
		{"dashboard-refresh-slowed", dash, mut(dashboardRel, `"refresh": "30s"`, `"refresh": "1m"`), dashboardCoversRuntimeSignals, "refresh"},
		{"dashboard-pool-legend-lost", dash, mut(dashboardRel, `"legendFormat": "{{pool}}"`, `"legendFormat": "pool"`), dashboardCoversRuntimeSignals, "legend by"},
		{"dashboard-aggregation-drifts", dash, mut(dashboardRel, lagPanel, `"expr": "max (worker_stream_lag)"`), dashboardUsesRuntimeDimensions, "no panel expression"},
		{"dashboard-gains-a-profile-label", dash, mut(dashboardRel, `"refresh": "30s"`, `"refresh": "30s", "tags": ["profile"]`), dashboardNoFixedProfileLabel, "profile"},
		{"dashboard-gains-a-payload-label", dash, mut(dashboardRel, `"refresh": "30s"`, `"refresh": "30s", "tags": ["payload"]`), dashboardLowCardinality, "payload"},
		{"deleted-deploy-tree-returns", []string{"deploy/go-workers/deployment.json"},
			func(t *testing.T, root string) {
				writeFile(t, root, "deploy/docker-compose/stack.cfg", "services: {}\n")
			},
			deployAlternatesStayDeleted, "deploy/docker-compose"},
		{"deploy-artefact-names-the-python-image", []string{"deploy/go-workers/deployment.json"},
			func(t *testing.T, root string) {
				writeFile(t, root, "deploy/go-api/x.yml", "image: ghcr.io/x/dev-hops-api:latest\n")
			},
			deployNoArtefactNamesPythonImage, "references the Python api image"},
		{"sweep-ships-in-shadow-in-compose", []string{"compose.yml"}, mut("compose.yml", "${SYNC_UNRECLAIMABLE_SWEEP:-active}", "${SYNC_UNRECLAIMABLE_SWEEP:-shadow}"), sweepComposeFlag, "must ship ACTIVE"},
		{"sweep-ships-in-shadow-in-helm", chartOnly, mut(chartFile("values.yaml"), `SYNC_UNRECLAIMABLE_SWEEP: "active"`, `SYNC_UNRECLAIMABLE_SWEEP: "shadow"`), sweepHelmValue, "want active"},
		{"env-example-documents-shadow", []string{".env.example"}, mut(".env.example", `SYNC_UNRECLAIMABLE_SWEEP="active"`, `SYNC_UNRECLAIMABLE_SWEEP="shadow"`), sweepEnvExample, "shipped default"},
		{"otel-key-dropped-from-helm-config", chartOnly, mut(chartFile("values.yaml"), `  OTEL_METRIC_EXPORT_INTERVAL: "60000"`, `  OTEL_METRIC_EXPORT_INTERVAL_X: "60000"`), otelHelmConfig, "OTEL_METRIC_EXPORT_INTERVAL"},
	})
}
