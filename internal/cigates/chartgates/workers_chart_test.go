package chartgates_test

import (
	"fmt"
	"regexp"
	"strings"
	"testing"
)

// Ported from tests/workers/test_helm_no_cronjobs.py, test_helm_billing_edge_removed.py (CHAOS-6903),
// test_helm_query_api_internal.py, test_helm_go_worker_labels.py (CHAOS-4428) and
// test_helm_worker_disruption_and_drain.py (CHAOS-4428, CHAOS-4984).

func noCronJobsChartRenders(g *guard) {
	sets := map[string][]string{
		"default":               nil,
		"go-workers-and-go-api": set("goWorkers.enabled=true", "goApi.enabled=true"),
	}
	for n, args := range sets {
		docs := g.docs("no-cronjobs", args...)
		g.expect(len(docs) > 0, "the %s value set rendered nothing; the guard would pass vacuously", n)
		var cj []string
		for _, d := range ofKind(docs, "CronJob") {
			cj = append(cj, name(d))
		}
		g.expect(len(cj) == 0, "the %s value set renders CronJobs: %v", n, cj)
	}
}

func noCronJobsValuesBlock(g *guard) {
	values := g.yamlFile(chartFile("values.yaml"))
	_, ok := values["cronjobs"]
	g.expect(len(values) > 0, "values.yaml did not parse")
	g.expect(!ok, "values.yaml still has a `cronjobs` block; no template reads it")
}

const ingressHosts = `ingress.hosts=[{"host":"h","paths":[{"path":"/","pathType":"Prefix","service":"%s"}]}]`

func billingEdgeNothingNamed(g *guard) {
	docs := g.docs("r", join(set("ingress.enabled=true"), setJSON(fmt.Sprintf(ingressHosts, "web")))...)
	for _, d := range docs {
		g.expect(!strings.Contains(name(d), "billing-edge"), "%s %s is named billing-edge", kind(d), name(d))
	}
}

func billingEdgeEnableFailsRender(g *guard) {
	stderr := g.refusal("enabling the removed Python billing edge", "r", set("billingEdge.enabled=true")...)
	g.expect(strings.Contains(stderr, "billingEdge") && strings.Contains(stderr, "goApiBillingEdge"), "the refusal does not name billingEdge and its replacement goApiBillingEdge: %s", firstLines(stderr, 2))
}

func billingEdgeDisabledStillRenders(g *guard) {
	g.mustRender("billingEdge.enabled=false (what older values files carry)", "r", set("billingEdge.enabled=false")...)
}

func billingEdgeIngressPathFailsRender(g *guard) {
	stderr := g.refusal("an ingress path routed to the removed edge", "r", join(set("ingress.enabled=true"), setJSON(fmt.Sprintf(ingressHosts, "billing-edge")))...)
	g.expect(strings.Contains(stderr, "billing-edge") && strings.Contains(stderr, "goApiBillingEdge"), "the refusal does not name billing-edge and goApiBillingEdge: %s", firstLines(stderr, 2))
}

const qaiFull = "qai-dev-health"

func qaiDocs(g *guard, sets ...string) []doc {
	return g.docs("qai", join(set("queryApi.enabled=true"), set(sets...))...)
}

func qaiContainer(g *guard, docs []doc) doc {
	d := named(docs, "Deployment", qaiFull+"-query-api")
	if d == nil {
		g.failf("the query-api Deployment was not rendered")
		return doc{}
	}
	return containers(d)[0]
}

func portNames(c doc) []string {
	var out []string
	for _, p := range list(c["ports"]) {
		out = append(out, str(at(p, "name")))
	}
	return out
}

func qaiInternalOffByDefault(g *guard) {
	docs := qaiDocs(g)
	for _, d := range docs {
		g.expect(!strings.Contains(name(d), "query-api-internal"), "%s %s renders with the internal listener off", kind(d), name(d))
	}
	c := qaiContainer(g, docs)
	g.expect(!has(envMap(c), "QUERY_API_INTERNAL_ADDR"), "QUERY_API_INTERNAL_ADDR is set with the internal listener off")
	g.expect(equal(portNames(c), []string{"http"}), "ports %v with the internal listener off, want [http]", portNames(c))
}

func qaiEnabledSetsAddressAndNamedPort(g *guard) {
	c := qaiContainer(g, qaiDocs(g, "queryApi.internal.enabled=true"))
	g.expect(str(envMap(c)["QUERY_API_INTERNAL_ADDR"]["value"]) == ":8091", "QUERY_API_INTERNAL_ADDR is %q, want :8091", str(envMap(c)["QUERY_API_INTERNAL_ADDR"]["value"]))
	ports := map[string]any{}
	for _, p := range list(c["ports"]) {
		ports[str(at(p, "name"))] = at(p, "containerPort")
	}
	g.expect(equal(ports, map[string]any{"http": 8090, "internal": 8091}), "ports %v, want http 8090 and internal 8091", ports)
}

func qaiInternalServiceTargetsNamedPort(g *guard) {
	docs := qaiDocs(g, "queryApi.internal.enabled=true")
	svc := named(docs, "Service", qaiFull+"-query-api-internal")
	if svc == nil {
		g.failf("the internal Service was not rendered")
		return
	}
	g.expect(str(at(svc, "spec", "type")) == "ClusterIP", "the internal Service type is %q", str(at(svc, "spec", "type")))
	ports := list(at(svc, "spec", "ports"))
	g.expect(len(ports) == 1 && at(ports, 0, "port") == 8091 && at(ports, 0, "targetPort") == "internal", "internal Service ports %v, want 8091 -> internal", ports)
	dep := named(docs, "Deployment", qaiFull+"-query-api")
	labels := mp(at(dep, "spec", "template", "metadata", "labels"))
	for k, v := range mp(at(svc, "spec", "selector")) {
		g.expect(str(labels[k]) == str(v), "the internal Service selector %s=%v does not match the pod labels", k, v)
	}
	pub := named(docs, "Service", qaiFull+"-query-api")
	var pp []any
	for _, p := range list(at(pub, "spec", "ports")) {
		pp = append(pp, at(p, "port"))
	}
	g.expect(equal(pp, []any{8090}), "the public Service ports %v, want [8090]: it must never name the internal port", pp)
}

func qaiExtraAllowedPodsAdded(g *guard) {
	docs := qaiDocs(g, "queryApi.internal.enabled=true", "queryApi.internal.allowedFrom[0].matchLabels.app=tools")
	pol := named(docs, "NetworkPolicy", qaiFull+"-query-api-internal")
	if pol == nil {
		g.failf("the internal NetworkPolicy was not rendered")
		return
	}
	var sels []any
	for _, s := range list(at(pol, "spec", "ingress", 1, "from")) {
		sels = append(sels, at(s, "podSelector"))
	}
	g.expect(equal(sels, []any{doc{"matchLabels": doc{"app": "tools"}}}), "internal port sources %v, want exactly the tools selector", sels)
}

func qaiInternalPortEqualPublicRefused(g *guard) {
	stderr := g.refusal("the internal port equal to the public port", "qai", set("queryApi.enabled=true", "queryApi.internal.enabled=true", "queryApi.internal.port=8090")...)
	g.expect(strings.Contains(stderr, "must differ from queryApi.port"), "the refusal does not say the ports must differ: %s", firstLines(stderr, 2))
}

func qaiMetricsCommentIsAccurate(g *guard) {
	text := g.read(chartFile("templates/query-api-deployment.yaml"))
	g.expect(!strings.Contains(text, "Scrape via `http`"), "the template comment still points the scrape at the public port")
	g.expect(strings.Contains(text, "listener only (queryApi.internal.enabled)"), "the template comment no longer says /metrics lives on the internal listener only")
}

const toolsPodSelector = `[{"matchLabels":{"run":"dev-health-go-api-tools-oneoff"}}]`

func qaiDocumentedToolsPodSelector(g *guard) {
	runbook := g.read("docs/operate/runbooks/query-api-bootstrap.md")
	g.expect(strings.Contains(runbook, "queryApi.internal.allowedFrom="+toolsPodSelector), "the bootstrap runbook no longer documents queryApi.internal.allowedFrom=%s", toolsPodSelector)
	tools := doc{"podSelector": doc{"matchLabels": doc{"run": "dev-health-go-api-tools-oneoff"}}}
	def := named(qaiDocs(g, "queryApi.internal.enabled=true"), "NetworkPolicy", qaiFull+"-query-api-internal")
	for _, rule := range list(at(def, "spec", "ingress")) {
		for _, from := range list(at(rule, "from")) {
			g.expect(!equal(from, any(tools)), "the default policy already admits the tools pod; it must be admitted only deliberately")
		}
	}
	docs := g.docs("qai", join(set("queryApi.enabled=true", "queryApi.internal.enabled=true"), setJSON("queryApi.internal.allowedFrom="+toolsPodSelector))...)
	pol := named(docs, "NetworkPolicy", qaiFull+"-query-api-internal")
	found := false
	for _, from := range list(at(pol, "spec", "ingress", 1, "from")) {
		found = found || equal(from, any(tools))
	}
	g.expect(found, "the documented tools pod selector does not render as an extra source")
}

var labelValue = regexp.MustCompile(`^(([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9])?$`)

var labelContractSets = join(
	set("goWorkers.enabled=true", "goWorkers.pgbouncer.enabled=true", "goWorkers.pgbouncer.postgres.host=postgres.internal"),
	setString("goWorkers.pgbouncer.secret.data.RIVER_DOMAIN_DATABASE_PASSWORD=d", "goWorkers.pgbouncer.secret.data.RIVER_QUEUE_DATABASE_PASSWORD=q", "goWorkers.pgbouncer.secret.data.RIVER_COORDINATOR_DATABASE_PASSWORD=c"))

func labelWorkers(g *guard) map[string]doc {
	out := map[string]doc{}
	for _, d := range ofKind(g.docs("label-contract", labelContractSets...), "Deployment") {
		if label(d, "app.kubernetes.io/component") == "go-worker" {
			out[strings.TrimPrefix(name(d), "label-contract-dev-health-")] = d
		}
	}
	return out
}

func labelsLegalValues(g *guard) {
	workers := labelWorkers(g)
	for _, want := range []string{"go-heavy", "go-ops"} {
		g.expect(workers[want] != nil, "the chart's own multi-queue default group %s did not render: %d groups", want, len(workers))
	}
	for n, d := range workers {
		sites := map[string]doc{
			"metadata.labels":               mp(at(d, "metadata", "labels")),
			"spec.template.metadata.labels": mp(at(d, "spec", "template", "metadata", "labels")),
		}
		for where, ls := range sites {
			for k, v := range ls {
				text := str(v)
				g.expect(labelValue.MatchString(text) && len(text) <= 63, "illegal Kubernetes label value rendered: %s %s[%s]=%q", n, where, k, text)
			}
		}
	}
}

func labelsQueueSetEncodesEveryQueue(g *guard) {
	for n, d := range labelWorkers(g) {
		if n != "go-heavy" && n != "go-ops" {
			continue
		}
		var queues []string
		for _, a := range strs(containers(d)[0]["args"]) {
			if strings.HasPrefix(a, "--queues=") {
				queues = strings.Split(strings.TrimPrefix(a, "--queues="), ",")
				break
			}
		}
		if len(queues) < 2 {
			g.failf("%s is not a multi-queue group (queues %v)", n, queues)
			continue
		}
		value := label(d, "dev-health.io/queue-set")
		g.expect(!strings.Contains(value, ","), "%s queue-set label reintroduced a comma: %q", n, value)
		for _, q := range queues {
			g.expect(strings.Contains(value, q), "%s queue-set label %q dropped queue %q", n, value, q)
		}
	}
}

// --- disruption and drain ---

func drainGroup(name string, queues []string, replicas, grace int, autoscaling string) string {
	if autoscaling == "" {
		autoscaling = "{enabled: false}"
	}
	conc := make([]string, len(queues))
	for i, q := range queues {
		conc[i] = q + ": 1"
	}
	return fmt.Sprintf(`
goWorkers:
  enabled: true
  groups:
    - name: %s
      image: ghcr.io/full-chaos/dev-health-go-dho:latest
      subcommand: worker
      queues: [%s]
      queueConcurrency: {%s}
      replicas: %d
      terminationGracePeriodSeconds: %d
      resources: {requests: {cpu: 250m, memory: 256Mi}, limits: {cpu: "1", memory: 1Gi}}
      autoscaling: %s
      bridgeUrl: ""
`, name, strings.Join(queues, ", "), strings.Join(conc, ", "), replicas, grace, autoscaling)
}

const (
	metricsFloor    = 7260
	autoscaledTwo   = "{enabled: true, minReplicas: 1, maxReplicas: 2, queueDepth: 1, oldestAgeSeconds: 60, saturationMilli: 800m}"
	autoscaledOne   = "{enabled: true, minReplicas: 1, maxReplicas: 1, queueDepth: 1, oldestAgeSeconds: 60, saturationMilli: 800m}"
	drainRelease    = "drain-contract"
	minReplicasTmpl = "{enabled: true, minReplicas: %d, maxReplicas: 2, queueDepth: 1, oldestAgeSeconds: 60, saturationMilli: 800m}"
)

func drainRender(g *guard, values string) render {
	if values == "" {
		return g.tpl(drainRelease, set("goWorkers.enabled=true")...)
	}
	return g.tpl(drainRelease, "-f", g.valuesFile(values))
}

func drainDocs(g *guard, values string) []doc {
	r := drainRender(g, values)
	if r.code != 0 {
		g.failf("render failed: %s", firstLines(r.stderr, 3))
		return nil
	}
	docs, err := parseDocs(r.stdout)
	if err != nil {
		g.failf("output is not YAML: %v", err)
	}
	return docs
}

func drainMetricsBelowFloorFails(g *guard) {
	r := drainRender(g, drainGroup("heavy", []string{"metrics"}, 1, 960, ""))
	g.expect(r.code != 0, "a 960s metrics group must not render: it is SIGKILLed while holding a claimed job")
	g.expect(r.code == 0 || strings.Contains(r.stderr, fmt.Sprint(metricsFloor)), "the error must name the %ds floor: %s", metricsFloor, firstLines(r.stderr, 2))
	g.expect(r.code == 0 || strings.Contains(r.stderr, "--shutdown-timeout"), "the error must name the --shutdown-timeout flag the value becomes: %s", firstLines(r.stderr, 2))
}

func drainMetricsAtFloorRenders(g *guard) {
	r := drainRender(g, drainGroup("heavy", []string{"metrics"}, 1, metricsFloor, ""))
	g.expect(r.code == 0, "%ds is the floor itself and must be allowed: %s", metricsFloor, firstLines(r.stderr, 2))
}

func drainNonMetricsNotConstrained(g *guard) {
	r := drainRender(g, drainGroup("ops", []string{"heartbeat"}, 1, 60, ""))
	g.expect(r.code == 0, "a 60s non-metrics group is legitimate: %s", firstLines(r.stderr, 2))
}

func drainDefaultsSatisfyFloor(g *guard) {
	n := 0
	for _, d := range ofKind(drainDocs(g, ""), "Deployment") {
		var args []string
		for _, c := range containers(d) {
			args = append(args, strs(c["args"])...)
		}
		isMetrics := false
		for _, a := range args {
			isMetrics = isMetrics || (strings.Contains(a, "--queues=") && strings.Contains(a, "metrics"))
		}
		if !isMetrics {
			continue
		}
		n++
		grace, _ := at(d, "spec", "template", "spec", "terminationGracePeriodSeconds").(int)
		g.expect(grace >= metricsFloor, "%s ships below its own floor (%d)", name(d), grace)
	}
	g.expect(n > 0, "no default metrics group found; the guard would pass vacuously")
}

func pdbsOf(g *guard, values string) []doc {
	return ofKind(drainDocs(g, values), "PodDisruptionBudget")
}

func drainSingleReplicaNoPDB(g *guard) {
	p := pdbsOf(g, drainGroup("heavy", []string{"metrics"}, 1, metricsFloor, ""))
	g.expect(len(p) == 0, "a PDB with maxUnavailable: 1 over one replica allows zero evictions and blocks node drain forever (%d rendered)", len(p))
}

func drainMultiReplicaPDB(g *guard) {
	p := pdbsOf(g, drainGroup("heavy", []string{"metrics"}, 2, metricsFloor, ""))
	if len(p) != 1 {
		g.failf("expected exactly one PDB for a 2-replica group, got %d", len(p))
		return
	}
	spec := mp(p[0]["spec"])
	g.expect(spec["maxUnavailable"] == 1, "the PDB maxUnavailable is %v, want 1", spec["maxUnavailable"])
	_, ma := spec["minAvailable"]
	g.expect(!ma, "minAvailable silently changes meaning when an HPA moves the replica count; maxUnavailable does not")
	g.expect(str(at(spec, "selector", "matchLabels", "dev-health.io/worker-group")) == "heavy", "a PDB whose selector does not target its own group budgets the wrong pods")
}

func drainPDBSelectorMatchesDeployment(g *guard) {
	docs := drainDocs(g, drainGroup("heavy", []string{"metrics"}, 2, metricsFloor, ""))
	pdbs := ofKind(docs, "PodDisruptionBudget")
	var deps []doc
	for _, d := range ofKind(docs, "Deployment") {
		if label(d, "app.kubernetes.io/component") == "go-worker" {
			deps = append(deps, d)
		}
	}
	if len(pdbs) != 1 || len(deps) != 1 {
		g.failf("want one PDB and one go-worker Deployment, got %d and %d", len(pdbs), len(deps))
		return
	}
	pod := mp(at(deps[0], "spec", "template", "metadata", "labels"))
	for k, v := range mp(at(pdbs[0], "spec", "selector", "matchLabels")) {
		g.expect(str(pod[k]) == str(v), "PDB selector %s=%v does not match the pod template (%q): the budget would apply to no pods", k, v, str(pod[k]))
	}
}

func drainAutoscaledZeroStaticReplicasHasPDB(g *guard) {
	p := pdbsOf(g, drainGroup("heavy", []string{"metrics"}, 0, metricsFloor, autoscaledTwo))
	g.expect(len(p) == 1, "an HPA group that reaches 2 pods has no budget (%d PDBs)", len(p))
	if len(p) == 1 {
		g.expect(at(p[0], "spec", "maxUnavailable") == 1, "the PDB maxUnavailable is %v, want 1", at(p[0], "spec", "maxUnavailable"))
	}
}

func drainAutoscaledToOneNoPDB(g *guard) {
	p := pdbsOf(g, drainGroup("heavy", []string{"metrics"}, 0, metricsFloor, autoscaledOne))
	g.expect(len(p) == 0, "a group capped at one pod still permits zero voluntary evictions: no PDB expected (%d rendered)", len(p))
}

func drainDefaultsBudgetAutoscaledGroups(g *guard) {
	got := map[string]bool{}
	for _, p := range pdbsOf(g, "") {
		got[label(p, "dev-health.io/worker-group")] = true
	}
	g.expect(equal(got, map[string]bool{"heavy": true, "ops": true, "sync": true, "sync-provider": true}),
		"exactly the four groups whose HPA reaches maxReplicas: 2 must be budgeted, the singletons must not: %v", got)
}

func drainServingDeploymentsNeverDip(g *guard) {
	found := 0
	for _, d := range ofKind(drainDocs(g, "metricsApi:\n  enabled: true\n"), "Deployment") {
		if label(d, "app.kubernetes.io/component") != "web" {
			continue
		}
		found++
		strat := mp(at(d, "spec", "strategy"))
		g.expect(str(strat["type"]) == "RollingUpdate", "web must roll, not Recreate: %v", strat)
		r := mp(strat["rollingUpdate"])
		g.expect(r["maxUnavailable"] == 0, "web would drop below serving capacity mid-rollout: %v", r)
		surge, _ := r["maxSurge"].(int)
		g.expect(surge >= 1, "web with maxUnavailable 0 and no surge cannot roll at all: %v", r)
	}
	g.expect(found > 0, "no web Deployment rendered")
}

func drainGroupWithoutAutoscalingRejected(g *guard) {
	values := `
goWorkers:
  enabled: true
  groups:
    - name: solo
      image: ghcr.io/full-chaos/dev-health-go-dho:latest
      subcommand: worker
      queues: [heartbeat]
      queueConcurrency: {heartbeat: 1}
      replicas: 3
      terminationGracePeriodSeconds: 60
      resources: {requests: {cpu: 250m, memory: 256Mi}, limits: {cpu: "1", memory: 1Gi}}
`
	r := drainRender(g, values)
	g.expect(r.code != 0, "a group with no autoscaling block must not render silently")
	g.expect(r.code == 0 || strings.Contains(r.stderr, "autoscaling"), "the error must name the missing block: %s", firstLines(r.stderr, 2))
}

func drainMinReplicasZeroFails(g *guard) {
	r := drainRender(g, drainGroup("heavy", []string{"investment"}, 1, 60, fmt.Sprintf(minReplicasTmpl, 0)))
	g.expect(r.code != 0, "an autoscaling-enabled group with minReplicas: 0 must not render")
	g.expect(r.code == 0 || (strings.Contains(r.stderr, "minReplicas") && strings.Contains(r.stderr, "1")), "the error must name the field and the floor: %s", firstLines(r.stderr, 2))
}

func drainMinReplicasOneRenders(g *guard) {
	r := drainRender(g, drainGroup("heavy", []string{"investment"}, 1, 60, fmt.Sprintf(minReplicasTmpl, 1)))
	g.expect(r.code == 0, "1 is the floor itself and must be allowed: %s", firstLines(r.stderr, 2))
}

func drainAutoscalingDisabledNotConstrained(g *guard) {
	r := drainRender(g, drainGroup("ops", []string{"coverage"}, 1, 60, "{enabled: false}"))
	g.expect(r.code == 0, "autoscaling: {enabled: false} must never trip the minReplicas floor: %s", firstLines(r.stderr, 2))
}

func drainDefaultsSatisfyMinReplicasFloor(g *guard) {
	docs := drainDocs(g, "")
	n := 0
	for _, d := range docs {
		switch kind(d) {
		case "HorizontalPodAutoscaler":
			n++
			v, _ := at(d, "spec", "minReplicas").(int)
			g.expect(v >= 1, "%s ships minReplicas=%v, below the chart's own floor", name(d), at(d, "spec", "minReplicas"))
		case "ScaledObject":
			n++
			v, _ := at(d, "spec", "minReplicaCount").(int)
			g.expect(v >= 1, "%s ships minReplicaCount=%v, below the chart's own floor", name(d), at(d, "spec", "minReplicaCount"))
		}
	}
	g.expect(n > 0, "no autoscaler rendered by default; the guard would pass vacuously")
}

func TestWorkersChartGuardsRealTree(t *testing.T) {
	runReal(t,
		check{"no-cronjobs-chart-renders", noCronJobsChartRenders},
		check{"no-cronjobs-values-block", noCronJobsValuesBlock},
		check{"billing-edge-nothing-named", billingEdgeNothingNamed},
		check{"billing-edge-enable-fails-render", billingEdgeEnableFailsRender},
		check{"billing-edge-disabled-still-renders", billingEdgeDisabledStillRenders},
		check{"billing-edge-ingress-path-fails-render", billingEdgeIngressPathFailsRender},
		check{"query-api-internal-off-by-default", qaiInternalOffByDefault},
		check{"query-api-internal-address-and-port", qaiEnabledSetsAddressAndNamedPort},
		check{"query-api-internal-service", qaiInternalServiceTargetsNamedPort},
		check{"query-api-internal-extra-allowed-pods", qaiExtraAllowedPodsAdded},
		check{"query-api-internal-port-equal-public-refused", qaiInternalPortEqualPublicRefused},
		check{"query-api-internal-metrics-comment", qaiMetricsCommentIsAccurate},
		check{"query-api-internal-documented-tools-pod", qaiDocumentedToolsPodSelector},
		check{"labels-legal-values", labelsLegalValues},
		check{"labels-queue-set-encodes-every-queue", labelsQueueSetEncodesEveryQueue},
		check{"drain-metrics-below-floor-fails", drainMetricsBelowFloorFails},
		check{"drain-metrics-at-floor-renders", drainMetricsAtFloorRenders},
		check{"drain-non-metrics-not-constrained", drainNonMetricsNotConstrained},
		check{"drain-defaults-satisfy-floor", drainDefaultsSatisfyFloor},
		check{"drain-single-replica-no-pdb", drainSingleReplicaNoPDB},
		check{"drain-multi-replica-pdb", drainMultiReplicaPDB},
		check{"drain-pdb-selector-matches-deployment", drainPDBSelectorMatchesDeployment},
		check{"drain-autoscaled-zero-static-has-pdb", drainAutoscaledZeroStaticReplicasHasPDB},
		check{"drain-autoscaled-to-one-no-pdb", drainAutoscaledToOneNoPDB},
		check{"drain-defaults-budget-autoscaled-groups", drainDefaultsBudgetAutoscaledGroups},
		check{"drain-serving-deployments-never-dip", drainServingDeploymentsNeverDip},
		check{"drain-group-without-autoscaling-rejected", drainGroupWithoutAutoscalingRejected},
		check{"drain-min-replicas-zero-fails", drainMinReplicasZeroFails},
		check{"drain-min-replicas-one-renders", drainMinReplicasOneRenders},
		check{"drain-autoscaling-disabled-not-constrained", drainAutoscalingDisabledNotConstrained},
		check{"drain-defaults-satisfy-min-replicas-floor", drainDefaultsSatisfyMinReplicasFloor},
	)
}

func mutAll(rel, old, repl string) func(*testing.T, string) {
	return func(t *testing.T, root string) {
		t.Helper()
		p := rootPath(root, rel)
		b := mustRead(t, p)
		if !strings.Contains(b, old) {
			t.Fatalf("plant %s: text %q not found, the defect did not land", rel, old)
		}
		writeFile(t, root, rel, strings.ReplaceAll(b, old, repl))
	}
}

func TestWorkersChartGuardsPlantedDefects(t *testing.T) {
	const workers = "templates/go-workers.yaml"
	const pdb = "templates/go-worker-pdb.yaml"
	const keda = "templates/go-worker-keda.yaml"
	const qapi = "templates/query-api-deployment.yaml"
	const runbook = "docs/operate/runbooks/query-api-bootstrap.md"
	withRunbook := append([]string{runbook}, chartOnly...)
	cronJob := func(t *testing.T, root string) {
		writeFile(t, root, chartFile("templates/cronjob.yaml"), "apiVersion: batch/v1\nkind: CronJob\nmetadata:\n  name: stray\nspec:\n  schedule: \"0 * * * *\"\n  jobTemplate:\n    spec:\n      template:\n        spec:\n          restartPolicy: Never\n          containers:\n            - name: c\n              image: x\n")
	}
	billingDeployment := func(t *testing.T, root string) {
		writeFile(t, root, chartFile("templates/billing-edge.yaml"), "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: billing-edge\nspec:\n  selector:\n    matchLabels: {a: b}\n  template:\n    metadata:\n      labels: {a: b}\n    spec:\n      containers:\n        - name: c\n          image: x\n")
	}
	runPlanted(t, []planted{
		{"chart-renders-a-cronjob", chartOnly, cronJob, noCronJobsChartRenders, "renders CronJobs"},
		{"values-regain-a-cronjobs-block", chartOnly,
			func(t *testing.T, root string) {
				p := chartFile("values.yaml")
				writeFile(t, root, p, mustRead(t, rootPath(root, p))+"\ncronjobs: {}\n")
			},
			noCronJobsValuesBlock, "cronjobs"},
		{"a-workload-named-billing-edge-returns", chartOnly, billingDeployment, billingEdgeNothingNamed, "is named billing-edge"},
		{"enabling-the-removed-edge-renders-clean", chartOnly,
			mut(chartFile("templates/billing-edge-removed-guard.yaml"), "{{- if (.Values.billingEdge | default dict).enabled }}", "{{- if false }}"),
			billingEdgeEnableFailsRender, "must be refused"},
		{"disabled-billing-edge-values-stop-rendering", chartOnly,
			mut(chartFile("templates/billing-edge-removed-guard.yaml"), "{{- if (.Values.billingEdge | default dict).enabled }}", "{{- if .Values.billingEdge }}"),
			billingEdgeDisabledStillRenders, "billingEdge.enabled=false"},
		{"ingress-path-to-the-removed-edge-renders-clean", chartOnly,
			mut(chartFile("templates/ingress.yaml"), `{{- else if eq .service "billing-edge" }}`, `{{- else if eq .service "billing-edge-never" }}`),
			billingEdgeIngressPathFailsRender, "must be refused"},
		{"internal-listener-address-wrong", chartOnly,
			mut(chartFile(qapi), `value: {{ printf ":%v" .Values.queryApi.internal.port | quote }}`, `value: ":9999"`),
			qaiEnabledSetsAddressAndNamedPort, "want :8091"},
		{"internal-port-unnamed", chartOnly,
			mut(chartFile(qapi), "            - name: internal\n              containerPort: {{ .Values.queryApi.internal.port }}", "            - name: internal2\n              containerPort: {{ .Values.queryApi.internal.port }}"),
			qaiEnabledSetsAddressAndNamedPort, "ports"},
		{"internal-service-targets-the-public-port", chartOnly,
			mut(chartFile(qapi), "      port: {{ .Values.queryApi.internal.port }}\n      targetPort: internal", "      port: {{ .Values.queryApi.internal.port }}\n      targetPort: http"),
			qaiInternalServiceTargetsNamedPort, "internal Service ports"},
		{"internal-policy-admits-every-pod", chartOnly,
			mut(chartFile(qapi), "        - podSelector:\n            {{- toYaml . | nindent 12 }}", "        - podSelector: {}"),
			qaiExtraAllowedPodsAdded, "want exactly the tools selector"},
		{"internal-equal-public-port-accepted", chartOnly,
			mut(chartFile(qapi), "{{- if and .Values.queryApi.internal.enabled (eq (int .Values.queryApi.internal.port) (int .Values.queryApi.port)) }}", "{{- if false }}"),
			qaiInternalPortEqualPublicRefused, "must be refused"},
		{"internal-listener-comment-points-at-the-public-port", chartOnly,
			mut(chartFile(qapi), "listener only (queryApi.internal.enabled)", "listener (queryApi.internal.enabled)"),
			qaiMetricsCommentIsAccurate, "no longer says"},
		{"runbook-drops-the-documented-selector", withRunbook,
			mut(runbook, `queryApi.internal.allowedFrom=[{"matchLabels":{"run":"dev-health-go-api-tools-oneoff"}}]`, `queryApi.internal.allowedFrom=[]`),
			qaiDocumentedToolsPodSelector, "no longer documents"},
		{"default-policy-admits-the-tools-pod", withRunbook,
			mut(chartFile("values.yaml"), "    #    (the `run` label `kubectl run` gives the one-off tools pod), then remove it again.\n    allowedFrom: []", "    #    (the `run` label `kubectl run` gives the one-off tools pod), then remove it again.\n    allowedFrom: [{matchLabels: {run: dev-health-go-api-tools-oneoff}}]"),
			qaiDocumentedToolsPodSelector, "already admits the tools pod"},
		{"queue-set-label-regains-the-comma", chartOnly,
			mutAll(chartFile(workers), `{{ join "_" . | quote }}`, `{{ join "," . | quote }}`),
			labelsLegalValues, "illegal Kubernetes label value"},
		{"queue-set-label-drops-queues", chartOnly,
			mutAll(chartFile(workers), `{{ join "_" . | quote }}`, `{{ first . | quote }}`),
			labelsQueueSetEncodesEveryQueue, "dropped queue"},
		{"metrics-floor-lowered", chartOnly,
			mut(chartFile(workers), "{{- if lt (int $group.terminationGracePeriodSeconds) 7260 }}", "{{- if lt (int $group.terminationGracePeriodSeconds) 100 }}"),
			drainMetricsBelowFloorFails, "must not render"},
		{"metrics-floor-applies-to-every-group", chartOnly,
			mut(chartFile(workers), `{{- if has "metrics" ($group.queues | default (list)) }}`, `{{- if true }}`),
			drainNonMetricsNotConstrained, "legitimate"},
		{"pdb-for-a-single-replica", chartOnly,
			mut(chartFile(pdb), "{{- if ge $ceiling 2 }}", "{{- if ge $ceiling 1 }}"),
			drainSingleReplicaNoPDB, "blocks node drain"},
		{"pdb-uses-min-available", chartOnly,
			mut(chartFile(pdb), "  maxUnavailable: 1\n", "  minAvailable: 1\n"),
			drainMultiReplicaPDB, "maxUnavailable"},
		{"pdb-ignores-the-autoscaling-ceiling", chartOnly,
			mut(chartFile(pdb), "(ternary (int $autoscaling.maxReplicas) 0 $autoscaling.enabled)", "0"),
			drainAutoscaledZeroStaticReplicasHasPDB, "no budget"},
		{"pdb-selector-loses-its-group", chartOnly,
			mut(chartFile(pdb), "      dev-health.io/worker-group: {{ $group.name }}\n{{- end }}", "{{- end }}"),
			drainMultiReplicaPDB, "does not target its own group"},
		{"web-rollout-dips-below-capacity", chartOnly,
			mut(chartFile("templates/web-deployment.yaml"), "rollingUpdate: {maxUnavailable: 0, maxSurge: 1}", "rollingUpdate: {maxUnavailable: 1, maxSurge: 0}"),
			drainServingDeploymentsNeverDip, "would drop below serving capacity"},
		{"group-without-autoscaling-renders", chartOnly,
			all(mutAll(chartFile(workers), "$group.autoscaling", "($group.autoscaling | default dict)"),
				mutAll(chartFile(keda), "$group.autoscaling", "($group.autoscaling | default dict)"),
				mutAll(chartFile(pdb), "$group.autoscaling", "($group.autoscaling | default dict)"),
				mutAll(chartFile("templates/go-pgbouncer.yaml"), "$group.autoscaling.enabled", "($group.autoscaling | default dict).enabled"),
				mut(chartFile(pdb), "0 $autoscaling.enabled) }}", "0 ($autoscaling.enabled | default false)) }}")),
			drainGroupWithoutAutoscalingRejected, "must not render silently"},
		{"min-replicas-zero-accepted", chartOnly,
			all(mut(chartFile(keda), "{{- if lt (int $group.autoscaling.minReplicas) 1 }}", "{{- if false }}"),
				mut(chartFile("values.schema.json"), `"minReplicas": { "type": "integer", "minimum": 1 }`, `"minReplicas": { "type": "integer", "minimum": 0 }`)),
			drainMinReplicasZeroFails, "minReplicas: 0 must not render"},
	})
}
