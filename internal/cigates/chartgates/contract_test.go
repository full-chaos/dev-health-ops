package chartgates_test

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
)

// Ported from tests/workers/test_go_worker_cli_contract.py (CHAOS-4020; the Celery queue comparison test
// only guards Python code and is deleted) and tests/workers/test_go_worker_deployment_contract.py (every test
// except the one that read the last pyproject.toml and the Python Dockerfile, deleted with the Python image).

const (
	deploymentJSON = "deploy/go-workers/deployment.json"
	goDockerfile   = "docker/go-worker.Dockerfile"
	optionsGo      = "internal/platform/config/options.go"
	configGo       = "internal/platform/config/config.go"
	routesGo       = "internal/platform/config/routes.go"
)

var (
	credentials = []string{
		"POSTGRES_URI", "WORKER_DATABASE_URI", "COORDINATOR_DATABASE_URI", "API_DATABASE_URI", "CLICKHOUSE_URI",
		"DEV_HEALTH_CH_PASSWORD", "DEV_HEALTH_PG_DOMAIN_PASSWORD", "DEV_HEALTH_PG_QUEUE_PASSWORD", "DEV_HEALTH_PG_COORDINATOR_PASSWORD",
		"DEV_HEALTH_PG_API_PASSWORD", "API_CLICKHOUSE_URI", "DEV_HEALTH_CH_API_PASSWORD", "VALKEY_URI",
		"SETTINGS_ENCRYPTION_KEY", "SETTINGS_ENCRYPTION_SALT", "PAGER_DUTY_CLIENT_ID", "PAGER_DUTY_SECRET",
		"JWT_SECRET_KEY", "STRIPE_SECRET_KEY", "STRIPE_WEBHOOK_SECRET", "LICENSE_PRIVATE_KEY",
		"GO_API_EDGE_JWT_SECRET", "GO_API_REGISTRY_POSTGRES_URI", "LINK_TOKEN_SECRET", "LINK_TOKEN_SECRET_PREVIOUS",
	}
	cmdFlag      = regexp.MustCompile(`^--([a-z][a-z0-9-]*)=`)
	routeSwitch  = regexp.MustCompile(`WORKER_[A-Z0-9_]+_ENABLED`)
	routeField   = regexp.MustCompile(`\bWorker\w+Enabled\b`)
	bridgeNames  = []string{"WORKER_OPERATIONAL_BRIDGE_URL", "WORKER_OPERATIONAL_BRIDGE_ALLOW_INSECURE", "WORKER_OPERATIONAL_BRIDGE_TOKEN"}
	bridgeFlags  = []string{"--operational-bridge-url", "--operational-bridge-allow-insecure"}
	allNineNames = []string{"heavy", "ops", "reconciler", "scheduler", "stream-external", "stream-ingest", "stream-pagerduty", "sync", "sync-provider"}
)

func goFlagNames(g *guard) map[string]bool {
	out := registryFlagNames()
	g.expect(len(out) > 0, "no flags in the option registry")
	return out
}

func commandFlags(items any) map[string]bool {
	out := map[string]bool{}
	for _, it := range list(items) {
		if m := cmdFlag.FindStringSubmatch(str(it)); m != nil {
			out[m[1]] = true
		}
	}
	return out
}

func composeWorkerServices(g *guard) map[string]doc {
	out := map[string]doc{}
	for n, s := range mp(at(g.yamlFile("compose.yml"), "services")) {
		if labels := mp(mp(s)["labels"]); labels != nil {
			if _, ok := labels["dev-health.io/worker-group"]; ok {
				out[n] = mp(s)
			}
		}
	}
	return out
}

func cliRegistryDeclaresCredentials(g *guard) {
	got := registrySecretEnv()
	want := append([]string(nil), credentials...)
	sort.Strings(want)
	g.expect(equal(got, want), "the Secret-marked registry entries differ from the documented credential set: got %v, want %v (a credential that loses its marker gains a flag and lands in `ps` output)", got, want)
}

func cliComposeFlagsAccepted(g *guard) {
	passed := map[string]bool{}
	for _, s := range composeWorkerServices(g) {
		for f := range commandFlags(s["command"]) {
			passed[f] = true
		}
	}
	g.expect(len(passed) > 0, "compose.yml passes no flags at all")
	accepted := goFlagNames(g)
	for f := range passed {
		g.expect(accepted[f], "compose.yml passes the flag --%s which no Go worker binary declares: add it to internal/platform/config/options.go or fix the spelling", f)
	}
}

func cliHelmFlagsAccepted(g *guard) {
	passed := map[string]bool{}
	for _, d := range ofKind(g.docs("flags-contract"), "Deployment") {
		if label(d, "app.kubernetes.io/component") != "go-worker" {
			continue
		}
		for _, c := range containers(d) {
			for f := range commandFlags(c["args"]) {
				passed[f] = true
			}
		}
	}
	g.expect(len(passed) > 0, "the rendered go worker Deployments pass no flags at all")
	accepted := goFlagNames(g)
	for f := range passed {
		g.expect(accepted[f], "a rendered go worker Deployment passes the flag --%s which no Go worker binary declares: add it to internal/platform/config/options.go or fix the spelling", f)
	}
}

func cliEveryComposeWorkerStatesDrainBudget(g *guard) {
	svcs := composeWorkerServices(g)
	g.expect(len(svcs) > 0, "no compose worker service found")
	for n, s := range svcs {
		found := false
		for _, a := range list(s["command"]) {
			found = found || strings.HasPrefix(str(a), "--shutdown-timeout=")
		}
		g.expect(found, "compose.yml:%s does not state a drain budget", n)
	}
}

func goQueueNames(g *guard) map[string]bool {
	out := map[string]bool{}
	for _, p := range list(g.jsonFile(deploymentJSON)["processes"]) {
		for _, q := range list(mp(p)["queues"]) {
			out[str(q)] = true
		}
	}
	return out
}

func cliEveryComposeQueueIsServed(g *guard) {
	served := goQueueNames(g)
	g.expect(len(served) > 0, "deployment.json lists no queues")
	for n, s := range composeWorkerServices(g) {
		for _, a := range list(s["command"]) {
			if v, ok := strings.CutPrefix(str(a), "--queues="); ok {
				for _, q := range strings.Split(v, ",") {
					g.expect(served[q], "compose.yml:%s selects the queue %q the Go fleet does not serve", n, q)
				}
			}
		}
	}
}

func cliProviderRoutesHaveNoEnablementSurface(g *guard) {
	g.expect(!exists(g.path(routesGo)), "routes.go is back; the provider route switch registry was deleted with the enablement plane and must not return")
	for _, o := range config.Options() {
		g.expect(!routeSwitch.MatchString(o.Env), "the option registry carries the route enablement variable %q: capability is always on in the binary", o.Env)
		g.expect(o.Env != "GO_PROVIDER_ROUTES", "the option registry carries the all-routes preset GO_PROVIDER_ROUTES, deleted with the switches it defaulted on")
		g.expect(o.Flag != "routes", "the option registry offers a --routes flag; queue topology is the only serving lever")
	}
	for _, f := range configFieldNames() {
		g.expect(!routeField.MatchString(f), "config.Config reintroduced the route enablement field %s", f)
	}
}

// --- deployment contract ---

func anyStrs(s ...string) []any {
	out := make([]any, len(s))
	for i, x := range s {
		out[i] = x
	}
	return out
}

func processes(g *guard) []doc {
	var out []doc
	for _, p := range list(g.jsonFile(deploymentJSON)["processes"]) {
		out = append(out, mp(p))
	}
	return out
}

func riverProcesses(g *guard) map[string]doc {
	out := map[string]doc{}
	for _, p := range processes(g) {
		if str(p["runtime"]) == "river" && str(p["subcommand"]) == "worker" {
			out[str(p["name"])] = p
		}
	}
	return out
}

func valuesGoWorkers(g *guard) doc { return mp(g.yamlFile(chartFile("values.yaml"))["goWorkers"]) }

func valuesGroupsByName(g *guard) map[string]doc {
	out := map[string]doc{}
	for _, grp := range list(valuesGoWorkers(g)["groups"]) {
		out[str(at(grp, "name"))] = mp(grp)
	}
	return out
}

func depGoDefaultState(g *guard) {
	m := g.jsonFile(deploymentJSON)
	g.expect(str(m["deployment_state"]) == "go_default", "deployment_state is %q, want go_default", str(m["deployment_state"]))
	g.expect(equal(m["runtime_role_env"], any(anyStrs("RIVER_COORDINATOR_DATABASE_ROLE", "RIVER_DOMAIN_DATABASE_ROLE", "RIVER_QUEUE_DATABASE_ROLE"))), "runtime_role_env is %v", m["runtime_role_env"])
	ps := processes(g)
	g.expect(len(ps) == 9, "deployment.json lists %d processes, want 9", len(ps))
	maxReplicas := map[string]any{}
	for _, p := range ps {
		n := str(p["name"])
		floor := 0
		if n == "sync-provider" {
			floor = 1
		}
		g.expect(p["min_replicas"] == floor, "%s min_replicas is %v, want %d (sync-provider keeps a floor of one: a zero-replica window leaves the dimension tables unfolded)", n, p["min_replicas"], floor)
		g.expect(p["enabled_by_default"] == true, "%s is not enabled_by_default", n)
		g.expect(p["desired_replicas"] == 1, "%s desired_replicas is %v, want 1", n, p["desired_replicas"])
		var qs []any
		for _, w := range list(p["queue_workers"]) {
			qs = append(qs, at(w, "queue"))
			mw, _ := at(w, "max_workers").(int)
			g.expect(mw > 0, "%s queue %v has max_workers %v", n, at(w, "queue"), at(w, "max_workers"))
		}
		g.expect(equal(qs, p["queues"]) || (len(qs) == 0 && len(list(p["queues"])) == 0), "%s queue_workers queues %v differ from queues %v", n, qs, p["queues"])
		sec := strs(p["secret_env"])
		g.expect(sortedHas(sec, "POSTGRES_URI") && !sortedHas(sec, "MIGRATION_DATABASE_URI"), "%s secret_env %v must carry POSTGRES_URI and never MIGRATION_DATABASE_URI", n, sec)
		if n == "heavy" || n == "ops" {
			maxReplicas[n] = p["max_replicas"]
		}
	}
	g.expect(equal(maxReplicas, map[string]any{"heavy": 2, "ops": 2}), "heavy/ops max_replicas %v, want 2 each", maxReplicas)
	g.expect(equal(m["operator_cli"], any(doc{
		"name": "worker-operator", "binary": "dho", "max_concurrent_invocations": 1,
		"queue_control_max_connections": 2, "domain_max_connections": 2, "coordinator_max_connections": 2,
		"config_env": anyStrs("COORDINATOR_DATABASE_MODE", "PGBOUNCER_TRANSACTION_MODE", "RIVER_COORDINATOR_DATABASE_ROLE", "RIVER_DATABASE_SCHEMA", "RIVER_DOMAIN_DATABASE_ROLE", "RIVER_QUEUE_DATABASE_ROLE", "WORKER_DATABASE_MODE"),
		"secret_env": anyStrs("CLICKHOUSE_URI", "COORDINATOR_DATABASE_URI", "POSTGRES_URI", "WORKER_DATABASE_URI"),
	})), "operator_cli differs from the contract: %v", m["operator_cli"])
}

func depImagePackagesWorkItemConfig(g *guard) {
	ins := parseDockerfile(g.read(goDockerfile))
	const file = "status_mapping.yaml"
	src := "src/dev_health_ops/config/" + file
	g.expect(exists(g.path(src)), "%s does not exist", src)
	g.expect(hasInstr(ins, "COPY", src+" ./"+src), "the Go worker Dockerfile has no COPY instruction for %s", src)
	for _, image := range []string{"dho", "operator"} {
		g.expect(hasRunCommand(ins, "cp /src/"+src+" /runtime/"+image+"/app/config/"+file), "the %s image does not stage /app/config/%s (no such cp command in a RUN instruction)", image, file)
	}
}

func depImagePackagesLifecycleOperator(g *guard) {
	g.expect(hasRunCommand(parseDockerfile(g.read(goDockerfile)), "cp /out/dho /runtime/dho/usr/local/bin/dho"), "the dho image no longer carries the dho binary (no such cp command in a RUN instruction)")
}

func depSurfacesAdditiveAndGroupComplete(g *guard) {
	var names []string
	for _, p := range processes(g) {
		names = append(names, str(p["name"]))
	}
	sort.Strings(names)
	g.expect(equal(names, allNineNames), "deployment.json process names %v, want %v", names, allNineNames)
	gw := valuesGoWorkers(g)
	g.expect(gw["enabled"] == true, "goWorkers.enabled must default to true")
	_, hasProfiles := gw["profiles"]
	g.expect(!hasProfiles, "goWorkers.profiles is back")
	groups := valuesGroupsByName(g)
	var gn []string
	for n, grp := range groups {
		gn = append(gn, n)
		g.expect(grp["replicas"] == 1, "group %s replicas is %v, want 1", n, grp["replicas"])
	}
	sort.Strings(gn)
	g.expect(equal(gn, allNineNames), "chart groups %v, want %v", gn, allNineNames)
	g.expect(equal(groups["sync"]["queues"], any(anyStrs("sync"))), "the sync group queues are %v", groups["sync"]["queues"])
	tpl := tplCode(g.read(chartFile("templates/go-workers.yaml")))
	g.expect(!strings.Contains(tpl, ".Values.goWorkers.profiles"), "go-workers.yaml reads .Values.goWorkers.profiles")
	g.expect(!strings.Contains(tpl, "$profile"), "go-workers.yaml still uses $profile")
	docs := g.docs("surface-contract")
	scaled := scaledTargets(docs)
	for n, grp := range groups {
		if at(grp, "autoscaling", "enabled") == true {
			g.expect(scaled["surface-contract-dev-health-go-"+n], "group %s autoscales in values but renders no ScaledObject", n)
		}
	}
}

func depRiverRenderersSelectManifestQueues(g *guard) {
	river := riverProcesses(g)
	var rn []string
	for n := range river {
		rn = append(rn, n)
	}
	sort.Strings(rn)
	g.expect(equal(rn, []string{"heavy", "ops", "sync", "sync-provider"}), "river worker processes %v", rn)
	groups := valuesGroupsByName(g)
	for n, p := range river {
		grp := groups[n]
		g.expect(grp != nil && str(grp["subcommand"]) == "worker", "group %s subcommand is %v, want worker", n, at(grp, "subcommand"))
		g.expect(grp != nil && equal(grp["queues"], p["queues"]), "group %s queues %v differ from the manifest %v", n, at(grp, "queues"), p["queues"])
		_, hasProfile := grp["profile"]
		g.expect(!hasProfile, "group %s carries a profile key", n)
	}
	image := func(grp doc) string {
		if i := str(grp["image"]); i != "" {
			return i
		}
		return str(valuesGoWorkers(g)["image"])
	}
	for grpName, rp := range map[string]string{"stream-external": "external", "stream-ingest": "ingest", "stream-pagerduty": "pagerduty"} {
		grp := groups[grpName]
		g.expect(str(grp["runtimeProfile"]) == rp, "group %s runtimeProfile is %q, want %q", grpName, str(grp["runtimeProfile"]), rp)
		g.expect(str(grp["subcommand"]) == "stream-runner", "group %s subcommand is %q, want stream-runner", grpName, str(grp["subcommand"]))
		g.expect(strings.Contains(image(grp), "/dev-health-go-dho:"), "group %s image %q is not the dho image", grpName, image(grp))
	}
	for _, verb := range []string{"reconciler", "scheduler"} {
		grp := groups[verb]
		g.expect(str(grp["subcommand"]) == verb, "group %s subcommand is %q, want %q", verb, str(grp["subcommand"]), verb)
		g.expect(strings.Contains(image(grp), "/dev-health-go-dho:"), "group %s image %q is not the dho image", verb, image(grp))
	}
}

func depReplicaAndDrainContract(g *guard) {
	groups := valuesGroupsByName(g)
	ps := processes(g)
	g.expect(len(groups) == len(ps), "chart has %d groups, manifest %d processes", len(groups), len(ps))
	rendered := workersByGroup(g.docs("drain-budget"))
	for _, p := range ps {
		n := str(p["name"])
		grp := groups[n]
		if grp == nil {
			g.failf("group %s is missing from the chart", n)
			continue
		}
		g.expect(grp["replicas"] == p["desired_replicas"], "group %s replicas %v != desired_replicas %v", n, grp["replicas"], p["desired_replicas"])
		g.expect(grp["terminationGracePeriodSeconds"] == p["shutdown_grace_seconds"], "group %s terminationGracePeriodSeconds %v != shutdown_grace_seconds %v", n, grp["terminationGracePeriodSeconds"], p["shutdown_grace_seconds"])
		if str(p["runtime"]) == "river" {
			g.expect(at(grp, "autoscaling", "maxReplicas") == p["max_replicas"], "group %s autoscaling.maxReplicas %v != max_replicas %v", n, at(grp, "autoscaling", "maxReplicas"), p["max_replicas"])
		}
		d := rendered[n]
		if d == nil {
			g.failf("group %s did not render a Deployment", n)
			continue
		}
		want := fmt.Sprintf("--shutdown-timeout=%vs", p["shutdown_grace_seconds"])
		g.expect(sortedHas(strs(containers(d)[0]["args"]), want), "group %s renders no %s argument (the drain budget must be the manifest's shutdown_grace_seconds)", n, want)
	}
}

func depImagesPackageContracts(g *guard) {
	ins := parseDockerfile(g.read(goDockerfile))
	for _, c := range []struct{ image, cmd string }{
		{"dho", "cp -R /src/contracts/jobs/v1 /runtime/dho/app/contracts/jobs/v1"},
		{"dho", "cp -R /src/contracts/sync-dispatch/v1 /runtime/dho/app/contracts/sync-dispatch/v1"},
		{"operator", "cp -R /src/contracts/jobs/v1 /runtime/operator/app/contracts/jobs/v1"},
		{"operator", "cp -R /src/contracts/sync-dispatch/v1 /runtime/operator/app/contracts/sync-dispatch/v1"},
		{"operator", "cp /src/deploy/go-workers/deployment.json /runtime/operator/app/deploy/go-workers/deployment.json"},
	} {
		g.expect(hasRunCommand(ins, c.cmd), "the %s image does not package %q (no such command in a RUN instruction)", c.image, c.cmd)
	}
	g.expect(hasStageInstr(ins, "dho", "WORKDIR", "/app"), "the dho stage must declare WORKDIR /app as an instruction: the scheduler loads contracts/jobs/v1 relative to it")
}

func depPgbouncerBudgetMatchesHelm(g *guard) {
	budget := mp(g.jsonFile(deploymentJSON)["postgres_budget"])
	pools := mp(at(g.yamlFile(chartFile("values.yaml")), "goWorkers", "pgbouncer"))
	for _, c := range []struct{ key, pool string }{
		{"pgbouncer_transaction_pool_size", "transaction"},
		{"pgbouncer_queue_session_pool_size", "queueSession"},
		{"pgbouncer_coordinator_session_pool_size", "coordinatorSession"},
	} {
		g.expect(budget[c.key] != nil && budget[c.key] == at(pools, c.pool, "poolSize"), "%s %v != helm goWorkers.pgbouncer.%s.poolSize %v", c.key, budget[c.key], c.pool, at(pools, c.pool, "poolSize"))
	}
}

var forbiddenSharedMigrationSecrets = []string{"MIGRATION_DATABASE_URI", "MIGRATION_DATABASE_URI_FILE"}

func depMigrationWiring(g *guard) {
	manifest := g.jsonFile(deploymentJSON)
	values := g.yamlFile(chartFile("values.yaml"))
	for k, want := range map[string]string{"RIVER_DATABASE_SCHEMA": "river", "RIVER_DOMAIN_DATABASE_ROLE": "devhealth_domain", "RIVER_QUEUE_DATABASE_ROLE": "devhealth_queue"} {
		g.expect(str(at(values, "config", k)) == want, "config.%s is %q, want %q", k, str(at(values, "config", k)), want)
	}
	secretData := mp(at(values, "migrations", "hook", "secretData"))
	for _, k := range strs(at(manifest, "migration_job", "secret_env")) {
		_, ok := secretData[k]
		g.expect(ok, "migrations.hook.secretData lacks %s, which the manifest's migration_job needs", k)
	}
	_, hasPG := secretData["POSTGRES_URI"]
	g.expect(hasPG, "migrations.hook.secretData lacks the POSTGRES_URI compatibility path")
	shared := mp(at(values, "secrets", "data"))
	for _, k := range forbiddenSharedMigrationSecrets {
		_, in := shared[k]
		g.expect(!in, "secrets.data carries %s: the elevated DSN must not live in the shared application Secret", k)
	}
	helpers := tplCode(g.read(chartFile("templates/_helpers.tpl")))
	g.expect(regexp.MustCompile(`\{\{-?\s*define\s+"dev-health\.migrationSecretData"`).MatchString(helpers), "_helpers.tpl lost the migrationSecretData helper")
	g.expect(strings.Contains(helpers, ".Values.migrations.hook.secretData"), "_helpers.tpl code no longer reads migrations.hook.secretData")
	g.expect(strings.Contains(tplCode(g.read(chartFile("templates/migrate-job.yaml"))), ".Values.migrations.hook.externalSecretName"), "migrate-job.yaml code no longer reads migrations.hook.externalSecretName")
	var clauses []string
	for _, c := range list(at(g.jsonFile(chartFile("values.schema.json")), "properties", "secrets", "properties", "data", "not", "anyOf")) {
		clauses = append(clauses, str(at(c, "required", 0)))
	}
	sort.Strings(clauses)
	g.expect(equal(clauses, forbiddenSharedMigrationSecrets), "the schema forbids %v in secrets.data, want %v", clauses, forbiddenSharedMigrationSecrets)
}

func depHelmRejectsMigrationDSNInSharedSecret(g *guard) {
	g.mustRender("the baseline render", "phase1")
	for _, k := range forbiddenSharedMigrationSecrets {
		stderr := g.refusal("secrets.data."+k, "phase1", setString("secrets.data."+k+"=postgresql://migration@direct/app")...)
		g.expect(strings.Contains(stderr, "values don't meet the specifications of the schema"), "the refusal of secrets.data.%s is not a schema refusal: %s", k, firstLines(stderr, 2))
	}
}

func depHelmAcceptsDedicatedMigrationDSN(g *guard) {
	docs := g.docs("phase1", setString("migrations.hook.secretData.MIGRATION_DATABASE_URI=postgresql://migration@direct/app")...)
	var holders []doc
	for _, s := range ofKind(docs, "Secret") {
		if _, ok := mp(s["stringData"])["MIGRATION_DATABASE_URI"]; ok {
			holders = append(holders, s)
		}
	}
	if len(holders) != 1 {
		g.failf("exactly one Secret must hold MIGRATION_DATABASE_URI, got %d", len(holders))
		return
	}
	g.expect(strings.HasSuffix(name(holders[0]), "-migrate-secrets"), "the holder %s does not end in -migrate-secrets", name(holders[0]))
	g.expect(strings.Contains(str(at(holders[0], "metadata", "annotations", "helm.sh/hook")), "pre-install,pre-upgrade"), "the holder is not a pre-install,pre-upgrade hook Secret")
}

func depMigrationJobUsesDedicatedExternalSecret(g *guard) {
	docs := g.docs("phase1", join(set("secrets.create=false"), setString("secrets.externalSecretName=shared-app-secrets", "migrations.hook.externalSecretName=elevated-migration-secrets"))...)
	jobs := ofKind(docs, "Job")
	if len(jobs) == 0 {
		g.failf("no Job rendered")
		return
	}
	var refs []string
	for _, s := range list(containers(jobs[0])[0]["envFrom"]) {
		if n := str(at(s, "secretRef", "name")); n != "" {
			refs = append(refs, n)
		}
	}
	g.expect(equal(refs, []string{"elevated-migration-secrets"}), "the migrate Job reads secrets %v, want only elevated-migration-secrets", refs)
}

func hasBridgeFlag(args []string) bool {
	for _, a := range args {
		for _, f := range bridgeFlags {
			if strings.HasPrefix(a, f+"=") || a == f {
				return true
			}
		}
	}
	return false
}

func composeEnvNames(env any) map[string]bool {
	out := map[string]bool{}
	switch e := env.(type) {
	case map[string]any:
		for k := range e {
			out[k] = true
		}
	case []any:
		for _, x := range e {
			k, _, _ := strings.Cut(str(x), "=")
			out[k] = true
		}
	}
	return out
}

func dependsOn(svc doc, dep string) bool {
	switch d := svc["depends_on"].(type) {
	case map[string]any:
		_, ok := d[dep]
		return ok
	case []any:
		for _, x := range d {
			if str(x) == dep {
				return true
			}
		}
	}
	return false
}

func depNoRendererEmitsOperationalBridge(g *guard) {
	services := mp(at(g.yamlFile("compose.yml"), "services"))
	g.expect(len(services) > 0, "compose.yml has no services")
	for n, s := range services {
		svc := mp(s)
		g.expect(!hasBridgeFlag(strs(svc["command"])), "compose.yml:%s still renders an operational-bridge flag", n)
		env := composeEnvNames(svc["environment"])
		for _, b := range bridgeNames {
			g.expect(!env[b], "compose.yml:%s still carries %s", n, b)
		}
		g.expect(!dependsOn(svc, "metrics-api"), "compose.yml:%s still depends on metrics-api", n)
	}
	values := g.yamlFile(chartFile("values.yaml"))
	resolved := map[string]bool{}
	for k := range mp(values["config"]) {
		resolved[k] = true
	}
	for k := range mp(at(values, "secrets", "data")) {
		resolved[k] = true
	}
	for _, b := range bridgeNames {
		g.expect(!resolved[b], "helm values.yaml still declares %s", b)
	}
	g.expect(!strings.Contains(tplCode(g.read(chartFile("templates/_helpers.tpl"))), "operationalBridgeURL"), "_helpers.tpl still defines the deleted operationalBridgeURL helper")
	n := 0
	for _, d := range ofKind(g.docs("phase1"), "Deployment") {
		for _, c := range containers(d) {
			n++
			g.expect(!hasBridgeFlag(strs(c["args"])), "%s still renders an operational-bridge flag", name(d))
		}
	}
	g.expect(n > 0, "no Deployment container rendered; the guard would pass vacuously")
}

func depPagerDutyProfileBinding(g *guard) {
	var pd []doc
	for _, grp := range valuesGroupsByName(g) {
		if str(grp["runtimeProfile"]) == "pagerduty" {
			pd = append(pd, grp)
		}
	}
	g.expect(len(pd) == 1, "exactly one goWorkers group must run the pagerduty profile, got %d", len(pd))
	rendered := workersByGroup(g.docs("profile-contract"))
	for n, grp := range valuesGroupsByName(g) {
		d := rendered[n]
		if d == nil {
			g.failf("group %s did not render a Deployment", n)
			continue
		}
		var profiles []string
		for _, a := range strs(containers(d)[0]["args"]) {
			if v, ok := strings.CutPrefix(a, "--profile="); ok {
				profiles = append(profiles, v)
			}
		}
		if want := str(grp["runtimeProfile"]); want != "" {
			g.expect(equal(profiles, []string{want}), "group %s renders --profile %v, want [%s]: the chart must bind --profile to $group.runtimeProfile", n, profiles, want)
		} else {
			g.expect(len(profiles) == 0, "group %s has no runtimeProfile but renders --profile %v", n, profiles)
		}
	}
	for _, d := range rendered {
		refs := envFromRefs(containers(d)[0])
		var cm, sec bool
		for _, r := range refs {
			cm = cm || r[0] == "configMapRef"
			sec = sec || r[0] == "secretRef"
		}
		g.expect(cm && sec, "%s lost the shared ConfigMap or Secret envFrom source: %v", name(d), refs)
	}
}

var helmGoWorkerSets = join(
	set("goWorkers.enabled=true", "goWorkers.pgbouncer.enabled=true"),
	setString("goWorkers.pgbouncer.postgres.host=pg", "goWorkers.pgbouncer.postgres.database=db",
		"goWorkers.pgbouncer.secret.data.RIVER_DOMAIN_DATABASE_PASSWORD=x", "goWorkers.pgbouncer.secret.data.RIVER_QUEUE_DATABASE_PASSWORD=x", "goWorkers.pgbouncer.secret.data.RIVER_COORDINATOR_DATABASE_PASSWORD=x"))

func workersByGroup(docs []doc) map[string]doc {
	out := map[string]doc{}
	for _, d := range ofKind(docs, "Deployment") {
		if grp := label(d, "dev-health.io/worker-group"); grp != "" {
			out[grp] = d
		}
	}
	return out
}

func depEveryGroupNamesItselfForOtel(g *guard) {
	by := workersByGroup(g.docs("phase1", join(helmGoWorkerSets, []string{"--show-only", "templates/go-workers.yaml"})...))
	for _, n := range allNineNames {
		g.expect(by[n] != nil, "the walk did not reach group %s", n)
	}
	seen := map[string]string{}
	for grp, d := range by {
		env := envMap(containers(d)[0])
		v := str(env["OTEL_SERVICE_NAME"]["value"])
		g.expect(v == "dev-health-go-"+grp, "group %s OTEL_SERVICE_NAME is %q, want dev-health-go-%s", grp, v, grp)
		if other, dup := seen[v]; dup {
			g.failf("two groups share the OTEL service name %q: %s and %s", v, other, grp)
		}
		seen[v] = grp
		g.expect(v != "dev-health-ops", "group %s reports as the python api's service name", grp)
	}
}

func depComposeHeavyAloneOwnsMetricsQueue(g *guard) {
	var owners []string
	count := 0
	for n, s := range mp(at(g.yamlFile("compose.yml"), "services")) {
		if !(strings.HasPrefix(n, "go-worker") || strings.HasPrefix(n, "go-reconciler") || strings.HasPrefix(n, "go-scheduler") || strings.HasPrefix(n, "go-stream")) {
			continue
		}
		count++
		svc := mp(s)
		for _, a := range list(svc["command"]) {
			if v, ok := strings.CutPrefix(str(a), "--queues="); ok {
				for _, q := range strings.Split(v, ",") {
					if q == "metrics" {
						owners = append(owners, n)
					}
				}
			}
		}
		g.expect(!hasBridgeFlag(strs(svc["command"])), "%s: an operational-bridge flag is deleted (CHAOS-6279)", n)
		g.expect(!dependsOn(svc, "metrics-api"), "%s: the metrics-api depends_on edge is deleted (CHAOS-6279)", n)
	}
	g.expect(count >= 7, "only %d go worker services in compose.yml, want at least 7", count)
	g.expect(equal(owners, []string{"go-worker-heavy"}), "expected only go-worker-heavy to own the metrics queue, found %v", owners)
}

func depGroupsRollOnSharedConfigOrSecretChange(g *guard) {
	render := func(extra ...string) map[string]doc {
		by := workersByGroup(g.docs("phase1", join(extra, []string{"--show-only", "templates/go-workers.yaml"})...))
		var names []string
		for n := range by {
			names = append(names, n)
		}
		sort.Strings(names)
		g.expect(equal(names, allNineNames), "rendered groups %v, want %v", names, allNineNames)
		return by
	}
	finger := func(d doc) []any {
		c := containers(d)[0]
		return []any{c["image"], c["livenessProbe"], c["readinessProbe"]}
	}
	annotations := func(d doc) doc { return mp(at(d, "spec", "template", "metadata", "annotations")) }
	base := render()
	cfg := render(setString("config.LOG_LEVEL=DEBUG")...)
	sec := render(setString("secrets.data.DATABASE_URI=postgresql://rotated")...)
	for _, n := range allNineNames {
		b, c, s := annotations(base[n]), annotations(cfg[n]), annotations(sec[n])
		_, hc := b["checksum/config"]
		_, hs := b["checksum/secret"]
		g.expect(hc && hs, "%s lacks the checksum/config and checksum/secret pod annotations", n)
		g.expect(str(c["checksum/config"]) != str(b["checksum/config"]), "%s: a ConfigMap-only change must roll this group", n)
		g.expect(str(c["checksum/secret"]) == str(b["checksum/secret"]), "%s: a ConfigMap-only change must not touch the secret checksum", n)
		g.expect(equal(finger(cfg[n]), finger(base[n])), "%s: a config value change must not touch image or probes", n)
		g.expect(str(s["checksum/secret"]) != str(b["checksum/secret"]), "%s: a Secret-only change must roll this group", n)
		g.expect(str(s["checksum/config"]) == str(b["checksum/config"]), "%s: a Secret-only change must not touch the config checksum", n)
		g.expect(equal(finger(sec[n]), finger(base[n])), "%s: a secret value change must not touch image or probes", n)
	}
}

func depNativeClickhouseURI(g *guard) {
	docs := g.docs("phase1")
	var uri string
	for _, s := range ofKind(docs, "Secret") {
		if v := str(at(s, "stringData", "CLICKHOUSE_NATIVE_URI")); v != "" {
			uri = v
		}
	}
	g.expect(strings.Contains(uri, ":9000/"), "the rendered CLICKHOUSE_NATIVE_URI %q is not the native port", uri)
	g.expect(!strings.Contains(uri, ":8123"), "the rendered CLICKHOUSE_NATIVE_URI %q carries the HTTP port", uri)
	by := workersByGroup(docs)
	g.expect(len(by) > 0, "no go worker group rendered")
	for grp, d := range by {
		ref := at(envMap(containers(d)[0])["CLICKHOUSE_URI"], "valueFrom", "secretKeyRef", "key")
		g.expect(str(ref) == "CLICKHOUSE_NATIVE_URI", "group %s reads CLICKHOUSE_URI from %v, want the native key CLICKHOUSE_NATIVE_URI", grp, ref)
	}
	_, native, ok := strings.Cut(tplCode(g.read(chartFile("templates/_helpers.tpl"))), `define "dev-health.clickhouseNativeURI"`)
	g.expect(ok, "_helpers.tpl lost the clickhouseNativeURI helper")
	body, _, _ := strings.Cut(native, "{{- end }}")
	g.expect(strings.Contains(body, ":9000/"), "the native-protocol helper code does not use port 9000")
}

func TestContractGuardsRealTree(t *testing.T) {
	runReal(t,
		check{"cli-registry-declares-credentials", cliRegistryDeclaresCredentials},
		check{"cli-compose-flags-accepted", cliComposeFlagsAccepted},
		check{"cli-helm-flags-accepted", cliHelmFlagsAccepted},
		check{"cli-every-compose-worker-states-drain-budget", cliEveryComposeWorkerStatesDrainBudget},
		check{"cli-every-compose-queue-is-served", cliEveryComposeQueueIsServed},
		check{"cli-provider-routes-have-no-enablement-surface", cliProviderRoutesHaveNoEnablementSurface},
		check{"dep-go-default-state", depGoDefaultState},
		check{"dep-image-packages-work-item-config", depImagePackagesWorkItemConfig},
		check{"dep-image-packages-lifecycle-operator", depImagePackagesLifecycleOperator},
		check{"dep-surfaces-additive-and-group-complete", depSurfacesAdditiveAndGroupComplete},
		check{"dep-river-renderers-select-manifest-queues", depRiverRenderersSelectManifestQueues},
		check{"dep-replica-and-drain-contract", depReplicaAndDrainContract},
		check{"dep-images-package-contracts", depImagesPackageContracts},
		check{"dep-pgbouncer-budget-matches-helm", depPgbouncerBudgetMatchesHelm},
		check{"dep-migration-wiring", depMigrationWiring},
		check{"dep-helm-rejects-migration-dsn-in-shared-secret", depHelmRejectsMigrationDSNInSharedSecret},
		check{"dep-helm-accepts-dedicated-migration-dsn", depHelmAcceptsDedicatedMigrationDSN},
		check{"dep-migration-job-uses-dedicated-external-secret", depMigrationJobUsesDedicatedExternalSecret},
		check{"dep-no-renderer-emits-operational-bridge", depNoRendererEmitsOperationalBridge},
		check{"dep-pagerduty-profile-binding", depPagerDutyProfileBinding},
		check{"dep-every-group-names-itself-for-otel", depEveryGroupNamesItselfForOtel},
		check{"dep-compose-heavy-alone-owns-metrics-queue", depComposeHeavyAloneOwnsMetricsQueue},
		check{"dep-groups-roll-on-shared-config-or-secret-change", depGroupsRollOnSharedConfigOrSecretChange},
		check{"dep-native-clickhouse-uri", depNativeClickhouseURI},
	)
}

func TestContractGuardsPlantedDefects(t *testing.T) {
	const workers = "templates/go-workers.yaml"
	const helpers = "templates/_helpers.tpl"
	const mjob = "templates/migrate-job.yaml"
	const schema = "values.schema.json"
	cfgSrc := []string{optionsGo, configGo}
	compose := []string{"compose.yml"}
	manifest := []string{deploymentJSON}
	docker := []string{goDockerfile, "src/dev_health_ops/config/status_mapping.yaml", deploymentJSON}
	chartAndManifest := append([]string{deploymentJSON}, chartOnly...)
	chartAndCompose := append([]string{"compose.yml"}, chartOnly...)
	chartAndOptions := append([]string{optionsGo, "compose.yml", deploymentJSON}, chartOnly...)
	runPlanted(t, []planted{
		{"compose-misspells-a-flag", append([]string{optionsGo}, compose...), mut("compose.yml", "--unreclaimable-sweep=${", "--unreclaimable-swep=${"), cliComposeFlagsAccepted, "which no Go worker binary declares"},
		{"helm-misspells-a-flag", append([]string{optionsGo}, chartOnly...), mut(chartFile(workers), `"--http-addr=:8080"`, `"--http-adr=:8080"`), cliHelmFlagsAccepted, "which no Go worker binary declares"},
		{"compose-group-loses-its-drain-budget", compose, mut("compose.yml", "      - reconciler\n      - --shutdown-timeout=60s\n", "      - reconciler\n"), cliEveryComposeWorkerStatesDrainBudget, "does not state a drain budget"},
		{"compose-selects-an-unserved-queue", append([]string{deploymentJSON}, compose...), mut("compose.yml", "- --queues=sync_provider", "- --queues=sync_ghost"), cliEveryComposeQueueIsServed, "does not serve"},
		{"route-registry-returns", cfgSrc, func(t *testing.T, root string) { writeFile(t, root, routesGo, "package config\n") }, cliProviderRoutesHaveNoEnablementSurface, "routes.go is back"},
		{"dho-binary-copy-only-in-a-dockerfile-comment", docker,
			mut(goDockerfile, "cp /out/dho /runtime/dho/usr/local/bin/dho; \\", "echo \"cp /out/dho /runtime/dho/usr/local/bin/dho\"; \\\n    # cp /out/dho /runtime/dho/usr/local/bin/dho; \\"),
			depImagePackagesLifecycleOperator, "no longer carries"},
		{"workdir-only-in-a-dockerfile-comment", docker, mut(goDockerfile, "/runtime/dho/ /\nWORKDIR /app\n", "/runtime/dho/ /\n# WORKDIR /app\n"), depImagesPackageContracts, "as an instruction"},
		{"migration-secret-name-mentioned-only-in-a-template-comment", chartAndManifest,
			all(mutAll(chartFile(mjob), ".Values.migrations.hook.externalSecretName", ".Values.migrations.hook.extSecretName"), appendText(chartFile(mjob), "\n{{- /* .Values.migrations.hook.externalSecretName */ -}}\n")),
			depMigrationWiring, "externalSecretName"},
		{"drain-flag-renders-another-value", chartAndManifest, mut(chartFile(workers), `printf "--shutdown-timeout=%ds" (int $group.terminationGracePeriodSeconds)`, `printf "--shutdown-timeout=%ds" (int 30)`), depReplicaAndDrainContract, "drain budget"},
		{"deployment-state-moves", manifest, mut(deploymentJSON, `"deployment_state": "go_default"`, `"deployment_state": "coexistence_disabled"`), depGoDefaultState, "deployment_state"},
		{"sync-provider-loses-its-replica-floor", manifest, mut(deploymentJSON, `"min_replicas": 1,
      "desired_replicas": 1,
      "max_replicas": 2,
      "shutdown_grace_seconds": 960`, `"min_replicas": 0,
      "desired_replicas": 1,
      "max_replicas": 2,
      "shutdown_grace_seconds": 960`), depGoDefaultState, "sync-provider"},
		{"image-stops-staging-the-work-item-config", docker, mut(goDockerfile, "/runtime/dho/app/config/status_mapping.yaml", "/runtime/dho/app/cfg/status_mapping.yaml"), depImagePackagesWorkItemConfig, "does not stage"},
		{"image-stops-copying-the-work-item-config", docker, mut(goDockerfile, "COPY src/dev_health_ops/config/status_mapping.yaml ./src/dev_health_ops/config/status_mapping.yaml", "COPY src/dev_health_ops/config/other.yaml ./src/dev_health_ops/config/other.yaml"), depImagePackagesWorkItemConfig, "no COPY instruction"},
		{"dho-image-loses-the-binary", docker, mut(goDockerfile, "cp /out/dho /runtime/dho/usr/local/bin/dho;", "cp /out/dho /runtime/dho/usr/local/bin/dho2;"), depImagePackagesLifecycleOperator, "no longer carries"},
		{"group-replicas-drift", chartAndManifest, mut(chartFile("values.yaml"), "    - name: sync\n      subcommand: worker\n      queues: [sync]\n      queueConcurrency: {sync: 4}\n      replicas: 1", "    - name: sync\n      subcommand: worker\n      queues: [sync]\n      queueConcurrency: {sync: 4}\n      replicas: 2"), depSurfacesAdditiveAndGroupComplete, "replicas"},
		{"profiles-block-returns", chartOnly, mut(chartFile(workers), "{{- if .Values.goWorkers.enabled }}", "{{- $unused := .Values.goWorkers.profiles }}\n{{- if .Values.goWorkers.enabled }}"), depSurfacesAdditiveAndGroupComplete, "goWorkers.profiles"},
		{"group-queues-drift-from-the-manifest", chartAndManifest, mut(chartFile("values.yaml"), "    - name: sync\n      subcommand: worker\n      queues: [sync]", "    - name: sync\n      subcommand: worker\n      queues: [sync, extra]"), depRiverRenderersSelectManifestQueues, "queues"},
		{"stream-group-loses-its-runtime-profile", chartAndManifest, mut(chartFile("values.yaml"), "runtimeProfile: ingest", "runtimeProfile: ingest2"), depRiverRenderersSelectManifestQueues, "runtimeProfile"},
		{"group-drain-budget-drifts", chartAndManifest, mut(chartFile("values.yaml"), "terminationGracePeriodSeconds: 7260", "terminationGracePeriodSeconds: 7261"), depReplicaAndDrainContract, "terminationGracePeriodSeconds"},
		{"template-stops-rendering-the-drain-flag", chartAndManifest, mut(chartFile(workers), `printf "--shutdown-timeout=%ds"`, `printf "--drain=%ds"`), depReplicaAndDrainContract, "--shutdown-timeout="},
		{"operator-image-stops-packaging-the-manifest", docker, mut(goDockerfile, "cp /src/deploy/go-workers/deployment.json /runtime/operator/app/deploy/go-workers/deployment.json;", "cp /src/deploy/go-workers/deployment.json /runtime/operator/app/deployment.json;"), depImagesPackageContracts, "operator image does not package"},
		{"dho-stage-loses-its-workdir", docker, mut(goDockerfile, "/runtime/dho/ /\nWORKDIR /app\n", "/runtime/dho/ /\n"), depImagesPackageContracts, "must declare WORKDIR /app"},
		{"pgbouncer-pool-drifts-from-the-budget", chartAndManifest, mut(chartFile("values.yaml"), "poolSize: 20", "poolSize: 21"), depPgbouncerBudgetMatchesHelm, "poolSize"},
		{"migration-hook-stops-reading-the-external-secret", chartAndManifest, mutAll(chartFile(mjob), ".Values.migrations.hook.externalSecretName", ".Values.migrations.hook.extSecretName"), depMigrationWiring, "externalSecretName"},
		{"shared-secret-schema-drops-the-forbidden-key", chartAndManifest, mut(chartFile(schema), `"required": ["MIGRATION_DATABASE_URI"]
              },`, `"required": ["MIGRATION_DATABASE_URI_NEVER"]
              },`), depMigrationWiring, "the schema forbids"},
		{"shared-secret-accepts-the-migration-dsn", chartOnly, mut(chartFile(schema), `"required": ["MIGRATION_DATABASE_URI"]
              },`, `"required": ["MIGRATION_DATABASE_URI_NEVER"]
              },`), depHelmRejectsMigrationDSNInSharedSecret, "must be refused"},
		{"dedicated-dsn-secret-renders-empty", chartOnly, mut(chartFile(mjob), `  {{- include "dev-health.migrationSecretData" . | trim | nindent 2 }}`, `  {}`), depHelmAcceptsDedicatedMigrationDSN, "exactly one Secret"},
		{"migrate-job-reads-the-shared-secret", chartOnly, mut(chartFile(mjob), `name: {{ default (include "dev-health.secretName" .) .Values.migrations.hook.externalSecretName }}`, `name: {{ include "dev-health.secretName" . }}`), depMigrationJobUsesDedicatedExternalSecret, "reads secrets"},
		{"compose-regains-a-bridge-flag", compose, mut("compose.yml", "      - reconciler\n      - --shutdown-timeout=60s\n", "      - reconciler\n      - --operational-bridge-url=http://x\n      - --shutdown-timeout=60s\n"), depNoRendererEmitsOperationalBridge, "operational-bridge flag"},
		{"helm-values-regain-a-bridge-variable", chartAndCompose, mut(chartFile("values.yaml"), `  OTEL_ENABLED: "true"`, `  WORKER_OPERATIONAL_BRIDGE_URL: "http://x"
  OTEL_ENABLED: "true"`), depNoRendererEmitsOperationalBridge, "WORKER_OPERATIONAL_BRIDGE_URL"},
		{"helm-helper-regains-the-bridge-url", chartAndCompose, appendText(chartFile(helpers), `{{- define "dev-health.operationalBridgeURL" -}}x{{- end }}`), depNoRendererEmitsOperationalBridge, "operationalBridgeURL"},
		{"pagerduty-profile-unbound", chartOnly, mut(chartFile(workers), "{{- with $group.runtimeProfile }}", "{{- with $group.runtimeProfileX }}"), depPagerDutyProfileBinding, "must bind --profile"},
		{"two-groups-run-the-pagerduty-profile", chartOnly, mut(chartFile("values.yaml"), "runtimeProfile: ingest", "runtimeProfile: pagerduty"), depPagerDutyProfileBinding, "exactly one"},
		{"group-reports-as-the-python-api", chartOnly, mut(chartFile(workers), `value: {{ printf "dev-health-go-%s" $group.name | quote }}`, `value: "dev-health-ops"`), depEveryGroupNamesItselfForOtel, "OTEL_SERVICE_NAME"},
		{"another-group-gains-the-metrics-queue", chartAndCompose, mut("compose.yml", "- --queues=sync_provider", "- --queues=sync_provider,metrics"), depComposeHeavyAloneOwnsMetricsQueue, "expected only go-worker-heavy"},
		{"config-change-stops-rolling-the-groups", chartOnly, mut(chartFile(workers), `checksum/config: {{ include (print $.Template.BasePath "/configmap.yaml") $ | sha256sum }}`, `checksum/config: "static"`), depGroupsRollOnSharedConfigOrSecretChange, "must roll this group"},
		{"secret-change-stops-rolling-the-groups", chartOnly, mut(chartFile(workers), `checksum/secret: {{ include (print $.Template.BasePath "/secret.yaml") $ | sha256sum }}`, `checksum/secret: "static"`), depGroupsRollOnSharedConfigOrSecretChange, "Secret-only change must roll"},
		{"native-clickhouse-helper-uses-the-http-port", chartOnly, mut(chartFile(helpers), `"clickhouse://%s:%s@%s-clickhouse:9000/%s"`, `"clickhouse://%s:%s@%s-clickhouse:8123/%s"`), depNativeClickhouseURI, "9000"},
		{"workers-read-the-http-clickhouse-key", chartOnly, mut(chartFile(workers), "key: CLICKHOUSE_NATIVE_URI", "key: CLICKHOUSE_URI"), depNativeClickhouseURI, "native key"},
	})
	_ = chartAndOptions
}
