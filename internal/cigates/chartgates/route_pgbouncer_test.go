package chartgates_test

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// Ported from tests/workers/test_helm_route_activate_hooks.py (CHAOS-4272, CHAOS-4455, CHAOS-6902,
// CHAOS-6958) and tests/workers/test_helm_go_pgbouncer.py (CHAOS-3872, CHAOS-5594, CHAOS-5616).

const (
	raRelease      = "route-activate"
	raMigrate      = raRelease + "-dev-health-migrate"
	raProvision    = raRelease + "-dev-health-provision-roles"
	raRiver        = raRelease + "-dev-health-river-migrate"
	raRoute        = raRelease + "-dev-health-route-activate"
	raRouteSecrets = raRelease + "-dev-health-route-activate-secrets"
	raCredSecrets  = raRelease + "-dev-health-route-activate-credential-secrets"
	routeTpl       = "templates/route-activate-hooks.yaml"
)

var routeKinds = []string{"dispatch_sync_run", "finalize_sync_run", "post_sync", "reference_discovery"}

var fullChainOn = set("migrations.hook.provisionRoles.enabled=true", "migrations.hook.riverMigrate.enabled=true", "migrations.hook.routeActivate.image="+pinnedOperator)

func kindName(k string) string { return "route-activate-" + strings.ReplaceAll(k, "_", "-") }

func raJobs(g *guard, sets ...string) map[string]doc { return jobsOf(g.docs(raRelease, sets...)) }

func raInitByName(g *guard, sets ...string) map[string]doc {
	job := raJobs(g, sets...)[raRoute]
	out := map[string]doc{}
	if job == nil {
		g.failf("the route-activate Job did not render")
		return out
	}
	for _, c := range initContainers(job) {
		out[str(c["name"])] = c
	}
	return out
}

func routeAbsentByDefault(g *guard) {
	jobs := raJobs(g)
	g.expect(jobs[raRoute] == nil, "route-activate rendered on an untouched install")
	g.expect(jobs[raMigrate] != nil, "the pre-existing migrate hook must still render")
}

func routeNeedsBothPrerequisites(g *guard) {
	for _, sets := range [][]string{
		nil,
		set("migrations.hook.provisionRoles.enabled=true", "migrations.hook.provisionRoles.image="+pinnedOperator),
		set("migrations.hook.riverMigrate.enabled=true", "migrations.hook.riverMigrate.image="+pinnedOperator),
	} {
		g.expect(raJobs(g, sets...)[raRoute] == nil, "route-activate rendered with only %v, which leaves either the runtime roles or the River schema missing", sets)
	}
}

func routeFlagOptsOut(g *guard) {
	args := join(fullChainOn, set("migrations.hook.routeActivate.enabled=false"))
	docs := g.docs(raRelease, args...)
	g.expect(jobsOf(docs)[raRoute] == nil, "routeActivate.enabled=false must remove the Job")
	g.expect(secretsOf(docs)[raRouteSecrets] == nil, "routeActivate.enabled=false must remove its Secret")
}

func routeRendersWithFullChain(g *guard) {
	g.expect(raJobs(g, fullChainOn...)[raRoute] != nil, "route-activate did not render with the full chain on")
}

func routeRespectsOuterHookGate(g *guard) {
	jobs := raJobs(g, join(fullChainOn, set("migrations.hook.enabled=false"))...)
	g.expect(jobs[raRoute] == nil, "migrations.hook.enabled=false must remove route-activate")
	g.expect(jobs[raMigrate] == nil, "migrations.hook.enabled=false must disable every hook Job")
}

func routeWeightFollowsRiver(g *guard) {
	jobs := raJobs(g, fullChainOn...)
	var w []int
	for _, n := range []string{raMigrate, raProvision, raRiver, raRoute} {
		if jobs[n] == nil {
			g.failf("%s did not render", n)
			return
		}
		w = append(w, weight(jobs[n]))
	}
	g.expect(w[0] < w[1] && w[1] < w[2] && w[2] < w[3], "hook weights %v must increase: migrate < provision-roles < river-migrate < route-activate", w)
}

func routeRunsOnInstallAndUpgrade(g *guard) {
	ev := strings.Split(ann(raJobs(g, fullChainOn...)[raRoute], "helm.sh/hook"), ",")
	g.expect(sortedHas(ev, "pre-install") && sortedHas(ev, "pre-upgrade"), "route-activate hook events %v lack pre-install or pre-upgrade", ev)
}

func routeFailedPodRetained(g *guard) {
	pol := ann(raJobs(g, fullChainOn...)[raRoute], "helm.sh/hook-delete-policy")
	g.expect(strings.Contains(pol, "before-hook-creation"), "delete policy %q lacks before-hook-creation", pol)
	g.expect(!strings.Contains(pol, "hook-succeeded"), "a failed activation's pod must survive for its logs")
}

func routeFourKindsAreOrderedInitContainers(g *guard) {
	job := raJobs(g, fullChainOn...)[raRoute]
	var names []string
	for _, c := range initContainers(job) {
		names = append(names, str(c["name"]))
	}
	var want []string
	for _, k := range routeKinds {
		want = append(want, kindName(k))
	}
	g.expect(equal(names, want), "the four routes must run in Compose's own serialized order: got %v", names)
	g.expect(!sortedHas(names, "route-dsn"), "the retired route-dsn init container is back")
}

func routeDoneContainerExitsZero(g *guard) {
	cs := containers(raJobs(g, fullChainOn...)[raRoute])
	g.expect(len(cs) == 1, "want one containers entry, got %d", len(cs))
	if len(cs) != 1 {
		return
	}
	_, cmd := cs[0]["command"]
	g.expect(str(cs[0]["name"]) == "done" && str(cs[0]["image"]) == pinnedOperator && !cmd && equal(cs[0]["args"], any(anyStrs("version"))), "the no-op container must be the operator image running `dho version`: %v", cs[0])
}

func routeEachKindInvokesRoutesApply(g *guard) {
	init := raInitByName(g, fullChainOn...)
	for _, k := range routeKinds {
		c := init[kindName(k)]
		if c == nil {
			g.failf("no init container %s", kindName(k))
			continue
		}
		_, cmd := c["command"]
		g.expect(!cmd, "%s has a command: the operator image has no shell", k)
		args := strs(c["args"])
		g.expect(len(args) >= 3 && equal(args[:3], []string{"workers", "routes", "apply"}), "%s args %v do not start with workers routes apply", k, args)
		g.expect(len(args) > 0 && args[len(args)-1] == k, "%s: the kind argument must be last and not fused: %v", k, args)
		g.expect(sortedHas(args, "--reason") && sortedHas(args, "--correlation-id"), "%s: routes apply requires --reason and --correlation-id: %v", k, args)
		env := envMap(c)
		g.expect(str(env["DEV_HEALTH_PG_DB"]["value"]) == "devhealth", "%s: DEV_HEALTH_PG_DB is %q", k, str(env["DEV_HEALTH_PG_DB"]["value"]))
		for _, r := range [][2]string{{"DOMAIN", "RIVER_DOMAIN_DATABASE_PASSWORD"}, {"QUEUE", "RIVER_QUEUE_DATABASE_PASSWORD"}, {"COORDINATOR", "RIVER_COORDINATOR_DATABASE_PASSWORD"}} {
			p := "DEV_HEALTH_PG_" + r[0] + "_"
			g.expect(str(env[p+"HOST"]["value"]) == "postgres.example.com", "%s: %sHOST is %q", k, p, str(env[p+"HOST"]["value"]))
			g.expect(str(env[p+"PORT"]["value"]) == "5432", "%s: %sPORT is %q", k, p, str(env[p+"PORT"]["value"]))
			g.expect(str(env[p+"USER"]["value"]) == "devhealth_"+strings.ToLower(r[0]), "%s: %sUSER is %q", k, p, str(env[p+"USER"]["value"]))
			_, v := env[p+"PASSWORD"]["value"]
			g.expect(!v && str(at(env[p+"PASSWORD"], "valueFrom", "secretKeyRef", "key")) == r[1], "%s: %sPASSWORD must be a secretKeyRef to %s", k, p, r[1])
		}
		for _, f := range []string{"POSTGRES_URI", "WORKER_DATABASE_URI", "COORDINATOR_DATABASE_URI", "POSTGRES_URI_FILE", "WORKER_DATABASE_URI_FILE", "COORDINATOR_DATABASE_URI_FILE", "WORKER_OPERATOR_TOKEN_FILE"} {
			g.expect(!has(env, f), "%s: the forbidden variable %s is set", k, f)
		}
		g.expect(str(env["RIVER_DATABASE_SCHEMA"]["value"]) == "river", "%s: RIVER_DATABASE_SCHEMA is %q", k, str(env["RIVER_DATABASE_SCHEMA"]["value"]))
		_, vm := c["volumeMounts"]
		g.expect(!vm, "%s mounts volumes", k)
	}
}

func routeUsesPublishedOperatorImage(g *guard) {
	init := raInitByName(g, fullChainOn...)
	for _, k := range routeKinds {
		g.expect(str(init[kindName(k)]["image"]) == pinnedOperator, "%s image is %q, want the pinned operator image", k, str(init[kindName(k)]["image"]))
	}
}

func routeOperatorImageOverrideHonoured(g *guard) {
	mirrored := "registry.example.com/mirror/dev-health-go-operator@sha256:" + hex64("c")
	init := raInitByName(g, join(fullChainOn, set("migrations.hook.routeActivate.image="+mirrored))...)
	for _, k := range routeKinds {
		g.expect(str(init[kindName(k)]["image"]) == mirrored, "%s ignored the same-name mirror override: %q", k, str(init[kindName(k)]["image"]))
	}
}

func routeRefusesForeignOperatorName(g *guard) {
	r := g.tpl(raRelease, set("migrations.hook.provisionRoles.enabled=true", "migrations.hook.riverMigrate.enabled=true", "migrations.hook.routeActivate.image=ghcr.io/example/custom-operator@sha256:"+hex64("d"))...)
	g.expect(r.code != 0, "a digest-pinned foreign-named operator image rendered clean")
	g.expect(r.code == 0 || strings.Contains(r.stderr, "is not a pinned dho image"), "refusal lacks 'is not a pinned dho image'")
}

func routeContainersNeverInvokeAShell(g *guard) {
	for _, k := range routeKinds {
		c := raInitByName(g, fullChainOn...)[kindName(k)]
		_, cmd := c["command"]
		g.expect(!cmd, "%s has a command (a shell invocation)", k)
		for _, a := range strs(c["args"]) {
			g.expect(a != "/bin/sh" && a != "/bin/bash" && a != "sh" && a != "bash", "%s args invoke a shell: %v", k, c["args"])
		}
	}
}

func routePodSharesNoVolumesNeedsNoFsGroup(g *guard) {
	job := raJobs(g, fullChainOn...)[raRoute]
	pod := podSpec(job)
	g.expect(equal(pod["securityContext"], any(doc{"runAsNonRoot": true})), "pod securityContext is %v, want only runAsNonRoot", pod["securityContext"])
	_, vols := pod["volumes"]
	g.expect(!vols, "the pod shares volumes")
	for _, c := range append(initContainers(job), containers(job)...) {
		_, vm := c["volumeMounts"]
		g.expect(!vm, "%v mounts volumes", c["name"])
	}
}

func routeHookRunsNoPythonAndNoShell(g *guard) {
	job := raJobs(g, fullChainOn...)[raRoute]
	images := map[string]bool{}
	for _, c := range append(initContainers(job), containers(job)...) {
		images[str(c["image"])] = true
	}
	g.expect(equal(images, map[string]bool{pinnedOperator: true}), "the Job runs images %v, want only the operator image", images)
	rendered := serialized(job)
	for _, n := range []string{"/bin/sh", "python3", "dev-hops-api", "urllib"} {
		g.expect(!strings.Contains(rendered, n), "the rendered Job names %q", n)
	}
}

func routeWorkerGroupUsesItsOwnImage(g *guard) {
	values := `
goWorkers:
  enabled: true
  groups:
    - name: sync-provider
      image: ghcr.io/example/dev-health-go-dho:sync-provider-test-pin-v42
      subcommand: worker
      queues: [sync_provider]
      queueConcurrency: {sync_provider: 2}
      replicas: 1
      terminationGracePeriodSeconds: 960
      autoscaling: {enabled: false}
`
	for _, d := range ofKind(g.docs(raRelease, "-f", g.valuesFile(values)), "Deployment") {
		if strings.HasSuffix(name(d), "-go-sync-provider") {
			img := str(containers(d)[0]["image"])
			g.expect(img == "ghcr.io/example/dev-health-go-dho:sync-provider-test-pin-v42", "the Deployment used image %q, not the group's declared one", img)
			return
		}
	}
	g.failf("no sync-provider Deployment rendered")
}

func routeSyncProviderDefaultImage(g *guard) {
	for _, d := range ofKind(g.docs(raRelease, set("goWorkers.enabled=true")...), "Deployment") {
		if strings.HasSuffix(name(d), "-go-sync-provider") {
			img := str(containers(d)[0]["image"])
			g.expect(img == "ghcr.io/full-chaos/dev-health-go-dho:latest", "the checked-in default sync-provider image is %q, want the published dho image", img)
			return
		}
	}
	g.failf("no sync-provider Deployment rendered")
}

func routePasswordsReachOnlyTheRouteSecret(g *guard) {
	docs := g.docs(raRelease, join(fullChainOn, set("goWorkers.pgbouncer.secret.data.RIVER_DOMAIN_DATABASE_PASSWORD=s3cret-route"))...)
	jobs, secrets := jobsOf(docs), secretsOf(docs)
	g.expect(!strings.Contains(serialized(jobs[raRoute]), "s3cret-route"), "the plaintext password is inlined into the Job spec")
	var holders []string
	for n, s := range secrets {
		if strings.Contains(serialized(s), "s3cret-route") {
			holders = append(holders, n)
		}
	}
	sort.Strings(holders)
	allowedA := []string{raRouteSecrets}
	allowedB := []string{raRouteSecrets, raRelease + "-dev-health-provision-roles-secrets", raRelease + "-dev-health-go-pgbouncer"}
	sort.Strings(allowedB)
	g.expect(equal(holders, allowedA) || equal(holders, allowedB), "the password must reach only the chart's own established Secrets, never an extra copy: %v", holders)
	for _, k := range routeKinds {
		env := envMap(raInitByName(g, join(fullChainOn, set("goWorkers.pgbouncer.secret.data.RIVER_DOMAIN_DATABASE_PASSWORD=s3cret-route"))...)[kindName(k)])
		for _, role := range []string{"DOMAIN", "QUEUE", "COORDINATOR"} {
			p := env["DEV_HEALTH_PG_"+role+"_PASSWORD"]
			_, v := p["value"]
			g.expect(at(p, "valueFrom", "secretKeyRef") != nil && !v, "%s: the %s password must be a secretKeyRef", k, role)
			g.expect(!has(env, "RIVER_"+role+"_DATABASE_PASSWORD"), "%s still holds the old RIVER_%s_DATABASE_PASSWORD name", k, role)
		}
	}
}

func routeSecretCreatedBeforeJob(g *guard) {
	secrets := secretsOf(g.docs(raRelease, join(fullChainOn, set("postgresql.enabled=true"))...))
	g.expect(secrets[raRouteSecrets] != nil, "%s did not render", raRouteSecrets)
	g.expect(secrets[raCredSecrets] == nil, "the operator-token credential Secret is back")
	if s := secrets[raRouteSecrets]; s != nil {
		g.expect(sortedHas(strings.Split(ann(s, "helm.sh/hook"), ","), "pre-install"), "%s must be a pre-install hook resource", raRouteSecrets)
		g.expect(weight(s) < 15, "%s must precede the weight-15 Job: weight %d", raRouteSecrets, weight(s))
	}
}

func routeAcceptsPreCreatedExternalSecret(g *guard) {
	ext := join(fullChainOn, set("goWorkers.pgbouncer.secret.create=false", "goWorkers.pgbouncer.secret.externalSecretName=river-role-credentials"))
	init := raInitByName(g, ext...)
	for _, k := range routeKinds {
		env := envMap(init[kindName(k)])
		for _, role := range []string{"DOMAIN", "QUEUE", "COORDINATOR"} {
			ref := mp(at(env["DEV_HEALTH_PG_"+role+"_PASSWORD"], "valueFrom", "secretKeyRef"))
			g.expect(str(ref["name"]) == "river-role-credentials" && str(ref["key"]) == "RIVER_"+role+"_DATABASE_PASSWORD", "%s %s password ref is %v", k, role, ref)
		}
	}
	g.expect(secretsOf(g.docs(raRelease, ext...))[raRouteSecrets] == nil, "the chart must not fabricate a copy of credentials it does not own")
}

func routeWithoutAnyPasswordSecretFails(g *guard) {
	r := g.tpl(raRelease, join(fullChainOn, set("goWorkers.pgbouncer.secret.create=false"))...)
	g.expect(r.code != 0, "route-activate rendered with no password Secret at all")
	g.expect(r.code == 0 || strings.Contains(r.stderr, "RIVER_"), "the error must name what the Secret has to carry")
}

func routeEnabledInputDomain(g *guard) {
	for _, c := range []struct {
		value string
		want  bool
	}{{"", true}, {"true", true}, {"false", false}} {
		sets := fullChainOn
		if c.value != "" {
			sets = join(fullChainOn, set("migrations.hook.routeActivate.enabled="+c.value))
		}
		g.expect((raJobs(g, sets...)[raRoute] != nil) == c.want, "routeActivate.enabled=%q: Job rendered=%v, want %v", c.value, !c.want, c.want)
	}
}

func routeEnabledRejects(g *guard) {
	for _, c := range []struct {
		flag, arg string
		wants     []string
	}{
		{"--set-string", "migrations.hook.routeActivate.enabled=maybe", []string{"/migrations/hook/routeActivate/enabled", "boolean"}},
		{"--set-json", "migrations.hook.routeActivate.enabled=null", []string{"/migrations/hook/routeActivate", "enabled"}},
		{"--set-string", "migrations.hook.routeActivate.enabled=", []string{"got string, want boolean"}},
		{"--set-string", "migrations.hook.routeActivate.enabled=true", []string{"got string, want boolean"}},
		{"--set-json", "migrations.hook.routeActivate.enabled=1", []string{"got number, want boolean"}},
	} {
		r := g.tpl(raRelease, join(fullChainOn, []string{c.flag, c.arg})...)
		g.expect(r.code != 0, "%s %s rendered: an out-of-vocabulary value must fail the render, not silently decide route-activate", c.flag, c.arg)
		for _, w := range c.wants {
			g.expect(r.code == 0 || strings.Contains(r.stderr, w), "%s %s: refusal lacks %q: %s", c.flag, c.arg, w, firstLines(r.stderr, 2))
		}
	}
}

func routeEnabledAcceptsRealBooleans(g *guard) {
	for _, v := range []string{"true", "false"} {
		r := g.tpl(raRelease, join(fullChainOn, set("migrations.hook.routeActivate.enabled="+v))...)
		g.expect(r.code == 0, "routeActivate.enabled=%s must render: %s", v, firstLines(r.stderr, 2))
	}
}

func routePullPolicyIndependent(g *guard) {
	job := raJobs(g, join(fullChainOn, set("image.pullPolicy=Never"))...)[raRoute]
	for _, c := range initContainers(job) {
		g.expect(str(c["imagePullPolicy"]) == "IfNotPresent", "%v inherited image.pullPolicy=Never, the ErrImageNeverPull class this fix prevents", c["name"])
	}
	cs := containers(job)
	g.expect(len(cs) == 1 && str(cs[0]["imagePullPolicy"]) == "IfNotPresent", "the done container must take the operator pull policy, not the application's")
}

func routePullPolicyOverrideWins(g *guard) {
	init := raInitByName(g, join(fullChainOn, set("image.pullPolicy=Never", "migrations.hook.routeActivate.pullPolicy=Always"))...)
	g.expect(str(init["route-activate-dispatch-sync-run"]["imagePullPolicy"]) == "Always", "the per-hook pullPolicy override did not win")
}

func routePullPolicyDefaultInputDomain(g *guard) {
	for _, p := range []string{"IfNotPresent", "Always", "Never"} {
		init := raInitByName(g, join(fullChainOn, set("image.pullPolicy=Never", "migrations.hook.routeActivate.pullPolicy="+p))...)
		g.expect(str(init["route-activate-dispatch-sync-run"]["imagePullPolicy"]) == p, "pullPolicy %s did not render through unmodified", p)
	}
}

func routeRefusesEmptyBundledDatabase(g *guard) {
	r := g.tpl(raRelease, join(fullChainOn, set("postgresql.enabled=true"), setString("postgresql.credentials.database="))...)
	g.expect(r.code != 0, "an empty bundled postgresql.credentials.database rendered")
	g.expect(r.code == 0 || strings.Contains(r.stderr, "postgresql.credentials.database"), "refusal does not name postgresql.credentials.database")
}

// --- go pgbouncer ---

const (
	poolRelease = "pool-contract"
	poolSecret  = poolRelease + "-dev-health-go-pgbouncer"
)

var poolGoValues = join(
	set("goWorkers.enabled=true", "goWorkers.pgbouncer.enabled=true", "goWorkers.pgbouncer.postgres.host=postgres.internal", "goWorkers.pgbouncer.postgres.database=devhealth"),
	setString("goWorkers.pgbouncer.secret.data.RIVER_DOMAIN_DATABASE_PASSWORD=domain-password", "goWorkers.pgbouncer.secret.data.RIVER_QUEUE_DATABASE_PASSWORD=queue-password", "goWorkers.pgbouncer.secret.data.RIVER_COORDINATOR_DATABASE_PASSWORD=coordinator-password"))

var roleSecretKeys = []string{"RIVER_DOMAIN_DATABASE_PASSWORD", "RIVER_QUEUE_DATABASE_PASSWORD", "RIVER_COORDINATOR_DATABASE_PASSWORD", "POSTGRES_URI", "WORKER_DATABASE_URI", "COORDINATOR_DATABASE_URI", "RIVER_KEDA_READONLY_PASSWORD", "userlist.txt"}

type pooler struct {
	port            int
	mode            string
	poolKey, maxKey string
	role            string
}

var poolers = map[string]pooler{
	"transaction":         {6432, "transaction", "pgbouncer_transaction_pool_size", "pgbouncer_transaction_max_client_connections", "devhealth_domain"},
	"queue-session":       {6433, "session", "pgbouncer_queue_session_pool_size", "pgbouncer_queue_session_max_client_connections", "devhealth_queue"},
	"coordinator-session": {6434, "session", "pgbouncer_coordinator_session_pool_size", "pgbouncer_coordinator_session_max_client_connections", "devhealth_coordinator"},
}

func pgRequiresTopology(g *guard) {
	r := g.tpl(poolRelease, set("goWorkers.enabled=true", "goWorkers.pgbouncer.enabled=false")...)
	g.expect(r.code != 0, "goWorkers.enabled with goWorkers.pgbouncer.enabled=false rendered: the schema must reject the pairing")
	g.expect(r.code == 0 || strings.Contains(r.stderr, "values don't meet the specifications"), "the refusal is not a schema refusal: %s", firstLines(r.stderr, 2))
}

func pgScopesRoleDSNsAndPreservesDirectMigrations(g *guard) {
	docs := g.docs(poolRelease, poolGoValues...)
	budget := mp(g.jsonFile(deploymentJSON)["postgres_budget"])
	secrets := secretsOf(docs)
	rs := secrets[poolSecret]
	if rs == nil {
		g.failf("the runtime Secret %s did not render", poolSecret)
		return
	}
	var keys []string
	for k := range mp(rs["stringData"]) {
		keys = append(keys, k)
	}
	g.expect(sameSet(keys, roleSecretKeys), "the runtime Secret keys are %v, want exactly the role-scoped set %v", keys, roleSecretKeys)
	_, m1 := mp(rs["stringData"])["MIGRATION_DATABASE_URI"]
	_, m2 := mp(rs["stringData"])["MIGRATION_DATABASE_URI_FILE"]
	g.expect(!m1 && !m2, "the runtime Secret carries a migration DSN")
	for k, suffix := range map[string]string{"POSTGRES_URI": "-go-pgbouncer-transaction:6432/devhealth", "WORKER_DATABASE_URI": "-go-pgbouncer-queue-session:6433/devhealth", "COORDINATOR_DATABASE_URI": "-go-pgbouncer-coordinator-session:6434/devhealth"} {
		g.expect(strings.HasSuffix(str(at(rs, "stringData", k)), suffix), "%s is %q, want a ...%s endpoint", k, str(at(rs, "stringData", k)), suffix)
	}
	deployments := map[string]doc{}
	for _, d := range ofKind(docs, "Deployment") {
		if c := label(d, "app.kubernetes.io/component"); strings.HasPrefix(c, "go-pgbouncer-") {
			deployments[c] = d
		}
	}
	var got, want []string
	for n := range deployments {
		got = append(got, n)
	}
	for n := range poolers {
		want = append(want, "go-pgbouncer-"+n)
	}
	g.expect(sameSet(got, want), "pooler Deployments %v, want %v", got, want)
	for n, p := range poolers {
		d := deployments["go-pgbouncer-"+n]
		if d == nil {
			continue
		}
		c := containers(d)[0]
		env := envMap(c)
		g.expect(strings.Contains(str(c["image"]), "@sha256:"), "%s image %q is not digest pinned", n, str(c["image"]))
		g.expect(str(env["DB_USER"]["value"]) == p.role, "%s DB_USER is %q, want %q", n, str(env["DB_USER"]["value"]), p.role)
		g.expect(str(env["POOL_MODE"]["value"]) == p.mode, "%s POOL_MODE is %q, want %q", n, str(env["POOL_MODE"]["value"]), p.mode)
		g.expect(str(env["DEFAULT_POOL_SIZE"]["value"]) == str(budget[p.poolKey]), "%s DEFAULT_POOL_SIZE is %q, want the budget's %v", n, str(env["DEFAULT_POOL_SIZE"]["value"]), budget[p.poolKey])
		g.expect(str(env["MAX_CLIENT_CONN"]["value"]) == str(budget[p.maxKey]), "%s MAX_CLIENT_CONN is %q, want the budget's %v", n, str(env["MAX_CLIENT_CONN"]["value"]), budget[p.maxKey])
		g.expect(str(at(c, "readinessProbe", "tcpSocket", "port")) == "pgbouncer", "%s readiness probe does not target the pgbouncer port", n)
		g.expect(equal(podSpec(d)["securityContext"], any(doc{"runAsNonRoot": true, "runAsUser": 70, "runAsGroup": 70, "fsGroup": 70, "seccompProfile": doc{"type": "RuntimeDefault"}})), "%s pod securityContext is %v", n, podSpec(d)["securityContext"])
		g.expect(equal(c["securityContext"], any(doc{"allowPrivilegeEscalation": false, "readOnlyRootFilesystem": true, "capabilities": doc{"drop": anyStrs("ALL")}})), "%s container securityContext is %v", n, c["securityContext"])
		mounts := []any{doc{"name": "generated-config", "mountPath": "/etc/pgbouncer"}}
		volumes := []any{doc{"name": "generated-config", "emptyDir": doc{}}}
		if n == "transaction" {
			mounts = append(mounts, doc{"name": "userlist", "mountPath": "/etc/pgbouncer-userlist", "readOnly": true})
			volumes = append(volumes, doc{"name": "userlist", "secret": doc{"secretName": poolSecret, "defaultMode": 288, "items": []any{doc{"key": "userlist.txt", "path": "userlist.txt"}}}})
		}
		g.expect(equal(c["volumeMounts"], mounts), "%s volumeMounts are %v", n, c["volumeMounts"])
		g.expect(equal(podSpec(d)["volumes"], volumes), "%s volumes are %v", n, podSpec(d)["volumes"])
	}
	for _, s := range ofKind(docs, "Service") {
		comp := label(s, "app.kubernetes.io/component")
		if !strings.HasPrefix(comp, "go-pgbouncer-") {
			continue
		}
		p := poolers[strings.TrimPrefix(comp, "go-pgbouncer-")]
		g.expect(equal(at(s, "spec", "ports", 0), any(doc{"name": "pgbouncer", "port": p.port, "targetPort": "pgbouncer"})), "%s Service port is %v", comp, at(s, "spec", "ports", 0))
	}
	workers := map[string]doc{}
	for _, d := range ofKind(docs, "Deployment") {
		if label(d, "app.kubernetes.io/component") == "go-worker" {
			workers[label(d, "dev-health.io/worker-group")] = d
		}
	}
	var wn []string
	for n := range workers {
		wn = append(wn, n)
	}
	sort.Strings(wn)
	g.expect(equal(wn, allNineNames), "worker groups %v, want %v", wn, allNineNames)
	for grp, d := range workers {
		c := containers(d)[0]
		env := envMap(c)
		args := strs(c["args"])
		g.expect(str(at(env["POSTGRES_URI"], "valueFrom", "secretKeyRef", "key")) == "POSTGRES_URI", "%s POSTGRES_URI must be a secretKeyRef to POSTGRES_URI", grp)
		g.expect(str(at(env["WORKER_DATABASE_URI"], "valueFrom", "secretKeyRef", "key")) == "WORKER_DATABASE_URI", "%s WORKER_DATABASE_URI must be a secretKeyRef to WORKER_DATABASE_URI", grp)
		g.expect(sortedHas(args, "--queue-database-mode=session") && sortedHas(args, "--domain-transaction-pooler=true"), "%s lacks the pool-mode flags: %v", grp, args)
		for _, n := range []string{"WORKER_DATABASE_MODE", "PGBOUNCER_TRANSACTION_MODE", "MIGRATION_DATABASE_URI", "MIGRATION_DATABASE_URI_FILE"} {
			g.expect(!has(env, n), "%s sets %s in its environment", grp, n)
		}
		if grp == "reconciler" || grp == "scheduler" {
			g.expect(str(at(env["COORDINATOR_DATABASE_URI"], "valueFrom", "secretKeyRef", "key")) == "COORDINATOR_DATABASE_URI", "%s must read COORDINATOR_DATABASE_URI", grp)
			g.expect(sortedHas(args, "--coordinator-database-mode=session") && !has(env, "COORDINATOR_DATABASE_MODE"), "%s coordinator mode must be a flag", grp)
		} else {
			g.expect(!has(env, "COORDINATOR_DATABASE_URI") && !has(env, "COORDINATOR_DATABASE_MODE"), "%s receives the coordinator DSN", grp)
		}
	}
	jobs := ofKind(docs, "Job")
	g.expect(len(jobs) > 0, "no migration Job rendered")
	if len(jobs) > 0 {
		for _, n := range envFromSecretNames(jobs[0]) {
			g.expect(n != poolSecret, "the migration Job reads the runtime pooler Secret")
		}
	}
}

func pgNetworkPolicyPinsPostgresPaths(g *guard) {
	docs := g.docs(poolRelease, join(poolGoValues, set("networkPolicy.enabled=true", "postgresql.enabled=true"))...)
	n := 0
	for _, p := range ofKind(docs, "NetworkPolicy") {
		if strings.HasPrefix(name(p), poolRelease+"-dev-health-go-pgbouncer-") {
			n++
			g.expect(str(at(p, "spec", "egress", 0, "to", 0, "podSelector", "matchLabels", "app.kubernetes.io/component")) == "postgresql", "%s egress does not select the bundled PostgreSQL", name(p))
		}
	}
	g.expect(n == 3, "want three pooler NetworkPolicies, got %d", n)
	r := g.tpl(poolRelease, join(poolGoValues, set("networkPolicy.enabled=true"))...)
	g.expect(r.code != 0, "networkPolicy.enabled with external PostgreSQL and no CIDR rendered")
	g.expect(r.code == 0 || strings.Contains(r.stderr, "networkPolicyCIDR"), "the refusal does not name networkPolicyCIDR")
}

var queueIn = regexp.MustCompile(`queue IN \(([^)]*)\)`)

func pgRiverWorkersSelectManifestQueues(g *guard) {
	docs := g.docs(poolRelease, poolGoValues...)
	river := riverProcesses(g)
	workers := map[string]doc{}
	for _, d := range ofKind(docs, "Deployment") {
		grp := label(d, "dev-health.io/worker-group")
		if label(d, "app.kubernetes.io/component") == "go-worker" && river[grp] != nil {
			workers[grp] = d
		}
	}
	g.expect(len(workers) == len(river), "rendered river worker groups %d, manifest %d", len(workers), len(river))
	for grp, p := range river {
		d := workers[grp]
		if d == nil {
			continue
		}
		_, hasProfile := mp(at(d, "metadata", "labels"))["dev-health.io/profile"]
		g.expect(!hasProfile, "%s carries a dev-health.io/profile label", grp)
		c := containers(d)[0]
		env := envMap(c)
		for _, n := range []string{"DEV_HEALTH_PROFILE", "DEV_HEALTH_QUEUES", "DEV_HEALTH_QUEUE_CONCURRENCY", "DEV_HEALTH_WORKER_GROUP"} {
			g.expect(!has(env, n), "%s sets %s", grp, n)
		}
		args := strs(c["args"])
		var qc []string
		for _, w := range list(p["queue_workers"]) {
			qc = append(qc, fmt.Sprintf("%s=%v", str(at(w, "queue")), at(w, "max_workers")))
		}
		want := []string{"--queues=" + strings.Join(strs(p["queues"]), ","), "--queue-concurrency=" + strings.Join(qc, ","), "--worker-group=" + grp, fmt.Sprintf("--shutdown-timeout=%vs", p["shutdown_grace_seconds"])}
		g.expect(len(args) >= 5 && args[0] == "worker" && equal(args[1:5], want), "%s args lead with %v, want worker then %v", grp, args, want)
		g.expect(sortedHas(args, "--http-addr=:8080"), "%s lacks --http-addr=:8080", grp)
		for _, a := range args {
			g.expect(!strings.HasPrefix(a, "--profile="), "%s is a queue worker and must not run a runtime profile", grp)
		}
		var scaler doc
		for _, s := range ofKind(docs, "ScaledObject") {
			if str(at(s, "spec", "scaleTargetRef", "name")) == name(d) {
				scaler = s
			}
		}
		if scaler == nil {
			g.failf("%s has no ScaledObject", grp)
			continue
		}
		tr := list(at(scaler, "spec", "triggers"))
		g.expect(len(tr) > 0, "%s: a scaled group renders at least its queue-scoped trigger", grp)
		for _, extra := range tr[min(1, len(tr)):] {
			g.expect(str(at(extra, "name")) != "", "%s: an extra trigger without a name", grp)
		}
		if len(tr) == 0 {
			continue
		}
		g.expect(str(at(tr[0], "type")) == "postgresql", "%s: the first trigger is not postgresql", grp)
		wantHost := poolRelease + "-dev-health-go-pgbouncer-transaction." + str(at(scaler, "metadata", "namespace")) + ".svc.cluster.local"
		g.expect(str(at(tr[0], "metadata", "host")) == wantHost, "%s: trigger host is %q, want the in-cluster FQDN %q (keda-operator runs in another namespace)", grp, str(at(tr[0], "metadata", "host")), wantHost)
		m := queueIn.FindStringSubmatch(str(at(tr[0], "metadata", "query")))
		if m == nil {
			g.failf("%s: no `queue IN (...)` in the trigger query", grp)
			continue
		}
		var queried []string
		for _, q := range strings.Split(m[1], ",") {
			queried = append(queried, strings.Trim(strings.TrimSpace(q), "'"))
		}
		g.expect(sameSet(queried, strs(p["queues"])), "%s: the scaler signal covers queues %v, want exactly this group's %v", grp, queried, strs(p["queues"]))
	}
	for grp, d := range func() map[string]doc {
		out := map[string]doc{}
		for _, d := range ofKind(docs, "Deployment") {
			if label(d, "app.kubernetes.io/component") == "go-worker" {
				out[label(d, "dev-health.io/worker-group")] = d
			}
		}
		return out
	}() {
		if river[grp] != nil {
			continue
		}
		env := envMap(containers(d)[0])
		for _, n := range []string{"DEV_HEALTH_QUEUES", "DEV_HEALTH_QUEUE_CONCURRENCY", "DEV_HEALTH_WORKER_GROUP"} {
			g.expect(!has(env, n), "%s sets %s", grp, n)
		}
	}
}

func TestRoutePgbouncerGuardsRealTree(t *testing.T) {
	runReal(t,
		check{"route-absent-by-default", routeAbsentByDefault},
		check{"route-needs-both-prerequisites", routeNeedsBothPrerequisites},
		check{"route-flag-opts-out", routeFlagOptsOut},
		check{"route-renders-with-full-chain", routeRendersWithFullChain},
		check{"route-respects-outer-hook-gate", routeRespectsOuterHookGate},
		check{"route-weight-follows-river", routeWeightFollowsRiver},
		check{"route-runs-on-install-and-upgrade", routeRunsOnInstallAndUpgrade},
		check{"route-failed-pod-retained", routeFailedPodRetained},
		check{"route-four-kinds-ordered-init-containers", routeFourKindsAreOrderedInitContainers},
		check{"route-done-container-exits-zero", routeDoneContainerExitsZero},
		check{"route-each-kind-invokes-routes-apply", routeEachKindInvokesRoutesApply},
		check{"route-uses-published-operator-image", routeUsesPublishedOperatorImage},
		check{"route-operator-image-override-honoured", routeOperatorImageOverrideHonoured},
		check{"route-refuses-foreign-operator-name", routeRefusesForeignOperatorName},
		check{"route-containers-never-invoke-a-shell", routeContainersNeverInvokeAShell},
		check{"route-pod-shares-no-volumes", routePodSharesNoVolumesNeedsNoFsGroup},
		check{"route-hook-runs-no-python-and-no-shell", routeHookRunsNoPythonAndNoShell},
		check{"route-worker-group-uses-its-own-image", routeWorkerGroupUsesItsOwnImage},
		check{"route-sync-provider-default-image", routeSyncProviderDefaultImage},
		check{"route-passwords-reach-only-the-route-secret", routePasswordsReachOnlyTheRouteSecret},
		check{"route-secret-created-before-job", routeSecretCreatedBeforeJob},
		check{"route-accepts-pre-created-external-secret", routeAcceptsPreCreatedExternalSecret},
		check{"route-without-any-password-secret-fails", routeWithoutAnyPasswordSecretFails},
		check{"route-enabled-input-domain", routeEnabledInputDomain},
		check{"route-enabled-rejects-non-booleans", routeEnabledRejects},
		check{"route-enabled-accepts-real-booleans", routeEnabledAcceptsRealBooleans},
		check{"route-pull-policy-independent", routePullPolicyIndependent},
		check{"route-pull-policy-override-wins", routePullPolicyOverrideWins},
		check{"route-pull-policy-default-input-domain", routePullPolicyDefaultInputDomain},
		check{"route-refuses-empty-bundled-database", routeRefusesEmptyBundledDatabase},
		check{"pgbouncer-requires-topology", pgRequiresTopology},
		check{"pgbouncer-scopes-role-dsns", pgScopesRoleDSNsAndPreservesDirectMigrations},
		check{"pgbouncer-network-policy-pins-postgres-paths", pgNetworkPolicyPinsPostgresPaths},
		check{"pgbouncer-river-workers-select-manifest-queues", pgRiverWorkersSelectManifestQueues},
	)
}

func TestRoutePgbouncerGuardsPlantedDefects(t *testing.T) {
	const route = chartRel + "/" + routeTpl
	const pgb = chartRel + "/templates/go-pgbouncer.yaml"
	const gate = "{{- if and .Values.migrations.hook.provisionRoles.enabled .Values.migrations.hook.riverMigrate.enabled .Values.migrations.hook.routeActivate.enabled }}"
	const hookHead = "    helm.sh/hook: {{ join \",\" $hookEvents }}\n    helm.sh/hook-weight: \"15\""
	const initHead = "          imagePullPolicy: {{ $operatorPullPolicy }}\n          securityContext:\n            allowPrivilegeEscalation: false\n          args:\n            - workers"
	chartAndManifest := append([]string{deploymentJSON}, chartOnly...)
	runPlanted(t, []planted{
		{"route-gate-drops-the-river-prerequisite", chartOnly,
			mut(route, gate, "{{- if and .Values.migrations.hook.provisionRoles.enabled .Values.migrations.hook.routeActivate.enabled }}"),
			routeNeedsBothPrerequisites, "failed"},
		{"route-opt-out-flag-ignored", chartOnly,
			mut(route, gate, "{{- if and .Values.migrations.hook.provisionRoles.enabled .Values.migrations.hook.riverMigrate.enabled }}"),
			routeFlagOptsOut, "routeActivate.enabled=false must remove the Job"},
		{"route-outer-hook-gate-removed", chartOnly,
			all(mut(route, "{{- if .Values.migrations.hook.enabled }}", "{{- if true }}")),
			routeRespectsOuterHookGate, "must remove route-activate"},
		{"route-weight-moves-before-river", chartOnly,
			mut(route, hookHead, "    helm.sh/hook: {{ join \",\" $hookEvents }}\n    helm.sh/hook-weight: \"3\""),
			routeWeightFollowsRiver, "must increase"},
		{"route-skips-upgrades", chartOnly,
			mut(route, hookHead, "    helm.sh/hook: pre-install\n    helm.sh/hook-weight: \"15\""),
			routeRunsOnInstallAndUpgrade, "lack pre-install or pre-upgrade"},
		{"route-pod-deleted-on-success", chartOnly,
			mut(route, "    helm.sh/hook-delete-policy: before-hook-creation\nspec:\n  backoffLimit", "    helm.sh/hook-delete-policy: before-hook-creation,hook-succeeded\nspec:\n  backoffLimit"),
			routeFailedPodRetained, "must survive for its logs"},
		{"route-kinds-reordered", chartOnly,
			mut(route, `{{- $routeKinds := list "dispatch_sync_run" "finalize_sync_run" "post_sync" "reference_discovery" }}`, `{{- $routeKinds := list "finalize_sync_run" "dispatch_sync_run" "post_sync" "reference_discovery" }}`),
			routeFourKindsAreOrderedInitContainers, "serialized order"},
		{"route-done-container-no-longer-exits-zero", chartOnly,
			mut(route, `args: ["version"]`, `args: ["--help"]`),
			routeDoneContainerExitsZero, "dho version"},
		{"route-invokes-the-wrong-verb", chartOnly,
			mut(route, "            - workers\n            - routes\n            - apply\n", "            - workers\n            - routes\n            - show\n"),
			routeEachKindInvokesRoutesApply, "do not start with workers routes apply"},
		{"route-database-name-wrong", chartOnly,
			mut(route, "            - name: DEV_HEALTH_PG_DB\n              value: {{ $postgresDatabase | quote }}", "            - name: DEV_HEALTH_PG_DB\n              value: \"other\""),
			routeEachKindInvokesRoutesApply, "DEV_HEALTH_PG_DB"},
		{"route-init-container-runs-another-image", chartOnly,
			mutAll(route, "          image: {{ $operatorImage }}\n          imagePullPolicy: {{ $operatorPullPolicy }}\n          securityContext:\n            allowPrivilegeEscalation: false\n          args:\n            - workers", "          image: ghcr.io/full-chaos/dev-health-go-dho:latest\n          imagePullPolicy: {{ $operatorPullPolicy }}\n          securityContext:\n            allowPrivilegeEscalation: false\n          args:\n            - workers"),
			routeUsesPublishedOperatorImage, "want the pinned operator image"},
		{"route-ignores-the-image-override", chartOnly,
			mut(route, "{{- $operatorImage := .Values.migrations.hook.routeActivate.image }}", "{{- $operatorImage := \"ghcr.io/full-chaos/dev-health-go-operator@sha256:a6acfd0b8cc78d4d2fd4160c68ab3f8b69eb282b1c2433117af01c954f576534\" }}"),
			routeOperatorImageOverrideHonoured, "ignored the same-name mirror override"},
		{"route-foreign-operator-name-accepted", chartOnly,
			mut(chartRel+"/"+helpersTpl, `{{- if not (or (regexMatch "(^|/)dev-health-go-operator([:@]|$)" $image) (regexMatch "(^|/)dev-health-go-dho([:@]|$)" $image)) -}}`, `{{- if false -}}`),
			routeRefusesForeignOperatorName, "rendered clean"},
		{"route-container-gains-a-shell-command", chartOnly,
			mut(route, initHead, "          command: [sh, -c]\n"+initHead),
			routeContainersNeverInvokeAShell, "has a command"},
		{"route-pod-gains-an-fsgroup", chartOnly,
			mut(route, "      securityContext:\n        runAsNonRoot: true\n      initContainers:", "      securityContext:\n        runAsNonRoot: true\n        fsGroup: 1000\n      initContainers:"),
			routePodSharesNoVolumesNeedsNoFsGroup, "pod securityContext"},
		{"route-pod-gains-a-volume", chartOnly,
			mut(route, "      securityContext:\n        runAsNonRoot: true\n      initContainers:", "      securityContext:\n        runAsNonRoot: true\n      volumes: []\n      initContainers:"),
			routePodSharesNoVolumesNeedsNoFsGroup, "shares volumes"},
		{"route-done-container-runs-the-python-image", chartOnly,
			mut(route, "        - name: done\n          image: {{ $operatorImage }}", "        - name: done\n          image: ghcr.io/full-chaos/dev-hops-api:latest"),
			routeHookRunsNoPythonAndNoShell, "images"},
		{"worker-group-ignores-its-declared-image", chartOnly,
			mut(chartFile("templates/go-workers.yaml"), "          image: {{ $image }}", "          image: ghcr.io/full-chaos/dev-health-go-dho:latest"),
			routeWorkerGroupUsesItsOwnImage, "not the group's declared one"},
		{"sync-provider-default-image-drifts", chartOnly,
			mut(chartFile("values.yaml"), "  image: ghcr.io/full-chaos/dev-health-go-dho:latest\n", "  image: ghcr.io/full-chaos/dev-health-go-operator:latest\n"),
			routeSyncProviderDefaultImage, "checked-in default"},
		{"route-password-inlined-into-the-job", chartOnly,
			mut(route, "              valueFrom:\n                secretKeyRef:\n                  name: {{ $routeActivateSecretName }}\n                  key: RIVER_{{ $role }}_DATABASE_PASSWORD", "              value: \"s3cret-route\""),
			routePasswordsReachOnlyTheRouteSecret, "inlined"},
		{"route-secret-created-after-the-job", chartOnly,
			mut(route, "    helm.sh/hook-weight: \"-5\"", "    helm.sh/hook-weight: \"20\""),
			routeSecretCreatedBeforeJob, "must precede the weight-15 Job"},
		{"route-ignores-the-external-secret", chartOnly,
			mut(route, `{{- $routeActivateSecretName := ternary (printf "%s-route-activate-secrets" (include "dev-health.fullname" .)) (toString $poolerSecret.externalSecretName) $poolerSecret.create }}`, `{{- $routeActivateSecretName := printf "%s-route-activate-secrets" (include "dev-health.fullname" .) }}`),
			routeAcceptsPreCreatedExternalSecret, "password ref"},
		{"route-enabled-schema-accepts-strings", chartOnly,
			mut(chartFile("values.schema.json"), `"enabled": {
                  "type": "boolean",
                  "description": "Gates the CHAOS-4272`, `"enabled": {
                  "description": "Gates the CHAOS-4272`),
			routeEnabledRejects, "rendered"},
		{"route-enabled-null-deletes-the-key-silently", chartOnly,
			mut(chartFile("values.schema.json"), `"required": ["enabled"],
              "description": "codex review (r3, P1`, `"required": [],
              "description": "codex review (r3, P1`),
			routeEnabledRejects, "null"},
		{"route-pull-policy-follows-the-application-image", chartOnly,
			mut(route, `{{- $operatorPullPolicy := .Values.migrations.hook.routeActivate.pullPolicy | default "IfNotPresent" }}`, `{{- $operatorPullPolicy := .Values.migrations.hook.routeActivate.pullPolicy | default .Values.image.pullPolicy }}`),
			routePullPolicyIndependent, "inherited image.pullPolicy=Never"},
		{"route-pull-policy-override-ignored", chartOnly,
			mut(route, `{{- $operatorPullPolicy := .Values.migrations.hook.routeActivate.pullPolicy | default "IfNotPresent" }}`, `{{- $operatorPullPolicy := "IfNotPresent" }}`),
			routePullPolicyOverrideWins, "did not win"},
		{"route-accepts-an-empty-bundled-database", chartOnly,
			mut(chartRel+"/"+helpersTpl, `{{- required "postgresql.credentials.database must not be empty when postgresql.enabled=true" .Values.postgresql.credentials.database }}`, `{{- .Values.postgresql.credentials.database }}`),
			routeRefusesEmptyBundledDatabase, "rendered"},
		{"pgbouncer-topology-not-required", chartAndManifest,
			mut(chartFile("values.schema.json"), `"pgbouncer": {
                "properties": { "enabled": { "const": true } },`, `"pgbouncer": {
                "properties": { "enabled": { "type": "boolean" } },`),
			pgRequiresTopology, "not a schema refusal"},
		{"pgbouncer-secret-gains-a-migration-dsn", chartAndManifest,
			mut(pgb, "  COORDINATOR_DATABASE_URI: {{ printf", "  MIGRATION_DATABASE_URI: \"postgresql://x\"\n  COORDINATOR_DATABASE_URI: {{ printf"),
			pgScopesRoleDSNsAndPreservesDirectMigrations, "migration DSN"},
		{"pgbouncer-transaction-pool-points-at-the-queue-port", chartAndManifest,
			mut(pgb, ".Values.goWorkers.pgbouncer.transaction.port $postgresDatabase | quote }}", ".Values.goWorkers.pgbouncer.queueSession.port $postgresDatabase | quote }}"),
			pgScopesRoleDSNsAndPreservesDirectMigrations, "endpoint"},
		{"pgbouncer-pool-size-ignores-the-value", chartAndManifest,
			mut(pgb, "\"poolSize\" .Values.goWorkers.pgbouncer.transaction.poolSize", "\"poolSize\" 5"),
			pgScopesRoleDSNsAndPreservesDirectMigrations, "DEFAULT_POOL_SIZE"},
		{"pgbouncer-pod-runs-as-root", chartAndManifest,
			mut(pgb, "securityContext: {runAsNonRoot: true, runAsUser: 70,", "securityContext: {runAsNonRoot: false, runAsUser: 70,"),
			pgScopesRoleDSNsAndPreservesDirectMigrations, "pod securityContext"},
		{"pgbouncer-root-filesystem-writable", chartAndManifest,
			mut(pgb, "readOnlyRootFilesystem: true, capabilities: {drop: [ALL]}}", "readOnlyRootFilesystem: false, capabilities: {drop: [ALL]}}"),
			pgScopesRoleDSNsAndPreservesDirectMigrations, "container securityContext"},
		{"pgbouncer-userlist-mounted-writable", chartAndManifest,
			mut(pgb, "- {name: userlist, mountPath: /etc/pgbouncer-userlist, readOnly: true}", "- {name: userlist, mountPath: /etc/pgbouncer-userlist}"),
			pgScopesRoleDSNsAndPreservesDirectMigrations, "volumeMounts"},
		{"pgbouncer-service-targets-another-port", chartAndManifest,
			mut(pgb, "targetPort: pgbouncer}", "targetPort: 6432}"),
			pgScopesRoleDSNsAndPreservesDirectMigrations, "Service port"},
		{"worker-loses-the-pool-mode-flag", chartAndManifest,
			mut(chartFile("templates/go-workers.yaml"), `- "--queue-database-mode=session"`, `- "--queue-database-mode=transaction"`),
			pgScopesRoleDSNsAndPreservesDirectMigrations, "pool-mode flags"},
		{"every-worker-gets-the-coordinator-dsn", chartAndManifest,
			mut(chartFile("templates/go-workers.yaml"), "            - name: COORDINATOR_DATABASE_URI\n", "            - name: COORDINATOR_DATABASE_URI_PLACEHOLDER\n"),
			pgScopesRoleDSNsAndPreservesDirectMigrations, "COORDINATOR_DATABASE_URI"},
		{"pgbouncer-policy-egress-targets-the-wrong-pods", chartAndManifest,
			mut(pgb, "              {{- include \"dev-health.componentSelectorLabels\" (dict \"component\" \"postgresql\" \"context\" $) | nindent 14 }}", "              {{- include \"dev-health.componentSelectorLabels\" (dict \"component\" \"redis\" \"context\" $) | nindent 14 }}"),
			pgNetworkPolicyPinsPostgresPaths, "egress does not select the bundled PostgreSQL"},
		{"pgbouncer-policy-cidr-not-required", chartAndManifest,
			mut(pgb, `{cidr: {{ required "goWorkers.pgbouncer.postgres.networkPolicyCIDR is required for external PostgreSQL when networkPolicy.enabled" $.Values.goWorkers.pgbouncer.postgres.networkPolicyCIDR | quote }}}`, `{cidr: {{ $.Values.goWorkers.pgbouncer.postgres.networkPolicyCIDR | default "0.0.0.0/0" | quote }}}`),
			pgNetworkPolicyPinsPostgresPaths, "networkPolicy.enabled with external PostgreSQL"},
		{"river-workers-queues-flag-drifts", chartAndManifest,
			mut(chartFile("templates/go-workers.yaml"), `printf "--queues=%s" (join "," $group.queues)`, `printf "--queues=%s" (join ";" $group.queues)`),
			pgRiverWorkersSelectManifestQueues, "args lead with"},
		{"keda-trigger-uses-the-bare-service-name", chartAndManifest,
			mut(chartFile("templates/go-worker-keda.yaml"), "        host: {{ include \"dev-health.fullname\" $ }}-go-pgbouncer-transaction.{{ include \"dev-health.namespace\" $ }}.svc.cluster.local\n        port: {{ $.Values.goWorkers.pgbouncer.transaction.port | quote }}\n        userName: {{ $kedaRole | quote }}\n        dbName: {{ $postgresDatabase | quote }}\n        sslmode: disable\n        # river_job backlog", "        host: {{ include \"dev-health.fullname\" $ }}-go-pgbouncer-transaction\n        port: {{ $.Values.goWorkers.pgbouncer.transaction.port | quote }}\n        userName: {{ $kedaRole | quote }}\n        dbName: {{ $postgresDatabase | quote }}\n        sslmode: disable\n        # river_job backlog"),
			pgRiverWorkersSelectManifestQueues, "in-cluster FQDN"},
		{"keda-trigger-covers-another-groups-queues", chartAndManifest,
			mut(chartFile("templates/go-worker-keda.yaml"), "{{- $queueList := join \"','\" $group.queues }}", "{{- $queueList := join \"','\" (concat $group.queues (list \"extra\")) }}"),
			pgRiverWorkersSelectManifestQueues, "scaler signal covers queues"},
		{"worker-gains-a-profile-label", chartAndManifest,
			mut(chartFile("templates/go-workers.yaml"), "    dev-health.io/worker-group: {{ $group.name }}\n", "    dev-health.io/worker-group: {{ $group.name }}\n    dev-health.io/profile: {{ $group.name }}\n"),
			pgRiverWorkersSelectManifestQueues, "dev-health.io/profile label"},
	})
}
