package bigboy_test

import (
	"fmt"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
)

func ccfgDrainContract(g *guard) {
	services := ccfgServices(g)
	procs := ccfgProcessByName(g)
	for _, process := range []string{"sync", "heavy", "ops", "sync-provider"} {
		name := ccfgServiceByProcess[process]
		grace := fmt.Sprintf("%vs", procs[process]["shutdown_grace_seconds"])
		svc := ccfgService(g, services, name)
		if got := ccfgWorkerArgs(g, name, svc)["--shutdown-timeout"]; got != grace {
			g.failf("%s: --shutdown-timeout=%s, want %s (deployment.json %s shutdown_grace_seconds)", name, got, grace, process)
		}
		if got := str(svc["stop_grace_period"]); got != grace {
			g.failf("%s: stop_grace_period=%s, want %s (deployment.json %s shutdown_grace_seconds)", name, got, grace, process)
		}
	}
}

func ccfgMigrateOneShot(g *guard) {
	migrate, ok := ccfgServices(g)["migrate"].(map[string]any)
	if !ok {
		g.failf("compose.yml must define a migrate service")
		return
	}
	g.expect(str(migrate["restart"]) == "no", "migrate must be one-shot (restart: \"no\"), got %v", migrate["restart"])
}

func ccfgMigrateWaitsForHealth(g *guard) {
	migrate := ccfgService(g, ccfgServices(g), "migrate")
	for _, dep := range []string{"postgres", "clickhouse"} {
		g.expect(ccfgCondition(migrate, dep) == "service_healthy",
			"migrate must wait for %s with condition service_healthy, got %q", dep, ccfgCondition(migrate, dep))
	}
}

var ccfgImagePinRe = regexp.MustCompile(`^\$\{([A-Z0-9_]+):-(.+)\}$`)

func ccfgOverridableImage(g *guard) {
	families := map[string]bool{
		"ghcr.io/full-chaos/dev-health-go-worker":        true,
		"ghcr.io/full-chaos/dev-health-go-scheduler":     true,
		"ghcr.io/full-chaos/dev-health-go-operator":      true,
		"ghcr.io/full-chaos/dev-health-go-dho":           true,
		"ghcr.io/full-chaos/dev-health-go-contractcheck": true,
		"ghcr.io/full-chaos/dev-health-go-migrate":       true,
		"ghcr.io/full-chaos/dev-health-go-auth-service":  true,
		"ghcr.io/full-chaos/dev-health-go-auth-migrate":  true,
	}
	services := ccfgServices(g)
	found := 0
	for _, name := range ccfgKeys(services) {
		svc := mp(services[name])
		if mp(svc["build"]) == nil {
			continue
		}
		found++
		image := str(svc["image"])
		if image == "" {
			g.failf("%s declares build: with no image:, so a release pin cannot reach it", name)
			continue
		}
		m := ccfgImagePinRe.FindStringSubmatch(image)
		if m == nil {
			g.failf("%s's image must be an overridable ${VAR:-default} pin: %q", name, image)
			continue
		}
		if !strings.HasSuffix(m[1], "_IMAGE") {
			g.failf("%s's pin variable should end in _IMAGE: %q", name, m[1])
		}
		family, tag := m[2], ""
		if i := strings.LastIndex(m[2], ":"); i >= 0 {
			family, tag = m[2][:i], m[2][i+1:]
		}
		if !families[family] {
			g.failf("%s's default image %q does not name a family the release workflow publishes", name, m[2])
		}
		if tag != "local" {
			g.failf("%s's default must be the :local tag no registry publishes, got %q", name, m[2])
		}
	}
	g.expect(found > 0, "expected compose.yml to declare buildable services")
}

func ccfgInitDBsOnlyViaPostgres(g *guard) {
	services := ccfgServices(g)
	var mounting []string
	for _, name := range ccfgKeys(services) {
		for _, v := range list(mp(services[name])["volumes"]) {
			if strings.Contains(fmt.Sprint(v), "init-extra-dbs.sh") {
				mounting = append(mounting, name)
				break
			}
		}
	}
	g.expect(equal(mounting, []string{"postgres"}), "init-extra-dbs.sh must be mounted by exactly the postgres service, found: %v", mounting)
	initdb := false
	for _, v := range list(at(services, "postgres", "volumes")) {
		if strings.Contains(fmt.Sprint(v), "init-extra-dbs.sh:/docker-entrypoint-initdb.d/") {
			initdb = true
		}
	}
	g.expect(initdb, "init-extra-dbs.sh must be mounted into /docker-entrypoint-initdb.d/ (initdb-only)")
	for _, name := range ccfgKeys(services) {
		if name == "postgres" {
			continue
		}
		for _, field := range []string{"volumes", "entrypoint", "command"} {
			if v := mp(services[name])[field]; v != nil && strings.Contains(fmt.Sprint(v), "init-extra-dbs.sh") {
				g.failf("service %s references init-extra-dbs.sh in its %s: it must only be reachable through the postgres initdb mount", name, field)
			}
		}
	}
}

func ccfgProjectName(g *guard) {
	name := str(g.yamlFile(ccfgCompose)["name"])
	g.expect(name != "dev-health", "compose project name must not be 'dev-health' (CHAOS-3142 collision with the repo-root project)")
	g.expect(name != "", "compose.yml must declare an explicit project name")
}

func ccfgPostgresPinnedPGDATA(g *guard) {
	pg := ccfgService(g, ccfgServices(g), "postgres")
	image := str(pg["image"])
	tag, _, pinned := strings.Cut(image, "@sha256:")
	g.expect(pinned, "postgres image must be pinned by digest, not a moving tag: %q", image)
	g.expect(!strings.HasSuffix(tag, ":latest"), "postgres image tag must not be 'latest', even when digest-pinned: %q", image)
	env := ccfgEnv(pg)
	g.expect(!ccfgHas(env, "PG_DATA"), "PG_DATA is not read by the postgres image (typo for PGDATA)")
	if !ccfgHas(env, "PGDATA") {
		g.failf("PGDATA must be set explicitly so the cluster path is pinned")
		return
	}
	mountRoot := ""
	for _, v := range strs(pg["volumes"]) {
		if after, ok := strings.CutPrefix(v, "postgres_data:"); ok {
			mountRoot = after
			break
		}
	}
	if mountRoot == "" {
		g.failf("postgres has no postgres_data: volume mount")
		return
	}
	root := strings.TrimRight(mountRoot, "/")
	pgdata := strings.TrimRight(str(env["PGDATA"]), "/")
	g.expect(pgdata != root, "PGDATA must be a subdirectory of the volume mount, not the mount root itself")
	g.expect(strings.HasPrefix(pgdata, root+"/"), "PGDATA %q must be nested under the volume mount %q", env["PGDATA"], mountRoot)
}

func ccfgComposeNoRouteSwitches(g *guard) {
	services := ccfgServices(g)
	for _, name := range ccfgKeys(services) {
		if !strings.HasPrefix(name, "go-") {
			continue
		}
		var matched []string
		for k := range ccfgEnv(mp(services[name])) {
			if loc := ccfgRouteSwitchRe.FindStringIndex(k); loc != nil && loc[0] == 0 && loc[1] == len(k) {
				matched = append(matched, k)
			}
		}
		sort.Strings(matched)
		g.expect(len(matched) == 0, "%s still declares route switches: %v", name, matched)
	}
}

var ccfgOneShotSetup = map[string]bool{
	"go-river-provision":               true,
	"go-river-migrate":                 true,
	"go-contractcheck":                 true,
	"go-sync-dispatch-route-activate":  true,
	"go-sync-finalize-route-activate":  true,
	"go-sync-post-route-activate":      true,
	"go-sync-reference-route-activate": true,
}

func ccfgNoGoDependsOnMigrate(g *guard) {
	services := ccfgServices(g)
	n := 0
	for _, name := range ccfgKeys(services) {
		if !strings.HasPrefix(name, "go-") || ccfgOneShotSetup[name] {
			continue
		}
		n++
		g.expect(!ccfgDeps(mp(services[name]))["migrate"], "%s must not depend on the `migrate` service: a long-running Go process never gates on it", name)
	}
	g.expect(n > 0, "compose.yml must define the go-* services it exists to carry")
}

func ccfgGoServicesUnconditional(g *guard) {
	services := ccfgServices(g)
	n := 0
	for _, name := range ccfgKeys(services) {
		if !strings.HasPrefix(name, "go-") {
			continue
		}
		n++
		g.expect(!ccfgHas(mp(services[name]), "profiles"), "%s must not declare profiles: the go-* fleet is the unconditional default", name)
	}
	g.expect(n > 0, "compose.yml must define the go-* services it exists to carry")
}

func ccfgOneReplica(g *guard) {
	services := ccfgServices(g)
	for _, process := range ccfgKeys(ccfgServiceByProcess) {
		name := ccfgServiceByProcess[process]
		v := at(ccfgService(g, services, name), "deploy", "replicas")
		if n, ok := ccfgInt(v); !ok || n != 1 {
			g.failf("%s (registry process %q) must default to deploy.replicas: 1 (an int, not a bool), got %#v", name, process, v)
		}
	}
}

func ccfgNoPullPolicy(g *guard) {
	services := ccfgServices(g)
	names := append(ccfgSortedValues(ccfgServiceByProcess), "go-river-provision", "go-river-migrate")
	for _, name := range names {
		g.expect(!ccfgHas(ccfgService(g, services, name), "pull_policy"), "%s must not override pull_policy: Compose's default lets an *_IMAGE pin be used", name)
	}
}

func ccfgProvisionIdentity(g *guard) {
	services := ccfgServices(g)
	want := func(svc, key, value string) {
		got := str(ccfgEnv(ccfgService(g, services, svc))[key])
		g.expect(got == value, "%s %s = %q, want %q", svc, key, got, value)
	}
	want("postgres", "POSTGRES_USER", "${POSTGRES_USER:-postgres}")
	want("postgres", "POSTGRES_PASSWORD", "${POSTGRES_PASSWORD:-postgres}")
	want("postgres", "POSTGRES_DB", "${POSTGRES_DB:-postgres}")
	for _, svc := range []string{"go-river-provision", "go-river-migrate"} {
		want(svc, "DEV_HEALTH_MIGRATION_PG_HOST", "${POSTGRES_HOST:-postgres}")
		want(svc, "DEV_HEALTH_MIGRATION_PG_USER", "${POSTGRES_USER:-postgres}")
		want(svc, "DEV_HEALTH_MIGRATION_PG_PASSWORD", "${POSTGRES_PASSWORD:-postgres}")
		want(svc, "DEV_HEALTH_MIGRATION_PG_DB", "${POSTGRES_DB:-postgres}")
	}
}

func ccfgManifestQueues(g *guard) {
	services := ccfgServices(g)
	procs := ccfgProcessByName(g)
	for _, group := range []string{"heavy", "ops", "sync", "sync-provider"} {
		name := ccfgServiceByProcess[group]
		svc := ccfgService(g, services, name)
		env := ccfgEnv(svc)
		for _, k := range []string{"DEV_HEALTH_PROFILE", "DEV_HEALTH_QUEUES", "DEV_HEALTH_QUEUE_CONCURRENCY", "DEV_HEALTH_WORKER_GROUP"} {
			g.expect(!ccfgHas(env, k), "%s must not set %s: queues are flags from deployment.json", name, k)
		}
		g.expect(str(at(svc, "command", 0)) == "worker", "%s command must start with worker", name)
		args := ccfgWorkerArgs(g, name, svc)
		proc := procs[group]
		queues := strings.Join(strs(proc["queues"]), ",")
		var conc []string
		for _, qw := range list(proc["queue_workers"]) {
			conc = append(conc, fmt.Sprintf("%s=%v", str(at(qw, "queue")), at(qw, "max_workers")))
		}
		g.expect(args["--queues"] == queues, "%s --queues=%s, want %s", name, args["--queues"], queues)
		g.expect(args["--queue-concurrency"] == strings.Join(conc, ","), "%s --queue-concurrency=%s, want %s", name, args["--queue-concurrency"], strings.Join(conc, ","))
		g.expect(args["--worker-group"] == group, "%s --worker-group=%s, want %s", name, args["--worker-group"], group)
		g.expect(!ccfgHas(svc, "profiles"), "%s must not declare profiles", name)
	}
}

func ccfgNoBillingEdgeService(g *guard) {
	services := ccfgServices(g)
	g.expect(!ccfgHas(services, "billing-edge"), "compose.yml defines billing-edge")
	var running []string
	for _, name := range ccfgKeys(services) {
		svc := mp(services[name])
		if strings.Contains(fmt.Sprint(svc["command"]), "billing_edge") || strings.Contains(fmt.Sprint(svc["entrypoint"]), "billing_edge") {
			running = append(running, name)
		}
	}
	g.expect(len(running) == 0, "compose.yml: %v run the deleted billing_edge module", running)
}

var ccfgDurationRe = regexp.MustCompile(`^(\d+(?:\.\d+)?)(ms|s|m|h)$`)

func ccfgReconcilerHealthcheck(g *guard) {
	services := ccfgServices(g)
	hc, ok := ccfgService(g, services, "go-reconciler")["healthcheck"].(map[string]any)
	if !ok {
		g.failf("go-reconciler must declare a healthcheck")
		return
	}
	test := strs(hc["test"])
	if len(test) == 0 || test[0] != "CMD" {
		g.failf("go-reconciler healthcheck must use exec ('CMD', ...) form, never CMD-SHELL: the image is distroless, got %v", hc["test"])
	} else if !equal(test, []string{"CMD", "/usr/local/bin/dho", "reconciler", "healthcheck"}) {
		g.failf("go-reconciler healthcheck must invoke `dho reconciler healthcheck`, got %v", test)
	}
	keys := ccfgKeys(hc)
	g.expect(equal(keys, []string{"interval", "retries", "start_period", "test", "timeout"}), "go-reconciler healthcheck keys = %v, want test/interval/timeout/retries/start_period", keys)
	for _, k := range []string{"interval", "timeout", "start_period"} {
		m := ccfgDurationRe.FindStringSubmatch(str(hc[k]))
		var f float64
		if m != nil {
			f, _ = strconv.ParseFloat(m[1], 64)
		}
		g.expect(m != nil && f > 0, "go-reconciler healthcheck %s=%v must be a positive Compose duration", k, hc[k])
	}
	if n, ok := ccfgInt(hc["retries"]); !ok || n <= 0 {
		g.failf("go-reconciler healthcheck retries=%#v must be a positive integer, not a bool", hc["retries"])
	}
	for _, name := range ccfgKeys(services) {
		if name == "go-reconciler" || name == "go-api" || !strings.HasPrefix(name, "go-") {
			continue
		}
		g.expect(!ccfgHas(mp(services[name]), "healthcheck"), "%s declares a healthcheck too: only go-reconciler and go-api may in the go-* fleet", name)
	}
}

func ccfgOperatorCommand(g *guard) {
	services := ccfgServices(g)
	kinds := map[string]bool{"dispatch_sync_run": true, "finalize_sync_run": true, "post_sync": true, "reference_discovery": true}
	n := 0
	for _, name := range ccfgKeys(services) {
		svc := mp(services[name])
		if str(at(svc, "build", "target")) != "operator" {
			continue
		}
		n++
		cmd, ok := svc["command"].([]any)
		if !ok || len(cmd) == 0 {
			g.failf("%s builds the distroless 'operator' target but declares no non-empty command: list", name)
			continue
		}
		c := strs(cmd)
		switch name {
		case "go-river-migrate":
			g.expect(equal(c, []string{"migrate", "river", "--apply-and-check"}), "go-river-migrate command = %v, want [migrate river --apply-and-check]", c)
			continue
		case "go-river-provision":
			g.expect(equal(c, []string{"migrate", "roles"}), "go-river-provision command = %v, want [migrate roles]", c)
			continue
		case "go-api":
			g.expect(c[0] == "api", "go-api command must start with api, got %v", c)
			g.expect(ccfgContains(c, "--api-billing-edge-addr=:8010"), "go-api command must carry --api-billing-edge-addr=:8010, got %v", c)
			continue
		}
		if len(c) < 3 || !equal(c[:3], []string{"workers", "routes", "apply"}) {
			g.failf("%s command %v no longer starts with [workers routes apply]", name, c)
			continue
		}
		if len(c) != 8 || c[3] != "--reason" || c[5] != "--correlation-id" {
			g.failf("%s command %v is missing --reason/--correlation-id or has extra/missing arguments", name, c)
			continue
		}
		g.expect(kinds[c[7]], "%s command ends in %q, not one of the four canonical route kinds", name, c[7])
	}
	g.expect(n > 0, "expected services that build the 'operator' target, found none")
}

func ccfgRegistryMatchesMap(g *guard) {
	var names []string
	for _, p := range ccfgProcesses(g) {
		names = append(names, str(p["name"]))
	}
	sort.Strings(names)
	g.expect(equal(names, ccfgKeys(ccfgServiceByProcess)), "deployment.json processes %v do not match the compose service map keys %v", names, ccfgKeys(ccfgServiceByProcess))
}

func ccfgComposeOrderingContract(g *guard) {
	services := ccfgServices(g)
	for _, p := range ccfgProcesses(g) {
		process := str(p["name"])
		name, ok := ccfgServiceByProcess[process]
		if !ok {
			g.failf("registry process %q has no compose service mapping", process)
			continue
		}
		g.expect(ccfgHas(ccfgEnv(ccfgService(g, services, name)), "OPERATIONAL_ORDERING_CONTRACT"),
			"compose.yml:%s (registry process %q) is missing OPERATIONAL_ORDERING_CONTRACT", name, process)
	}
}

var ccfgProvenanceArgs = []string{"COMMIT", "BUILD_TIME"}

var ccfgDockerArgRe = regexp.MustCompile(`(?m)^ARG\s+([A-Za-z_][A-Za-z0-9_]*)\s*=\s*"?([^"\n]*)"?\s*$`)

func ccfgBuildServices(g *guard) (doc, []string) {
	services := ccfgServices(g)
	var names []string
	for _, name := range ccfgKeys(services) {
		if ccfgHas(mp(services[name]), "build") {
			names = append(names, name)
		}
	}
	if len(names) == 0 {
		g.failf("no build services found in compose.yml")
	}
	return services, names
}

func ccfgProvenancePassed(g *guard) {
	services, names := ccfgBuildServices(g)
	for _, name := range names {
		args := mp(at(services, name, "build", "args"))
		for _, k := range ccfgProvenanceArgs {
			g.expect(ccfgHas(args, k), "compose.yml build service %s is missing provenance build arg %s (image label would read revision=unknown)", name, k)
		}
	}
}

// Compared with the Dockerfile each service builds: that is the ARG its compose build arg feeds.
func ccfgProvenanceDefaults(g *guard) {
	services, names := ccfgBuildServices(g)
	parsed := map[string]map[string][]string{}
	for _, name := range names {
		build := mp(at(services, name, "build"))
		dockerfile := str(build["dockerfile"])
		if dockerfile == "" {
			dockerfile = "Dockerfile"
		}
		rel := path.Clean(path.Join(str(build["context"]), dockerfile))
		defaults, ok := parsed[rel]
		if !ok {
			defaults = map[string][]string{}
			for _, m := range ccfgDockerArgRe.FindAllStringSubmatch(g.read(rel), -1) {
				defaults[m[1]] = append(defaults[m[1]], m[2])
			}
			parsed[rel] = defaults
		}
		args := mp(build["args"])
		for _, k := range ccfgProvenanceArgs {
			if len(defaults[k]) == 0 {
				g.failf("%s no longer declares ARG %s; %s's build arg has nothing to match", rel, k, name)
				continue
			}
			if !ccfgHas(args, k) {
				continue
			}
			for _, d := range defaults[k] {
				if c := ccfgComposeDefault(args[k]); c != d {
					g.failf("%s.%s compose default %q disagrees with %s ARG default %q", name, k, c, rel, d)
				}
			}
		}
	}
}

func ccfgGoAPIBillingEdge(g *guard) {
	services := ccfgServices(g)
	api := ccfgService(g, services, "go-api")
	operator := ccfgService(g, services, "go-river-migrate")
	g.expect(str(api["image"]) == str(operator["image"]), "go-api must reuse the operator image: %v vs %v", api["image"], operator["image"])
	g.expect(str(at(api, "build", "target")) == "operator", "go-api must build the operator target, got %v", at(api, "build", "target"))
	c := strs(api["command"])
	g.expect(len(c) > 0 && c[0] == "api", "go-api command must start with api, got %v", c)
	g.expect(ccfgContains(c, "--api-billing-edge-addr=:8010"), "go-api command must carry --api-billing-edge-addr=:8010, got %v", c)
	ports := strs(api["ports"])
	g.expect(ccfgContains(ports, "8010:8010"), "go-api must publish 8010:8010, got %v", ports)
	for _, p := range ports {
		g.expect(!strings.HasSuffix(p, ":8000"), "go-api must not publish the api listener (router owns host port 8000): %v", ports)
	}
	for _, dep := range []string{"go-river-provision", "go-river-migrate"} {
		g.expect(ccfgCondition(api, dep) == "service_completed_successfully", "go-api must wait for %s to complete successfully, got %q", dep, ccfgCondition(api, dep))
	}
	g.expect(!ccfgDeps(api)["migrate"], "go-api depends on migrate: a Go service never gates on the migrate service")
	env := ccfgEnv(api)
	g.expect(!ccfgHas(env, "API_DATABASE_URI"), "go-api sets API_DATABASE_URI: component form only, never both forms")
	for _, k := range []string{"DEV_HEALTH_PG_API_HOST", "DEV_HEALTH_PG_API_USER", "DEV_HEALTH_PG_API_PASSWORD", "DEV_HEALTH_PG_DB", "VALKEY_URI", "JWT_SECRET_KEY", "STRIPE_SECRET_KEY", "STRIPE_WEBHOOK_SECRET", "LICENSE_PRIVATE_KEY"} {
		g.expect(ccfgHas(env, k), "go-api environment is missing %s", k)
	}
}

func ccfgAPIRoleProvisioned(g *guard) {
	services := ccfgServices(g)
	prov := ccfgService(g, services, "go-river-provision")
	g.expect(equal(strs(prov["command"]), []string{"migrate", "roles"}), "go-river-provision command = %v, want [migrate roles]", prov["command"])
	env, api := ccfgEnv(prov), ccfgEnv(ccfgService(g, services, "go-api"))
	g.expect(str(env["API_DATABASE_ROLE"]) == str(api["API_DATABASE_ROLE"]), "API_DATABASE_ROLE differs: provision %v, go-api %v", env["API_DATABASE_ROLE"], api["API_DATABASE_ROLE"])
	g.expect(str(env["API_DATABASE_ROLE"]) == str(api["DEV_HEALTH_PG_API_USER"]), "provisioned API_DATABASE_ROLE %v is not the go-api login DEV_HEALTH_PG_API_USER %v", env["API_DATABASE_ROLE"], api["DEV_HEALTH_PG_API_USER"])
	g.expect(str(env["API_DATABASE_PASSWORD"]) == str(api["DEV_HEALTH_PG_API_PASSWORD"]), "provisioned API_DATABASE_PASSWORD %v is not the go-api DEV_HEALTH_PG_API_PASSWORD %v", env["API_DATABASE_PASSWORD"], api["DEV_HEALTH_PG_API_PASSWORD"])
}

func ccfgHostPortsOnce(g *guard) {
	services := ccfgServices(g)
	published := map[string]string{}
	for _, name := range ccfgKeys(services) {
		for _, p := range list(mp(services[name])["ports"]) {
			parts := strings.Split(fmt.Sprint(p), ":")
			if len(parts) < 2 {
				continue
			}
			host, _, _ := strings.Cut(parts[len(parts)-2], "-")
			if prev, ok := published[host]; ok {
				g.failf("host port %s is published by both %s and %s", host, prev, name)
				continue
			}
			published[host] = name
		}
	}
}

func ccfgKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func ccfgSortedValues(m map[string]string) []string {
	var out []string
	for _, k := range ccfgKeys(m) {
		out = append(out, m[k])
	}
	return out
}

func ccfgContains(l []string, s string) bool {
	for _, x := range l {
		if x == s {
			return true
		}
	}
	return false
}

var ccfgComposeChecks = []check{
	{"drain_contract_matches_groups", ccfgDrainContract},
	{"legacy_compose_has_one_shot_migrate_service", ccfgMigrateOneShot},
	{"legacy_compose_migrate_waits_for_postgres_health", ccfgMigrateWaitsForHealth},
	{"every_buildable_service_declares_an_overridable_image", ccfgOverridableImage},
	{"init_extra_dbs_sh_is_only_reachable_through_postgres_container_init", ccfgInitDBsOnlyViaPostgres},
	{"ops_compose_project_name_is_not_dev_health", ccfgProjectName},
	{"local_postgres_image_pinned_and_pgdata_is_subdirectory", ccfgPostgresPinnedPGDATA},
	{"compose_declares_no_provider_route_switches", ccfgComposeNoRouteSwitches},
	{"go_profile_overlay_never_depends_on_python_migrate", ccfgNoGoDependsOnMigrate},
	{"go_services_are_unconditional", ccfgGoServicesUnconditional},
	{"go_workers_run_at_one_replica_by_default", ccfgOneReplica},
	{"go_worker_family_has_no_pull_policy_override", ccfgNoPullPolicy},
	{"go_river_provision_chain_uses_this_files_postgres_identity", ccfgProvisionIdentity},
	{"go_workers_select_manifest_queues_without_runtime_profile", ccfgManifestQueues},
	{"no_compose_file_defines_a_python_billing_edge_service", ccfgNoBillingEdgeService},
	{"go_reconciler_declares_a_readyz_healthcheck", ccfgReconcilerHealthcheck},
	{"go_operator_target_services_declare_a_nonempty_command", ccfgOperatorCommand},
	{"go_worker_process_registry_matches_ordering_contract_coverage_maps", ccfgRegistryMatchesMap},
	{"compose_go_workers_wire_operational_ordering_contract", ccfgComposeOrderingContract},
	{"every_compose_build_service_passes_commit_and_build_time", ccfgProvenancePassed},
	{"compose_provenance_defaults_match_the_dockerfile_arg_defaults", ccfgProvenanceDefaults},
	{"go_api_serves_the_billing_edge_on_the_operator_image", ccfgGoAPIBillingEdge},
	{"go_river_provision_creates_the_api_role_the_go_api_connects_as", ccfgAPIRoleProvisioned},
	{"root_compose_publishes_each_host_port_once", ccfgHostPortsOnce},
}

func TestComposeCfgComposeRealTree(t *testing.T) { runReal(t, ccfgComposeChecks...) }

func ccfgComposeCase(name string, check func(*guard), want string, mutate func(*testing.T, string)) planted {
	return planted{name: name, copy: ccfgComposeFiles, mutate: mutate, check: check, want: want}
}

func TestComposeCfgComposePlanted(t *testing.T) {
	const c = ccfgCompose
	runPlanted(t, []planted{
		ccfgComposeCase("drain_shutdown_timeout_drifts", ccfgDrainContract, "go-worker-ops: --shutdown-timeout=961s",
			mut(c, "--shutdown-timeout=960s", "--shutdown-timeout=961s")),
		ccfgComposeCase("drain_stop_grace_drifts", ccfgDrainContract, "go-worker-heavy: stop_grace_period=7200s",
			mut(c, "stop_grace_period: 7260s", "stop_grace_period: 7200s")),
		ccfgComposeCase("migrate_restarts", ccfgMigrateOneShot, "migrate must be one-shot",
			ccfgMutAfter(c, "\n  migrate:\n", `restart: "no"`, "restart: always")),
		ccfgComposeCase("migrate_starts_before_postgres_healthy", ccfgMigrateWaitsForHealth, "wait for postgres",
			ccfgMutAfter(c, "\n  migrate:\n", "      postgres:\n        condition: service_healthy", "      postgres:\n        condition: service_started")),
		ccfgComposeCase("buildable_image_moving_tag", ccfgOverridableImage, "migrate's default must be the :local tag",
			mut(c, "dev-health-go-dho:local}", "dev-health-go-dho:main}")),
		ccfgComposeCase("buildable_without_image", ccfgOverridableImage, "go-contractcheck declares build: with no image:",
			mut(c, "    image: ${DEV_HEALTH_GO_CONTRACTCHECK_IMAGE:-ghcr.io/full-chaos/dev-health-go-contractcheck:local}\n", "")),
		ccfgComposeCase("buildable_unpublished_family", ccfgOverridableImage, "does not name a family",
			mut(c, "ghcr.io/full-chaos/dev-health-go-contractcheck:local}", "ghcr.io/someone/contractcheck:local}")),
		ccfgComposeCase("init_dbs_mounted_by_second_service", ccfgInitDBsOnlyViaPostgres, "found: [clickhouse postgres]",
			mut(c, "      - clickhouse_data:/var/lib/clickhouse\n", "      - clickhouse_data:/var/lib/clickhouse\n      - ./docker/init-extra-dbs.sh:/init.sh:ro\n")),
		ccfgComposeCase("init_dbs_run_by_command", ccfgInitDBsOnlyViaPostgres, "service migrate references init-extra-dbs.sh in its command",
			mut(c, `command: ["migrate", "upgrade", "--river"]`, `command: ["/bin/sh", "/docker/init-extra-dbs.sh"]`)),
		ccfgComposeCase("init_dbs_not_in_initdb", ccfgInitDBsOnlyViaPostgres, "/docker-entrypoint-initdb.d/",
			mut(c, "init-extra-dbs.sh:/docker-entrypoint-initdb.d/init-extra-dbs.sh:ro", "init-extra-dbs.sh:/usr/local/bin/init-extra-dbs.sh:ro")),
		ccfgComposeCase("project_name_dev_health", ccfgProjectName, "must not be 'dev-health'",
			mut(c, "\nname: dev-health-ops\n", "\nname: dev-health\n")),
		ccfgComposeCase("project_name_missing", ccfgProjectName, "explicit project name",
			mut(c, "\nname: dev-health-ops\n", "\n")),
		ccfgComposeCase("postgres_latest_tag", ccfgPostgresPinnedPGDATA, "must not be 'latest'",
			mut(c, "image: postgres:18-alpine@sha256:", "image: postgres:latest@sha256:")),
		ccfgComposeCase("postgres_no_digest", ccfgPostgresPinnedPGDATA, "pinned by digest",
			mut(c, "image: postgres:18-alpine@sha256:9a8afca54e7861fd90fab5fdf4c42477a6b1cb7d293595148e674e0a3181de15\n    container_name: postgres", "image: postgres:18-alpine\n    container_name: postgres")),
		ccfgComposeCase("postgres_pg_data_typo", ccfgPostgresPinnedPGDATA, "PG_DATA is not read",
			mut(c, "PGDATA: /var/lib/postgresql/pgdata", "PG_DATA: /var/lib/postgresql/")),
		ccfgComposeCase("postgres_pgdata_mount_root", ccfgPostgresPinnedPGDATA, "not the mount root",
			mut(c, "PGDATA: /var/lib/postgresql/pgdata", "PGDATA: /var/lib/postgresql/")),
		ccfgComposeCase("route_switch_on_go_fleet", ccfgComposeNoRouteSwitches, "WORKER_GITHUB_SYNC_ENABLED",
			mut(c, "\n  AUTO_RUN_MIGRATIONS: \"false\"\n", "\n  AUTO_RUN_MIGRATIONS: \"false\"\n  WORKER_GITHUB_SYNC_ENABLED: \"true\"\n")),
		ccfgComposeCase("long_running_go_depends_on_migrate", ccfgNoGoDependsOnMigrate, "go-worker-heavy must not depend on the `migrate` service",
			mut(c, "    depends_on:\n      go-contractcheck:\n", "    depends_on:\n      migrate:\n        condition: service_completed_successfully\n      go-contractcheck:\n")),
		ccfgComposeCase("go_service_profile_gated", ccfgGoServicesUnconditional, "go-worker-sync must not declare profiles",
			mut(c, "  go-worker-sync:\n    <<: *go-worker\n", "  go-worker-sync:\n    <<: *go-worker\n    profiles: [go]\n")),
		ccfgComposeCase("replicas_bool", ccfgOneReplica, "an int, not a bool), got true",
			mut(c, "      replicas: 1\n", "      replicas: true\n")),
		ccfgComposeCase("replicas_zero", ccfgOneReplica, "go-reconciler (registry process \"reconciler\") must default to deploy.replicas: 1",
			mut(c, "      replicas: 1\n", "      replicas: 0\n")),
		ccfgComposeCase("pull_policy_build", ccfgNoPullPolicy, "go-worker-sync must not override pull_policy",
			mut(c, "  go-worker-sync:\n    <<: *go-worker\n", "  go-worker-sync:\n    <<: *go-worker\n    pull_policy: build\n")),
		ccfgComposeCase("provision_user_devhealth", ccfgProvisionIdentity, "go-river-provision DEV_HEALTH_MIGRATION_PG_USER",
			mut(c, "      DEV_HEALTH_MIGRATION_PG_USER: ${POSTGRES_USER:-postgres}", "      DEV_HEALTH_MIGRATION_PG_USER: ${POSTGRES_USER:-devhealth}")),
		ccfgComposeCase("postgres_db_hardcoded", ccfgProvisionIdentity, "postgres POSTGRES_DB",
			mut(c, "      POSTGRES_DB: ${POSTGRES_DB:-postgres}", "      POSTGRES_DB: postgres")),
		ccfgComposeCase("queue_concurrency_drifts", ccfgManifestQueues, "go-worker-sync --queue-concurrency=sync=5",
			mut(c, "--queue-concurrency=sync=4", "--queue-concurrency=sync=5")),
		ccfgComposeCase("worker_group_drifts", ccfgManifestQueues, "go-worker-ops --worker-group=operations",
			mut(c, "--worker-group=ops\n", "--worker-group=operations\n")),
		ccfgComposeCase("billing_edge_service", ccfgNoBillingEdgeService, "compose.yml defines billing-edge",
			mut(c, "\n  bugsink:\n", "\n  billing-edge:\n")),
		ccfgComposeCase("billing_edge_module_run", ccfgNoBillingEdgeService, "[go-api] run the deleted billing_edge module",
			mut(c, "      - api\n", "      - api\n      - --module=billing_edge\n")),
		ccfgComposeCase("reconciler_healthcheck_shell", ccfgReconcilerHealthcheck, "never CMD-SHELL",
			ccfgMutAfter(c, "\n  go-reconciler:\n", `test: ["CMD", "/usr/local/bin/dho", "reconciler", "healthcheck"]`, `test: ["CMD-SHELL", "wget -q http://localhost:8080/readyz"]`)),
		ccfgComposeCase("reconciler_healthcheck_wrong_binary", ccfgReconcilerHealthcheck, "must invoke `dho reconciler healthcheck`",
			ccfgMutAfter(c, "\n  go-reconciler:\n", `"reconciler", "healthcheck"]`, `"healthcheck"]`)),
		ccfgComposeCase("reconciler_healthcheck_negative_start", ccfgReconcilerHealthcheck, "start_period=-1s must be a positive",
			ccfgMutAfter(c, "\n  go-reconciler:\n", "start_period: 20s", "start_period: -1s")),
		ccfgComposeCase("reconciler_healthcheck_bool_retries", ccfgReconcilerHealthcheck, "retries=true must be a positive integer",
			ccfgMutAfter(c, "\n  go-reconciler:\n", "      retries: 3\n", "      retries: true\n")),
		ccfgComposeCase("reconciler_healthcheck_extra_key", ccfgReconcilerHealthcheck, "healthcheck keys",
			ccfgMutAfter(c, "\n  go-reconciler:\n", "      retries: 3\n", "      retries: 3\n      disable: false\n")),
		ccfgComposeCase("second_go_healthcheck", ccfgReconcilerHealthcheck, "go-scheduler declares a healthcheck too",
			ccfgMutAfter(c, "\n  go-scheduler:\n", "    stop_grace_period: 60s\n", "    stop_grace_period: 60s\n    healthcheck:\n      test: [\"CMD\", \"/usr/local/bin/dho\", \"reconciler\", \"healthcheck\"]\n")),
		ccfgComposeCase("route_activate_missing_correlation", ccfgOperatorCommand, "go-sync-post-route-activate command",
			mut(c, "      - --correlation-id\n      - local-compose-post-sync\n", "")),
		ccfgComposeCase("route_activate_unknown_kind", ccfgOperatorCommand, "ends in \"post_sync_v2\"",
			mut(c, "      - post_sync\n", "      - post_sync_v2\n")),
		ccfgComposeCase("route_activate_empty_command", ccfgOperatorCommand, "go-sync-dispatch-route-activate builds the distroless 'operator' target but declares no non-empty command",
			mut(c, "    command:\n      - workers\n      - routes\n      - apply\n      - --reason\n      - local_compose_start\n      - --correlation-id\n      - local-compose-dispatch-sync-run\n      - dispatch_sync_run\n", "")),
		ccfgComposeCase("river_migrate_argv_drifts", ccfgOperatorCommand, "go-river-migrate command",
			mut(c, "command: [migrate, river, --apply-and-check]", "command: [migrate, river]")),
		ccfgComposeCase("registry_process_renamed", ccfgRegistryMatchesMap, "do not match the compose service map",
			mut(ccfgManifest, `"name": "stream-pagerduty"`, `"name": "stream-pd"`)),
		ccfgComposeCase("ordering_contract_unwired", ccfgComposeOrderingContract, "go-worker-heavy (registry process \"heavy\") is missing OPERATIONAL_ORDERING_CONTRACT",
			mut(c, "\n  OPERATIONAL_ORDERING_CONTRACT: ${OPERATIONAL_ORDERING_CONTRACT:-2}\n", "\n")),
		ccfgComposeCase("provenance_build_time_missing", ccfgProvenancePassed, "go-api is missing provenance build arg BUILD_TIME",
			mut(c, "        BUILD_TIME: ${BUILD_TIME:-1970-01-01T00:00:00Z}\n", "")),
		{
			name:   "provenance_commit_default_drifts",
			copy:   append([]string{"docker/go-worker.Dockerfile"}, ccfgComposeFiles...),
			mutate: mut(c, "COMMIT: ${COMMIT:-unknown}", "COMMIT: ${COMMIT:-dev}"),
			check:  ccfgProvenanceDefaults,
			want:   `go-api.COMMIT compose default "dev"`,
		},
		{
			name:   "provenance_dockerfile_default_drifts",
			copy:   append([]string{"docker/go-worker.Dockerfile"}, ccfgComposeFiles...),
			mutate: mut("docker/go-worker.Dockerfile", `ARG COMMIT="unknown"`, `ARG COMMIT="none"`),
			check:  ccfgProvenanceDefaults,
			want:   `disagrees with docker/go-worker.Dockerfile ARG default "none"`,
		},
		ccfgComposeCase("go_api_port_moved", ccfgGoAPIBillingEdge, "go-api must publish 8010:8010",
			mut(c, `      - "8010:8010"`, `      - "8011:8010"`)),
		ccfgComposeCase("go_api_gates_on_migrate", ccfgGoAPIBillingEdge, "go-api depends on migrate",
			ccfgMutAfter(c, "\n  go-api:\n", "      go-river-provision:\n", "      migrate:\n        condition: service_completed_successfully\n      go-river-provision:\n")),
		ccfgComposeCase("go_api_uri_form", ccfgGoAPIBillingEdge, "go-api sets API_DATABASE_URI",
			mut(c, "      DEV_HEALTH_PG_API_HOST: postgres\n", "      DEV_HEALTH_PG_API_HOST: postgres\n      API_DATABASE_URI: postgresql://x@postgres/db\n")),
		ccfgComposeCase("api_password_mismatch", ccfgAPIRoleProvisioned, "API_DATABASE_PASSWORD",
			mut(c, "DEV_HEALTH_PG_API_PASSWORD: ${API_DATABASE_PASSWORD:-devhealth_api}", "DEV_HEALTH_PG_API_PASSWORD: ${API_DATABASE_PASSWORD:-other}")),
		ccfgComposeCase("api_login_not_provisioned_role", ccfgAPIRoleProvisioned, "is not the go-api login DEV_HEALTH_PG_API_USER",
			mut(c, "DEV_HEALTH_PG_API_USER: ${API_DATABASE_ROLE:-devhealth_api}", "DEV_HEALTH_PG_API_USER: devhealth_domain")),
		ccfgComposeCase("host_port_twice", ccfgHostPortsOnce, "host port 8010 is published by both",
			mut(c, `      - "8800:8000"`, `      - "8010:8000"`)),
	})
}
