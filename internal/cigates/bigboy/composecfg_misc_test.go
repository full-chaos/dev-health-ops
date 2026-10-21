package bigboy_test

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

const (
	ccfgMigrateJob = ccfgHelm + "/templates/migrate-job.yaml"
	ccfgGoWorkers  = ccfgHelm + "/templates/go-workers.yaml"
	ccfgHelpers    = ccfgHelm + "/templates/_helpers.tpl"
	ccfgHelmValues = ccfgHelm + "/values.yaml"
	ccfgEnvExample = ".env.example"
	ccfgConfigPkg  = "internal/platform/config"
)

var ccfgHelmFiles = []string{ccfgMigrateJob, ccfgGoWorkers, ccfgHelpers, ccfgHelmValues}

// Helm templates are Go-templated and helm is not run here, so template guards read text; values.yaml is parsed.
func ccfgHelmMigrationHook(g *guard) {
	tpl := g.read(ccfgMigrateJob)
	for _, frag := range []string{`helm.sh/hook: {{ join "," $hookEvents }}`, "helm.sh/hook-delete-policy: before-hook-creation,hook-succeeded"} {
		g.expect(strings.Contains(tpl, frag), "%s is missing %q", ccfgMigrateJob, frag)
	}
	helpers := g.read(ccfgHelpers)
	for _, frag := range []string{"AUTO_RUN_MIGRATIONS", `.Values.secrets.data "DATABASE_URI"`} {
		g.expect(strings.Contains(helpers, frag), "%s is missing %q", ccfgHelpers, frag)
	}
	hook := mp(at(g.yamlFile(ccfgHelmValues), "migrations", "hook"))
	g.expect(hook["enabled"] == true, "values.yaml migrations.hook.enabled must be true, got %#v", hook["enabled"])
	g.expect(equal(strs(hook["events"]), []string{"pre-install", "pre-upgrade"}), "values.yaml migrations.hook.events = %v, want [pre-install pre-upgrade]", hook["events"])
	g.expect(hook["localBundledPostgres"] == false, "values.yaml migrations.hook.localBundledPostgres must be false, got %#v", hook["localBundledPostgres"])
}

func ccfgHelmWorkersOrdering(g *guard) {
	tpl := g.read(ccfgGoWorkers)
	for _, frag := range []string{"name: OPERATIONAL_ORDERING_CONTRACT", `include "dev-health.operationalOrderingContract"`} {
		g.expect(strings.Contains(tpl, frag), "%s is missing %q", ccfgGoWorkers, frag)
	}
	g.expect(strings.Contains(g.read(ccfgHelpers), ".Values.goWorkers.operationalOrderingContract"), "%s no longer reads .Values.goWorkers.operationalOrderingContract", ccfgHelpers)
}

func ccfgMigrateOrdering(g *guard) {
	g.expect(ccfgHas(ccfgEnv(ccfgService(g, ccfgServices(g), "migrate")), "OPERATIONAL_ORDERING_CONTRACT"), "compose.yml:migrate is missing OPERATIONAL_ORDERING_CONTRACT")
	tpl := g.read(ccfgMigrateJob)
	g.expect(strings.Contains(tpl, "name: OPERATIONAL_ORDERING_CONTRACT"), "%s does not set OPERATIONAL_ORDERING_CONTRACT", ccfgMigrateJob)
	g.expect(strings.Contains(g.read(ccfgHelpers), ".Values.goWorkers.operationalOrderingContract"), "%s no longer reads .Values.goWorkers.operationalOrderingContract", ccfgHelpers)
	g.expect(strings.Contains(tpl, `include "dev-health.operationalOrderingContract"`), "helm migrate job should reuse the one operational ordering contract helper, not a second, driftable value")
}

func ccfgEnvExampleNoSwitches(g *guard) {
	lines := strings.Split(g.read(ccfgEnvExample), "\n")
	declared := map[string]bool{}
	var matched []string
	for _, l := range lines {
		l = strings.TrimSuffix(l, "\r")
		declared[l] = true
		if ccfgRouteSwitchRe.MatchString(l) {
			matched = append(matched, l)
		}
		g.expect(!strings.HasPrefix(l, "COMPOSE_PROFILES="), ".env.example must not activate the opt-in Go profile: %q", l)
	}
	g.expect(len(matched) == 0, ".env.example still mentions route switches: %v", matched)
	for _, name := range []string{"WORKER_GITHUB_WORK_ITEMS_STATUS_MAPPING_PATH", "WORKER_GITHUB_WORK_ITEMS_INVESTMENT_CONFIG_PATH"} {
		g.expect(declared[fmt.Sprintf(`# %s=""`, name)], ".env.example must inventory %s as a commented example", name)
		for _, l := range lines {
			g.expect(!strings.HasPrefix(l, name+"="), ".env.example must not set %s: %q", name, l)
		}
	}
}

func ccfgGoConfigNoSwitches(g *guard) {
	files, err := filepath.Glob(filepath.Join(g.path(ccfgConfigPkg), "*.go"))
	if err != nil || len(files) == 0 {
		g.failf("no Go sources under %s: %v", ccfgConfigPkg, err)
		return
	}
	set := map[string]bool{}
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		b, err := os.ReadFile(f)
		if err != nil {
			g.failf("read %s: %v", f, err)
			continue
		}
		for _, m := range ccfgRouteSwitchRe.FindAllString(string(b), -1) {
			set[m] = true
		}
	}
	g.expect(len(set) == 0, "Go typed config must declare no route switches, found: %v", ccfgKeys(set))
}

func ccfgOperatorCredentialLeftovers(compose doc) []string {
	var found []string
	services := mp(compose["services"])
	if ccfgHas(services, "go-worker-operator-credential") {
		found = append(found, "service go-worker-operator-credential")
	}
	if ccfgHas(mp(compose["volumes"]), "go_worker_operator_token") {
		found = append(found, "volume go_worker_operator_token")
	}
	for _, name := range ccfgKeys(services) {
		svc := mp(services[name])
		if ccfgDeps(svc)["go-worker-operator-credential"] {
			found = append(found, name+": depends_on go-worker-operator-credential")
		}
		for _, k := range ccfgKeys(ccfgEnv(svc)) {
			if strings.HasPrefix(k, "WORKER_OPERATOR_TOKEN") {
				found = append(found, name+": environment "+k)
			}
		}
		for _, v := range list(svc["volumes"]) {
			if strings.Contains(fmt.Sprint(v), "go_worker_operator_token") {
				found = append(found, fmt.Sprintf("%s: volume mount %v", name, v))
			}
		}
	}
	return found
}

// The platform files live above the repo only in the monorepo layout; compose.yml is always checked.
func ccfgNoOperatorCredential(g *guard) {
	docs := []string{g.path(ccfgCompose)}
	for dir := filepath.Dir(g.root); ; dir = filepath.Dir(dir) {
		if exists(filepath.Join(dir, "compose", "compose.go.workers.yml")) {
			docs = append(docs, filepath.Join(dir, "compose", "compose.go.workers.yml"))
			if exists(filepath.Join(dir, "compose.yml")) {
				docs = append(docs, filepath.Join(dir, "compose.yml"))
			}
			break
		}
		if filepath.Dir(dir) == dir {
			break
		}
	}
	for _, p := range docs {
		rel, err := filepath.Rel(g.root, p)
		if err != nil {
			rel = p
		}
		d := g.yamlFile(filepath.ToSlash(rel))
		for _, v := range ccfgOperatorCredentialLeftovers(d) {
			g.failf("%s: retired operator credential leftover: %s", rel, v)
		}
	}
}

func TestComposeCfgOperatorCredentialDetector(t *testing.T) {
	stale := doc{
		"volumes": doc{"go_worker_operator_token": doc{}},
		"services": doc{
			"go-worker-operator-credential": doc{},
			"go-worker": doc{
				"depends_on":  doc{"go-worker-operator-credential": doc{}},
				"environment": doc{"WORKER_OPERATOR_TOKEN_FILE": "/run/go-worker-operator/token"},
				"volumes":     []any{"go_worker_operator_token:/run/go-worker-operator:ro"},
			},
		},
	}
	got := ccfgOperatorCredentialLeftovers(stale)
	sort.Strings(got)
	want := []string{
		"go-worker: depends_on go-worker-operator-credential",
		"go-worker: environment WORKER_OPERATOR_TOKEN_FILE",
		"go-worker: volume mount go_worker_operator_token:/run/go-worker-operator:ro",
		"service go-worker-operator-credential",
		"volume go_worker_operator_token",
	}
	if !equal(got, want) {
		t.Fatalf("leftovers = %q, want %q", got, want)
	}
	if clean := ccfgOperatorCredentialLeftovers(doc{"services": doc{"go-worker": doc{}}}); len(clean) != 0 {
		t.Fatalf("clean document reported leftovers: %q", clean)
	}
}

func TestComposeCfgMiscRealTree(t *testing.T) {
	runReal(t,
		check{"helm_chart_runs_migrations_as_pre_upgrade_hook", ccfgHelmMigrationHook},
		check{"helm_go_workers_wire_operational_ordering_contract", ccfgHelmWorkersOrdering},
		check{"migrate_jobs_can_also_receive_operational_ordering_contract", ccfgMigrateOrdering},
		check{"provider_route_env_example_declares_no_route_switches", ccfgEnvExampleNoSwitches},
		check{"go_config_package_declares_no_route_switches", ccfgGoConfigNoSwitches},
		check{"platform_compose_has_no_unused_operator_credential", ccfgNoOperatorCredential},
	)
}

func TestComposeCfgMiscPlanted(t *testing.T) {
	helm := func(name string, check func(*guard), want string, m func(*testing.T, string)) planted {
		return planted{name: name, copy: append([]string{ccfgCompose}, ccfgHelmFiles...), mutate: m, check: check, want: want}
	}
	envCase := func(name, want string, m func(*testing.T, string)) planted {
		return planted{name: name, copy: []string{ccfgEnvExample}, mutate: m, check: ccfgEnvExampleNoSwitches, want: want}
	}
	cred := func(name, want string, m func(*testing.T, string)) planted {
		return planted{name: name, copy: []string{ccfgCompose}, mutate: m, check: ccfgNoOperatorCredential, want: want}
	}
	const c = ccfgCompose
	runPlanted(t, []planted{
		helm("hook_annotation_removed", ccfgHelmMigrationHook, "helm.sh/hook: {{ join",
			ccfgMutAll(ccfgMigrateJob, `helm.sh/hook: {{ join "," $hookEvents }}`, "helm.sh/hook: pre-install")),
		helm("hook_delete_policy_drifts", ccfgHelmMigrationHook, "hook-delete-policy",
			ccfgMutAll(ccfgMigrateJob, "helm.sh/hook-delete-policy: before-hook-creation,hook-succeeded", "helm.sh/hook-delete-policy: hook-succeeded")),
		helm("auto_run_migrations_helper_removed", ccfgHelmMigrationHook, `"AUTO_RUN_MIGRATIONS"`,
			ccfgMutAll(ccfgHelpers, "AUTO_RUN_MIGRATIONS", "AUTO_MIGRATE")),
		helm("hook_disabled", ccfgHelmMigrationHook, "migrations.hook.enabled must be true",
			ccfgMutAfter(ccfgHelmValues, "\nmigrations:\n  hook:\n", "    enabled: true\n", "    enabled: false\n")),
		helm("hook_post_install", ccfgHelmMigrationHook, "migrations.hook.events",
			ccfgMutAfter(ccfgHelmValues, "\nmigrations:\n  hook:\n", "      - pre-upgrade\n", "      - post-install\n")),
		helm("workers_ordering_env_removed", ccfgHelmWorkersOrdering, ccfgGoWorkers+` is missing "name: OPERATIONAL_ORDERING_CONTRACT"`,
			mut(ccfgGoWorkers, "name: OPERATIONAL_ORDERING_CONTRACT", "name: OPERATIONAL_ORDERING")),
		helm("workers_ordering_helper_bypassed", ccfgHelmWorkersOrdering, `include \"dev-health.operationalOrderingContract\"`,
			mut(ccfgGoWorkers, `{{ include "dev-health.operationalOrderingContract" $ | quote }}`, `"2"`)),
		helm("helper_ignores_knob", ccfgHelmWorkersOrdering, "no longer reads .Values.goWorkers.operationalOrderingContract",
			mut(ccfgHelpers, ".Values.goWorkers.operationalOrderingContract", ".Values.operationalOrderingContract")),
		helm("compose_migrate_ordering_unwired", ccfgMigrateOrdering, "compose.yml:migrate is missing OPERATIONAL_ORDERING_CONTRACT",
			mut(c, "      OPERATIONAL_ORDERING_CONTRACT: ${OPERATIONAL_ORDERING_CONTRACT:-2}\n", "")),
		helm("helm_migrate_second_value", ccfgMigrateOrdering, "not a second, driftable value",
			mut(ccfgMigrateJob, `{{ include "dev-health.operationalOrderingContract" . | quote }}`, `{{ .Values.migrations.operationalOrderingContract | quote }}`)),
		envCase("route_switch_listed", "still mentions route switches",
			mut(ccfgEnvExample, `# WORKER_GITHUB_WORK_ITEMS_STATUS_MAPPING_PATH=""`, "WORKER_JIRA_SYNC_ENABLED=\"true\"\n# WORKER_GITHUB_WORK_ITEMS_STATUS_MAPPING_PATH=\"\"")),
		envCase("route_config_set", "must not set WORKER_GITHUB_WORK_ITEMS_STATUS_MAPPING_PATH",
			mut(ccfgEnvExample, `# WORKER_GITHUB_WORK_ITEMS_STATUS_MAPPING_PATH=""`, "# WORKER_GITHUB_WORK_ITEMS_STATUS_MAPPING_PATH=\"\"\nWORKER_GITHUB_WORK_ITEMS_STATUS_MAPPING_PATH=/etc/map.yaml")),
		envCase("route_config_uninventoried", "must inventory WORKER_GITHUB_WORK_ITEMS_INVESTMENT_CONFIG_PATH",
			mut(ccfgEnvExample, `# WORKER_GITHUB_WORK_ITEMS_INVESTMENT_CONFIG_PATH=""`, "")),
		envCase("compose_profiles_activated", "must not activate the opt-in Go profile",
			mut(ccfgEnvExample, `# WORKER_GITHUB_WORK_ITEMS_STATUS_MAPPING_PATH=""`, "COMPOSE_PROFILES=go\n# WORKER_GITHUB_WORK_ITEMS_STATUS_MAPPING_PATH=\"\"")),
		{
			name:   "go_config_route_switch",
			copy:   []string{ccfgConfigPkg + "/config.go"},
			mutate: mut(ccfgConfigPkg+"/config.go", "package config\n", "package config\n\nconst ccfgPlanted = \"WORKER_GITHUB_SYNC_ENABLED\"\n"),
			check:  ccfgGoConfigNoSwitches,
			want:   "found: [WORKER_GITHUB_SYNC_ENABLED]",
		},
		cred("credential_service", "service go-worker-operator-credential",
			mut(c, "\n  bugsink:\n", "\n  go-worker-operator-credential:\n    image: busybox\n  bugsink:\n")),
		cred("credential_volume", "volume go_worker_operator_token",
			mut(c, "\nvolumes:\n  postgres_data:\n", "\nvolumes:\n  go_worker_operator_token:\n    driver: local\n  postgres_data:\n")),
		cred("credential_depends_on", "go-worker-sync: depends_on go-worker-operator-credential",
			ccfgMutAfter(c, "\n  go-worker-sync:\n", "    depends_on:\n", "    depends_on:\n      go-worker-operator-credential:\n        condition: service_completed_successfully\n")),
		cred("credential_env", "go-worker-heavy: environment WORKER_OPERATOR_TOKEN_FILE",
			mut(c, "\n  AUTO_RUN_MIGRATIONS: \"false\"\n", "\n  AUTO_RUN_MIGRATIONS: \"false\"\n  WORKER_OPERATOR_TOKEN_FILE: /run/go-worker-operator/token\n")),
		cred("credential_mount", "clickhouse: volume mount go_worker_operator_token",
			mut(c, "      - clickhouse_data:/var/lib/clickhouse\n", "      - clickhouse_data:/var/lib/clickhouse\n      - go_worker_operator_token:/run/go-worker-operator:ro\n")),
	})
}
