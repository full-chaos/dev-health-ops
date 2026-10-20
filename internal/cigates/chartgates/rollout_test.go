package chartgates_test

import (
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// Ported from tests/test_helm_rolling_update.py (CHAOS-6647), tests/test_helm_api_probes.py and
// tests/test_helm_migration_hook.py.

var (
	defaultRolling = doc{"maxUnavailable": 0, "maxSurge": 1}
	tightRolling   = doc{"maxUnavailable": 1, "maxSurge": 0}
)

func strategies(docs []doc) map[string]doc {
	out := map[string]doc{}
	for _, d := range ofKind(docs, "Deployment") {
		if strings.HasSuffix(name(d), "query-api") || strings.Contains(label(d, "app.kubernetes.io/component"), "go-worker") {
			out[name(d)] = mp(at(d, "spec", "strategy", "rollingUpdate"))
		}
	}
	return out
}

func rollingDefault(g *guard) {
	s := strategies(g.docs("t", set("queryApi.enabled=true")...))
	g.expect(len(s) >= 5, "expected query-api plus the go worker groups, got %d Deployments", len(s))
	for n, r := range s {
		g.expect(equal(r, defaultRolling), "default rollingUpdate of %s is %v, want %v", n, r, defaultRolling)
	}
}

func rollingQueryAPIValuesDriven(g *guard) {
	s := strategies(g.docs("t", join(set("queryApi.enabled=true"), setJSON(`queryApi.rollingUpdate={"maxUnavailable":1,"maxSurge":0}`))...))
	g.expect(equal(s["t-dev-health-query-api"], tightRolling), "queryApi.rollingUpdate was not honoured: got %v", s["t-dev-health-query-api"])
	for n, r := range s {
		if !strings.HasSuffix(n, "query-api") {
			g.expect(equal(r, defaultRolling), "queryApi.rollingUpdate leaked into %s: %v", n, r)
		}
	}
}

func rollingWorkersDefault(g *guard) {
	s := strategies(g.docs("t", join(set("queryApi.enabled=true"), setJSON(`goWorkers.rollingUpdate={"maxUnavailable":1,"maxSurge":0}`))...))
	n := 0
	for name, r := range s {
		if strings.HasSuffix(name, "query-api") {
			continue
		}
		n++
		g.expect(equal(r, tightRolling), "goWorkers.rollingUpdate was not honoured by %s: %v", name, r)
	}
	g.expect(n > 0, "no go worker Deployment rendered")
}

func rollingGroupWins(g *guard) {
	show := g.helm(nil, "show", "values", g.path(chartRel))
	if show.code != 0 {
		g.failf("helm show values failed: %s", firstLines(show.stderr, 2))
		return
	}
	var values doc
	if err := yaml.Unmarshal([]byte(show.stdout), &values); err != nil {
		g.failf("helm show values is not YAML: %v", err)
		return
	}
	groups := list(at(values, "goWorkers", "groups"))
	if len(groups) == 0 {
		g.failf("goWorkers.groups is empty")
		return
	}
	mp(groups[0])["rollingUpdate"] = doc{"maxUnavailable": 2, "maxSurge": 0}
	out, _ := yaml.Marshal(doc{"goWorkers": doc{"rollingUpdate": tightRolling, "groups": groups}})
	docs := g.docs("t", "-f", g.valuesFile(string(out)))
	var seenTwo, seenTight bool
	for _, d := range ofKind(docs, "Deployment") {
		if label(d, "app.kubernetes.io/component") != "go-worker" {
			continue
		}
		r := mp(at(d, "spec", "strategy", "rollingUpdate"))
		seenTwo = seenTwo || equal(r, doc{"maxUnavailable": 2, "maxSurge": 0})
		seenTight = seenTight || equal(r, tightRolling)
	}
	g.expect(seenTwo, "a group-level rollingUpdate did not win over goWorkers.rollingUpdate")
	g.expect(seenTight, "the other groups did not take the goWorkers.rollingUpdate default")
}

func rollingSchemaRejectsUnknownKey(g *guard) {
	g.refusal("an unknown rollingUpdate key", "t", setJSON(`goWorkers.rollingUpdate={"nonsense":1}`)...)
}

func apiProbesPortsUnique(g *guard) {
	r := g.mustRender("query-api probe contract", "probe-contract", join(set("queryApi.enabled=true"), []string{"--show-only", "templates/query-api-deployment.yaml"})...)
	docs, err := parseDocs(r.stdout)
	if err != nil {
		g.failf("output is not YAML: %v", err)
		return
	}
	deps := ofKind(docs, "Deployment")
	if len(deps) == 0 {
		g.failf("no Deployment rendered from templates/query-api-deployment.yaml")
		return
	}
	cs := containers(deps[0])
	if len(cs) == 0 {
		g.failf("the query-api Deployment has no container")
		return
	}
	ports := list(cs[0]["ports"])
	seen := map[string]bool{}
	var names []string
	for _, p := range ports {
		proto := str(at(p, "protocol"))
		if proto == "" {
			proto = "TCP"
		}
		k := str(at(p, "containerPort")) + "/" + proto
		g.expect(!seen[k], "query-api container ports must be unique on (containerPort, protocol): %s repeats; server-side apply keys the ports list on that pair and rejects a duplicate", k)
		seen[k] = true
		names = append(names, str(at(p, "name")))
	}
	g.expect(equal(names, []string{"http"}), "query-api container ports named %v, want [http]: /metrics is scraped through the http port", names)
}

func migrationHookDefault(g *guard) {
	r := g.mustRender("default migration hook", "migration-contract")
	docs, err := parseDocs(r.stdout)
	if err != nil {
		g.failf("output is not YAML: %v", err)
		return
	}
	var job doc
	for _, d := range ofKind(docs, "Job") {
		if label(d, "app.kubernetes.io/component") == "migrate" {
			job = d
		}
	}
	if job == nil {
		g.failf("no migrate Job rendered")
		return
	}
	g.expect(str(at(job, "metadata", "annotations", "helm.sh/hook")) == "pre-install,pre-upgrade",
		"default migrate hook is %q, want pre-install,pre-upgrade", str(at(job, "metadata", "annotations", "helm.sh/hook")))
	g.expect(at(job, "spec", "template", "spec", "initContainers") == nil, "the default migrate Job has initContainers")
}

func migrationHookPostInstallRefused(g *guard) {
	stderr := g.refusal("post-install without localBundledPostgres", "migration-contract",
		join(setJSON(`migrations.hook.events=["post-install","pre-upgrade"]`), set("postgresql.enabled=true"))...)
	g.expect(strings.Contains(stderr, "localBundledPostgres"), "the refusal does not name localBundledPostgres: %s", firstLines(stderr, 2))
}

func migrationHookLocalNeedsBundled(g *guard) {
	stderr := g.refusal("localBundledPostgres without postgresql.enabled", "migration-contract",
		join(set("migrations.hook.localBundledPostgres=true"), setJSON(`migrations.hook.events=["post-install","pre-upgrade"]`))...)
	g.expect(strings.Contains(stderr, "/postgresql/enabled") || strings.Contains(stderr, "postgresql.enabled"),
		"the refusal does not name postgresql.enabled: %s", firstLines(stderr, 2))
}

func TestRolloutGuardsRealTree(t *testing.T) {
	runReal(t,
		check{"rolling-default", rollingDefault},
		check{"rolling-query-api-values-driven", rollingQueryAPIValuesDriven},
		check{"rolling-workers-default", rollingWorkersDefault},
		check{"rolling-group-wins", rollingGroupWins},
		check{"rolling-schema-rejects-unknown-key", rollingSchemaRejectsUnknownKey},
		check{"api-probes-ports-unique", apiProbesPortsUnique},
		check{"migration-hook-default", migrationHookDefault},
		check{"migration-hook-post-install-refused", migrationHookPostInstallRefused},
		check{"migration-hook-local-needs-bundled", migrationHookLocalNeedsBundled},
	)
}

func TestRolloutGuardsPlantedDefects(t *testing.T) {
	const qapi = "templates/query-api-deployment.yaml"
	const workers = "templates/go-workers.yaml"
	runPlanted(t, []planted{
		{"workers-default-drifts", chartOnly,
			mut(chartFile(workers), `(dict "maxUnavailable" 0 "maxSurge" 1) | toJson`, `(dict "maxUnavailable" 1 "maxSurge" 1) | toJson`),
			rollingDefault, "default rollingUpdate"},
		{"query-api-ignores-its-value", chartOnly,
			mut(chartFile(qapi), `{{ .Values.queryApi.rollingUpdate | default (dict "maxUnavailable" 0 "maxSurge" 1) | toJson }}`, `{{ dict "maxUnavailable" 0 "maxSurge" 1 | toJson }}`),
			rollingQueryAPIValuesDriven, "queryApi.rollingUpdate was not honoured"},
		{"workers-level-default-dropped", chartOnly,
			mut(chartFile(workers), `$group.rollingUpdate | default $.Values.goWorkers.rollingUpdate | default (dict`, `$group.rollingUpdate | default (dict`),
			rollingWorkersDefault, "goWorkers.rollingUpdate was not honoured"},
		{"group-level-ignored", chartOnly,
			mut(chartFile(workers), `$group.rollingUpdate | default $.Values.goWorkers.rollingUpdate | default (dict`, `$.Values.goWorkers.rollingUpdate | default (dict`),
			rollingGroupWins, "did not win"},
		{"schema-accepts-unknown-keys", chartOnly,
			mut(chartFile("values.schema.json"), `"additionalProperties": false`, `"additionalProperties": true`),
			rollingSchemaRejectsUnknownKey, "must be refused"},
		{"query-api-duplicate-port", chartOnly,
			mut(chartFile(qapi), "            - name: http\n              containerPort: {{ .Values.queryApi.port }}\n              protocol: TCP\n",
				"            - name: http\n              containerPort: {{ .Values.queryApi.port }}\n              protocol: TCP\n            - name: dup\n              containerPort: {{ .Values.queryApi.port }}\n              protocol: TCP\n"),
			apiProbesPortsUnique, "must be unique"},
		{"query-api-port-renamed", chartOnly,
			mut(chartFile(qapi), "            - name: http\n              containerPort: {{ .Values.queryApi.port }}", "            - name: web\n              containerPort: {{ .Values.queryApi.port }}"),
			apiProbesPortsUnique, "named [web]"},
		{"migrate-hook-default-moves", chartOnly,
			mut(chartFile("templates/migrate-job.yaml"), `{{- $hookEvents := .Values.migrations.hook.events }}`, `{{- $hookEvents := list "pre-install" }}`),
			migrationHookDefault, "default migrate hook"},
		{"migrate-job-gains-an-init-container-by-default", chartOnly,
			mut(chartFile("templates/migrate-job.yaml"), "      {{- if $localBundledPostgres }}\n      initContainers:", "      {{- if true }}\n      initContainers:"),
			migrationHookDefault, "initContainers"},
		{"post-install-fail-closed-removed", chartOnly,
			all(mut(chartFile("templates/migrate-job.yaml"), `{{- if and (has "post-install" $hookEvents) (not $localBundledPostgres) }}`, `{{- if false }}`),
				mut(chartFile("values.schema.json"), `"localBundledPostgres": { "const": true }
                },
                "required": ["localBundledPostgres"]`, `"localBundledPostgres": { "type": "boolean" }
                },
                "required": []`)),
			migrationHookPostInstallRefused, "post-install without localBundledPostgres"},
		{"local-without-bundled-accepted", chartOnly,
			all(mut(chartFile("templates/migrate-job.yaml"), `{{- if and $localBundledPostgres (not .Values.postgresql.enabled) }}`, `{{- if false }}`),
				mut(chartFile("values.schema.json"), `"enabled": { "const": true }
            },
            "required": ["enabled"]
          }
        },
        "required": ["postgresql"]`, `"enabled": { "type": "boolean" }
            },
            "required": []
          }
        },
        "required": ["postgresql"]`)),
			migrationHookLocalNeedsBundled, "localBundledPostgres without postgresql.enabled"},
	})
}
