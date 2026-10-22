package chartgates_test

import (
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/platform/dsnclass"
)

// Every worker-group Deployment of the rendered chart may carry only the
// database DSN secrets of its own process class, whether the name arrives as a
// container env entry or as a key of a ConfigMap or Secret the container reads
// through envFrom. The class of a group is the runtime the deployment manifest
// gives its process, and the allowed set is deploymentcontract's own table.

func dsnNamesByGroup(g *guard, docs []doc) map[string][]string {
	keys := map[string][]string{}
	for _, d := range docs {
		switch kind(d) {
		case "ConfigMap":
			for k := range mp(d["data"]) {
				keys["configmap/"+name(d)] = append(keys["configmap/"+name(d)], k)
			}
		case "Secret":
			for k := range mp(d["stringData"]) {
				keys["secret/"+name(d)] = append(keys["secret/"+name(d)], k)
			}
			for k := range mp(d["data"]) {
				keys["secret/"+name(d)] = append(keys["secret/"+name(d)], k)
			}
		}
	}
	out := map[string][]string{}
	for _, d := range ofKind(docs, "Deployment") {
		group := label(d, "dev-health.io/worker-group")
		if group == "" {
			continue
		}
		for _, c := range containers(d) {
			// An explicit env entry beats an envFrom key of the same name, and an
			// entry with an empty value is absent to the process.
			explicit := map[string]bool{}
			for env, entry := range envMap(c) {
				explicit[env] = true
				if str(entry["value"]) != "" || entry["valueFrom"] != nil {
					out[group] = append(out[group], env)
				}
			}
			for _, ref := range list(c["envFrom"]) {
				source := ""
				if n := str(at(ref, "configMapRef", "name")); n != "" {
					source = "configmap/" + n
				}
				if n := str(at(ref, "secretRef", "name")); n != "" {
					source = "secret/" + n
				}
				for _, k := range keys[source] {
					if !explicit[k] {
						out[group] = append(out[group], k)
					}
				}
			}
		}
	}
	for group := range out {
		sort.Strings(out[group])
		out[group] = slices.Compact(out[group])
	}
	return out
}

func dsnClassViolations(g *guard, docs []doc) []string {
	runtimes := map[string]string{}
	for _, p := range processes(g) {
		runtimes[str(p["name"])] = str(p["runtime"])
	}
	var out []string
	for group, names := range dsnNamesByGroup(g, docs) {
		class, known := runtimes[group]
		if !known {
			out = append(out, "worker group "+group+" is not a process of the deployment manifest")
			continue
		}
		if foreign := dsnclass.Foreign(class, names); len(foreign) > 0 {
			out = append(out, "worker group "+group+" (class "+class+") carries "+strings.Join(foreign, ", ")+" outside its class")
		}
	}
	sort.Strings(out)
	return out
}

func workerGroupsRenderOnlyTheirClassDSNSecrets(g *guard) {
	for _, release := range []struct {
		name string
		args []string
	}{
		{"dsn-class-default", nil},
		{"dsn-class-pooled", poolGoValues},
		// The bundled PostgreSQL adds a database DSN alias to the shared Secret
		// every worker group reads through envFrom.
		{"dsn-class-bundled-pg", join(poolGoValues, set("postgresql.enabled=true"))},
	} {
		docs := g.docs(release.name, release.args...)
		names := dsnNamesByGroup(g, docs)
		g.expect(len(names) == len(processes(g)), "%s: %d worker groups rendered, the manifest lists %d", release.name, len(names), len(processes(g)))
		for _, v := range dsnClassViolations(g, docs) {
			g.failf("%s: %s", release.name, v)
		}
	}
	docs := g.docs("dsn-class-pooled", poolGoValues...)
	names := dsnNamesByGroup(g, docs)
	for _, p := range processes(g) {
		group := str(p["name"])
		g.expect(sortedHas(names[group], "POSTGRES_URI"), "worker group %s renders no POSTGRES_URI, so the check measured nothing for it", group)
		if str(p["runtime"]) == "stream" {
			g.expect(!sortedHas(names[group], "WORKER_DATABASE_URI"), "stream group %s still renders WORKER_DATABASE_URI", group)
		} else {
			g.expect(sortedHas(names[group], "WORKER_DATABASE_URI"), "group %s renders no WORKER_DATABASE_URI", group)
		}
	}
}

// DSN-shaped keys an operator puts in the shared Secret or ConfigMap reach every
// group through envFrom. The names the chart knows are blanked by explicit empty
// env entries (which beat envFrom and are absent to the process), so none of them
// is visible to a worker; a name the chart does not know still is, and the
// start-up guard refuses it.
func workerGroupsBlankSharedDSNNames(g *guard) {
	known := []string{"DATABASE_URI", "API_DATABASE_URI", "GO_API_REGISTRY_POSTGRES_URI", "COORDINATOR_DATABASE_URI", "WORKER_DATABASE_URI"}
	var sets []string
	for _, k := range known {
		sets = append(sets, "secrets.data."+k+"=postgresql://x@h/db")
	}
	sets = append(sets, "config.WORKER_DATABASE_URI=postgresql://x@h/db", "config.COORDINATOR_DATABASE_URI=postgresql://x@h/db")
	docs := g.docs("dsn-class-shared", join(poolGoValues, setString(sets...), set("postgresql.enabled=true"))...)
	for _, v := range dsnClassViolations(g, docs) {
		g.failf("%s", v)
	}
	g.expect(len(dsnNamesByGroup(g, docs)) == len(processes(g)), "not every worker group rendered, so the check measured nothing")
	unknown := g.docs("dsn-class-unknown", join(poolGoValues, setString("secrets.data.ANALYTICS_DATABASE_URI=postgresql://x@h/db"))...)
	violations := strings.Join(dsnClassViolations(g, unknown), "\n")
	for _, p := range processes(g) {
		g.expect(strings.Contains(violations, "worker group "+str(p["name"])+" (class "+str(p["runtime"])+") carries ANALYTICS_DATABASE_URI"),
			"an unlisted DSN-shaped key of the shared Secret was not found on group %s", str(p["name"]))
	}
}

// A container env list that names a variable twice leaves the value to the last
// entry (a blank and a secretKeyRef on one name), so no worker container may do it.
func workerContainersRenderNoDuplicateEnvNames(g *guard) {
	for _, release := range []struct {
		name string
		args []string
	}{
		{"dsn-env-default", nil},
		{"dsn-env-pooled", poolGoValues},
		{"dsn-env-bundled-pg", join(poolGoValues, set("postgresql.enabled=true"))},
	} {
		docs := g.docs(release.name, release.args...)
		seen := 0
		for _, d := range ofKind(docs, "Deployment") {
			group := label(d, "dev-health.io/worker-group")
			if group == "" {
				continue
			}
			for _, c := range containers(d) {
				seen++
				count := map[string]int{}
				for _, e := range list(c["env"]) {
					count[str(at(e, "name"))]++
				}
				for n, k := range count {
					g.expect(k == 1, "%s: worker group %s names env %s %d times", release.name, group, n, k)
				}
			}
		}
		g.expect(seen == len(processes(g)), "%s: %d worker containers rendered, the manifest lists %d", release.name, seen, len(processes(g)))
	}
}

func TestDSNClassGuardsRealTree(t *testing.T) {
	runReal(t,
		check{"worker-groups-render-only-their-class-dsn-secrets", workerGroupsRenderOnlyTheirClassDSNSecrets},
		check{"shared-dsn-names-are-blanked-on-every-group", workerGroupsBlankSharedDSNNames},
		check{"worker-containers-render-no-duplicate-env-names", workerContainersRenderNoDuplicateEnvNames},
	)
}

func TestDSNClassGuardsPlantedDefects(t *testing.T) {
	chartAndManifest := []string{chartRel, deploymentJSON}
	const workers = "templates/go-workers.yaml"
	const values = "values.yaml"
	runPlanted(t, []planted{
		{"one-group-env-carries-the-coordinator-dsn", chartAndManifest,
			mut(chartFile(values), "    - name: heavy\n      subcommand: worker\n", "    - name: heavy\n      subcommand: worker\n      extraEnv: [{name: COORDINATOR_DATABASE_URI, value: \"postgresql://x@h/db\"}]\n"),
			workerGroupsRenderOnlyTheirClassDSNSecrets, "heavy (class river) carries COORDINATOR_DATABASE_URI"},
		{"stream-groups-render-the-queue-dsn-again", chartAndManifest,
			mut(chartFile(workers), "            {{- if not $group.runtimeProfile }}\n            - name: WORKER_DATABASE_URI", "            {{- if true }}\n            - name: WORKER_DATABASE_URI"),
			workerGroupsRenderOnlyTheirClassDSNSecrets, "outside its class"},
		{"blank-and-secret-ref-on-one-name", chartAndManifest,
			mut(chartFile(workers), "{{- if not $coordinator }}\n            - {name: COORDINATOR_DATABASE_URI, value: \"\"}\n            {{- end }}", "{{- if true }}\n            - {name: COORDINATOR_DATABASE_URI, value: \"\"}\n            {{- end }}"),
			workerContainersRenderNoDuplicateEnvNames, "names env COORDINATOR_DATABASE_URI 2 times"},
		{"workers-stop-blanking-the-other-roles-dsn-names", chartAndManifest,
			mut(chartFile(workers), `list "DATABASE_URI" "API_DATABASE_URI" "GO_API_REGISTRY_POSTGRES_URI" "MIGRATION_DATABASE_URI" "AUTH_DATABASE_URI" "AUTH_MIGRATION_DATABASE_URI"`, `list`),
			workerGroupsBlankSharedDSNNames, "carries API_DATABASE_URI"},
		{"non-coordinator-groups-stop-blanking-the-coordinator-dsn", chartAndManifest,
			mut(chartFile(workers), "{{- if not $coordinator }}\n            - {name: COORDINATOR_DATABASE_URI, value: \"\"}", "{{- if false }}\n            - {name: COORDINATOR_DATABASE_URI, value: \"\"}"),
			workerGroupsBlankSharedDSNNames, "carries COORDINATOR_DATABASE_URI outside its class"},
		{"stream-groups-stop-blanking-the-queue-dsn", chartAndManifest,
			mut(chartFile(workers), "{{- if $group.runtimeProfile }}\n            - {name: WORKER_DATABASE_URI, value: \"\"}", "{{- if false }}\n            - {name: WORKER_DATABASE_URI, value: \"\"}"),
			workerGroupsBlankSharedDSNNames, "(class stream) carries WORKER_DATABASE_URI"},
		{"river-groups-render-the-coordinator-dsn", chartAndManifest,
			mut(chartFile(workers), "            {{- if $coordinator }}\n            - name: COORDINATOR_DATABASE_URI", "            {{- if true }}\n            - name: COORDINATOR_DATABASE_URI"),
			workerGroupsRenderOnlyTheirClassDSNSecrets, "outside its class"},
	})
}

// holds reports whether a container env entry gives the process a value: an entry with
// an empty value is absent, which is how the chart blanks a name an envFrom key would set.
func holds(env map[string]doc, name string) bool {
	entry, ok := env[name]
	return ok && (str(entry["value"]) != "" || entry["valueFrom"] != nil)
}
