package chartgates_test

import (
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/deploymentcontract"
)

// Every worker-group Deployment of the rendered chart may carry only the
// database DSN secrets of its own process class, whether the name arrives as a
// container env entry or as a key of a ConfigMap or Secret the container reads
// through envFrom. The class of a group is the runtime the deployment manifest
// gives its process, and the allowed set is deploymentcontract's own table.

func dsnNamesByGroup(g *guard, docs []doc) map[string][]string {
	names, _ := scanWorkerEnv(docs)
	return names
}

// scanWorkerEnv returns, per worker group, the env names it can see (container
// env plus the keys of every rendered ConfigMap or Secret it reads through
// envFrom) and the envFrom sources it reads that the chart does not render,
// whose keys are not visible to a render.
func scanWorkerEnv(docs []doc) (map[string][]string, map[string][]string) {
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
	unresolved := map[string][]string{}
	for _, d := range ofKind(docs, "Deployment") {
		group := label(d, "dev-health.io/worker-group")
		if group == "" {
			continue
		}
		for _, c := range containers(d) {
			for env := range envMap(c) {
				out[group] = append(out[group], env)
			}
			for _, ref := range list(c["envFrom"]) {
				if n := str(at(ref, "configMapRef", "name")); n != "" {
					if _, rendered := keys["configmap/"+n]; rendered {
						out[group] = append(out[group], keys["configmap/"+n]...)
					} else {
						unresolved[group] = append(unresolved[group], "configmap/"+n)
					}
				}
				if n := str(at(ref, "secretRef", "name")); n != "" {
					if _, rendered := keys["secret/"+n]; rendered {
						out[group] = append(out[group], keys["secret/"+n]...)
					} else {
						unresolved[group] = append(unresolved[group], "secret/"+n)
					}
				}
			}
		}
	}
	for group := range out {
		sort.Strings(out[group])
		out[group] = slices.Compact(out[group])
	}
	return out, unresolved
}

func dsnClassViolations(g *guard, docs []doc) []string {
	runtimes := map[string]string{}
	for _, p := range processes(g) {
		runtimes[str(p["name"])] = str(p["runtime"])
	}
	var out []string
	names, unresolved := scanWorkerEnv(docs)
	for group, names := range names {
		class, known := runtimes[group]
		if !known {
			out = append(out, "worker group "+group+" is not a process of the deployment manifest")
			continue
		}
		// A stream group is the one class whose whole environment must be visible to
		// the render: it reads no envFrom source the chart does not render, so no
		// operator-supplied Secret can carry a DSN into it unseen.
		if class == "stream" && len(unresolved[group]) > 0 {
			out = append(out, "worker group "+group+" (class stream) reads envFrom source "+strings.Join(unresolved[group], ", ")+" the chart does not render")
		}
		if foreign := deploymentcontract.ForeignDSNSecrets(class, names); len(foreign) > 0 {
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

// A foreign DSN name reaching every group through the shared Secret or
// ConfigMap is found by the same scan.
func workerGroupsScanSeesSharedDSNNames(g *guard) {
	shared := g.docs("dsn-class-shared", join(poolGoValues, setString("secrets.data.COORDINATOR_DATABASE_URI=postgresql://x@h/db"))...)
	violations := strings.Join(dsnClassViolations(g, shared), "\n")
	for _, p := range processes(g) {
		if str(p["runtime"]) == "control" {
			continue
		}
		g.expect(strings.Contains(violations, "worker group "+str(p["name"])+" (class "+str(p["runtime"])+") carries COORDINATOR_DATABASE_URI"),
			"a COORDINATOR_DATABASE_URI key of the shared Secret was not found on group %s", str(p["name"]))
	}
	cm := g.docs("dsn-class-config", join(poolGoValues, setString("config.WORKER_DATABASE_URI=postgresql://x@h/db"))...)
	g.expect(strings.Contains(strings.Join(dsnClassViolations(g, cm), "\n"), "stream-ingest (class stream) carries WORKER_DATABASE_URI"),
		"a WORKER_DATABASE_URI key of the shared ConfigMap was not found on a stream group")
}

// A shared operator-supplied envFrom source must not reach a stream group;
// it still reaches the other classes, so the check is not vacuous.
func streamGroupsReadOnlyChartRenderedEnvFrom(g *guard) {
	docs := g.docs("dsn-class-external", join(poolGoValues, set(
		"goWorkers.extraEnvFrom[0].secretRef.name=external-worker-dsns",
		"goWorkers.extraEnvFrom[1].configMapRef.name=external-worker-config"))...)
	for _, v := range dsnClassViolations(g, docs) {
		g.failf("%s", v)
	}
	_, unresolved := scanWorkerEnv(docs)
	g.expect(sortedHas(unresolved["reconciler"], "secret/external-worker-dsns"), "the shared envFrom Secret no longer reaches the reconciler group, so the check measured nothing: %v", unresolved["reconciler"])
}

func TestDSNClassGuardsRealTree(t *testing.T) {
	runReal(t,
		check{"worker-groups-render-only-their-class-dsn-secrets", workerGroupsRenderOnlyTheirClassDSNSecrets},
		check{"scan-sees-shared-dsn-names", workerGroupsScanSeesSharedDSNNames},
		check{"stream-groups-read-only-chart-rendered-envfrom", streamGroupsReadOnlyChartRenderedEnvFrom},
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
		{"stream-groups-read-the-shared-external-envfrom", chartAndManifest,
			mut(chartFile(workers), "{{- $groupEnvFrom := list }}\n          {{- if not $group.runtimeProfile }}\n          {{- $groupEnvFrom = $.Values.goWorkers.extraEnvFrom | default (list) }}\n          {{- end }}", "{{- $groupEnvFrom := $.Values.goWorkers.extraEnvFrom | default (list) }}"),
			streamGroupsReadOnlyChartRenderedEnvFrom, "reads envFrom source secret/external-worker-dsns"},
		{"river-groups-render-the-coordinator-dsn", chartAndManifest,
			mut(chartFile(workers), "            {{- if $coordinator }}\n            - name: COORDINATOR_DATABASE_URI", "            {{- if true }}\n            - name: COORDINATOR_DATABASE_URI"),
			workerGroupsRenderOnlyTheirClassDSNSecrets, "outside its class"},
	})
}
