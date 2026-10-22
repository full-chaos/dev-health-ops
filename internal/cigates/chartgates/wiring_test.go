package chartgates_test

import (
	"encoding/json"
	"strings"
	"testing"
)

// Ported from tests/test_helm_valkey_keyspace.py (CHAOS-4226), tests/test_helm_go_worker_pull_policy.py
// (CHAOS-4984), tests/test_helm_go_worker_replicas_keda.py (CHAOS-6767), tests/test_helm_extra_envfrom.py
// (CHAOS-4984) and tests/test_helm_web_extra_volumes.py (CHAOS-6188, CHAOS-8310).

func mainConfigMap(docs []doc) (doc, string) {
	for _, d := range ofKind(docs, "ConfigMap") {
		if strings.HasSuffix(name(d), "-config") {
			return mp(d["data"]), name(d)
		}
	}
	return nil, ""
}

func valkeySharedURLOnDB1(g *guard) {
	data, _ := mainConfigMap(g.docs("keyspace-contract"))
	if data == nil {
		g.failf("main ConfigMap not rendered")
		return
	}
	g.expect(str(data["REDIS_URL"]) == str(data["VALKEY_URI"]), "REDIS_URL %q and VALKEY_URI %q must be one URL", str(data["REDIS_URL"]), str(data["VALKEY_URI"]))
	g.expect(strings.HasSuffix(str(data["VALKEY_URI"]), ":6379/1"), "VALKEY_URI %q must end :6379/1: the Go client refuses any DB but 1", str(data["VALKEY_URI"]))
}

func valkeyWorkersGetSharedConfigMap(g *guard) {
	docs := g.docs("keyspace-contract")
	_, cfgName := mainConfigMap(docs)
	var workers []doc
	names := map[string]bool{}
	for _, d := range ofKind(docs, "Deployment") {
		n := name(d)
		if strings.HasPrefix(n, "keyspace-contract-dev-health-go-") && !strings.Contains(n, "pgbouncer") {
			workers = append(workers, d)
			names[n] = true
		}
	}
	g.expect(names["keyspace-contract-dev-health-go-sync"], "the sync coordinator Deployment was not rendered: %v", names)
	for _, d := range workers {
		cs := containers(d)
		if len(cs) == 0 {
			g.failf("%s has no container", name(d))
			continue
		}
		found := false
		for _, ref := range list(cs[0]["envFrom"]) {
			if str(at(ref, "configMapRef", "name")) == cfgName {
				found = true
			}
		}
		g.expect(found, "%s does not read the shared ConfigMap %s", name(d), cfgName)
		g.expect(!has(envMap(cs[0]), "VALKEY_URI"), "%s pins its own VALKEY_URI, so the ConfigMap value would not win", name(d))
	}
}

func valkeyExternalURISuppressesDerived(g *guard) {
	data, _ := mainConfigMap(g.docs("keyspace-contract",
		set("secrets.data.VALKEY_URI=redis://external-valkey:6379/1", "secrets.data.REDIS_URL=redis://external-valkey:6379/1")...))
	_, v := data["VALKEY_URI"]
	_, r := data["REDIS_URL"]
	g.expect(!v && !r, "an explicit external URI must suppress the derived one in the ConfigMap (VALKEY_URI=%v REDIS_URL=%v)", v, r)
}

var pullPolicyHost = set("goWorkers.pgbouncer.postgres.host=postgres.example.com")

func goWorkerDeployments(g *guard, extra ...string) []doc {
	args := join(pullPolicyHost, []string{"--show-only", "templates/go-workers.yaml"}, extra)
	deps := ofKind(g.docs("pull-policy-contract", args...), "Deployment")
	g.expect(len(deps) > 0, "no go-worker Deployment rendered")
	return deps
}

func pullPolicyDefault(g *guard) {
	for _, d := range goWorkerDeployments(g) {
		got := str(containers(d)[0]["imagePullPolicy"])
		g.expect(got == "IfNotPresent", "%s: imagePullPolicy %q, want IfNotPresent", name(d), got)
	}
}

func pullPolicyIgnoresLocalProfile(g *guard) {
	for _, d := range goWorkerDeployments(g, set("image.pullPolicy=Never")...) {
		got := str(containers(d)[0]["imagePullPolicy"])
		g.expect(got != "Never", "%s: image.pullPolicy=Never leaked into a go-worker container (the CHAOS-4984 ErrImageNeverPull incident)", name(d))
		g.expect(got == "IfNotPresent", "%s: imagePullPolicy %q, want IfNotPresent", name(d), got)
	}
}

func pullPolicyOverridable(g *guard) {
	for _, d := range goWorkerDeployments(g, set("goWorkers.imagePullPolicy=Always")...) {
		got := str(containers(d)[0]["imagePullPolicy"])
		g.expect(got == "Always", "%s: goWorkers.imagePullPolicy=Always was not honoured, got %q", name(d), got)
	}
}

func valuesGroups(g *guard) []any {
	return list(at(g.yamlFile(chartFile("values.yaml")), "goWorkers", "groups"))
}

func cloneGroups(g *guard, groups []any) []any {
	b, _ := json.Marshal(groups)
	var out []any
	_ = json.Unmarshal(b, &out)
	return out
}

func groupsArg(groups []any) []string {
	b, _ := json.Marshal(groups)
	return setJSON("goWorkers.groups=" + string(b))
}

func workerDeployments(docs []doc) map[string]doc {
	out := map[string]doc{}
	for _, d := range ofKind(docs, "Deployment") {
		if strings.Contains(label(d, "app.kubernetes.io/component"), "go-worker") {
			out[name(d)] = d
		}
	}
	return out
}

func scaledTargets(docs []doc) map[string]bool {
	out := map[string]bool{}
	for _, d := range ofKind(docs, "ScaledObject") {
		out[str(at(d, "spec", "scaleTargetRef", "name"))] = true
	}
	return out
}

func renderGroup0(g *guard, merge map[string]any) []doc {
	if merge == nil {
		return g.docs("t")
	}
	groups := cloneGroups(g, valuesGroups(g))
	if len(groups) == 0 {
		g.failf("goWorkers.groups is empty")
		return nil
	}
	g0 := mp(groups[0])
	for k, v := range merge {
		g0[k] = v
	}
	return g.docs("t", groupsArg(groups)...)
}

func kedaScaledGroupsOmitReplicas(g *guard) {
	docs := renderGroup0(g, nil)
	workers, scaled := workerDeployments(docs), scaledTargets(docs)
	g.expect(len(scaled) > 0, "default values must render at least one ScaledObject")
	unscaled := 0
	for n, d := range workers {
		_, hasReplicas := mp(d["spec"])["replicas"]
		if scaled[n] {
			g.expect(!hasReplicas, "%s is KEDA-scaled but renders spec.replicas: a manifest that also sets replicas conflicts with the HPA's scale subresource", n)
		} else {
			unscaled++
			r, _ := mp(d["spec"])["replicas"].(int)
			g.expect(r >= 1, "%s is not autoscaled but renders replicas %v", n, mp(d["spec"])["replicas"])
		}
	}
	g.expect(unscaled > 0, "the coordinator and stream groups stay fixed-replica; none found")
}

func kedaFixedReplicasIgnoredForScaled(g *guard) {
	docs := renderGroup0(g, map[string]any{"replicas": 3})
	workers, scaled := workerDeployments(docs), scaledTargets(docs)
	for n, d := range workers {
		if scaled[n] {
			_, hasReplicas := mp(d["spec"])["replicas"]
			g.expect(!hasReplicas, "%s renders spec.replicas although a fixed replicas value is ignored for a scaled group", n)
			return
		}
	}
	g.failf("no scaled worker group rendered")
}

func kedaDisablingAutoscalingRestoresReplicas(g *guard) {
	groups := cloneGroups(g, valuesGroups(g))
	if len(groups) == 0 {
		g.failf("goWorkers.groups is empty")
		return
	}
	mp(groups[0])["autoscaling"] = doc{"enabled": false}
	mp(groups[0])["replicas"] = 3
	docs := g.docs("t", groupsArg(groups)...)
	scaled := scaledTargets(docs)
	three := 0
	for n, d := range workerDeployments(docs) {
		if r, _ := mp(d["spec"])["replicas"].(int); !scaled[n] && r == 3 {
			three++
		}
	}
	g.expect(three > 0, "disabling autoscaling must restore fixed replicas: no unscaled group renders replicas 3")
}

func kedaMinimumAboveOneRefused(g *guard) {
	groups := cloneGroups(g, valuesGroups(g))
	if len(groups) == 0 {
		g.failf("goWorkers.groups is empty")
		return
	}
	g0 := mp(groups[0])
	g0["replicas"] = 3
	as := mp(g0["autoscaling"])
	as["minReplicas"], as["maxReplicas"] = 3, 4
	stderr := g.refusal("an autoscaled group with minReplicas above 1", "t", groupsArg(groups)...)
	g.expect(strings.Contains(stderr, "would run below its own minimum"), "the refusal does not say the group would run below its own minimum: %s", firstLines(stderr, 2))
}

var envFromSets = join(set("goWorkers.enabled=true", "goWorkers.pgbouncer.postgres.host=postgres.example.com"), []string{"--show-only", "templates/go-workers.yaml"})

func envFromRefs(c doc) [][2]string {
	var out [][2]string
	for _, s := range list(c["envFrom"]) {
		for _, k := range []string{"configMapRef", "secretRef"} {
			if m := mp(s)[k]; m != nil {
				out = append(out, [2]string{k, str(at(m, "name"))})
			}
		}
	}
	return out
}

func envFromDeployments(g *guard, extra ...string) []doc {
	return ofKind(g.docs("extra-envfrom-test", join(envFromSets, extra)...), "Deployment")
}

func envFromExtraFirst(g *guard) {
	deps := envFromDeployments(g, set("goWorkers.extraEnvFrom[0].secretRef.name=local-dotenv")...)
	if len(deps) == 0 {
		g.failf("no Deployment rendered from templates/go-workers.yaml")
		return
	}
	refs := envFromRefs(containers(deps[0])[0])
	cm := [2]string{"configMapRef", "extra-envfrom-test-dev-health-config"}
	sec := [2]string{"secretRef", "extra-envfrom-test-dev-health-secrets"}
	g.expect(len(refs) > 0 && refs[0] == [2]string{"secretRef", "local-dotenv"}, "goWorkers.extraEnvFrom must render first (the last source wins a duplicate key); got %v", refs)
	ci, si := -1, -1
	for i, r := range refs {
		if r == cm {
			ci = i
		}
		if r == sec {
			si = i
		}
	}
	g.expect(ci >= 0 && si >= 0 && ci < si, "the chart's own config then secret sources must follow, in that order: %v", refs)
}

func envFromSharedAcrossGroups(g *guard) {
	deps := envFromDeployments(g, set("goWorkers.extraEnvFrom[0].secretRef.name=local-dotenv")...)
	g.expect(len(deps) >= 2, "expected more than one worker-group Deployment, got %d", len(deps))
	for _, d := range deps {
		refs := envFromRefs(containers(d)[0])
		if label(d, "dev-health.io/worker-group") != "" && strings.HasPrefix(label(d, "dev-health.io/worker-group"), "stream-") {
			// A stream group reads only the chart's own sources: an operator-supplied
			// Secret has keys no render can see, and the stream class carries one DSN.
			g.expect(len(refs) == 2 && refs[0][0] == "configMapRef" && refs[1][0] == "secretRef", "%s must read only the chart's own config and secret sources: %v", name(d), refs)
			continue
		}
		g.expect(len(refs) > 0 && refs[0] == [2]string{"secretRef", "local-dotenv"}, "%s did not get the shared extraEnvFrom entry first: %v", name(d), refs)
	}
}

func envFromUnchangedWhenUnset(g *guard) {
	deps := envFromDeployments(g)
	g.expect(len(deps) >= 2, "expected more than one worker-group Deployment, got %d", len(deps))
	want := []any{
		doc{"configMapRef": doc{"name": "extra-envfrom-test-dev-health-config"}},
		doc{"secretRef": doc{"name": "extra-envfrom-test-dev-health-secrets"}},
	}
	for _, d := range deps {
		got := containers(d)[0]["envFrom"]
		g.expect(equal(got, want), "%s: default envFrom must be exactly the chart's config + secret, in that order; got %v", name(d), got)
	}
}

func envFromRejectsUnknownField(g *guard) {
	r := g.tpl("extra-envfrom-test", join(envFromSets, set("goWorkers.extraEnvFrom[0].secretRef.name=local-dotenv", "goWorkers.extraEnvFrom[0].secretRef.badfield=oops"))...)
	g.expect(r.code != 0, "goWorkers.extraEnvFrom must be schema-constrained: an unknown field on a ref rendered")
	g.expect(r.code == 0 || strings.Contains(r.stderr, "badfield"), "the refusal does not name badfield: %s", firstLines(r.stderr, 2))
}

func envFromRejectsNonMapEntry(g *guard) {
	g.refusal("a non-map goWorkers.extraEnvFrom entry", "extra-envfrom-test", join(envFromSets, set("goWorkers.extraEnvFrom[0]=notamap"))...)
}

var (
	keyMount    = doc{"name": "acr-web-assertion-key", "mountPath": "/etc/dev-health/acr", "readOnly": true}
	keyVolume   = doc{"name": "acr-web-assertion-key", "secret": doc{"secretName": "acr-web-assertion-key", "defaultMode": 256}}
	localMount  = doc{"name": "localdev", "mountPath": "/app/local"}
	localVolume = doc{"name": "localdev", "hostPath": doc{"path": "/host/local"}}
)

func jsonOf(v any) string { b, _ := json.Marshal(v); return string(b) }

func webDeployment(g *guard, setJSONArgs []string, sets ...string) (container doc, pod doc) {
	args := join(setJSON(setJSONArgs...), set(sets...), []string{"--show-only", "templates/web-deployment.yaml"})
	for _, d := range ofKind(g.docs("extra-volumes-test", args...), "Deployment") {
		return containers(d)[0], podSpec(d)
	}
	g.failf("no Deployment rendered from web-deployment.yaml")
	return nil, nil
}

func webExtraVolumesRenderWithLocalDevOff(g *guard) {
	c, pod := webDeployment(g, []string{"web.extraVolumeMounts=[" + jsonOf(keyMount) + "]", "web.extraVolumes=[" + jsonOf(keyVolume) + "]"})
	g.expect(equal(c["volumeMounts"], []any{keyMount}), "web.extraVolumeMounts did not render with localDevelopment off: %v", c["volumeMounts"])
	g.expect(equal(pod["volumes"], []any{keyVolume}), "web.extraVolumes did not render with localDevelopment off: %v", pod["volumes"])
}

func webExtraVolumesAbsentWhenUnset(g *guard) {
	c, pod := webDeployment(g, nil)
	_, m := c["volumeMounts"]
	_, v := pod["volumes"]
	g.expect(!m && !v, "the default render must carry no volumeMounts/volumes (mounts=%v volumes=%v)", m, v)
}

func webExtraVolumesMergeUnderOneKey(g *guard) {
	c, pod := webDeployment(g, []string{
		"web.extraVolumeMounts=[" + jsonOf(keyMount) + "]", "web.extraVolumes=[" + jsonOf(keyVolume) + "]",
		"web.localDevelopment.extraVolumeMounts=[" + jsonOf(localMount) + "]", "web.localDevelopment.extraVolumes=[" + jsonOf(localVolume) + "]",
	}, "web.localDevelopment.enabled=true")
	g.expect(equal(c["volumeMounts"], []any{keyMount, localMount}), "web.extra* and localDevelopment.extra* mounts must merge under one key: %v", c["volumeMounts"])
	g.expect(equal(pod["volumes"], []any{keyVolume, localVolume}), "web.extra* and localDevelopment.extra* volumes must merge under one key: %v", pod["volumes"])
}

func webLocalDevOnlyUnchanged(g *guard) {
	c, pod := webDeployment(g, []string{
		"web.localDevelopment.extraVolumeMounts=[" + jsonOf(localMount) + "]", "web.localDevelopment.extraVolumes=[" + jsonOf(localVolume) + "]",
	}, "web.localDevelopment.enabled=true")
	g.expect(equal(c["volumeMounts"], []any{localMount}), "localDevelopment-only mounts changed shape: %v", c["volumeMounts"])
	g.expect(equal(pod["volumes"], []any{localVolume}), "localDevelopment-only volumes changed shape: %v", pod["volumes"])
}

func webBackendURLRequired(g *guard) {
	r := g.tplRaw("t", set("web.authSecret.secretName=web-auth-placeholder")...)
	g.expect(r.code != 0, "a render without web.env.BACKEND_URL succeeded")
	g.expect(r.code == 0 || strings.Contains(r.stderr, "web.env.BACKEND_URL is required whenever web.enabled=true"), "the refusal does not name web.env.BACKEND_URL: %s", firstLines(r.stderr, 2))
	if off := g.tplRaw("t", set("web.enabled=false")...); off.code != 0 {
		g.failf("web.enabled=false needs no BACKEND_URL but failed: %s", firstLines(off.stderr, 2))
	}
	ex := g.tplRaw("t", set("web.env.BACKEND_URL=http://explicit.example:9000", "web.authSecret.secretName=web-auth-placeholder")...)
	g.expect(ex.code == 0, "an explicit web.env.BACKEND_URL must render: %s", firstLines(ex.stderr, 2))
	docs, err := parseDocs(ex.stdout)
	if err != nil {
		g.failf("output is not YAML: %v", err)
		return
	}
	got := ""
	for _, d := range ofKind(docs, "Deployment") {
		if label(d, "app.kubernetes.io/component") == "web" {
			got = str(envMap(containers(d)[0])["BACKEND_URL"]["value"])
		}
	}
	g.expect(got == "http://explicit.example:9000", "the web Deployment's BACKEND_URL env is %q, want the explicit value verbatim", got)
}

func TestWiringGuardsRealTree(t *testing.T) {
	runReal(t,
		check{"valkey-shared-url-db1", valkeySharedURLOnDB1},
		check{"valkey-workers-get-shared-configmap", valkeyWorkersGetSharedConfigMap},
		check{"valkey-external-uri-suppresses-derived", valkeyExternalURISuppressesDerived},
		check{"pull-policy-default", pullPolicyDefault},
		check{"pull-policy-ignores-local-profile", pullPolicyIgnoresLocalProfile},
		check{"pull-policy-overridable", pullPolicyOverridable},
		check{"keda-scaled-groups-omit-replicas", kedaScaledGroupsOmitReplicas},
		check{"keda-fixed-replicas-ignored-for-scaled", kedaFixedReplicasIgnoredForScaled},
		check{"keda-disabling-autoscaling-restores-replicas", kedaDisablingAutoscalingRestoresReplicas},
		check{"keda-minimum-above-one-refused", kedaMinimumAboveOneRefused},
		check{"envfrom-extra-first", envFromExtraFirst},
		check{"envfrom-shared-across-groups", envFromSharedAcrossGroups},
		check{"envfrom-unchanged-when-unset", envFromUnchangedWhenUnset},
		check{"envfrom-rejects-unknown-field", envFromRejectsUnknownField},
		check{"envfrom-rejects-non-map-entry", envFromRejectsNonMapEntry},
		check{"web-extra-volumes-render-with-local-dev-off", webExtraVolumesRenderWithLocalDevOff},
		check{"web-extra-volumes-absent-when-unset", webExtraVolumesAbsentWhenUnset},
		check{"web-extra-volumes-merge-under-one-key", webExtraVolumesMergeUnderOneKey},
		check{"web-local-dev-only-unchanged", webLocalDevOnlyUnchanged},
		check{"web-backend-url-required", webBackendURLRequired},
	)
}

// sharedEnvFromBlock is the template text that starts a group's envFrom list from the shared
// extraEnvFrom, for every group except a stream group.
const sharedEnvFromBlock = "{{- $groupEnvFrom := list }}\n          {{- if not $group.runtimeProfile }}\n          {{- $groupEnvFrom = $.Values.goWorkers.extraEnvFrom | default (list) }}\n          {{- end }}"

func TestWiringGuardsPlantedDefects(t *testing.T) {
	const helpers = "templates/_helpers.tpl"
	const workers = "templates/go-workers.yaml"
	const web = "templates/web-deployment.yaml"
	const cmRef = `(dict "configMapRef" (dict "name" (include "dev-health.configMapName" $)))`
	schemaSecretRef := `"secretRef": {
              "type": "object",
              "properties": { "name": { "type": "string" } },
              "required": ["name"],
              "additionalProperties": false`
	runPlanted(t, []planted{
		{"valkey-derived-on-db-0", chartOnly,
			mut(chartFile(helpers), `VALKEY_URI: {{ include "dev-health.redisURL" . | quote }}`, `VALKEY_URI: "redis://x:6379/0"`),
			valkeySharedURLOnDB1, "must be one URL"},
		{"valkey-workers-lose-the-shared-configmap", chartOnly,
			mut(chartFile(workers), cmRef, `(dict "configMapRef" (dict "name" "other"))`),
			valkeyWorkersGetSharedConfigMap, "does not read the shared ConfigMap"},
		{"valkey-external-uri-no-longer-suppresses", chartOnly,
			mut(chartFile(helpers), `{{- $valkeyAuto := and .Values.valkey.enabled (not (index .Values.secrets.data "VALKEY_URI")) }}`, `{{- $valkeyAuto := .Values.valkey.enabled }}`),
			valkeyExternalURISuppressesDerived, "must suppress"},
		{"pull-policy-follows-the-local-profile", chartOnly,
			mut(chartFile(workers), `imagePullPolicy: {{ $.Values.goWorkers.imagePullPolicy }}`, `imagePullPolicy: {{ $.Values.image.pullPolicy }}`),
			pullPolicyIgnoresLocalProfile, "image.pullPolicy=Never leaked"},
		{"pull-policy-override-ignored", chartOnly,
			mut(chartFile(workers), `imagePullPolicy: {{ $.Values.goWorkers.imagePullPolicy }}`, `imagePullPolicy: IfNotPresent`),
			pullPolicyOverridable, "was not honoured"},
		{"pull-policy-default-changes", chartOnly,
			mut(chartFile("values.yaml"), "  imagePullPolicy: IfNotPresent\n  # -- Default liveness/readiness probes", "  imagePullPolicy: Always\n  # -- Default liveness/readiness probes"),
			pullPolicyDefault, "want IfNotPresent"},
		{"keda-group-renders-replicas", chartOnly,
			mut(chartFile(workers), "  {{- if not (and $group.autoscaling $group.autoscaling.enabled) }}\n  replicas:", "  {{- if true }}\n  replicas:"),
			kedaScaledGroupsOmitReplicas, "renders spec.replicas"},
		{"keda-fixed-replicas-ignored-when-autoscaling-off", chartOnly,
			mut(chartFile(workers), "  replicas: {{ $group.replicas }}", "  replicas: 1"),
			kedaDisablingAutoscalingRestoresReplicas, "no unscaled group renders replicas 3"},
		{"keda-minimum-guard-removed", chartOnly,
			mut(chartFile(workers), `{{- if and $group.autoscaling $group.autoscaling.enabled (gt (int $group.autoscaling.minReplicas) 1) }}`, `{{- if false }}`),
			kedaMinimumAboveOneRefused, "an autoscaled group with minReplicas above 1"},
		{"envfrom-extra-renders-after-the-chart-sources", chartOnly,
			all(mut(chartFile(workers), sharedEnvFromBlock, `{{- $groupEnvFrom := list }}`),
				mut(chartFile(workers), `{{- $groupEnvFrom = append $groupEnvFrom (dict "secretRef" (dict "name" (include "dev-health.secretName" $))) }}`,
					`{{- $groupEnvFrom = append $groupEnvFrom (dict "secretRef" (dict "name" (include "dev-health.secretName" $))) }}
          {{- $groupEnvFrom = concat $groupEnvFrom ($.Values.goWorkers.extraEnvFrom | default (list)) }}`)),
			envFromExtraFirst, "must render first"},
		{"envfrom-extra-dropped-for-all-but-one-group", chartOnly,
			mut(chartFile(workers), sharedEnvFromBlock, `{{- $groupEnvFrom := ternary ($.Values.goWorkers.extraEnvFrom | default (list)) (list) (eq $group.name "heavy") }}`),
			envFromSharedAcrossGroups, "did not get the shared extraEnvFrom entry"},
		{"envfrom-default-gains-an-entry", chartOnly,
			mut(chartFile(workers), cmRef+" }}\n", cmRef+" }}\n          {{- $groupEnvFrom = append $groupEnvFrom (dict \"secretRef\" (dict \"name\" \"extra\")) }}\n"),
			envFromUnchangedWhenUnset, "default envFrom must be exactly"},
		{"envfrom-schema-accepts-unknown-field", chartOnly,
			mut(chartFile("values.schema.json"), schemaSecretRef, strings.Replace(schemaSecretRef, `"additionalProperties": false`, `"additionalProperties": true`, 1)),
			envFromRejectsUnknownField, "must be schema-constrained"},
		{"envfrom-schema-accepts-non-map-entry", chartOnly,
			mut(chartFile("values.schema.json"), `"items": { "$ref": "#/definitions/envFromEntry" }`, `"items": {}`),
			envFromRejectsNonMapEntry, "must be refused"},
		{"web-extra-volumes-gated-behind-local-dev", chartOnly,
			mut(chartFile(web), `{{- $webVolumeMounts := .Values.web.extraVolumeMounts | default (list) }}`, `{{- $webVolumeMounts := list }}`),
			webExtraVolumesRenderWithLocalDevOff, "did not render with localDevelopment off"},
		{"web-local-dev-volumes-clobber-the-extra-ones", chartOnly,
			mut(chartFile(web), `{{- $webVolumes = concat $webVolumes (.Values.web.localDevelopment.extraVolumes | default (list)) }}`, `{{- $webVolumes = .Values.web.localDevelopment.extraVolumes | default (list) }}`),
			webExtraVolumesMergeUnderOneKey, "must merge under one key"},
		{"web-volumes-rendered-by-default", chartOnly,
			mut(chartFile(web), `{{- $webVolumes := .Values.web.extraVolumes | default (list) }}`, `{{- $webVolumes := list (dict "name" "x" "emptyDir" (dict)) }}`),
			webExtraVolumesAbsentWhenUnset, "default render must carry no"},
		{"explicit-backend-url-reaches-another-env-var-only", chartOnly,
			mut(chartFile(web), "            - name: BACKEND_URL\n              value: {{ required", "            - name: BACKEND_URL\n              value: \"http://wrong.example\"\n            - name: UNUSED_BACKEND\n              value: {{ required"),
			webBackendURLRequired, "BACKEND_URL env is"},
		{"web-backend-url-gets-a-default", chartOnly,
			mut(chartFile("values.yaml"), `    BACKEND_URL: ""`, `    BACKEND_URL: "http://dflt:1"`),
			webBackendURLRequired, "succeeded"},
	})
}
