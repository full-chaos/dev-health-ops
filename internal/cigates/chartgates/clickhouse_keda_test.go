package chartgates_test

import (
	"encoding/json"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// Ported from tests/test_helm_clickhouse_usersd.py (CHAOS-6181, CHAOS-6474) and
// tests/test_helm_go_worker_keda_extra_triggers.py (CHAOS-6938).

const usersdXML = "<clickhouse><users><ro><password_sha256_hex>" +
	"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" +
	"</password_sha256_hex><profile>ro_profile</profile></ro></users></clickhouse>"

func chDocs(g *guard, extra ...string) []doc {
	return g.docs("t", join([]string{"--show-only", "templates/clickhouse.yaml"}, extra)...)
}

func sortedKinds(docs []doc) []string {
	var out []string
	for _, d := range docs {
		out = append(out, kind(d))
	}
	sort.Strings(out)
	return out
}

func sts(docs []doc) doc {
	for _, d := range docs {
		if kind(d) == "StatefulSet" {
			return d
		}
	}
	return nil
}

func mountPaths(c doc) []string {
	var out []string
	for _, m := range list(c["volumeMounts"]) {
		out = append(out, str(at(m, "mountPath")))
	}
	return out
}

func usersdArg(file, content string) []string {
	return setString("clickhouse.usersd." + strings.ReplaceAll(file, ".", `\.`) + "=" + content)
}

func usersdDefaultRendersNothing(g *guard) {
	docs := chDocs(g)
	g.expect(equal(sortedKinds(docs), []string{"Service", "StatefulSet"}), "default render kinds %v, want [Service StatefulSet]", sortedKinds(docs))
	pod := mp(at(sts(docs), "spec", "template"))
	_, vols := mp(pod["spec"])["volumes"]
	_, anns := mp(pod["metadata"])["annotations"]
	g.expect(!vols, "the default pod carries volumes")
	g.expect(!anns, "the default pod carries annotations")
	g.expect(equal(mountPaths(containers(sts(docs))[0]), []string{"/var/lib/clickhouse"}), "default mounts %v", mountPaths(containers(sts(docs))[0]))
}

func usersdConfigMapDirectoryMountAndLinks(g *guard) {
	docs := chDocs(g, usersdArg("acr_ro.xml", usersdXML)...)
	cm := named(docs, "ConfigMap", "t-dev-health-clickhouse-usersd")
	g.expect(cm != nil, "the usersd ConfigMap t-dev-health-clickhouse-usersd was not rendered")
	g.expect(equal(mp(cm["data"]), doc{"acr_ro.xml": usersdXML}), "ConfigMap data %v", cm["data"])
	set := sts(docs)
	if set == nil {
		g.failf("no StatefulSet")
		return
	}
	pod := podSpec(set)
	wantVols := []any{
		doc{"name": "clickhouse-usersd", "configMap": doc{"name": "t-dev-health-clickhouse-usersd"}},
		doc{"name": "clickhouse-usersd-links", "emptyDir": doc{}},
	}
	g.expect(equal(pod["volumes"], wantVols), "pod volumes %v", pod["volumes"])
	c := containers(set)[0]
	mounts := map[string]doc{}
	for _, m := range list(c["volumeMounts"]) {
		mounts[str(at(m, "mountPath"))] = mp(m)
		_, sub := mp(m)["subPath"]
		g.expect(!sub, "mount %v uses a subPath: kubelet never refreshes those; the ConfigMap must be a directory mount", m)
	}
	g.expect(mounts["/var/lib/clickhouse"] != nil, "the data mount is missing")
	cmm := mounts["/etc/clickhouse-server/usersd-cm"]
	g.expect(cmm != nil && str(cmm["name"]) == "clickhouse-usersd" && cmm["readOnly"] == true, "the ConfigMap directory mount is %v", cmm)
	g.expect(mounts["/etc/clickhouse-server/users.d"] != nil && str(mounts["/etc/clickhouse-server/users.d"]["name"]) == "clickhouse-usersd-links", "users.d must be the emptyDir the links live in")
	inits := initContainers(set)
	if len(inits) != 1 {
		g.failf("want exactly one initContainer, got %d", len(inits))
		return
	}
	init := inits[0]
	g.expect(str(init["name"]) == "clickhouse-usersd-links", "initContainer name %q", str(init["name"]))
	g.expect(str(init["image"]) == str(c["image"]), "the link initContainer must use the server image")
	cmd := strs(init["command"])
	g.expect(len(cmd) >= 2 && equal(cmd[len(cmd)-2:], []string{"--", "acr_ro.xml"}), "initContainer command tail %v", cmd)
	g.expect(len(cmd) > 2 && strings.Contains(cmd[2], "../usersd-cm/$n"), "the link script does not point at ../usersd-cm/$n")
	g.expect(equal(init["volumeMounts"], []any{doc{"name": "clickhouse-usersd-links", "mountPath": "/links"}}), "initContainer mounts %v", init["volumeMounts"])
}

func usersdAnnotations(g *guard, args ...string) doc {
	return mp(at(sts(chDocs(g, args...)), "spec", "template", "metadata", "annotations"))
}

func usersdContentChangeDoesNotRoll(g *guard) {
	a := usersdAnnotations(g, usersdArg("a.xml", usersdXML)...)
	b := usersdAnnotations(g, usersdArg("a.xml", usersdXML+"x")...)
	g.expect(equal(a, b), "a users.d CONTENT change rolls the pod: kubelet refreshes the directory mount and ClickHouse reloads without a restart")
	_, sum := a["checksum/clickhouse-usersd"]
	g.expect(!sum, "the pod carries a content checksum annotation")
}

func usersdFileSetChangeRolls(g *guard) {
	a := usersdAnnotations(g, usersdArg("a.xml", usersdXML)...)
	b := usersdAnnotations(g, join(usersdArg("a.xml", usersdXML), usersdArg("b.xml", usersdXML))...)
	g.expect(!equal(a, b), "a new users.d file does not roll the pod, so the initContainer never links it")
}

func usersdWithoutPersistence(g *guard) {
	set := sts(chDocs(g, join(set("clickhouse.persistence.enabled=false"), usersdArg("a.xml", usersdXML))...))
	if set == nil {
		g.failf("no StatefulSet")
		return
	}
	got := mountPaths(containers(set)[0])
	g.expect(equal(got, []string{"/etc/clickhouse-server/users.d", "/etc/clickhouse-server/usersd-cm"}), "mounts without persistence %v", got)
}

func usersdNullRendersLikeDefault(g *guard) {
	docs := chDocs(g, setJSON("clickhouse.usersd=null")...)
	g.expect(equal(sortedKinds(docs), []string{"Service", "StatefulSet"}), "usersd=null kinds %v, want the default shape", sortedKinds(docs))
}

func usersdWhitespaceAndNumericNameRoundTrip(g *guard) {
	content := "  <clickhouse>\n<users/>\n</clickhouse>"
	b, _ := json.Marshal(map[string]string{"123": content, "b.xml": content})
	docs := chDocs(g, setJSON("clickhouse.usersd="+string(b))...)
	cm := named(docs, "ConfigMap", "t-dev-health-clickhouse-usersd")
	g.expect(cm != nil && equal(mp(cm["data"]), doc{"123": content, "b.xml": content}), "ConfigMap data did not round-trip leading whitespace and a numeric file name: %v", at(cm, "data"))
	inits := initContainers(sts(docs))
	if len(inits) == 1 {
		cmd := strs(inits[0]["command"])
		g.expect(len(cmd) >= 3 && equal(cmd[3:], []string{"--", "123", "b.xml"}), "link command %v", cmd)
	} else {
		g.failf("want one initContainer, got %d", len(inits))
	}
}

// --- KEDA extra triggers ---

const plannedQuery = "SELECT count(*) FROM public.sync_run_units WHERE status = 'planned' " +
	"AND (available_at IS NULL OR available_at <= now())"

func scaledByGroup(g *guard, groups []any) map[string]doc {
	var docs []doc
	if groups == nil {
		docs = g.docs("t")
	} else {
		docs = g.docs("t", groupsArg(groups)...)
	}
	out := map[string]doc{}
	for _, d := range ofKind(docs, "ScaledObject") {
		out[strings.TrimPrefix(str(at(d, "spec", "scaleTargetRef", "name")), "t-dev-health-go-")] = d
	}
	return out
}

func syncTriggers(g *guard, s map[string]doc) []any {
	d := s["sync"]
	if d == nil {
		g.failf("no ScaledObject for the sync group")
		return nil
	}
	return list(at(d, "spec", "triggers"))
}

func withSyncAutoscaling(g *guard, edit func(as doc)) []any {
	groups := cloneGroups(g, valuesGroups(g))
	for _, grp := range groups {
		if str(at(grp, "name")) == "sync" {
			edit(mp(mp(grp)["autoscaling"]))
		}
	}
	return groups
}

func kedaSyncHasRiverAndPlannedTriggers(g *guard) {
	tr := syncTriggers(g, scaledByGroup(g, nil))
	if len(tr) != 2 {
		g.failf("the sync scaler has %d triggers, want the river trigger and the planned-units trigger", len(tr))
		return
	}
	g.expect(strings.Contains(str(at(tr[0], "metadata", "query")), "river_job"), "the first trigger is not the river_job trigger")
	g.expect(str(at(tr[1], "type")) == "postgresql" && str(at(tr[1], "name")) == "planned-units", "second trigger is %v", tr[1])
	g.expect(strings.Join(strings.Fields(str(at(tr[1], "metadata", "query"))), " ") == plannedQuery, "planned-units query is %q", str(at(tr[1], "metadata", "query")))
	g.expect(str(at(tr[1], "metadata", "targetQueryValue")) == "150", "planned-units target is %q, want 150", str(at(tr[1], "metadata", "targetQueryValue")))
}

func kedaExtraTriggerConnectsLikeRiver(g *guard) {
	tr := syncTriggers(g, scaledByGroup(g, nil))
	if len(tr) != 2 {
		g.failf("the sync scaler has %d triggers, want 2", len(tr))
		return
	}
	for _, k := range []string{"host", "port", "userName", "dbName", "sslmode"} {
		g.expect(str(at(tr[0], "metadata", k)) == str(at(tr[1], "metadata", k)), "extra trigger %s %q differs from the river trigger %q", k, str(at(tr[1], "metadata", k)), str(at(tr[0], "metadata", k)))
	}
	g.expect(equal(at(tr[0], "authenticationRef"), at(tr[1], "authenticationRef")), "extra trigger authenticationRef differs from the river trigger")
}

func kedaSyncScalesToTwoAndNoFurther(g *guard) {
	d := scaledByGroup(g, nil)["sync"]
	g.expect(d != nil && at(d, "spec", "minReplicaCount") == 1 && at(d, "spec", "maxReplicaCount") == 2, "go-sync replica bounds are %v..%v, want 1..2", at(d, "spec", "minReplicaCount"), at(d, "spec", "maxReplicaCount"))
}

func kedaOtherGroupsKeepSingleTrigger(g *guard) {
	for n, d := range scaledByGroup(g, nil) {
		if n == "sync" {
			continue
		}
		tr := list(at(d, "spec", "triggers"))
		g.expect(len(tr) == 1 && strings.Contains(str(at(tr, 0, "metadata", "query")), "river_job"), "%s must keep its single river_job trigger, has %d", n, len(tr))
	}
}

var fromJoin = regexp.MustCompile(`(?i)\b(?:FROM|JOIN)\s+([\w.]+)`)

func kedaPlannedQueryReadsOnlyGrantedTable(g *guard) {
	tr := syncTriggers(g, scaledByGroup(g, nil))
	if len(tr) != 2 {
		g.failf("the sync scaler has %d triggers, want 2", len(tr))
		return
	}
	var tables []string
	for _, m := range fromJoin.FindAllStringSubmatch(str(at(tr[1], "metadata", "query")), -1) {
		tables = append(tables, m[1])
	}
	g.expect(equal(tables, []string{"public.sync_run_units"}), "the planned query reads %v; the KEDA login holds SELECT on public.sync_run_units and river_job only", tables)
}

func kedaEmptyExtraTriggersRendersLikeNone(g *guard) {
	groups := withSyncAutoscaling(g, func(as doc) { as["extraTriggers"] = []any{} })
	g.expect(len(syncTriggers(g, scaledByGroup(g, groups))) == 1, "an empty extraTriggers list must render like no list")
}

func kedaExtraTriggersOnNonAutoscalingRefused(g *guard) {
	groups := cloneGroups(g, valuesGroups(g))
	for _, grp := range groups {
		if str(at(grp, "name")) == "sync" {
			mp(grp)["autoscaling"] = doc{"enabled": false, "extraTriggers": []any{doc{"name": "x", "query": "SELECT 1", "targetQueryValue": 5}}}
		}
	}
	stderr := g.refusal("extraTriggers on a group that does not autoscale", "t", groupsArg(groups)...)
	g.expect(strings.Contains(stderr, "extraTriggers") && strings.Contains(stderr, "autoscaling.enabled"), "the refusal does not name extraTriggers and autoscaling.enabled: %s", firstLines(stderr, 2))
}

func kedaInvalidExtraTriggerRefused(g *guard) {
	bads := []doc{
		{"name": "x", "targetQueryValue": 5},
		{"name": "x", "query": "SELECT 1"},
		{"name": "x", "query": "", "targetQueryValue": 5},
		{"name": "x", "query": "SELECT 1", "targetQueryValue": 0},
	}
	for _, bad := range bads {
		groups := withSyncAutoscaling(g, func(as doc) { as["extraTriggers"] = []any{bad} })
		r := g.tpl("t", groupsArg(groups)...)
		g.expect(r.code != 0, "extra trigger %v was accepted: a trigger without a query or a target cannot be evaluated by KEDA", bad)
	}
}

func kedaDuplicateTriggerNamesRefused(g *guard) {
	trig := doc{"name": "dup", "query": "SELECT 1", "targetQueryValue": 5}
	groups := withSyncAutoscaling(g, func(as doc) { as["extraTriggers"] = []any{trig, trig} })
	stderr := g.refusal("duplicate extra trigger names", "t", groupsArg(groups)...)
	g.expect(strings.Contains(stderr, "dup"), "the refusal does not name the duplicate trigger: %s", firstLines(stderr, 2))
}

func TestClickHouseKedaGuardsRealTree(t *testing.T) {
	runReal(t,
		check{"usersd-default-renders-nothing", usersdDefaultRendersNothing},
		check{"usersd-configmap-directory-mount-and-links", usersdConfigMapDirectoryMountAndLinks},
		check{"usersd-content-change-does-not-roll", usersdContentChangeDoesNotRoll},
		check{"usersd-file-set-change-rolls", usersdFileSetChangeRolls},
		check{"usersd-without-persistence", usersdWithoutPersistence},
		check{"usersd-null-renders-like-default", usersdNullRendersLikeDefault},
		check{"usersd-whitespace-and-numeric-name", usersdWhitespaceAndNumericNameRoundTrip},
		check{"keda-sync-river-and-planned-triggers", kedaSyncHasRiverAndPlannedTriggers},
		check{"keda-extra-trigger-connects-like-river", kedaExtraTriggerConnectsLikeRiver},
		check{"keda-sync-scales-to-two", kedaSyncScalesToTwoAndNoFurther},
		check{"keda-other-groups-single-trigger", kedaOtherGroupsKeepSingleTrigger},
		check{"keda-planned-query-reads-granted-table", kedaPlannedQueryReadsOnlyGrantedTable},
		check{"keda-empty-extra-triggers-like-none", kedaEmptyExtraTriggersRendersLikeNone},
		check{"keda-extra-triggers-on-non-autoscaling-refused", kedaExtraTriggersOnNonAutoscalingRefused},
		check{"keda-invalid-extra-trigger-refused", kedaInvalidExtraTriggerRefused},
		check{"keda-duplicate-trigger-names-refused", kedaDuplicateTriggerNamesRefused},
	)
}

func TestClickHouseKedaGuardsPlantedDefects(t *testing.T) {
	const ch = "templates/clickhouse.yaml"
	const keda = "templates/go-worker-keda.yaml"
	const plannedBlock = "        maxReplicas: 2\n        queueDepthPerReplica: 5\n        cooldownPeriod: 300\n        pollingInterval: 30\n        extraTriggers:"
	runPlanted(t, []planted{
		{"usersd-default-gains-a-volume", chartOnly,
			mut(chartFile(ch), "      {{- if or .Values.clickhouse.usersd .Values.clickhouse.configd }}\n      volumes:", "      {{- if true }}\n      volumes:"),
			usersdDefaultRendersNothing, "default pod carries volumes"},
		{"usersd-configmap-mounted-by-subpath", chartOnly,
			mut(chartFile(ch), "              mountPath: /etc/clickhouse-server/usersd-cm\n              readOnly: true", "              mountPath: /etc/clickhouse-server/usersd-cm\n              subPath: a.xml\n              readOnly: true"),
			usersdConfigMapDirectoryMountAndLinks, "uses a subPath"},
		{"usersd-link-script-drops-the-file-names", chartOnly,
			mut(chartFile(ch), "            - {{ $name | quote }}\n            {{- end }}\n          volumeMounts:", "            - {{ printf \"%s.bak\" $name | quote }}\n            {{- end }}\n          volumeMounts:"),
			usersdConfigMapDirectoryMountAndLinks, "initContainer command tail"},
		{"usersd-content-rolls-the-pod", chartOnly,
			mut(chartFile(ch), "keys (.Values.clickhouse.usersd | default dict) | sortAlpha | toJson | sha256sum", ".Values.clickhouse.usersd | toJson | sha256sum"),
			usersdContentChangeDoesNotRoll, "CONTENT change rolls the pod"},
		{"usersd-file-set-change-does-not-roll", chartOnly,
			mut(chartFile(ch), "        {{- if .Values.clickhouse.usersd }}\n        checksum/clickhouse-usersd-files:", "        {{- if false }}\n        checksum/clickhouse-usersd-files:"),
			usersdFileSetChangeRolls, "does not roll the pod"},
		{"usersd-null-renders-the-objects", chartOnly,
			mut(chartFile(ch), "{{- if .Values.clickhouse.usersd }}\n---\n# CHAOS-6181", "{{- if true }}\n---\n# CHAOS-6181"),
			usersdNullRendersLikeDefault, "want the default shape"},
		{"keda-planned-query-reads-another-status", chartOnly,
			mut(chartFile("values.yaml"), "status = 'planned'", "status = 'queued'"),
			kedaSyncHasRiverAndPlannedTriggers, "planned-units query"},
		{"keda-planned-query-reads-an-ungranted-table", chartOnly,
			mut(chartFile("values.yaml"), "SELECT count(*) FROM public.sync_run_units\n", "SELECT count(*) FROM public.sync_run_units JOIN public.users ON true\n"),
			kedaPlannedQueryReadsOnlyGrantedTable, "the planned query reads"},
		{"keda-extra-trigger-uses-another-sslmode", chartOnly,
			mut(chartFile(keda), "        sslmode: disable\n        query: >-\n          {{ $trigger.query }}", "        sslmode: require\n        query: >-\n          {{ $trigger.query }}"),
			kedaExtraTriggerConnectsLikeRiver, "sslmode"},
		{"keda-sync-may-scale-to-three", chartOnly,
			mut(chartFile("values.yaml"), plannedBlock, strings.Replace(plannedBlock, "maxReplicas: 2", "maxReplicas: 3", 1)),
			kedaSyncScalesToTwoAndNoFurther, "replica bounds"},
		{"keda-every-group-gets-the-extra-trigger", chartOnly,
			mut(chartFile(keda), `{{- $extraTriggers := $group.autoscaling.extraTriggers | default (list) }}`, `{{- $extraTriggers := list (dict "name" "x" "query" "SELECT 1" "targetQueryValue" 5) }}`),
			kedaOtherGroupsKeepSingleTrigger, "must keep its single river_job trigger"},
		{"keda-empty-list-adds-a-trigger", chartOnly,
			mut(chartFile(keda), `{{- $extraTriggers := $group.autoscaling.extraTriggers | default (list) }}`, `{{- $extraTriggers := $group.autoscaling.extraTriggers | default (list (dict "name" "x" "query" "SELECT 1" "targetQueryValue" 5)) }}`),
			kedaEmptyExtraTriggersRendersLikeNone, "must render like no list"},
		{"keda-extra-triggers-silently-ignored-without-autoscaling", chartOnly,
			mut(chartFile(keda), "{{- if and (not $group.autoscaling.enabled) $group.autoscaling.extraTriggers }}", "{{- if false }}"),
			kedaExtraTriggersOnNonAutoscalingRefused, "extraTriggers on a group that does not autoscale"},
		{"keda-trigger-without-a-query-accepted", chartOnly,
			all(mut(chartFile(keda), "{{- if not $trigger.query }}", "{{- if false }}"),
				mut(chartFile("values.schema.json"), `"required": ["name", "query", "targetQueryValue"],`, `"required": ["name", "targetQueryValue"],`),
				mut(chartFile("values.schema.json"), `"query": { "type": "string", "minLength": 1 },`, `"query": { "type": "string" },`)),
			kedaInvalidExtraTriggerRefused, "was accepted"},
		{"keda-trigger-with-zero-target-accepted", chartOnly,
			all(mut(chartFile(keda), "{{- if lt (int $trigger.targetQueryValue) 1 }}", "{{- if false }}"),
				mut(chartFile("values.schema.json"), `"targetQueryValue": { "type": "integer", "minimum": 1 }`, `"targetQueryValue": { "type": "integer", "minimum": 0 }`)),
			kedaInvalidExtraTriggerRefused, "was accepted"},
		{"keda-duplicate-trigger-names-accepted", chartOnly,
			mut(chartFile(keda), "{{- if hasKey $extraNames $trigger.name }}", "{{- if false }}"),
			kedaDuplicateTriggerNamesRefused, "duplicate extra trigger names"},
	})
}
