package devhealth_test

import (
	"sort"
	"strings"
	"testing"
)

// CHAOS-13346: with networkPolicy.enabled the in-chart Valkey admits TCP 6379 from its 8 real clients only, sends nothing, and
// leaves the release-wide policy; no other workload carries a Valkey URL. Every check reads the render.

var valkeyURLNames = []string{"VALKEY_URI", "REDIS_URL", "AUTH_VALKEY_URI"}

var valkeyProdShape = append(append(append([]string{}, authBase...), authValkey...),
	"--set", "networkPolicy.enabled=true", "--set", "goWorkers.pgbouncer.postgres.networkPolicyCIDR=10.0.0.0/24",
	"--set", "goApi.enabled=true", "--set", "goApi.internal.enabled=true", "--set", "queryApi.enabled=true",
	"--set", "goApi.extraEnv[0].name=VALKEY_URI", "--set", "goApi.extraEnv[0].value=redis://x:6379/1")

var valkeyClientWorkloads = []string{
	"a-dev-health-auth-service", "a-dev-health-go-api", "a-dev-health-go-stream-external", "a-dev-health-go-stream-ingest",
	"a-dev-health-go-stream-pagerduty", "a-dev-health-go-sync", "a-dev-health-go-sync-provider", "a-dev-health-web",
}

type valkeyWorkload struct {
	kind, name string
	labels     map[string]string
	carries    []string
}

func valkeyRender(t *testing.T, args ...string) ([]map[string]any, []valkeyWorkload) {
	t.Helper()
	docs, out, err := renderAuth(t, args...)
	if err != nil {
		t.Fatalf("render failed: %v\n%.600s", err, out)
	}
	keysOf := func(kind, name string) []string {
		doc := authDoc(docs, kind, name)
		var keys []string
		for _, field := range []string{"data", "stringData"} {
			for k := range aMap(at(doc, field)) {
				keys = append(keys, k)
			}
		}
		return keys
	}
	var workloads []valkeyWorkload
	for _, doc := range docs {
		kind := aStr(doc["kind"])
		if kind != "Deployment" && kind != "StatefulSet" && kind != "Job" {
			continue
		}
		name := aStr(at(doc, "metadata", "name"))
		if name == "a-dev-health-valkey" {
			continue
		}
		w := valkeyWorkload{kind: kind, name: name, labels: map[string]string{}}
		for k, v := range aMap(at(doc, "spec", "template", "metadata", "labels")) {
			w.labels[k] = aStr(v)
		}
		carry := map[string]bool{}
		for _, c := range append(aList(at(doc, "spec", "template", "spec", "containers")), aList(at(doc, "spec", "template", "spec", "initContainers"))...) {
			for _, e := range aList(at(c, "env")) {
				n := aStr(at(e, "name"))
				for _, v := range valkeyURLNames {
					if n == v || n == v+"_FILE" {
						carry[n] = true
					}
				}
			}
			for _, f := range aList(at(c, "envFrom")) {
				var keys []string
				if cm := aStr(at(f, "configMapRef", "name")); cm != "" {
					keys = keysOf("ConfigMap", cm)
				}
				if sec := aStr(at(f, "secretRef", "name")); sec != "" {
					keys = keysOf("Secret", sec)
				}
				for _, k := range keys {
					for _, v := range valkeyURLNames {
						if k == v {
							carry[k+" (envFrom)"] = true
						}
					}
				}
			}
		}
		for k := range carry {
			w.carries = append(w.carries, k)
		}
		sort.Strings(w.carries)
		workloads = append(workloads, w)
	}
	return docs, workloads
}

func valkeyPolicy(docs []map[string]any) map[string]any {
	return authDoc(docs, "NetworkPolicy", "a-dev-health-valkey")
}

func peerMatches(peer any, labels map[string]string) bool {
	match := aMap(at(peer, "podSelector", "matchLabels"))
	if len(match) == 0 {
		return true
	}
	for k, v := range match {
		if labels[k] != aStr(v) {
			return false
		}
	}
	return true
}

func TestValkeyPolicyAdmitsExactlyTheEightClients(t *testing.T) {
	docs, workloads := valkeyRender(t, valkeyProdShape...)
	policy := valkeyPolicy(docs)
	if policy == nil {
		t.Fatal("no Valkey NetworkPolicy rendered")
	}
	if at(policy, "spec", "podSelector", "matchLabels", "app.kubernetes.io/component") != "valkey" {
		t.Errorf("the policy selects %v, want the valkey component", at(policy, "spec", "podSelector"))
	}
	if got := strings.Join(toStrings(at(policy, "spec", "policyTypes")), ","); got != "Ingress,Egress" {
		t.Errorf("policyTypes = %s", got)
	}
	// (v) empty egress
	if eg, ok := aMap(policy["spec"])["egress"]; !ok || len(aList(eg)) != 0 {
		t.Errorf("egress = %v, want an empty list", eg)
	}
	rules := aList(at(policy, "spec", "ingress"))
	if len(rules) != 1 {
		t.Fatalf("%d ingress rules, want 1", len(rules))
	}
	ports := aList(at(rules, 0, "ports"))
	if len(ports) != 1 || aStr(at(ports, 0, "protocol")) != "TCP" || at(ports, 0, "port") != 6379 {
		t.Errorf("ports = %v, want TCP 6379 only", ports)
	}
	peers := aList(at(rules, 0, "from"))
	if len(peers) != len(valkeyClientWorkloads) {
		t.Errorf("%d peers, want %d", len(peers), len(valkeyClientWorkloads))
	}
	for _, p := range peers {
		if len(aMap(at(p, "podSelector", "matchLabels"))) == 0 || at(p, "namespaceSelector") != nil || at(p, "ipBlock") != nil {
			t.Errorf("peer %v is not one pod selector with labels", p)
		}
	}
	// (i) allowed set == the 8 clients
	var allowed []string
	for _, w := range workloads {
		for _, p := range peers {
			if peerMatches(p, w.labels) {
				allowed = append(allowed, w.name)
				break
			}
		}
	}
	sort.Strings(allowed)
	if strings.Join(allowed, " ") != strings.Join(valkeyClientWorkloads, " ") {
		t.Errorf("admitted workloads = %v\nwant %v", allowed, valkeyClientWorkloads)
	}
}

func TestValkeyLeavesTheReleaseWidePolicy(t *testing.T) {
	docs, _ := valkeyRender(t, valkeyProdShape...)
	wide := authDoc(docs, "NetworkPolicy", "a-dev-health")
	if wide == nil {
		t.Fatal("no release-wide policy")
	}
	var notIn []string
	for _, e := range aList(at(wide, "spec", "podSelector", "matchExpressions")) {
		if at(e, "key") == "app.kubernetes.io/component" && at(e, "operator") == "NotIn" {
			notIn = toStrings(at(e, "values"))
		}
	}
	if !contains(notIn, "valkey") {
		t.Errorf("the release-wide policy NotIn list %v does not exclude valkey", notIn)
	}
}

func TestValkeyURLReachesOnlyTheClients(t *testing.T) {
	_, workloads := valkeyRender(t, valkeyProdShape...)
	clients := map[string]bool{}
	for _, n := range valkeyClientWorkloads {
		clients[n] = true
	}
	seen := map[string]bool{}
	for _, w := range workloads {
		if clients[w.name] {
			seen[w.name] = true
			if len(w.carries) == 0 {
				t.Errorf("client %s carries no Valkey URL", w.name) // (iv)
			}
			continue
		}
		if len(w.carries) != 0 { // (iii)
			t.Errorf("%s %s is not a Valkey client but carries %v", w.kind, w.name, w.carries)
		}
	}
	for n := range clients {
		if !seen[n] {
			t.Errorf("client workload %s not rendered", n)
		}
	}
}

func TestValkeyClientURLIsPerWorkloadNotInTheConfigMaps(t *testing.T) {
	docs, _ := valkeyRender(t, valkeyProdShape...)
	for _, doc := range docs {
		if aStr(doc["kind"]) != "ConfigMap" {
			continue
		}
		for _, k := range []string{"VALKEY_URI", "REDIS_URL"} {
			if _, ok := aMap(doc["data"])[k]; ok {
				t.Errorf("ConfigMap %v carries %s", at(doc, "metadata", "name"), k)
			}
		}
	}
}

// go-api's URL is the values file's (goApi.extraEnv); the chart renders none of its own for go-api.
func TestValkeyGoAPIURLComesFromExtraEnvOnly(t *testing.T) {
	var bare []string
	for i := 0; i < len(valkeyProdShape); i++ {
		if strings.HasPrefix(valkeyProdShape[i], "goApi.extraEnv[0]") {
			bare = bare[:len(bare)-1]
			continue
		}
		bare = append(bare, valkeyProdShape[i])
	}
	_, workloads := valkeyRender(t, bare...)
	for _, w := range workloads {
		if w.name == "a-dev-health-go-api" && len(w.carries) != 0 {
			t.Errorf("go-api carries %v without goApi.extraEnv", w.carries)
		}
	}
}

func TestValkeyPolicyOffChangesNothing(t *testing.T) {
	off := make([]string, 0, len(valkeyProdShape))
	for i := 0; i < len(valkeyProdShape); i++ {
		if valkeyProdShape[i] == "networkPolicy.enabled=true" || valkeyProdShape[i] == "goWorkers.pgbouncer.postgres.networkPolicyCIDR=10.0.0.0/24" {
			off = off[:len(off)-1] // drop the preceding --set
			continue
		}
		off = append(off, valkeyProdShape[i])
	}
	for name, args := range map[string][]string{
		"networkPolicy off":        off,
		"valkey.networkPolicy off": append(append([]string{}, valkeyProdShape...), "--set", "valkey.networkPolicy.enabled=false"),
	} {
		t.Run(name, func(t *testing.T) {
			docs, workloads := valkeyRender(t, args...)
			if valkeyPolicy(docs) != nil {
				t.Error("a Valkey policy rendered")
			}
			if wide := authDoc(docs, "NetworkPolicy", "a-dev-health"); wide != nil {
				for _, e := range aList(at(wide, "spec", "podSelector", "matchExpressions")) {
					if contains(toStrings(at(e, "values")), "valkey") {
						t.Error("the release-wide policy excludes valkey")
					}
				}
			}
			for _, w := range workloads {
				if contains(valkeyClientWorkloads, w.name) && len(w.carries) == 0 {
					t.Errorf("client %s lost its URL", w.name)
				}
			}
		})
	}
}
