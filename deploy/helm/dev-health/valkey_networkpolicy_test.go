package devhealth_test

import (
	"bytes"
	"os"
	"os/exec"
	"sort"
	"strings"
	"testing"
)

// CHAOS-13346: with networkPolicy.enabled the in-chart Valkey admits TCP 6379 from its 8 real clients only, sends nothing, and
// leaves the release-wide policy; no other workload carries a Valkey URL. Every check reads the render.

var valkeyURLNames = []string{"VALKEY_URI", "REDIS_URL", "AUTH_VALKEY_URI"}

var valkeyProdShape = append(append(append([]string{}, authBase...), authValkey...),
	"--set", "networkPolicy.enabled=true", "--set", "goWorkers.pgbouncer.postgres.networkPolicyCIDR=10.0.0.0/24",
	"--set", "goApi.enabled=true", "--set", "goApi.internal.enabled=true", "--set", "queryApi.enabled=true", "--set", "valkey.networkPolicy.enabled=true")

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

// (iv) every client still gets a URL: web, auth-service and the groups from the chart (envFrom of the shared ConfigMap for the
// groups); go-api from its values file, as the deploy values set it.
func TestValkeyEveryClientStillGetsItsURL(t *testing.T) {
	_, workloads := valkeyRender(t, append(append([]string{}, valkeyProdShape...), "--set", "goApi.extraEnv[0].name=VALKEY_URI", "--set", "goApi.extraEnv[0].value=redis://x:6379/1")...)
	seen := 0
	for _, w := range workloads {
		if contains(valkeyClientWorkloads, w.name) {
			seen++
			if len(w.carries) == 0 {
				t.Errorf("client %s carries no Valkey URL", w.name)
			}
		}
	}
	if seen != len(valkeyClientWorkloads) {
		t.Errorf("%d client workloads rendered, want %d", seen, len(valkeyClientWorkloads))
	}
}

// (vi) the gate is off by default and with networkPolicy.enabled=false: no Valkey policy, valkey not excluded.
func TestValkeyPolicyOffChangesNothing(t *testing.T) {
	var gateDefault, npOff []string
	for i := 0; i < len(valkeyProdShape); i++ {
		a := valkeyProdShape[i]
		if a == "valkey.networkPolicy.enabled=true" {
			gateDefault = gateDefault[:len(gateDefault)-1]
		} else {
			gateDefault = append(gateDefault, a)
		}
		if a == "valkey.networkPolicy.enabled=true" || a == "networkPolicy.enabled=true" || a == "goWorkers.pgbouncer.postgres.networkPolicyCIDR=10.0.0.0/24" {
			if len(npOff) > 0 {
				npOff = npOff[:len(npOff)-1]
			}
			continue
		}
		npOff = append(npOff, a)
	}
	for name, args := range map[string][]string{"gate default": gateDefault, "networkPolicy off": npOff} {
		t.Run(name, func(t *testing.T) {
			docs, _ := valkeyRender(t, args...)
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
		})
	}
}

// valkeyBaseSha is the ops main commit this change started from. The gate renders NetworkPolicy objects only, so against that
// chart: gate off is byte-equal, and gate on differs in NetworkPolicy manifests alone (no env, no checksum/config, no ConfigMap).
const valkeyBaseSha = "5b57fe79f46531f4e25cefbd056454df218c0778" // gitleaks:allow

func TestValkeyGateRendersNetworkPolicyObjectsOnly(t *testing.T) {
	ci := os.Getenv("CI") != ""
	for _, tool := range []string{"helm", "git", "tar"} {
		if _, err := exec.LookPath(tool); err != nil {
			if ci {
				t.Fatalf("CI is set and %s is not installed", tool)
			}
			t.Skip(tool + " is not installed")
		}
	}
	if skip, err := checkGoldenBase("../../..", valkeyBaseSha, ci); skip != "" {
		t.Skip(skip)
	} else if err != nil {
		t.Fatal(err)
	}
	baseDir := t.TempDir()
	tarball, err := exec.Command("git", "-c", "maintenance.auto=false", "-c", "gc.auto=0", "-C", "../../..", "archive", valkeyBaseSha, "deploy/helm/dev-health").Output()
	if err != nil {
		t.Fatalf("git archive: %v", err)
	}
	untar := exec.Command("tar", "-x", "-C", baseDir)
	untar.Stdin = bytes.NewReader(tarball)
	if out, err := untar.CombinedOutput(); err != nil {
		t.Fatalf("tar: %v %s", err, out)
	}
	render := func(dir string, args []string) []string {
		cmd := helmCommand(append([]string{"template", "a", "."}, args...)...)
		cmd.Dir = dir
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("render in %s: %v\n%.300s", dir, err, out)
		}
		return strings.Split(string(out), "\n---\n")
	}
	withoutPolicies := func(docs []string) string {
		var keep []string
		for _, d := range docs {
			if strings.Contains(d, "\nkind: NetworkPolicy\n") {
				continue
			}
			keep = append(keep, d)
		}
		return strings.Join(keep, "\n---\n")
	}
	chart := baseDir + "/deploy/helm/dev-health"
	var gateDefault []string
	for i := 0; i < len(valkeyProdShape); i++ {
		if valkeyProdShape[i] == "valkey.networkPolicy.enabled=true" {
			gateDefault = gateDefault[:len(gateDefault)-1]
			continue
		}
		gateDefault = append(gateDefault, valkeyProdShape[i])
	}
	// (vi) gate off: the whole render is the base's.
	if strings.Join(render(chart, gateDefault), "\n---\n") != strings.Join(render(".", gateDefault), "\n---\n") {
		t.Error("with the gate off the render differs from the base")
	}
	// (vii) gate on: only NetworkPolicy manifests differ.
	if withoutPolicies(render(chart, valkeyProdShape)) != withoutPolicies(render(".", valkeyProdShape)) {
		t.Error("with the gate on a manifest other than a NetworkPolicy differs from the base")
	}
}
