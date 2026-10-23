package devhealth_test

import (
	"fmt"
	"os/exec"
	"sort"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// The go-api request listeners (goApi.port and any extra one the deploy names, such as the billing edge) are open to every
// source today: the go-api-internal policy admits every TCP port but the internal one from anywhere. With
// goApi.networkPolicy.enabled the same policy closes the request ports to everything but the ingress controller pods and the
// peers named in goApi.networkPolicy.allowedFrom. NetworkPolicies are additive, so the fence has to take the ports out of the
// open ranges of that one policy; a second, narrower policy would change nothing.

var fenceControllerSource = netpolSource{
	"the ingress controller pod",
	map[string]string{namespaceNameLabel: defaultControllerNamespace},
	map[string]string{"app.kubernetes.io/name": "ingress-nginx", "app.kubernetes.io/component": "controller", "app.kubernetes.io/instance": "ingress-nginx"},
}

var (
	fenceControllerNamespaceOtherPod = netpolSource{
		"another pod of the ingress controller namespace",
		map[string]string{namespaceNameLabel: defaultControllerNamespace},
		map[string]string{"app.kubernetes.io/name": "ingress-nginx", "app.kubernetes.io/component": "admission-webhook"},
	}
	fenceLookalikeNamespace = netpolSource{
		"a controller-labelled pod of another namespace",
		map[string]string{namespaceNameLabel: "other-ns"},
		map[string]string{"app.kubernetes.io/name": "ingress-nginx", "app.kubernetes.io/component": "controller"},
	}
	fenceToolPod = netpolSource{"a labelled tools pod", map[string]string{namespaceNameLabel: releaseNamespace}, map[string]string{"role": "tools"}}
)

func renderFencePolicies(t *testing.T, args ...string) []netpolDoc {
	t.Helper()
	full := append([]string{"template", "fence", ".", "--namespace", releaseNamespace, "--set", "goApi.enabled=true",
		"--set", "goApi.networkPolicy.enabled=true", "--set", "goWorkers.pgbouncer.postgres.networkPolicyCIDR=10.0.0.0/24"}, args...)
	out, err := exec.Command("helm", full...).CombinedOutput()
	if err != nil {
		t.Fatalf("render: %v\n%s", err, out)
	}
	var policies []netpolDoc
	for _, document := range strings.Split(string(out), "\n---") {
		var parsed netpolDoc
		if err := yaml.Unmarshal([]byte(document), &parsed); err != nil {
			t.Fatalf("a document of the render does not parse: %v", err)
		}
		if parsed.Kind == "NetworkPolicy" {
			policies = append(policies, parsed)
		}
	}
	return policies
}

func fenceGoAPILabels() map[string]string {
	return map[string]string{"app.kubernetes.io/name": "dev-health", "app.kubernetes.io/instance": "fence", "app.kubernetes.io/component": "go-api"}
}

func allPorts() []int {
	ports := make([]int, 0, 65535)
	for port := 1; port <= 65535; port++ {
		ports = append(ports, port)
	}
	return ports
}

// allExcept is every TCP port but the named ones.
func allExcept(closed ...int) []int {
	shut := map[int]bool{}
	for _, port := range closed {
		shut[port] = true
	}
	var open []int
	for _, port := range allPorts() {
		if !shut[port] {
			open = append(open, port)
		}
	}
	return open
}

func sameInts(a, b []int) bool {
	sort.Ints(a)
	sort.Ints(b)
	return fmt.Sprint(a) == fmt.Sprint(b)
}

// closedPorts lists the ports (as a short list) the union of policies does NOT admit from source, over every TCP port.
func closedPorts(t *testing.T, policies []netpolDoc, source netpolSource) []int {
	t.Helper()
	admitted, selected := admittedFrom(policies, fenceGoAPILabels(), allPorts(), source)
	if !selected {
		t.Fatalf("no ingress policy selects the go-api pods")
	}
	open := map[int]bool{}
	for _, port := range admitted {
		open[port] = true
	}
	var closed []int
	for _, port := range allPorts() {
		if !open[port] {
			closed = append(closed, port)
		}
	}
	return closed
}

func TestGoAPIFenceClosesRequestPortsToEveryoneButTheController(t *testing.T) {
	for name, c := range map[string]struct {
		args            []string
		closedToOthers  []int
		closedToControl []int
	}{
		"api port only":              {nil, []int{8000, 8091}, []int{8091}},
		"api and billing edge":       {[]string{"--set-json", "goApi.networkPolicy.extraFencedPorts=[8010]"}, []int{8000, 8010, 8091}, []int{8091}},
		"another api port":           {[]string{"--set", "goApi.port=9000", "--set-json", "goApi.networkPolicy.extraFencedPorts=[8010]"}, []int{8010, 8091, 9000}, []int{8091}},
		"extra port that is the api": {[]string{"--set-json", "goApi.networkPolicy.extraFencedPorts=[8000,8010,8010]"}, []int{8000, 8010, 8091}, []int{8091}},
		"adjacent ports":             {[]string{"--set-json", "goApi.networkPolicy.extraFencedPorts=[8001,7999,8002]"}, []int{7999, 8000, 8001, 8002, 8091}, []int{8091}},
		"extremes":                   {[]string{"--set-json", "goApi.networkPolicy.extraFencedPorts=[1,65535]"}, []int{1, 8000, 8091, 65535}, []int{8091}},
	} {
		for _, topLevel := range []string{"false", "true"} {
			t.Run(name+" topLevel="+topLevel, func(t *testing.T) {
				policies := renderFencePolicies(t, append([]string{"--set", "networkPolicy.enabled=" + topLevel}, c.args...)...)
				for _, source := range []netpolSource{sourceStranger, sourceOtherNamespace, fenceControllerNamespaceOtherPod, fenceLookalikeNamespace, fenceToolPod} {
					if got := closedPorts(t, policies, source); !sameInts(got, c.closedToOthers) {
						t.Errorf("%s: closed ports %v, want %v", source.name, got, c.closedToOthers)
					}
				}
				if got := closedPorts(t, policies, fenceControllerSource); !sameInts(got, c.closedToControl) {
					t.Errorf("the ingress controller: closed ports %v, want %v", got, c.closedToControl)
				}
			})
		}
	}
}

// Probes and scrapes use the operator port (8080) and must stay open to every source, as before the fence.
func TestGoAPIFenceKeepsTheOperatorPortOpen(t *testing.T) {
	policies := renderFencePolicies(t, "--set-json", "goApi.networkPolicy.extraFencedPorts=[8010]")
	for _, source := range []netpolSource{sourceStranger, sourceOtherNamespace, fenceControllerSource} {
		admitted, _ := admittedFrom(policies, fenceGoAPILabels(), []int{8080}, source)
		if len(admitted) != 1 {
			t.Errorf("%s cannot reach the operator port 8080", source.name)
		}
	}
}

func TestGoAPIFenceAdmitsTheNamedPeersToTheRequestPorts(t *testing.T) {
	policies := renderFencePolicies(t, "--set-json", "goApi.networkPolicy.extraFencedPorts=[8010]",
		"--set-json", `goApi.networkPolicy.allowedFrom=[{"podSelector":{"matchLabels":{"role":"tools"}}}]`)
	if admitted, _ := admittedFrom(policies, fenceGoAPILabels(), []int{8000, 8010, 8091}, fenceToolPod); !sameInts(admitted, []int{8000, 8010}) {
		t.Errorf("the named tools pod is admitted to %v, want exactly 8000 and 8010", admitted)
	}
	if admitted, _ := admittedFrom(policies, fenceGoAPILabels(), []int{8000, 8010}, sourceStranger); len(admitted) != 0 {
		t.Errorf("an unnamed in-namespace pod is admitted to %v", admitted)
	}
}

func TestGoAPIFenceControllerPodLabelsAndNamespaceAreValues(t *testing.T) {
	policies := renderFencePolicies(t, "--set", "networkPolicy.ingressControllerNamespace=edge-proxy",
		"--set", "goApi.networkPolicy.ingressControllerPodLabels.app\\.kubernetes\\.io/component=ctl")
	moved := netpolSource{"the moved controller", map[string]string{namespaceNameLabel: "edge-proxy"},
		map[string]string{"app.kubernetes.io/name": "ingress-nginx", "app.kubernetes.io/component": "ctl"}}
	if admitted, _ := admittedFrom(policies, fenceGoAPILabels(), []int{8000}, moved); len(admitted) != 1 {
		t.Errorf("the configured controller cannot reach 8000")
	}
	if admitted, _ := admittedFrom(policies, fenceGoAPILabels(), []int{8000}, fenceControllerSource); len(admitted) != 0 {
		t.Errorf("the default controller is still admitted after the values moved")
	}
}

// The fence off renders exactly what it rendered before: every port but the internal one open to every source.
func TestGoAPIFenceOffChangesNothing(t *testing.T) {
	out, err := exec.Command("helm", "template", "fence", ".", "--namespace", releaseNamespace, "--set", "goApi.enabled=true").CombinedOutput()
	if err != nil {
		t.Fatalf("render: %v\n%s", err, out)
	}
	for _, document := range strings.Split(string(out), "\n---") {
		var parsed netpolDoc
		if err := yaml.Unmarshal([]byte(document), &parsed); err != nil {
			t.Fatalf("parse: %v", err)
		}
		if parsed.Kind != "NetworkPolicy" || !strings.HasSuffix(parsed.Metadata.Name, "-go-api-internal") {
			continue
		}
		const before = "  ingress:\n" // the port ranges render exactly as before the fence existed
		if !strings.Contains(document, "    - ports:\n        - protocol: TCP\n          port: 1\n          endPort: 8090\n        - protocol: TCP\n          port: 8092\n          endPort: 65535\n") || !strings.Contains(document, before) {
			t.Errorf("with the fence off the open ranges are not the two ranges around 8091:\n%s", document)
		}
		if got := closedPorts(t, []netpolDoc{parsed}, sourceStranger); !sameInts(got, []int{8091}) {
			t.Errorf("with the fence off a stranger is denied %v, want only 8091", got)
		}
		return
	}
	t.Fatal("no go-api-internal policy in the render")
}

func TestGoAPIFenceRefusals(t *testing.T) {
	for name, c := range map[string]struct {
		args []string
		want string
	}{
		"internal listener off":           {[]string{"--set", "goApi.internal.enabled=false"}, "goApi.networkPolicy.enabled requires goApi.internal.enabled"},
		"extra port is the internal port": {[]string{"--set-json", "goApi.networkPolicy.extraFencedPorts=[8091]"}, "extraFencedPorts"},
		"extra port is the operator port": {[]string{"--set-json", "goApi.networkPolicy.extraFencedPorts=[8080]"}, "extraFencedPorts"},
		"extra port zero":                 {[]string{"--set-json", "goApi.networkPolicy.extraFencedPorts=[0]"}, "extraFencedPorts"},
		"extra port too large":            {[]string{"--set-json", "goApi.networkPolicy.extraFencedPorts=[65536]"}, "extraFencedPorts"},
		"extra port not a number":         {[]string{"--set-json", `goApi.networkPolicy.extraFencedPorts=["billing"]`}, "extraFencedPorts"},
		"extra port fractional":           {[]string{"--set-json", "goApi.networkPolicy.extraFencedPorts=[8010.5]"}, "extraFencedPorts"},
		"api port is the operator port":   {[]string{"--set", "goApi.port=8080"}, "goApi.port"},
		"allowedFrom empty peer":          {[]string{"--set-json", `goApi.networkPolicy.allowedFrom=[{}]`}, "allowedFrom"},
		"allowedFrom empty selector":      {[]string{"--set-json", `goApi.networkPolicy.allowedFrom=[{"podSelector":{}}]`}, "allowedFrom"},
		"allowedFrom empty matchLabels":   {[]string{"--set-json", `goApi.networkPolicy.allowedFrom=[{"podSelector":{"matchLabels":{}}}]`}, "allowedFrom"},
		"allowedFrom ipBlock":             {[]string{"--set-json", `goApi.networkPolicy.allowedFrom=[{"ipBlock":{"cidr":"0.0.0.0/0"}}]`}, "allowedFrom"},
		"allowedFrom namespace-wide":      {[]string{"--set-json", `goApi.networkPolicy.allowedFrom=[{"namespaceSelector":{}}]`}, "allowedFrom"},
		"controller labels empty":         {[]string{"--set-json", `goApi.networkPolicy.ingressControllerPodLabels=null`}, "ingressControllerPodLabels"},
		"controller namespace empty":      {[]string{"--set", "networkPolicy.ingressControllerNamespace="}, "ingressControllerNamespace"},
	} {
		t.Run(name, func(t *testing.T) {
			full := append([]string{"template", "fence", ".", "--namespace", releaseNamespace, "--set", "goApi.enabled=true", "--set", "goApi.networkPolicy.enabled=true"}, c.args...)
			out, err := exec.Command("helm", full...).CombinedOutput()
			if err == nil {
				t.Fatalf("the render succeeded, want a refusal naming %q", c.want)
			}
			if !strings.Contains(string(out), c.want) {
				t.Errorf("the refusal does not name %q:\n%s", c.want, out)
			}
		})
	}
}
