package devhealth_test

import (
	"fmt"
	"os/exec"
	"sort"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// CHAOS-7520 (fix-forward): with networkPolicy.enabled the shared policy must still let the ingress controller reach the
// port the go-api Service and Ingress use (goApi.port), whether or not the go-api internal listener is on, and must not
// admit the ingress controller to any other port. The query-api internal policy admits only the sources an operator
// names in queryApi.internal.allowedFrom: no release-local peer, and never an empty `from` (which Kubernetes reads as
// "every source").

type netpolSelector struct {
	MatchLabels      map[string]string `yaml:"matchLabels"`
	MatchExpressions []struct {
		Key      string   `yaml:"key"`
		Operator string   `yaml:"operator"`
		Values   []string `yaml:"values"`
	} `yaml:"matchExpressions"`
}

type netpolPeer struct {
	PodSelector       *netpolSelector `yaml:"podSelector"`
	NamespaceSelector *netpolSelector `yaml:"namespaceSelector"`
}

type netpolRule struct {
	From  []netpolPeer `yaml:"from"`
	Ports []struct {
		Port    int `yaml:"port"`
		EndPort int `yaml:"endPort"`
	} `yaml:"ports"`
}

type netpolDoc struct {
	Kind     string `yaml:"kind"`
	Metadata struct {
		Name string `yaml:"name"`
	} `yaml:"metadata"`
	Spec struct {
		PodSelector netpolSelector `yaml:"podSelector"`
		PolicyTypes []string       `yaml:"policyTypes"`
		Ingress     []netpolRule   `yaml:"ingress"`
	} `yaml:"spec"`
}

func renderNetpols(t *testing.T, args ...string) []netpolDoc {
	t.Helper()
	full := append([]string{"template", "np", ".", "--namespace", releaseNamespace, "--set", "networkPolicy.enabled=true", "--set", "goApi.enabled=true",
		"--set", "goWorkers.pgbouncer.postgres.networkPolicyCIDR=10.0.0.0/24"}, args...)
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

func (s netpolSelector) selects(labels map[string]string) bool {
	for key, want := range s.MatchLabels {
		if labels[key] != want {
			return false
		}
	}
	for _, expression := range s.MatchExpressions {
		in := false
		for _, value := range expression.Values {
			in = in || labels[expression.Key] == value
		}
		if (expression.Operator == "In" && !in) || (expression.Operator == "NotIn" && in) {
			return false
		}
	}
	return true
}

// admittedFromIngressController returns which of the probe ports some policy selecting `pod` admits from a pod of the
// ingress-nginx namespace. selected reports whether any ingress policy selects the pod at all.
func admittedFromIngressController(policies []netpolDoc, pod map[string]string, probes []int) (admitted []int, selected bool) {
	return admittedFrom(policies, pod, probes, sourceController)
}

// netpolSource is the namespace labels and pod labels of one connecting pod. A peer admits it by the SELECTOR, whatever
// label form the selector uses (an empty namespaceSelector selects every namespace, an empty podSelector every pod).
type netpolSource struct {
	name            string
	namespaceLabels map[string]string
	podLabels       map[string]string
}

const (
	releaseNamespace           = "rel-ns"
	defaultControllerNamespace = "ingress-nginx"
	namespaceNameLabel         = "kubernetes.io/metadata.name"
)

// controllerSourceFor is a pod of the ingress controller's namespace; Kubernetes labels every namespace with its name.
func controllerSourceFor(namespace string) netpolSource {
	return netpolSource{"the ingress controller", map[string]string{namespaceNameLabel: namespace}, map[string]string{"app.kubernetes.io/name": "ingress-nginx"}}
}

var (
	sourceController     = controllerSourceFor(defaultControllerNamespace)
	sourceOtherNamespace = netpolSource{"a pod of an unrelated namespace", map[string]string{namespaceNameLabel: "other-ns", "name": "ingress-nginx"}, map[string]string{"app": "stranger"}}
	sourceStranger       = netpolSource{"an unnamed in-namespace pod", map[string]string{namespaceNameLabel: releaseNamespace}, map[string]string{"app": "stranger"}}
)

func (peer netpolPeer) admits(source netpolSource) bool {
	if peer.NamespaceSelector == nil && peer.PodSelector == nil {
		return false
	}
	if peer.NamespaceSelector != nil {
		if !peer.NamespaceSelector.selects(source.namespaceLabels) {
			return false
		}
	} else if source.namespaceLabels[namespaceNameLabel] != releaseNamespace {
		return false // a bare podSelector means the policy's own namespace
	}
	return peer.PodSelector == nil || peer.PodSelector.selects(source.podLabels)
}

func admittedFrom(policies []netpolDoc, pod map[string]string, probes []int, source netpolSource) (admitted []int, selected bool) {
	for _, probe := range probes {
		for _, policy := range policies {
			if !policy.Spec.PodSelector.selects(pod) {
				continue
			}
			ingress := false
			for _, kind := range policy.Spec.PolicyTypes {
				ingress = ingress || kind == "Ingress"
			}
			if !ingress {
				continue
			}
			selected = true
			if policyAdmitsPort(policy, probe, source) {
				admitted = append(admitted, probe)
				break
			}
		}
	}
	return admitted, selected
}

func policyAdmitsPort(policy netpolDoc, probe int, source netpolSource) bool {
	for _, rule := range policy.Spec.Ingress {
		admits := len(rule.From) == 0
		for _, peer := range rule.From {
			admits = admits || peer.admits(source)
		}
		if !admits {
			continue
		}
		if len(rule.Ports) == 0 {
			return true
		}
		for _, port := range rule.Ports {
			if probe == port.Port || (port.EndPort != 0 && probe >= port.Port && probe <= port.EndPort) {
				return true
			}
		}
	}
	return false
}

func goAPIPodLabels() map[string]string {
	return map[string]string{
		"app.kubernetes.io/name": "dev-health", "app.kubernetes.io/instance": "np",
		"app.kubernetes.io/component": "go-api",
	}
}

func TestNetworkPolicyLetsTheIngressControllerReachGoAPI(t *testing.T) {
	probes := []int{3000, 8000, 8010, 8090, 8091, 8092, 9000, 9001}
	for name, c := range map[string]struct {
		args []string
		port int
		want []int // the probe ports the ingress controller may reach on the go-api pods
	}{
		"internal listener off":          {[]string{"--set", "goApi.internal.enabled=false"}, 8000, []int{8000}},
		"internal off, another api port": {[]string{"--set", "goApi.internal.enabled=false", "--set", "goApi.port=9000"}, 9000, []int{9000}},
		// With the internal listener on, the go-api-internal policy governs the go-api pods: every port but the internal
		// one (8091), which only goApi.internal.allowedFrom reaches.
		"internal listener on":          {[]string{"--set", "goApi.internal.enabled=true"}, 8000, []int{3000, 8000, 8010, 8090, 8092, 9000, 9001}},
		"internal on, another api port": {[]string{"--set", "goApi.internal.enabled=true", "--set", "goApi.port=9000"}, 9000, []int{3000, 8000, 8010, 8090, 8092, 9000, 9001}},
	} {
		t.Run(name, func(t *testing.T) {
			policies := renderNetpols(t, c.args...)
			admitted, selected := admittedFromIngressController(policies, goAPIPodLabels(), probes)
			if !selected {
				t.Fatalf("no ingress policy selects the go-api pods")
			}
			reach := false
			for _, port := range admitted {
				reach = reach || port == c.port
			}
			if !reach {
				t.Errorf("the ingress controller cannot reach go-api port %d (the Service and Ingress port): admitted %v", c.port, admitted)
			}
			if fmt.Sprint(admitted) != fmt.Sprint(c.want) {
				t.Errorf("the ingress controller is admitted to go-api ports %v, want exactly %v", admitted, c.want)
			}
		})
	}
}

// CHAOS-8552: the ingress controller reaches only the pods the Ingress routes to (go-api, web and query-api), on the port
// of that component's Service, and no other pod of the release on any port. The check runs on the UNION of every rendered
// policy (NetworkPolicies are additive), per pod, so a broad rule on one policy cannot hide behind a narrow one on another.
func TestIngressControllerReachesOnlyGoAPIAndWebOnTheirOwnPorts(t *testing.T) {
	probes := []int{3000, 5432, 6379, 6432, 6433, 6434, 8000, 8010, 8080, 8090, 8091, 8092, 8123, 9000, 9001}
	pods := []string{"go-api", "web", "query-api", "go-worker", "valkey", "clickhouse", "postgresql",
		"go-pgbouncer-transaction", "go-pgbouncer-queue-session", "go-pgbouncer-coordinator-session",
		"migrate", "provision-roles", "river-migrate", "route-activate", "routing-carry", "routing-repoint"}
	for name, c := range map[string]struct {
		args     []string
		goPort   int
		webPort  int
		queryAPI bool
		goAll    bool // go-api-internal governs the go-api pods: every port but the internal one (CHAOS-7181)
	}{
		"defaults, internal listener off":    {[]string{"--set", "goApi.internal.enabled=false"}, 8000, 3000, false, false},
		"defaults, internal listener on":     {[]string{"--set", "goApi.internal.enabled=true"}, 8000, 3000, false, true},
		"go-api port = clickhouse native":    {[]string{"--set", "goApi.internal.enabled=false", "--set", "goApi.port=9000"}, 9000, 3000, false, false},
		"go-api port = pgbouncer, internal":  {[]string{"--set", "goApi.internal.enabled=true", "--set", "goApi.port=6432"}, 6432, 3000, false, true},
		"web port = pgbouncer queue session": {[]string{"--set", "goApi.internal.enabled=false", "--set", "web.port=6433"}, 8000, 6433, false, false},
		"query-api, no internal listener":    {[]string{"--set", "goApi.internal.enabled=false", "--set", "queryApi.enabled=true"}, 8000, 3000, true, false},
		"query-api with internal listener": {[]string{"--set", "goApi.internal.enabled=false", "--set", "queryApi.enabled=true",
			"--set", "queryApi.internal.enabled=true", "--set", "queryApi.internal.allowedFrom[0].matchLabels.app=tools"}, 8000, 3000, true, false},
		"go workers with pgbouncer, bundled postgresql": {[]string{"--set", "goApi.internal.enabled=false", "--set", "postgresql.enabled=true"}, 8000, 3000, false, false},
	} {
		t.Run(name, func(t *testing.T) {
			policies := renderNetpols(t, c.args...)
			for _, component := range pods {
				labels := map[string]string{"app.kubernetes.io/name": "dev-health", "app.kubernetes.io/instance": "np", "app.kubernetes.io/component": component}
				admitted, _ := admittedFromIngressController(policies, labels, probes)
				var want []int
				switch {
				case component == "go-api" && c.goAll:
					for _, probe := range probes {
						if probe != 8091 {
							want = append(want, probe)
						}
					}
				case component == "go-api":
					want = []int{c.goPort}
				case component == "web":
					want = []int{c.webPort}
				case component == "query-api" && c.queryAPI:
					want = []int{8090} // the query-api policy opens its public port to every source; the Ingress routes /graphql there
				}
				sort.Ints(want)
				sort.Ints(admitted)
				if fmt.Sprint(admitted) != fmt.Sprint(want) {
					t.Errorf("%s pods: the ingress controller is admitted to %v, want exactly %v", component, admitted, want)
				}
			}
			// Effective reachability from another namespace, whatever selector form a rule uses: nothing (the go-api pods
			// under go-api-internal keep every port but the internal one for every source, by its own design).
			for _, component := range pods {
				labels := map[string]string{"app.kubernetes.io/name": "dev-health", "app.kubernetes.io/instance": "np", "app.kubernetes.io/component": component}
				fromOther, _ := admittedFrom(policies, labels, probes, sourceOtherNamespace)
				if component == "go-api" && c.goAll {
					continue
				}
				if component == "query-api" && strings.Contains(strings.Join(c.args, " "), "queryApi.internal.enabled=true") {
					// The query-api-internal policy opens the public port to every source, exactly as before it existed.
					if fmt.Sprint(fromOther) != "[8090]" {
						t.Errorf("query-api pods: a pod of another namespace is admitted to %v, want only the public port [8090]", fromOther)
					}
					continue
				}
				if len(fromOther) != 0 {
					t.Errorf("%s pods: a pod of another namespace is admitted to %v, want nothing", component, fromOther)
				}
			}
			// Every pod's allowed sources, for the log of a failing run.
			for _, component := range pods {
				labels := map[string]string{"app.kubernetes.io/name": "dev-health", "app.kubernetes.io/instance": "np", "app.kubernetes.io/component": component}
				fromController, _ := admittedFrom(policies, labels, probes, sourceController)
				fromStranger, _ := admittedFrom(policies, labels, probes, sourceStranger)
				t.Logf("%-34s controller:%v in-namespace-stranger:%v", component, fromController, fromStranger)
			}
		})
	}
}

// The controller namespace is a value (default ingress-nginx), matched by the label Kubernetes puts on every namespace:
// with another namespace configured, that namespace is admitted to go-api, web and query-api on their ports and the
// default one is admitted nowhere.
func TestIngressControllerNamespaceIsConfigurable(t *testing.T) {
	policies := renderNetpols(t, "--set", "goApi.internal.enabled=false", "--set", "queryApi.enabled=true",
		"--set", "networkPolicy.ingressControllerNamespace=edge-proxy")
	probes := []int{3000, 8000, 8090, 9000}
	for component, want := range map[string][]int{"go-api": {8000}, "web": {3000}, "query-api": {8090}, "clickhouse": nil} {
		labels := map[string]string{"app.kubernetes.io/name": "dev-health", "app.kubernetes.io/instance": "np", "app.kubernetes.io/component": component}
		got, _ := admittedFrom(policies, labels, probes, controllerSourceFor("edge-proxy"))
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Errorf("%s: namespace edge-proxy is admitted to %v, want %v", component, got, want)
		}
		if old, _ := admittedFrom(policies, labels, probes, sourceController); len(old) != 0 {
			t.Errorf("%s: the default namespace is still admitted to %v after the value moved", component, old)
		}
	}
}

// The go-api internal port stays closed to the ingress controller and to any in-namespace pod no rule names, on the
// union of every policy (CHAOS-7181); the query-api internal port is checked from the ingress controller only (the shared
// in-namespace rule admits release pods on it by design, see the query-api-internal template comment).
func TestInternalPortsAreClosedOnTheUnion(t *testing.T) {
	policies := renderNetpols(t, "--set", "goApi.internal.enabled=true", "--set", "queryApi.enabled=true",
		"--set", "queryApi.internal.enabled=true", "--set", "queryApi.internal.allowedFrom[0].matchLabels.app=tools")
	for component, port := range map[string]int{"go-api": 8091} {
		labels := map[string]string{"app.kubernetes.io/name": "dev-health", "app.kubernetes.io/instance": "np", "app.kubernetes.io/component": component}
		for _, source := range []netpolSource{sourceController, sourceStranger} {
			who := source.name
			admitted, selected := admittedFrom(policies, labels, []int{port}, source)
			if !selected {
				t.Fatalf("no ingress policy selects the %s pods", component)
			}
			if len(admitted) != 0 {
				t.Errorf("%s is admitted to the %s internal port %d", who, component, port)
			}
		}
	}
	queryAPI := map[string]string{"app.kubernetes.io/name": "dev-health", "app.kubernetes.io/instance": "np", "app.kubernetes.io/component": "query-api"}
	if admitted, _ := admittedFrom(policies, queryAPI, []int{8091}, sourceController); len(admitted) != 0 {
		t.Errorf("the ingress controller is admitted to the query-api internal port: %v", admitted)
	}
}

// The query-api internal policy admits the internal port only from queryApi.internal.allowedFrom. A release-local `api`
// peer selects nothing since the Python api is gone, and an empty `from` would admit every source.
func TestQueryAPIInternalPolicyAdmitsOnlyAllowedFrom(t *testing.T) {
	internalPeers := func(t *testing.T, args ...string) (rules []netpolRule, found bool) {
		t.Helper()
		for _, policy := range renderNetpols(t, append([]string{"--set", "queryApi.enabled=true", "--set", "queryApi.internal.enabled=true"}, args...)...) {
			if policy.Metadata.Name != "np-dev-health-query-api-internal" {
				continue
			}
			found = true
			for _, rule := range policy.Spec.Ingress {
				for _, port := range rule.Ports {
					if port.Port == 8091 {
						rules = append(rules, rule)
					}
				}
			}
			// The public port stays open to every source.
			public := false
			for _, rule := range policy.Spec.Ingress {
				if len(rule.From) == 0 && len(rule.Ports) == 1 && rule.Ports[0].Port == 8090 {
					public = true
				}
			}
			if !public {
				t.Errorf("the public query-api port 8090 is no longer open to every source: %+v", policy.Spec.Ingress)
			}
		}
		return rules, found
	}
	rules, found := internalPeers(t)
	if !found {
		t.Fatalf("no query-api-internal policy rendered")
	}
	if len(rules) != 0 {
		t.Errorf("with no allowedFrom the internal port must be admitted to nobody, got rules %+v", rules)
	}
	rules, _ = internalPeers(t, "--set", "queryApi.internal.allowedFrom[0].matchLabels.app=tools")
	if len(rules) != 1 || len(rules[0].From) != 1 {
		t.Fatalf("want exactly one rule with exactly one peer (the allowedFrom entry), got %+v", rules)
	}
	peer := rules[0].From[0]
	if peer.PodSelector == nil || peer.PodSelector.MatchLabels["app"] != "tools" || len(peer.PodSelector.MatchLabels) != 1 {
		t.Errorf("the only peer must be the allowedFrom selector, got %+v", peer.PodSelector)
	}
}

// A controller namespace equal to the release namespace voids the per-component split (the shared policy's in-namespace
// rule admits the controller on every port), so the render refuses it when the policy is on, for the release namespace and
// for global.namespaceOverride, and renders as before when the policy is off.
func TestControllerNamespaceEqualToReleaseNamespaceFailsTheRender(t *testing.T) {
	render := func(args ...string) (string, error) {
		out, err := exec.Command("helm", append([]string{"template", "np", ".", "--namespace", releaseNamespace, "--set", "goApi.enabled=true",
			"--set", "goWorkers.pgbouncer.postgres.networkPolicyCIDR=10.0.0.0/24"}, args...)...).CombinedOutput()
		return string(out), err
	}
	for name, args := range map[string][]string{
		"release namespace": {"--set", "networkPolicy.enabled=true", "--set", "networkPolicy.ingressControllerNamespace=" + releaseNamespace},
		"namespace override": {"--set", "networkPolicy.enabled=true", "--set", "global.namespaceOverride=edge",
			"--set", "networkPolicy.ingressControllerNamespace=edge"},
	} {
		out, err := render(args...)
		if err == nil || !strings.Contains(out, "is the namespace this release is installed in") {
			t.Errorf("%s: the render must fail with the namespace message, err=%v out=%.300s", name, err, out)
		}
	}
	for name, args := range map[string][]string{
		"policy off, same namespace":                      {"--set", "networkPolicy.ingressControllerNamespace=" + releaseNamespace},
		"policy on, other namespace":                      {"--set", "networkPolicy.enabled=true"},
		"policy on, override differs from the controller": {"--set", "networkPolicy.enabled=true", "--set", "global.namespaceOverride=edge"},
	} {
		if out, err := render(args...); err != nil {
			t.Errorf("%s: the render must succeed: %v\n%.300s", name, err, out)
		}
	}
}
