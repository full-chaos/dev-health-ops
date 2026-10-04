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
	full := append([]string{"template", "np", ".", "--set", "networkPolicy.enabled=true", "--set", "goApi.enabled=true",
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

type netpolSource int

const (
	sourceController netpolSource = iota // a pod of the ingress-nginx namespace
	sourceStranger                       // a pod of the release namespace no rule names
)

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
			switch source {
			case sourceController:
				if peer.NamespaceSelector != nil && peer.PodSelector == nil && peer.NamespaceSelector.MatchLabels["name"] == "ingress-nginx" {
					admits = true
				}
			case sourceStranger:
				if peer.NamespaceSelector == nil && peer.PodSelector != nil && peer.PodSelector.selects(map[string]string{"app": "stranger"}) {
					admits = true
				}
			}
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

// CHAOS-8552: the ingress controller reaches only the pods the Ingress routes to (go-api and web), on the port of that
// component's Service, and no other pod of the release on any port. The check runs on the UNION of every rendered
// policy (NetworkPolicies are additive), per pod, so a broad rule on one policy cannot hide behind a narrow one on another.
func TestIngressControllerReachesOnlyGoAPIAndWebOnTheirOwnPorts(t *testing.T) {
	probes := []int{3000, 5432, 6379, 6432, 6433, 6434, 8000, 8010, 8080, 8090, 8091, 8092, 8123, 9000, 9001}
	pods := []string{"go-api", "web", "query-api", "go-worker", "valkey", "clickhouse", "postgresql",
		"go-pgbouncer-transaction", "go-pgbouncer-queue-session", "go-pgbouncer-coordinator-session",
		"migrate", "provision-roles", "river-migrate"}
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

// The go-api internal port stays closed to the ingress controller and to any in-namespace pod no rule names, on the
// union of every policy (CHAOS-7181), and the same for the query-api internal port.
func TestInternalPortsAreClosedOnTheUnion(t *testing.T) {
	policies := renderNetpols(t, "--set", "goApi.internal.enabled=true", "--set", "queryApi.enabled=true",
		"--set", "queryApi.internal.enabled=true", "--set", "queryApi.internal.allowedFrom[0].matchLabels.app=tools")
	for component, port := range map[string]int{"go-api": 8091} {
		labels := map[string]string{"app.kubernetes.io/name": "dev-health", "app.kubernetes.io/instance": "np", "app.kubernetes.io/component": component}
		for source, who := range map[netpolSource]string{sourceController: "the ingress controller", sourceStranger: "an unnamed in-namespace pod"} {
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
