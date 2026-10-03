package devhealth_test

import (
	"fmt"
	"os/exec"
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
			if policyAdmitsPort(policy, probe) {
				admitted = append(admitted, probe)
				break
			}
		}
	}
	return admitted, selected
}

func policyAdmitsPort(policy netpolDoc, probe int) bool {
	for _, rule := range policy.Spec.Ingress {
		admits := len(rule.From) == 0
		for _, peer := range rule.From {
			if peer.NamespaceSelector != nil && peer.PodSelector == nil && peer.NamespaceSelector.MatchLabels["name"] == "ingress-nginx" {
				admits = true
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
		"internal listener off":          {[]string{"--set", "goApi.internal.enabled=false"}, 8000, []int{3000, 8000}},
		"internal off, another api port": {[]string{"--set", "goApi.internal.enabled=false", "--set", "goApi.port=9000"}, 9000, []int{3000, 9000}},
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

// The shared policy opens exactly the go-api port and the web port to the ingress controller (what it opened before the
// Python api was deleted, with the api port now the go-api port) on the pods it selects.
func TestSharedNetworkPolicyIngressControllerPorts(t *testing.T) {
	for _, internal := range []string{"false", "true"} {
		policies := renderNetpols(t, "--set", "goApi.internal.enabled="+internal)
		for _, policy := range policies {
			if policy.Metadata.Name != "np-dev-health" {
				continue
			}
			var got []int
			for _, rule := range policy.Spec.Ingress {
				for _, peer := range rule.From {
					if peer.NamespaceSelector != nil && peer.PodSelector == nil {
						for _, port := range rule.Ports {
							got = append(got, port.Port)
						}
					}
				}
			}
			if want := []int{8000, 3000}; len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
				t.Errorf("internal=%s: the shared policy admits the ingress controller on %v, want %v", internal, got, want)
			}
		}
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
