package devhealth_test

import (
	"fmt"
	"net"
	"os/exec"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// With networkPolicy.enabled and a query-api listener that trusts internal identity headers (internal or MCP), the
// query-api pods leave the release-wide policy: queryApi.port admits the release namespace and the ingress controller,
// the internal and MCP ports admit their allowedFrom only, and every other port is closed. networkPolicy.extraEgress
// adds egress rules to every policy that carries the shared egress (never to the auth pods), and DNS runs over UDP and
// TCP. Every check reads the UNION of the rendered policies, since NetworkPolicies are additive.

type egressPeer struct {
	PodSelector       *netpolSelector `yaml:"podSelector"`
	NamespaceSelector *netpolSelector `yaml:"namespaceSelector"`
	IPBlock           *struct {
		CIDR   string   `yaml:"cidr"`
		Except []string `yaml:"except"`
	} `yaml:"ipBlock"`
}

// egressPort is a port number or a named port; a named port matches no numeric target here.
type egressPort struct {
	number int
	name   string
}

func (p *egressPort) UnmarshalYAML(node *yaml.Node) error {
	if err := node.Decode(&p.number); err == nil {
		return nil
	}
	return node.Decode(&p.name)
}

type egressRule struct {
	To    []egressPeer `yaml:"to"`
	Ports []struct {
		Protocol string     `yaml:"protocol"`
		Port     egressPort `yaml:"port"`
		EndPort  int        `yaml:"endPort"`
	} `yaml:"ports"`
}

type egressDoc struct {
	Kind     string `yaml:"kind"`
	Metadata struct {
		Name string `yaml:"name"`
	} `yaml:"metadata"`
	Spec struct {
		PodSelector netpolSelector `yaml:"podSelector"`
		PolicyTypes []string       `yaml:"policyTypes"`
		Egress      []egressRule   `yaml:"egress"`
	} `yaml:"spec"`
}

// egressTarget is one destination: a pod (namespace + pod labels) or an address outside the cluster.
type egressTarget struct {
	name            string
	namespaceLabels map[string]string
	podLabels       map[string]string
	ip              string
	protocol        string
	port            int
}

func (peer egressPeer) reaches(target egressTarget) bool {
	if peer.IPBlock != nil {
		if target.ip == "" {
			return false
		}
		_, block, err := net.ParseCIDR(peer.IPBlock.CIDR)
		if err != nil || !block.Contains(net.ParseIP(target.ip)) {
			return false
		}
		for _, except := range peer.IPBlock.Except {
			if _, excluded, err := net.ParseCIDR(except); err == nil && excluded.Contains(net.ParseIP(target.ip)) {
				return false
			}
		}
		return true
	}
	if target.ip != "" || (peer.NamespaceSelector == nil && peer.PodSelector == nil) {
		return false
	}
	if peer.NamespaceSelector != nil {
		if !peer.NamespaceSelector.selects(target.namespaceLabels) {
			return false
		}
	} else if target.namespaceLabels[namespaceNameLabel] != releaseNamespace {
		return false
	}
	return peer.PodSelector == nil || peer.PodSelector.selects(target.podLabels)
}

func (rule egressRule) allows(target egressTarget) bool {
	portOK := len(rule.Ports) == 0
	for _, port := range rule.Ports {
		protocol := port.Protocol
		if protocol == "" {
			protocol = "TCP"
		}
		if port.Port.name != "" {
			continue
		}
		start, end := port.Port.number, port.EndPort
		if start == 0 { // Kubernetes: an entry with no port matches every port of its protocol
			start, end = 1, 65535
		} else if end == 0 {
			end = start
		}
		portOK = portOK || (protocol == target.protocol && target.port >= start && target.port <= end)
	}
	if !portOK {
		return false
	}
	if len(rule.To) == 0 {
		return true
	}
	for _, peer := range rule.To {
		if peer.reaches(target) {
			return true
		}
	}
	return false
}

// egressAllowed reports whether the union of the egress policies selecting pod lets it reach target; selected is false
// when no egress policy selects the pod (then its egress is not restricted at all).
func egressAllowed(policies []egressDoc, pod map[string]string, target egressTarget) (allowed, selected bool) {
	for _, policy := range policies {
		if policy.Kind != "NetworkPolicy" || !policy.Spec.PodSelector.selects(pod) {
			continue
		}
		isEgress := false
		for _, policyType := range policy.Spec.PolicyTypes {
			isEgress = isEgress || policyType == "Egress"
		}
		if !isEgress {
			continue
		}
		selected = true
		for _, rule := range policy.Spec.Egress {
			allowed = allowed || rule.allows(target)
		}
	}
	return allowed, selected
}

// renderPolicySet renders with networkPolicy.enabled, go-api with its internal listener, and query-api, and returns the
// policies twice: for the ingress evaluator and for the egress evaluator.
func renderPolicySet(t *testing.T, args ...string) ([]netpolDoc, []egressDoc, string) {
	t.Helper()
	full := append([]string{"template", "np", ".", "--namespace", releaseNamespace, "--set", "networkPolicy.enabled=true",
		"--set", "goApi.enabled=true", "--set", "goApi.internal.enabled=true", "--set", "queryApi.enabled=true",
		"--set", "goWorkers.pgbouncer.postgres.networkPolicyCIDR=10.0.0.0/24"}, args...)
	out, err := exec.Command("helm", full...).CombinedOutput()
	if err != nil {
		t.Fatalf("render: %v\n%s", err, out)
	}
	var ingress []netpolDoc
	var egress []egressDoc
	for _, document := range strings.Split(string(out), "\n---") {
		var parsed netpolDoc
		if err := yaml.Unmarshal([]byte(document), &parsed); err != nil {
			t.Fatalf("a document of the render does not parse: %v", err)
		}
		if parsed.Kind != "NetworkPolicy" {
			continue
		}
		var parsedEgress egressDoc
		if err := yaml.Unmarshal([]byte(document), &parsedEgress); err != nil {
			t.Fatalf("a NetworkPolicy does not parse for egress: %v", err)
		}
		ingress = append(ingress, parsed)
		egress = append(egress, parsedEgress)
	}
	return ingress, egress, string(out)
}

func npComponent(component string) map[string]string {
	return map[string]string{"app.kubernetes.io/name": "dev-health", "app.kubernetes.io/instance": "np", "app.kubernetes.io/component": component}
}

var (
	acrSelectorJSON = `[{"matchLabels":{"app.kubernetes.io/name":"acr","app.kubernetes.io/component":"api"}}]`
	sourceACR       = netpolSource{"the acr api pod", map[string]string{namespaceNameLabel: releaseNamespace}, map[string]string{"app.kubernetes.io/name": "acr", "app.kubernetes.io/component": "api"}}
	queryAPIProbes  = []int{80, 3000, 5432, 6379, 6432, 8000, 8010, 8080, 8090, 8091, 8092, 8123, 9000, 9440}
)

func TestQueryAPIOwnPolicyAdmitsOnlyItsCallers(t *testing.T) {
	cases := map[string]struct {
		args []string
		want map[string][]int // source name -> admitted probe ports
	}{
		"mcp and internal": {
			[]string{"--set", "queryApi.mcp.enabled=true", "--set-json", "queryApi.mcp.allowedFrom=" + acrSelectorJSON,
				"--set", "queryApi.internal.enabled=true", "--set-json", "queryApi.internal.allowedFrom=" + acrSelectorJSON},
			map[string][]int{sourceStranger.name: {8090}, sourceACR.name: {8090, 8091, 8092}, sourceController.name: {8090}, sourceOtherNamespace.name: nil},
		},
		"mcp only": {
			[]string{"--set", "queryApi.mcp.enabled=true", "--set-json", "queryApi.mcp.allowedFrom=" + acrSelectorJSON},
			map[string][]int{sourceStranger.name: {8090}, sourceACR.name: {8090, 8092}, sourceController.name: {8090}, sourceOtherNamespace.name: nil},
		},
		"internal only": {
			[]string{"--set", "queryApi.internal.enabled=true", "--set-json", "queryApi.internal.allowedFrom=" + acrSelectorJSON},
			map[string][]int{sourceStranger.name: {8090}, sourceACR.name: {8090, 8091}, sourceController.name: {8090}, sourceOtherNamespace.name: nil},
		},
		"both on, allowedFrom empty": {
			[]string{"--set", "queryApi.mcp.enabled=true", "--set", "queryApi.internal.enabled=true"},
			map[string][]int{sourceStranger.name: {8090}, sourceACR.name: {8090}, sourceController.name: {8090}, sourceOtherNamespace.name: nil},
		},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			policies, _, _ := renderPolicySet(t, c.args...)
			for _, source := range []netpolSource{sourceStranger, sourceACR, sourceController, sourceOtherNamespace} {
				got, selected := admittedFrom(policies, npComponent("query-api"), queryAPIProbes, source)
				if !selected {
					t.Fatalf("no ingress policy selects the query-api pods")
				}
				if fmt.Sprint(got) != fmt.Sprint(c.want[source.name]) {
					t.Errorf("%s reaches query-api ports %v, want %v", source.name, got, c.want[source.name])
				}
			}
		})
	}
}

func TestQueryAPIWithoutTrustedListenersStaysInTheReleaseWidePolicy(t *testing.T) {
	policies, egress, out := renderPolicySet(t)
	got, _ := admittedFrom(policies, npComponent("query-api"), queryAPIProbes, sourceStranger)
	if fmt.Sprint(got) != fmt.Sprint(queryAPIProbes) {
		t.Errorf("with no internal or MCP listener the in-namespace pods reach query-api ports %v, want every port as before", got)
	}
	if strings.Contains(out, "np-dev-health-query-api-egress") {
		t.Error("a query-api egress policy renders although query-api is still in the release-wide policy")
	}
	if allowed, _ := egressAllowed(egress, npComponent("query-api"), egressTarget{name: "https", ip: "140.82.112.3", protocol: "TCP", port: 443}); !allowed {
		t.Error("query-api lost the release-wide egress")
	}
}

func TestQueryAPIPublicPortStaysOpenWithTheReleasePolicyOff(t *testing.T) {
	out, err := exec.Command("helm", "template", "np", ".", "--namespace", releaseNamespace, "--set", "queryApi.enabled=true",
		"--set", "queryApi.mcp.enabled=true", "--set", "queryApi.internal.enabled=true").CombinedOutput()
	if err != nil {
		t.Fatalf("render: %v\n%s", err, out)
	}
	for _, name := range []string{"np-dev-health-query-api-internal", "np-dev-health-query-api-mcp"} {
		open := false
		for _, document := range strings.Split(string(out), "\n---") {
			var parsed netpolDoc
			if err := yaml.Unmarshal([]byte(document), &parsed); err != nil || parsed.Kind != "NetworkPolicy" || parsed.Metadata.Name != name {
				continue
			}
			got, _ := admittedFrom([]netpolDoc{parsed}, npComponent("query-api"), []int{8090}, sourceOtherNamespace)
			open = open || len(got) == 1
		}
		if !open {
			t.Errorf("%s: with networkPolicy.enabled=false the public port must stay open to every source", name)
		}
	}
}

func TestQueryAPIInternalPortsClosedToTheReleaseNamespace(t *testing.T) {
	policies, _, _ := renderPolicySet(t, "--set", "queryApi.mcp.enabled=true", "--set-json", "queryApi.mcp.allowedFrom="+acrSelectorJSON,
		"--set", "queryApi.internal.enabled=true", "--set-json", "queryApi.internal.allowedFrom="+acrSelectorJSON)
	for _, component := range []string{"web", "go-api", "go-worker", "go-pgbouncer-transaction", "clickhouse"} {
		source := netpolSource{"a " + component + " pod", map[string]string{namespaceNameLabel: releaseNamespace}, npComponent(component)}
		if got, _ := admittedFrom(policies, npComponent("query-api"), []int{8091, 8092}, source); len(got) != 0 {
			t.Errorf("%s reaches the query-api trusted ports %v", source.name, got)
		}
	}
}

// The prod extraEgress entries (OTLP to the node collector, ClickHouse Cloud native and https, web to the ingress
// controller): every pod that carries the shared egress reaches each entry and nothing wider; the auth pods do not.
const prodExtraEgressJSON = `[` +
	`{"to":[{"ipBlock":{"cidr":"10.0.0.151/32"}}],"ports":[{"protocol":"TCP","port":4317}]},` +
	`{"ports":[{"protocol":"TCP","port":9440},{"protocol":"TCP","port":8443}]},` +
	`{"to":[{"namespaceSelector":{"matchLabels":{"kubernetes.io/metadata.name":"ingress-nginx"}},"podSelector":{"matchLabels":{"app.kubernetes.io/name":"ingress-nginx","app.kubernetes.io/component":"controller"}}}],"ports":[{"protocol":"TCP","port":80}]}]`

func TestExtraEgressReachesEveryEntryFromEverySharedEgressPod(t *testing.T) {
	_, egress, _ := renderPolicySet(t, append([]string{"--set", "queryApi.mcp.enabled=true", "--set", "queryApi.internal.enabled=true",
		"--set-json", "networkPolicy.extraEgress=" + prodExtraEgressJSON}, authBase...)...)
	controller := map[string]string{"app.kubernetes.io/name": "ingress-nginx", "app.kubernetes.io/component": "controller"}
	nginxNamespace := map[string]string{namespaceNameLabel: "ingress-nginx"}
	allow := []egressTarget{
		{name: "OTLP collector", ip: "10.0.0.151", protocol: "TCP", port: 4317},
		{name: "ClickHouse Cloud native", ip: "34.120.1.2", protocol: "TCP", port: 9440},
		{name: "ClickHouse Cloud https", ip: "34.120.1.2", protocol: "TCP", port: 8443},
		{name: "ingress controller :80", namespaceLabels: nginxNamespace, podLabels: controller, protocol: "TCP", port: 80},
		{name: "DNS over UDP", namespaceLabels: map[string]string{namespaceNameLabel: "kube-system"}, podLabels: map[string]string{"k8s-app": "kube-dns"}, protocol: "UDP", port: 53},
		{name: "DNS over TCP", namespaceLabels: map[string]string{namespaceNameLabel: "kube-system"}, podLabels: map[string]string{"k8s-app": "kube-dns"}, protocol: "TCP", port: 53},
		{name: "in-namespace Postgres", namespaceLabels: map[string]string{namespaceNameLabel: releaseNamespace}, podLabels: npComponent("postgresql"), protocol: "TCP", port: 5432},
		{name: "external HTTPS", ip: "140.82.112.3", protocol: "TCP", port: 443},
	}
	deny := []egressTarget{
		{name: "collector on another port", ip: "10.0.0.151", protocol: "TCP", port: 4318},
		{name: "another node on the OTLP port", ip: "10.0.0.152", protocol: "TCP", port: 4317},
		{name: "external on an unlisted port", ip: "34.120.1.2", protocol: "TCP", port: 9441},
		{name: "another ingress-nginx pod on :80", namespaceLabels: nginxNamespace, podLabels: map[string]string{"app.kubernetes.io/name": "ingress-nginx", "app.kubernetes.io/component": "admission"}, protocol: "TCP", port: 80},
		{name: "a pod of another namespace on :80", namespaceLabels: map[string]string{namespaceNameLabel: "other-ns"}, podLabels: controller, protocol: "TCP", port: 80},
	}
	for _, component := range []string{"query-api", "go-api", "web", "go-worker"} {
		for _, target := range allow {
			if allowed, selected := egressAllowed(egress, npComponent(component), target); !selected || !allowed {
				t.Errorf("%s: %s is not allowed (selected=%v)", component, target.name, selected)
			}
		}
		for _, target := range deny {
			if allowed, _ := egressAllowed(egress, npComponent(component), target); allowed {
				t.Errorf("%s: %s is allowed", component, target.name)
			}
		}
	}
	for _, component := range []string{"auth-service", "auth-migrate"} {
		for _, target := range allow[:4] {
			if allowed, selected := egressAllowed(egress, npComponent(component), target); !selected || allowed {
				t.Errorf("%s: extraEgress entry %s reaches the auth pods (selected=%v)", component, target.name, selected)
			}
		}
	}
}

func TestExtraEgressLeavesTheAuthPoliciesUnchanged(t *testing.T) {
	authPolicies := func(out string) string {
		var kept []string
		for _, document := range strings.Split(out, "\n---") {
			if strings.Contains(document, "kind: NetworkPolicy") && strings.Contains(document, "-auth-") {
				kept = append(kept, document)
			}
		}
		return strings.Join(kept, "\n---")
	}
	_, _, without := renderPolicySet(t, authBase...)
	_, _, with := renderPolicySet(t, append([]string{"--set-json", "networkPolicy.extraEgress=" + prodExtraEgressJSON}, authBase...)...)
	if authPolicies(without) == "" {
		t.Fatal("no auth NetworkPolicy rendered")
	}
	if authPolicies(without) != authPolicies(with) {
		t.Error("networkPolicy.extraEgress changed an auth NetworkPolicy")
	}
}

func TestExtraEgressRefusals(t *testing.T) {
	cases := map[string]struct {
		value string
		want  string
	}{
		"empty rule":                 {`[{}]`, "neither a non-empty to nor non-empty ports"},
		"empty to":                   {`[{"to":[]}]`, "neither a non-empty to nor non-empty ports"},
		"empty ports":                {`[{"ports":[]}]`, "neither a non-empty to nor non-empty ports"},
		"empty to and ports":         {`[{"to":[],"ports":[]}]`, "neither a non-empty to nor non-empty ports"},
		"not an object":              {`["x"]`, "must be a NetworkPolicy egress rule"},
		"unknown key":                {`[{"to":[{"ipBlock":{"cidr":"10.0.0.1/32"}}],"port":[{"port":4317}]}]`, `not "port"`},
		"to not a list":              {`[{"to":{"ipBlock":{"cidr":"10.0.0.1/32"}}}]`, "extraEgress[0].to must be a list"},
		"ports not a list":           {`[{"ports":{"port":4317}}]`, "extraEgress[0].ports must be a list"},
		"IPv4 everything, no ports":  {`[{"to":[{"ipBlock":{"cidr":"0.0.0.0/0"}}]}]`, "ipBlock 0.0.0.0/0 with no except and no ports"},
		"IPv6 everything, no ports":  {`[{"to":[{"ipBlock":{"cidr":"::/0"}}]}]`, "ipBlock ::/0 with no except and no ports"},
		"everything among peers":     {`[{"to":[{"ipBlock":{"cidr":"10.0.0.1/32"}},{"ipBlock":{"cidr":"0.0.0.0/0"}}]}]`, "ipBlock 0.0.0.0/0"},
		"second entry is refused":    {`[{"ports":[{"port":9440}]},{}]`, "extraEgress[1] has neither"},
		"everything, empty except":   {`[{"to":[{"ipBlock":{"cidr":"0.0.0.0/0","except":[]}}]}]`, "ipBlock 0.0.0.0/0 with no except"},
		"everything, ports empty":    {`[{"to":[{"ipBlock":{"cidr":"::/0"}}],"ports":[]}]`, "ipBlock ::/0 with no except and no ports"},
		"port entry with no port":    {`[{"ports":[{}]}]`, "ports[0] needs a port"},
		"UDP entry with no port":     {`[{"ports":[{"protocol":"UDP"}]}]`, "ports[0] needs a port"},
		"SCTP entry with no port":    {`[{"ports":[{"protocol":"TCP","port":9440},{"protocol":"SCTP"}]}]`, "ports[1] needs a port"},
		"port zero":                  {`[{"ports":[{"port":0}]}]`, "needs a port"},
		"port too large":             {`[{"ports":[{"port":65536}]}]`, "needs a port"},
		"port fractional":            {`[{"ports":[{"port":9440.5}]}]`, "needs a port"},
		"port empty name":            {`[{"ports":[{"port":""}]}]`, "needs a port"},
		"port entry not an object":   {`[{"ports":[9440]}]`, "ports[0] must be an object"},
		"port entry unknown key":     {`[{"ports":[{"port":9440,"endport":9450}]}]`, `not "endport"`},
		"protocol unknown":           {`[{"ports":[{"protocol":"ICMP","port":1}]}]`, "is not TCP, UDP or SCTP"},
		"endPort below port":         {`[{"ports":[{"port":9440,"endPort":9000}]}]`, "endPort needs a numeric port"},
		"endPort on a named port":    {`[{"ports":[{"port":"https","endPort":9000}]}]`, "endPort needs a numeric port"},
		"peer empty":                 {`[{"to":[{}]}]`, "to[0] is empty"},
		"peer not an object":         {`[{"to":["10.0.0.1/32"]}]`, "to[0] must be an object"},
		"peer unknown key":           {`[{"to":[{"ipblock":{"cidr":"10.0.0.1/32"}}]}]`, `not "ipblock"`},
		"peer ipBlock without cidr":  {`[{"to":[{"ipBlock":{}}],"ports":[{"port":9440}]}]`, "ipBlock needs a cidr"},
		"IPv6 everything, long form": {`[{"to":[{"ipBlock":{"cidr":"0:0:0:0:0:0:0:0/0"}}]}]`, "with no except and no ports"},
		"everything, /00":            {`[{"to":[{"ipBlock":{"cidr":"0.0.0.0/00"}}]}]`, "with no except and no ports"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			out, err := exec.Command("helm", "template", "np", ".", "--namespace", releaseNamespace, "--set", "networkPolicy.enabled=true",
				"--set", "goWorkers.pgbouncer.postgres.networkPolicyCIDR=10.0.0.0/24", "--set-json", "networkPolicy.extraEgress="+c.value).CombinedOutput()
			if err == nil || !strings.Contains(string(out), c.want) {
				t.Errorf("extraEgress %s must fail the render with %q: err=%v\n%.600s", c.value, c.want, err, out)
			}
		})
	}
	for name, value := range map[string]string{
		"everything with ports":  `[{"to":[{"ipBlock":{"cidr":"0.0.0.0/0"}}],"ports":[{"protocol":"TCP","port":9440}]}]`,
		"everything with except": `[{"to":[{"ipBlock":{"cidr":"0.0.0.0/0","except":["10.0.0.0/8"]}}]}]`,
		"ports only":             `[{"ports":[{"protocol":"TCP","port":9440}]}]`,
		"to only":                `[{"to":[{"ipBlock":{"cidr":"10.0.0.151/32"}}]}]`,
		"named port":             `[{"ports":[{"protocol":"TCP","port":"https"}]}]`,
		"port range":             `[{"ports":[{"protocol":"TCP","port":9440,"endPort":9450}]}]`,
		"selector peers":         `[{"to":[{"namespaceSelector":{"matchLabels":{"kubernetes.io/metadata.name":"ingress-nginx"}},"podSelector":{"matchLabels":{"app":"x"}}}],"ports":[{"port":80}]}]`,
	} {
		t.Run("renders: "+name, func(t *testing.T) {
			_, egress, _ := renderPolicySet(t, "--set-json", "networkPolicy.extraEgress="+value)
			if allowed, _ := egressAllowed(egress, npComponent("web"), egressTarget{name: "DNS", namespaceLabels: map[string]string{namespaceNameLabel: "kube-system"}, protocol: "TCP", port: 53}); !allowed {
				t.Error("the shared egress lost DNS over TCP")
			}
		})
	}
}

func TestDNSEgressAllowsUDPAndTCP(t *testing.T) {
	_, egress, _ := renderPolicySet(t, "--set", "queryApi.mcp.enabled=true")
	for _, component := range []string{"query-api", "go-api", "web", "go-pgbouncer-transaction"} {
		for _, protocol := range []string{"UDP", "TCP"} {
			target := egressTarget{name: "DNS", namespaceLabels: map[string]string{namespaceNameLabel: "kube-system"}, podLabels: map[string]string{"k8s-app": "kube-dns"}, protocol: protocol, port: 53}
			if allowed, selected := egressAllowed(egress, npComponent(component), target); !selected || !allowed {
				t.Errorf("%s: DNS over %s is not allowed (selected=%v)", component, protocol, selected)
			}
		}
	}
}

// The internal listener trusts identity headers, so an empty selector in queryApi.internal.allowedFrom (every pod of the
// namespace) is refused, as it is for the MCP listener, whether or not networkPolicy is enabled.
func TestQueryAPIInternalAllowedFromRefusesEmptySelectors(t *testing.T) {
	for _, np := range []string{"true", "false"} {
		for _, value := range []string{`[{}]`, `[{"matchLabels":{}}]`, `[{"matchExpressions":[]}]`, `[{"matchLabels":{"app":"tools"}},{}]`, `["tools"]`} {
			out, err := exec.Command("helm", "template", "np", ".", "--namespace", releaseNamespace, "--set", "networkPolicy.enabled="+np,
				"--set", "goWorkers.pgbouncer.postgres.networkPolicyCIDR=10.0.0.0/24", "--set", "queryApi.enabled=true",
				"--set", "queryApi.internal.enabled=true", "--set-json", "queryApi.internal.allowedFrom="+value).CombinedOutput()
			if err == nil || !strings.Contains(string(out), "queryApi.internal.allowedFrom entries must select pods by non-empty matchLabels or matchExpressions") {
				t.Errorf("networkPolicy.enabled=%s allowedFrom %s must fail the render: err=%v\n%.400s", np, value, err, out)
			}
		}
	}
	policies, _, _ := renderPolicySet(t, "--set", "queryApi.internal.enabled=true", "--set-json", "queryApi.internal.allowedFrom="+acrSelectorJSON)
	if got, _ := admittedFrom(policies, npComponent("query-api"), []int{8091}, sourceACR); len(got) != 1 {
		t.Errorf("a non-empty selector must still be admitted to the internal port, got %v", got)
	}
}
