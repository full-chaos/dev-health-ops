package devhealth_test

import (
	"os/exec"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// TestQueryAPIMCPListenerChart pins the CHAOS-7085 wiring: the MCP caller-class
// listener (QUERY_API_MCP_ADDR) has its own ClusterIP Service, its NetworkPolicy
// admits ONLY queryApi.mcp.allowedFrom on the MCP port (never the api pods, never
// every source; an empty list denies the port), the public Service and every
// Ingress backend never name it, and each configuration that would bypass that
// boundary fails the render.
func TestQueryAPIMCPListenerChart(t *testing.T) {
	const acr = `{"matchLabels":{"app.kubernetes.io/name":"acr","app.kubernetes.io/instance":"dev-health-acr","app.kubernetes.io/component":"api"}}`
	base := []string{"template", "b", ".", "--set", "queryApi.enabled=true", "--set", "queryApi.mcp.enabled=true"}
	render := func(args ...string) []map[string]any {
		t.Helper()
		out, err := exec.Command("helm", append(append([]string{}, base...), args...)...).CombinedOutput()
		if err != nil {
			t.Fatalf("render failed: %v\n%s", err, out)
		}
		var docs []map[string]any
		decoder := yaml.NewDecoder(strings.NewReader(string(out)))
		for {
			var doc map[string]any
			if err := decoder.Decode(&doc); err != nil {
				break
			}
			if doc != nil {
				docs = append(docs, doc)
			}
		}
		return docs
	}
	find := func(docs []map[string]any, kind, name string) map[string]any {
		for _, doc := range docs {
			meta, _ := doc["metadata"].(map[string]any)
			if doc["kind"] == kind && meta["name"] == name {
				return doc
			}
		}
		return nil
	}
	named := func(docs []map[string]any, kind, name string) map[string]any {
		t.Helper()
		doc := find(docs, kind, name)
		if doc == nil {
			t.Fatalf("no %s %s in the render", kind, name)
		}
		return doc
	}
	dig := func(v any, path ...any) any {
		for _, key := range path {
			switch k := key.(type) {
			case string:
				m, _ := v.(map[string]any)
				v = m[k]
			case int:
				l, _ := v.([]any)
				if k >= len(l) {
					return nil
				}
				v = l[k]
			}
		}
		return v
	}

	docs := render("--set-json", "queryApi.mcp.allowedFrom=["+acr+"]")

	// The Deployment binds the listener on a named port and passes the address.
	container := dig(named(docs, "Deployment", "b-dev-health-query-api"), "spec", "template", "spec", "containers", 0)
	hasPort := false
	for _, p := range dig(container, "ports").([]any) {
		if dig(p, "name") == "mcp" && dig(p, "containerPort") == 8092 {
			hasPort = true
		}
	}
	if !hasPort {
		t.Errorf("query-api container has no named mcp port 8092: %v", dig(container, "ports"))
	}
	hasEnv := false
	for _, e := range dig(container, "env").([]any) {
		if dig(e, "name") == "QUERY_API_MCP_ADDR" && dig(e, "value") == ":8092" {
			hasEnv = true
		}
	}
	if !hasEnv {
		t.Errorf("query-api container lacks QUERY_API_MCP_ADDR=:8092")
	}

	// The MCP Service: ClusterIP, the MCP port only. The public Service never names it.
	mcp := named(docs, "Service", "b-dev-health-query-api-mcp")
	if dig(mcp, "spec", "type") != "ClusterIP" || len(dig(mcp, "spec", "ports").([]any)) != 1 ||
		dig(mcp, "spec", "ports", 0, "port") != 8092 || dig(mcp, "spec", "ports", 0, "targetPort") != "mcp" {
		t.Errorf("MCP Service shape: %v", mcp["spec"])
	}
	for _, p := range dig(named(docs, "Service", "b-dev-health-query-api"), "spec", "ports").([]any) {
		if dig(p, "port") == 8092 || dig(p, "targetPort") == "mcp" {
			t.Errorf("the public query-api Service names the MCP port: %v", p)
		}
	}

	// The NetworkPolicy: 8092 admits exactly the acr selector; the public port stays open.
	policy := named(docs, "NetworkPolicy", "b-dev-health-query-api-mcp")
	rules := dig(policy, "spec", "ingress").([]any)
	if len(rules) != 2 {
		t.Fatalf("MCP NetworkPolicy has %d ingress rules, want the public port and the MCP rule: %v", len(rules), rules)
	}
	sawMCP := false
	for _, rule := range rules {
		for _, p := range dig(rule, "ports").([]any) {
			switch dig(p, "port") {
			case 8092:
				sawMCP = true
				from, ok := dig(rule, "from").([]any)
				if !ok || len(from) != 1 {
					t.Fatalf("the MCP rule's sources are %v, want exactly the acr selector", dig(rule, "from"))
				}
				labels := dig(from[0], "podSelector", "matchLabels").(map[string]any)
				if labels["app.kubernetes.io/name"] != "acr" || labels["app.kubernetes.io/component"] != "api" || len(labels) != 3 {
					t.Errorf("MCP source selector = %v (the api pods or any wider set must not be admitted)", labels)
				}
			case 8090:
				if dig(rule, "from") != nil {
					t.Errorf("the public port rule carries sources: %v", rule)
				}
			default:
				t.Errorf("unexpected port %v in the MCP NetworkPolicy", dig(p, "port"))
			}
		}
	}
	if !sawMCP {
		t.Fatal("the MCP NetworkPolicy has no rule for 8092")
	}

	// An empty allowedFrom denies the port: no rule for 8092 at all (a rule with no
	// `from` would admit every source).
	for _, rule := range dig(named(render(), "NetworkPolicy", "b-dev-health-query-api-mcp"), "spec", "ingress").([]any) {
		for _, p := range dig(rule, "ports").([]any) {
			if dig(p, "port") == 8092 {
				t.Fatalf("an empty allowedFrom rendered a rule for the MCP port: %v", rule)
			}
		}
	}

	// Off by default: no MCP Service, policy, port or env.
	out, err := exec.Command("helm", "template", "b", ".", "--set", "queryApi.enabled=true").CombinedOutput()
	if err != nil {
		t.Fatalf("default render failed: %v\n%s", err, out)
	}
	if strings.Contains(string(out), "query-api-mcp") || strings.Contains(string(out), "QUERY_API_MCP_ADDR") {
		t.Error("the MCP listener renders although queryApi.mcp.enabled defaults false")
	}

	// The public Ingress never carries an MCP path, even with every default host rendered.
	withIngress := render("--set-json", "queryApi.mcp.allowedFrom=["+acr+"]", "--set", "ingress.enabled=true")
	ingress := named(withIngress, "Ingress", "b-dev-health")
	for _, rule := range dig(ingress, "spec", "rules").([]any) {
		for _, path := range dig(rule, "http", "paths").([]any) {
			name, _ := dig(path, "backend", "service", "name").(string)
			if strings.Contains(name, "mcp") || dig(path, "backend", "service", "port", "number") == 8092 {
				t.Errorf("the public Ingress routes to the MCP listener: %v", path)
			}
		}
	}

	// Configurations that would bypass the boundary fail the render.
	for _, bad := range []struct {
		want string
		args []string
	}{
		{"routes to query-api-mcp", []string{"--set", "ingress.enabled=true", "--set-json", `ingress.hosts=[{"host":"h","paths":[{"path":"/mcp","pathType":"Prefix","service":"query-api-mcp"}]}]`}},
		{"must not set QUERY_API_MCP_ADDR", []string{"--set-json", `queryApi.extraEnv=[{"name":"QUERY_API_MCP_ADDR","value":":18092"}]`}},
		{"non-empty matchLabels or matchExpressions", []string{"--set-json", "queryApi.mcp.allowedFrom=[{}]"}},
		{"non-empty matchLabels or matchExpressions", []string{"--set-json", `queryApi.mcp.allowedFrom=[{"matchLabels":{}}]`}},
		{"with networkPolicy.enabled", []string{"--set", "networkPolicy.enabled=true"}},
		{"must differ from queryApi.port", []string{"--set", "queryApi.mcp.port=8090"}},
		{"must differ from queryApi.internal.port", []string{"--set", "queryApi.internal.enabled=true", "--set", "queryApi.mcp.port=8091"}},
	} {
		argv := append(append([]string{}, base...), bad.args...)
		if out, err := exec.Command("helm", argv...).CombinedOutput(); err == nil || !strings.Contains(string(out), bad.want) {
			t.Errorf("%v must fail the render with %q: err=%v\n%s", bad.args, bad.want, err, out)
		}
	}
}
