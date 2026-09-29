package devhealth_test

import (
	"os/exec"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// TestGoAPIInternalListenerChart pins the CHAOS-7181 wiring: the unauthenticated
// /api/v1/internal/* routes live on their own listener (--api-internal-addr), an
// internal-only ClusterIP Service carries it, the public Service never names it,
// and the NetworkPolicy admits only the listed pod selectors to it (an empty
// list denies the port; it never renders a `from` that admits every source).
func TestGoAPIInternalListenerChart(t *testing.T) {
	acr := `{"matchLabels":{"app.kubernetes.io/name":"acr","app.kubernetes.io/instance":"dev-health-acr","app.kubernetes.io/component":"api"}}`
	render := func(args ...string) []map[string]any {
		t.Helper()
		argv := append([]string{"template", "b", ".", "--set", "goApi.enabled=true"}, args...)
		out, err := exec.Command("helm", argv...).CombinedOutput()
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
	named := func(docs []map[string]any, kind, name string) map[string]any {
		t.Helper()
		for _, doc := range docs {
			meta, _ := doc["metadata"].(map[string]any)
			if doc["kind"] == kind && meta["name"] == name {
				return doc
			}
		}
		t.Fatalf("no %s %s in the render", kind, name)
		return nil
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

	docs := render("--set-json", "goApi.internal.allowedFrom=["+acr+"]")

	// The Deployment binds the listener on a named port and passes the flag.
	deployment := named(docs, "Deployment", "b-dev-health-go-api")
	container := dig(deployment, "spec", "template", "spec", "containers", 0)
	args := dig(container, "args").([]any)
	found := false
	for _, arg := range args {
		if arg == "--api-internal-addr=:8091" {
			found = true
		}
	}
	if !found {
		t.Errorf("go-api args lack --api-internal-addr=:8091: %v", args)
	}
	hasInternalPort := false
	for _, p := range dig(container, "ports").([]any) {
		if dig(p, "name") == "internal" && dig(p, "containerPort") == 8091 {
			hasInternalPort = true
		}
	}
	if !hasInternalPort {
		t.Errorf("go-api container has no named internal port 8091")
	}

	// The internal Service is ClusterIP and carries only the internal port; the
	// public Service never names it.
	internal := named(docs, "Service", "b-dev-health-go-api-internal")
	if dig(internal, "spec", "type") != "ClusterIP" || len(dig(internal, "spec", "ports").([]any)) != 1 ||
		dig(internal, "spec", "ports", 0, "port") != 8091 || dig(internal, "spec", "ports", 0, "targetPort") != "internal" {
		t.Errorf("internal Service shape: %v", internal["spec"])
	}
	public := named(docs, "Service", "b-dev-health-go-api")
	for _, p := range dig(public, "spec", "ports").([]any) {
		if dig(p, "port") == 8091 || dig(p, "targetPort") == "internal" {
			t.Errorf("the public Service names the internal port: %v", p)
		}
	}

	// The NetworkPolicy: the internal port admits exactly the listed selector.
	policy := named(docs, "NetworkPolicy", "b-dev-health-go-api-internal")
	rules := dig(policy, "spec", "ingress").([]any)
	if len(rules) != 2 {
		t.Fatalf("NetworkPolicy has %d ingress rules, want the open listeners and the internal rule: %v", len(rules), rules)
	}
	for _, rule := range rules {
		ports := dig(rule, "ports").([]any)
		for _, p := range ports {
			if dig(p, "port") == 8091 {
				from, ok := dig(rule, "from").([]any)
				if !ok || len(from) != 1 {
					t.Fatalf("the internal rule's sources are %v, want exactly the acr selector", dig(rule, "from"))
				}
				labels := dig(from[0], "podSelector", "matchLabels").(map[string]any)
				if labels["app.kubernetes.io/name"] != "acr" || labels["app.kubernetes.io/component"] != "api" || len(labels) != 3 {
					t.Errorf("internal source selector = %v", labels)
				}
			} else if dig(rule, "from") != nil {
				t.Errorf("a rule for port %v carries sources", dig(p, "port"))
			}
		}
	}

	// Default allowedFrom (empty): the internal port has NO admitting rule.
	defaults := render()
	policy = named(defaults, "NetworkPolicy", "b-dev-health-go-api-internal")
	for _, rule := range dig(policy, "spec", "ingress").([]any) {
		for _, p := range dig(rule, "ports").([]any) {
			if dig(p, "port") == 8091 {
				t.Errorf("an empty allowedFrom renders a rule admitting the internal port: %v", rule)
			}
		}
	}
}

// TestGoAPIInternalListenerNeverRoutedPublicly: no Ingress path may name the
// internal Service, and its port may not collide with a public listener.
func TestGoAPIInternalListenerNeverRoutedPublicly(t *testing.T) {
	out, err := exec.Command("helm", "template", "b", ".", "--set", "goApi.enabled=true", "--set", "ingress.enabled=true",
		"--set-json", `ingress.hosts=[{"host":"h","paths":[{"path":"/x","pathType":"Prefix","service":"go-api-internal"}]}]`).CombinedOutput()
	if err == nil || !strings.Contains(string(out), "routes to go-api-internal") {
		t.Errorf("an Ingress path to go-api-internal must fail the render: err=%v\n%s", err, out)
	}
	for _, port := range []string{"goApi.port=8091", "goApi.operatorPort=8091"} {
		out, err := exec.Command("helm", "template", "b", ".", "--set", "goApi.enabled=true", "--set", port).CombinedOutput()
		if err == nil || !strings.Contains(string(out), "goApi.internal.port must differ") {
			t.Errorf("%s with the internal port 8091 must fail the render: err=%v\n%s", port, err, out)
		}
	}
	// A rendered Ingress that carries the public api paths names no internal Service.
	out, err = exec.Command("helm", "template", "b", ".", "--set", "goApi.enabled=true", "--set", "ingress.enabled=true",
		"--set-json", `ingress.hosts=[{"host":"h","paths":[{"path":"/api/v1/orgs","pathType":"Prefix","service":"go-api"},{"path":"/","pathType":"Prefix","service":"web"}]}]`).CombinedOutput()
	if err != nil {
		t.Fatalf("render: %v\n%s", err, out)
	}
	for _, document := range strings.Split(string(out), "\n---") {
		if documentKind(document) == "Ingress" {
			if strings.Contains(document, "go-api-internal") || strings.Contains(document, "/api/v1/internal") || strings.Contains(document, "8091") {
				t.Errorf("the public Ingress references the internal listener:\n%s", document)
			}
		}
	}
}
