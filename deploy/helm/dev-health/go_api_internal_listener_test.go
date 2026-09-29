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
	compat := false
	for _, arg := range args {
		if arg == "--api-acr-public-compat=true" {
			compat = true
		}
	}
	if !compat {
		t.Errorf("acrPublicCompat defaults true in the chart (one-roll bridge): args %v", args)
	}
	off := render("--set", "goApi.internal.acrPublicCompat=false")
	offArgs := dig(dig(named(off, "Deployment", "b-dev-health-go-api"), "spec", "template", "spec", "containers", 0), "args").([]any)
	for _, arg := range offArgs {
		if strings.Contains(arg.(string), "acr-public-compat") {
			t.Errorf("acrPublicCompat=false still passes %v", arg)
		}
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

	// Every port except the internal one is open to every source (two ranges around it),
	// so no listener the process is started with can be silently denied, however it is
	// configured (extraArgs, extraEnv, envFrom, IPv6 host).
	covers := func(rule any, port int) bool {
		ports, has := dig(rule, "ports").([]any)
		if !has {
			return true // no ports = every port
		}
		for _, p := range ports {
			low := dig(p, "port").(int)
			high := low
			if end, ok := dig(p, "endPort").(int); ok {
				high = end
			}
			if port >= low && port <= high {
				return true
			}
		}
		return false
	}
	openToAll := func(docs []map[string]any, port int) bool {
		for _, rule := range dig(named(docs, "NetworkPolicy", "b-dev-health-go-api-internal"), "spec", "ingress").([]any) {
			if dig(rule, "from") == nil && covers(rule, port) {
				return true
			}
		}
		return false
	}
	for _, p := range dig(container, "ports").([]any) {
		port := dig(p, "containerPort").(int)
		if port != 8091 && !openToAll(docs, port) {
			t.Errorf("containerPort %d is denied by the NetworkPolicy", port)
		}
	}
	for _, port := range []int{1, 8000, 8010, 8080, 8090, 8092, 18010, 65535} {
		if !openToAll(docs, port) {
			t.Errorf("port %d is not open to every source", port)
		}
	}
	if openToAll(docs, 8091) {
		t.Errorf("the internal port is open to every source")
	}
	// A second --api-internal-addr (extraArgs/extraEnv) would move the listener off the
	// gated port; an empty selector entry would admit every pod.
	for _, bad := range []struct {
		want string
		args []string
	}{
		{"must not set --api-internal-addr", []string{"--set", "goApi.extraArgs[0]=--api-internal-addr=:18092"}},
		{"must not set DEV_HEALTH_API_INTERNAL_ADDR", []string{"--set-json", `goApi.extraEnv=[{"name":"DEV_HEALTH_API_INTERNAL_ADDR","value":":18092"}]`}},
		{"non-empty matchLabels or matchExpressions", []string{"--set-json", "goApi.internal.allowedFrom=[{}]"}},
		{"non-empty matchLabels or matchExpressions", []string{"--set-json", `goApi.internal.allowedFrom=[{"matchLabels":{}}]`}},
	} {
		argv := append([]string{"template", "b", ".", "--set", "goApi.enabled=true"}, bad.args...)
		if out, err := exec.Command("helm", argv...).CombinedOutput(); err == nil || !strings.Contains(string(out), bad.want) {
			t.Errorf("%v must fail the render with %q: err=%v\n%s", bad.args, bad.want, err, out)
		}
	}
	// The chart-wide policy (networkPolicy.enabled) admits every pod on every port; policies are
	// additive, so it must not select the go-api pods, or it would bypass the allowlist above.
	np := render("--set", "networkPolicy.enabled=true", "--set", "goWorkers.pgbouncer.postgres.networkPolicyCIDR=10.0.0.0/8",
		"--set-json", "goApi.internal.allowedFrom=["+acr+"]")
	goLabels := dig(named(np, "Deployment", "b-dev-health-go-api"), "spec", "template", "metadata", "labels").(map[string]any)
	selecting := 0
	for _, doc := range np {
		if doc["kind"] != "NetworkPolicy" {
			continue
		}
		sel := dig(doc, "spec", "podSelector")
		matched := true
		for k, v := range dig(sel, "matchLabels").(map[string]any) {
			if goLabels[k] != v {
				matched = false
			}
		}
		if exprs, ok := dig(sel, "matchExpressions").([]any); ok {
			for _, e := range exprs {
				if dig(e, "operator") == "NotIn" && dig(e, "key") == "app.kubernetes.io/component" {
					for _, v := range dig(e, "values").([]any) {
						if v == goLabels["app.kubernetes.io/component"] {
							matched = false
						}
					}
				}
			}
		}
		if !matched {
			continue
		}
		selecting++
		ingress := false
		for _, t := range dig(doc, "spec", "policyTypes").([]any) {
			ingress = ingress || t == "Ingress"
		}
		if !ingress {
			continue
		}
		for _, rule := range dig(doc, "spec", "ingress").([]any) {
			if !covers(rule, 8091) {
				continue
			}
			from, ok := dig(rule, "from").([]any)
			if !ok || len(from) != 1 || dig(from[0], "podSelector", "matchLabels", "app.kubernetes.io/name") != "acr" {
				name := dig(doc, "metadata", "name")
				if name != "b-dev-health-go-api-internal" || ok {
					t.Errorf("policy %v admits the internal port from %v", name, dig(rule, "from"))
				}
			}
		}
	}
	if selecting < 2 {
		t.Errorf("expected the go-api-internal and go-api-egress policies to select the go-api pods, got %d", selecting)
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
