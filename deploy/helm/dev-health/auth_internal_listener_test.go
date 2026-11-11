package devhealth_test

import (
	"reflect"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
)

const (
	internalListenerDoc = "a-dev-health-auth-service-internal"
	internalAddressEnv  = "AUTH_SERVICE_INTERNAL_HTTP_ADDR"
)

// webOn renders web, the first PEP peer (the auth test base turns it off).
var webOn = []string{"--set", "web.enabled=true", "--set", "web.env.BACKEND_URL=http://x", "--set", "web.authSecret.secretName=web-auth-placeholder"}

// internalBase is the prod form of auth-service, phase 3 with the workload exchange on, with web rendered.
func internalBase(extra ...string) []string {
	return exchangeSets(append(append([]string{}, webOn...), extra...)...)
}

func internalOn(peers string, extra ...string) []string {
	return internalBase(append([]string{"--set", "authService.internal.enabled=true", "--set-json", "authService.internal.peers=" + peers}, extra...)...)
}

func authServiceContainer(t *testing.T, docs []map[string]any) map[string]any {
	t.Helper()
	for _, c := range podContainers(authDoc(docs, "Deployment", "a-dev-health-auth-service")) {
		if at(c, "name") == "auth-service" {
			container, _ := c.(map[string]any)
			return container
		}
	}
	t.Fatal("no auth-service container")
	return nil
}

func selectorOf(t *testing.T, docs []map[string]any, deployment string) any {
	t.Helper()
	doc := authDoc(docs, "Deployment", deployment)
	if doc == nil {
		t.Fatalf("no Deployment %s", deployment)
	}
	return at(doc, "spec", "selector", "matchLabels")
}

// Off (the default, and enabled=false with peers listed), nothing of the internal listener renders: no Service, no
// NetworkPolicy, no arg, no container port, and the render is byte-equal to the default one.
func TestAuthInternalListenerOffRendersNothing(t *testing.T) {
	t.Parallel()

	_, plain, err := renderAuth(t, phaseArgs(3, internalBase()...)...)
	if err != nil {
		t.Fatalf("render: %v\n%.600s", err, plain)
	}
	docs, off, err := renderAuth(t, phaseArgs(3, internalBase("--set", "authService.internal.enabled=false", "--set-json", `authService.internal.peers=["web"]`)...)...)
	if err != nil {
		t.Fatalf("render: %v\n%.600s", err, off)
	}
	if off != plain {
		t.Error("internal.enabled=false with peers listed changes the render")
	}
	for _, kind := range []string{"Service", "NetworkPolicy"} {
		if authDoc(docs, kind, internalListenerDoc) != nil {
			t.Errorf("internal listener off renders %s %s", kind, internalListenerDoc)
		}
	}
	container := authServiceContainer(t, docs)
	for _, arg := range toStrings(at(container, "args")) {
		if strings.Contains(arg, "internal-http-addr") {
			t.Errorf("internal listener off renders arg %s", arg)
		}
	}
	ports, _ := at(container, "ports").([]any)
	if len(ports) != 2 {
		t.Errorf("internal listener off renders %d container ports, want http and operator", len(ports))
	}
	if strings.Contains(off, "internal-http-addr") || strings.Contains(off, internalListenerDoc) {
		t.Error("internal listener off: the render names the internal listener")
	}
}

// On, auth-service gets the internal port as an arg and a named container port, a Service of its own with that one
// port, and a NetworkPolicy of its own that admits only the listed peer workloads on that one port. The API port's
// policy, the API Service and the rest of the Deployment are as they are with the listener off.
func TestAuthInternalListenerRendersThePortServiceAndPolicy(t *testing.T) {
	offDocs := renderPhase(t, 3, internalBase()...)
	docs := renderPhase(t, 3, internalOn(`["web"]`)...)
	authSelector := selectorOf(t, docs, "a-dev-health-auth-service")
	webSelector := selectorOf(t, docs, "a-dev-health-web")
	if at(webSelector, "app.kubernetes.io/component") != "web" {
		t.Fatalf("web selector %v", webSelector)
	}

	container := authServiceContainer(t, docs)
	var internalArgs []string
	for _, arg := range toStrings(at(container, "args")) {
		if strings.Contains(arg, "internal-http-addr") {
			internalArgs = append(internalArgs, arg)
		}
	}
	if !reflect.DeepEqual(internalArgs, []string{"--internal-http-addr=:8097"}) {
		t.Errorf("internal listener args = %v, want --internal-http-addr=:8097", internalArgs)
	}
	wantPort := map[string]any{"name": "internal", "containerPort": 8097, "protocol": "TCP"}
	ports, _ := at(container, "ports").([]any)
	if len(ports) != 3 || !reflect.DeepEqual(ports[2], wantPort) {
		t.Errorf("container ports = %v, want http, operator and %v", ports, wantPort)
	}

	overrides := authServiceOverrides(t, authDoc(docs, "Deployment", "a-dev-health-auth-service"))
	if overrides[internalAddressEnv] != ":8097" {
		t.Errorf("the rendered args set %s = %q, want :8097", internalAddressEnv, overrides[internalAddressEnv])
	}
	if _, err := authconfig.Load(authconfig.Spec{Overrides: overrides, LookupEnv: withDefaultIssuer(containerLookup(t, authDoc(docs, "Deployment", "a-dev-health-auth-service"), "auth-service"))}); err != nil {
		t.Errorf("auth-service refuses the rendered internal listener: %v", err)
	}

	service := authDoc(docs, "Service", internalListenerDoc)
	if service == nil {
		t.Fatal("no internal listener Service")
	}
	if got := at(service, "spec", "selector"); !reflect.DeepEqual(got, authSelector) {
		t.Errorf("internal Service selector = %v, want auth-service's %v", got, authSelector)
	}
	wantServicePorts := []any{map[string]any{"name": "internal", "port": 8097, "targetPort": "internal", "protocol": "TCP"}}
	if got := at(service, "spec", "ports"); !reflect.DeepEqual(got, wantServicePorts) {
		t.Errorf("internal Service ports = %v, want %v", got, wantServicePorts)
	}
	if at(service, "spec", "type") != "ClusterIP" {
		t.Errorf("internal Service type = %v, want ClusterIP", at(service, "spec", "type"))
	}

	policy := authDoc(docs, "NetworkPolicy", internalListenerDoc)
	if policy == nil {
		t.Fatal("no internal listener NetworkPolicy")
	}
	if got := at(policy, "spec", "podSelector", "matchLabels"); !reflect.DeepEqual(got, authSelector) {
		t.Errorf("internal policy podSelector = %v, want auth-service's %v", got, authSelector)
	}
	if got := at(policy, "spec", "policyTypes"); !reflect.DeepEqual(got, []any{"Ingress"}) {
		t.Errorf("internal policy policyTypes = %v, want [Ingress] alone", got)
	}
	if egress := at(policy, "spec", "egress"); egress != nil {
		t.Errorf("internal policy has egress %v", egress)
	}
	wantIngress := []any{map[string]any{
		"from":  []any{map[string]any{"podSelector": map[string]any{"matchLabels": webSelector}}},
		"ports": []any{map[string]any{"protocol": "TCP", "port": 8097}},
	}}
	if got := at(policy, "spec", "ingress"); !reflect.DeepEqual(got, wantIngress) {
		t.Errorf("internal policy ingress = %v, want web's pods on 8097 alone: %v", got, wantIngress)
	}

	for _, kind := range []string{"NetworkPolicy", "Service"} {
		if got, want := authDoc(docs, kind, "a-dev-health-auth-service"), authDoc(offDocs, kind, "a-dev-health-auth-service"); !reflect.DeepEqual(got, want) {
			t.Errorf("the API %s changes with the internal listener on:\n got %v\nwant %v", kind, got, want)
		}
	}
	on, off := authDoc(docs, "Deployment", "a-dev-health-auth-service"), authDoc(offDocs, "Deployment", "a-dev-health-auth-service")
	stripInternal(t, on)
	if !reflect.DeepEqual(on, off) {
		t.Errorf("the auth-service Deployment differs beyond the internal arg and port:\n got %v\nwant %v", on, off)
	}
}

// stripInternal drops the internal listener's arg and container port from an auth-service Deployment.
func stripInternal(t *testing.T, doc map[string]any) {
	t.Helper()
	container := map[string]any{}
	for _, c := range podContainers(doc) {
		if at(c, "name") == "auth-service" {
			container, _ = c.(map[string]any)
		}
	}
	var args []any
	for _, arg := range container["args"].([]any) {
		if !strings.Contains(toString(arg), "internal-http-addr") {
			args = append(args, arg)
		}
	}
	container["args"] = args
	var ports []any
	for _, port := range container["ports"].([]any) {
		if at(port, "name") != "internal" {
			ports = append(ports, port)
		}
	}
	container["ports"] = ports
}

// Each listed peer is admitted by its own pod selector, in the listed order, and no other source.
func TestAuthInternalListenerAdmitsOnlyTheListedPeers(t *testing.T) {
	t.Parallel()

	docs := renderPhase(t, 3, internalOn(`["web","query-api","go-api"]`, "--set", "authService.internal.port=9443")...)
	var want []any
	for _, name := range []string{"a-dev-health-web", "a-dev-health-query-api", "a-dev-health-go-api"} {
		want = append(want, map[string]any{"podSelector": map[string]any{"matchLabels": selectorOf(t, docs, name)}})
	}
	policy := authDoc(docs, "NetworkPolicy", internalListenerDoc)
	rules, _ := at(policy, "spec", "ingress").([]any)
	if len(rules) != 1 {
		t.Fatalf("internal policy has %d ingress rules, want 1", len(rules))
	}
	if got := at(rules[0], "from"); !reflect.DeepEqual(got, want) {
		t.Errorf("internal policy from = %v, want %v", got, want)
	}
	if got := at(rules[0], "ports"); !reflect.DeepEqual(got, []any{map[string]any{"protocol": "TCP", "port": 9443}}) {
		t.Errorf("internal policy ports = %v, want 9443 alone", got)
	}
}

func TestAuthInternalListenerRefusals(t *testing.T) {
	t.Parallel()

	cases := map[string]struct {
		args []string
		want string
	}{
		"no peer":          {phaseArgs(3, internalOn(`[]`)...), "authService.internal.peers"},
		"unknown peer":     {phaseArgs(3, internalOn(`["acr-api"]`)...), "authService.internal.peers"},
		"selector peer":    {phaseArgs(3, internalOn(`[{"matchLabels":{"app":"x"}}]`)...), "authService.internal.peers"},
		"repeated peer":    {phaseArgs(3, internalOn(`["web","web"]`)...), "authService.internal.peers"},
		"peers not a list": {phaseArgs(3, internalOn(`"web"`)...), "authService.internal.peers"},
		"api port":         {phaseArgs(3, internalOn(`["web"]`, "--set", "authService.internal.port=8095")...), "authService.internal.port"},
		"operator port":    {phaseArgs(3, internalOn(`["web"]`, "--set", "authService.internal.port=8096")...), "authService.internal.port"},
		"port zero":        {phaseArgs(3, internalOn(`["web"]`, "--set", "authService.internal.port=0")...), "authService.internal.port"},
		"port too big":     {phaseArgs(3, internalOn(`["web"]`, "--set", "authService.internal.port=65536")...), "authService.internal.port"},
		"port string":      {phaseArgs(3, internalOn(`["web"]`, "--set-string", "authService.internal.port=8097")...), "authService.internal.port"},
		"enabled string":   {phaseArgs(3, internalBase("--set-string", "authService.internal.enabled=true", "--set-json", `authService.internal.peers=["web"]`)...), "authService.internal.enabled"},
		"no network policy": {phaseArgs(1, append(append(append(issuerSets("", hs256Issuer), exchangeInputs()...), webOn...),
			"--set", "authService.internal.enabled=true", "--set-json", `authService.internal.peers=["web"]`, "--set", "authService.networkPolicy.enabled=false")...), "authService.networkPolicy.enabled"},
		"extraArgs address":  {phaseArgs(3, internalBase("--set-json", `authService.extraArgs=["--internal-http-addr=:9000"]`)...), "authService.extraArgs must not set internal-http-addr"},
		"extraEnv address":   {phaseArgs(3, internalBase("--set-json", `authService.extraEnv=[{"name":"AUTH_SERVICE_INTERNAL_HTTP_ADDR","value":":9000"}]`)...), "authService.extraEnv must not set AUTH_SERVICE_INTERNAL_HTTP_ADDR"},
		"extraEnv lowercase": {phaseArgs(3, internalBase("--set-json", `authService.extraEnv=[{"name":"auth_service_internal_http_addr","value":":9000"}]`)...), "authService.extraEnv must not set"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, out, err := renderAuth(t, c.args...)
			if err == nil || !strings.Contains(out, c.want) {
				t.Fatalf("not refused with %q (err %v): %.300s", c.want, err, out)
			}
		})
	}
}
