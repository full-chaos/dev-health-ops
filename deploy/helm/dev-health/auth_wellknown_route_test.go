package devhealth_test

import (
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// The issuer's JWKS and discovery documents are public by design: pods fetch them through the public host
// (issuer + /.well-known/...). The chart may route exactly those two paths of the issuer's own host to auth-service,
// and nothing else, and admits the ingress controller to the API port of auth-service only while that route exists.

const (
	wkDiscovery = "/.well-known/openid-configuration"
	wkJWKS      = "/.well-known/jwks.json"
	wkHost      = "auth.example.test"
	wkIssuer    = "https://" + wkHost
	wkAuthSvc   = "a-dev-health-auth-service"
)

func wkPath(path, pathType string) string {
	return `{"path":"` + path + `","pathType":"` + pathType + `","service":"auth-service"}`
}

func wkHostValue(paths ...string) string {
	return `{"host":"` + wkHost + `","paths":[` + strings.Join(paths, ",") + `]}`
}

var wkBothPaths = []string{wkPath(wkDiscovery, "Exact"), wkPath(wkJWKS, "Exact")}

func wkArgs(phase int, hosts []string, extra ...string) []string {
	args := []string{
		"--set", "ingress.enabled=true", "--set", "goApi.enabled=true", "--set", "queryApi.enabled=true",
		"--set-json", `ingress.hosts=[{"host":"api.example.test","pythonAllowList":true,"paths":[{"path":"/","pathType":"Prefix","service":"go-api"}]},` +
			`{"host":"www.example.test","paths":[{"path":"/","pathType":"Prefix","service":"web"}]}` + wkJoin(hosts) + `]`,
	}
	return append(phaseArgs(phase, args...), extra...)
}

func wkJoin(hosts []string) string {
	if len(hosts) == 0 {
		return ""
	}
	return "," + strings.Join(hosts, ",")
}

type wkRule struct{ host, path, pathType, service string }

func wkIngressRules(docs []map[string]any) []wkRule {
	var rules []wkRule
	for _, doc := range docs {
		if doc["kind"] != "Ingress" {
			continue
		}
		for _, rule := range aList(at(doc, "spec", "rules")) {
			for _, p := range aList(at(rule, "http", "paths")) {
				rules = append(rules, wkRule{aStr(at(rule, "host")), aStr(at(p, "path")), aStr(at(p, "pathType")), aStr(at(p, "backend", "service", "name"))})
			}
		}
	}
	return rules
}

func wkNetpol(t *testing.T, docs []map[string]any) netpolDoc {
	t.Helper()
	raw, err := yaml.Marshal(authDoc(docs, "NetworkPolicy", wkAuthSvc))
	if err != nil {
		t.Fatal(err)
	}
	var policy netpolDoc
	if err := yaml.Unmarshal(raw, &policy); err != nil {
		t.Fatal(err)
	}
	return policy
}

func TestWellKnownRouteRendersTheTwoExactPathsOnly(t *testing.T) {
	docs, out, err := renderAuth(t, wkArgs(2, []string{wkHostValue(wkBothPaths...)}, "--set", "authService.accessToken.issuerURL="+wkIssuer)...)
	if err != nil {
		t.Fatalf("render: %v\n%.600s", err, out)
	}
	var toAuth []wkRule
	for _, r := range wkIngressRules(docs) {
		if r.service == wkAuthSvc {
			toAuth = append(toAuth, r)
		} else if r.host == wkHost {
			t.Errorf("host %s also routes %s to %s", wkHost, r.path, r.service)
		}
	}
	want := []wkRule{{wkHost, wkDiscovery, "Exact", wkAuthSvc}, {wkHost, wkJWKS, "Exact", wkAuthSvc}}
	if len(toAuth) != 2 || toAuth[0] != want[0] || toAuth[1] != want[1] {
		t.Errorf("rules to auth-service = %v, want exactly %v", toAuth, want)
	}
	if got := aList(at(authDoc(docs, "Service", wkAuthSvc), "spec", "ports")); len(got) == 0 {
		t.Fatal("no auth-service Service")
	}
	for _, d := range docs {
		if d["kind"] != "Ingress" {
			continue
		}
		for _, rule := range aList(at(d, "spec", "rules")) {
			for _, p := range aList(at(rule, "http", "paths")) {
				if aStr(at(p, "backend", "service", "name")) == wkAuthSvc && at(p, "backend", "service", "port", "number") != 8095 {
					t.Errorf("auth-service backend port = %v, want the API port 8095", at(p, "backend", "service", "port", "number"))
				}
			}
		}
	}
}

func TestWellKnownRouteIsOffUnlessAHostNamesIt(t *testing.T) {
	docs := renderOK(t, wkArgs(2, nil, "--set", "authService.accessToken.issuerURL="+wkIssuer))
	for _, r := range wkIngressRules(docs) {
		if r.service == wkAuthSvc {
			t.Errorf("rule %v routes to auth-service with no host naming it", r)
		}
	}
	policy := wkNetpol(t, docs)
	ctl := wkController(defaultControllerNamespace)
	if got, _ := admittedFrom([]netpolDoc{policy}, authPodLabels(), []int{8095, 8096}, ctl); len(got) != 0 {
		t.Errorf("ingress controller admitted to %v with no route to auth-service", got)
	}
}

func authPodLabels() map[string]string {
	return map[string]string{"app.kubernetes.io/name": "dev-health", "app.kubernetes.io/instance": "a", "app.kubernetes.io/component": "auth-service"}
}

func renderOK(t *testing.T, args []string) []map[string]any {
	t.Helper()
	docs, out, err := renderAuth(t, args...)
	if err != nil {
		t.Fatalf("render: %v\n%.600s", err, out)
	}
	return docs
}

func TestWellKnownRouteAdmitsTheIngressControllerOnTheAPIPortOnly(t *testing.T) {
	docs := renderOK(t, wkArgs(2, []string{wkHostValue(wkBothPaths...)}, "--set", "authService.accessToken.issuerURL="+wkIssuer))
	policy := wkNetpol(t, docs)
	pod := authPodLabels()
	probes := []int{80, 3000, 8000, 8090, 8091, 8095, 8096, 9000}
	ctl := wkController(defaultControllerNamespace)
	if got, sel := admittedFrom([]netpolDoc{policy}, pod, probes, ctl); !sel || len(got) != 1 || got[0] != 8095 {
		t.Errorf("ingress controller admitted to %v (selected %v), want [8095]", got, sel)
	}
	notController := wkController(defaultControllerNamespace)
	notController.name, notController.podLabels = "a non-controller pod of the controller namespace", map[string]string{"app": "stranger"}
	for _, src := range []netpolSource{sourceOtherNamespace, sourceStranger, notController} {
		if got, _ := admittedFrom([]netpolDoc{policy}, pod, probes, src); len(got) != 0 {
			t.Errorf("%s admitted to %v", src.name, got)
		}
	}
	goAPI := netpolSource{"go-api", map[string]string{namespaceNameLabel: releaseNamespace}, map[string]string{"app.kubernetes.io/name": "dev-health", "app.kubernetes.io/instance": "a", "app.kubernetes.io/component": "go-api"}}
	if got, _ := admittedFrom([]netpolDoc{policy}, pod, probes, goAPI); len(got) != 1 || got[0] != 8095 {
		t.Errorf("go-api admitted to %v, want [8095] unchanged", got)
	}
}

func TestWellKnownRouteControllerNamespaceFollowsNetworkPolicyValue(t *testing.T) {
	docs := renderOK(t, wkArgs(2, []string{wkHostValue(wkBothPaths...)}, "--set", "authService.accessToken.issuerURL="+wkIssuer, "--set", "networkPolicy.ingressControllerNamespace=edge"))
	policy := wkNetpol(t, docs)
	if got, _ := admittedFrom([]netpolDoc{policy}, authPodLabels(), []int{8095}, wkController("edge")); len(got) != 1 {
		t.Errorf("controller of namespace edge not admitted: %v", got)
	}
	if got, _ := admittedFrom([]netpolDoc{policy}, authPodLabels(), []int{8095}, wkController(defaultControllerNamespace)); len(got) != 0 {
		t.Errorf("controller of the default namespace admitted though the value names edge: %v", got)
	}
}

func TestWellKnownRouteRefusals(t *testing.T) {
	issuer := []string{"--set", "authService.accessToken.issuerURL=" + wkIssuer}
	cases := []struct {
		name  string
		phase int
		hosts []string
		extra []string
		want  string
	}{
		{"Prefix path type", 2, []string{wkHostValue(wkPath(wkDiscovery, "Prefix"), wkPath(wkJWKS, "Exact"))}, issuer, "only the two well-known documents as Exact paths"},
		{"root path", 2, []string{wkHostValue(wkPath("/", "Exact"))}, issuer, "only the two well-known documents as Exact paths"},
		{"a third path", 2, []string{wkHostValue(append([]string{wkPath("/healthz", "Exact")}, wkBothPaths...)...)}, issuer, "only the two well-known documents as Exact paths"},
		{"look-alike path", 2, []string{wkHostValue(wkPath(wkJWKS+"x", "Exact"))}, issuer, "only the two well-known documents as Exact paths"},
		{"duplicate path", 2, []string{wkHostValue(wkPath(wkJWKS, "Exact"), wkPath(wkJWKS, "Exact"))}, issuer, "path to auth-service twice"},
		{"host also routes web", 2, []string{`{"host":"` + wkHost + `","paths":[` + wkPath(wkJWKS, "Exact") + `,{"path":"/","pathType":"Prefix","service":"web"}]}`}, issuer, "only the two well-known documents"},
		{"host with allow-list", 2, []string{`{"host":"` + wkHost + `","pythonAllowList":true,"paths":[` + wkPath(wkJWKS, "Exact") + `]}`}, issuer, "must not carry a pythonAllowList"},
		{"no issuer", 2, []string{wkHostValue(wkBothPaths...)}, nil, "needs authService.accessToken.issuerURL"},
		{"issuer on another host", 2, []string{wkHostValue(wkBothPaths...)}, []string{"--set", "authService.accessToken.issuerURL=https://other.example.test"}, "must be https://" + wkHost},
		{"issuer with a path prefix", 2, []string{wkHostValue(wkBothPaths...)}, []string{"--set", "authService.accessToken.issuerURL=" + wkIssuer + "/v1"}, "must be https://" + wkHost},
		{"controller namespace is the release namespace", 2, []string{wkHostValue(wkBothPaths...)}, append(append([]string{}, issuer...), "--namespace", "ns1", "--set", "networkPolicy.ingressControllerNamespace=ns1"), "must name the ingress controller's namespace"},
		{"allow-list false", 2, []string{`{"host":"` + wkHost + `","pythonAllowList":false,"paths":[` + strings.Join(wkBothPaths, ",") + `]}`}, issuer, "must not carry a pythonAllowList"},
		{"allow-list empty list", 2, []string{`{"host":"` + wkHost + `","pythonAllowList":[],"paths":[` + strings.Join(wkBothPaths, ",") + `]}`}, issuer, "must not carry a pythonAllowList"},
		{"allow-list null", 2, []string{`{"host":"` + wkHost + `","pythonAllowList":null,"paths":[` + strings.Join(wkBothPaths, ",") + `]}`}, issuer, "must not carry a pythonAllowList"},
		{"second entry of the same host routes web", 2, []string{wkHostValue(wkBothPaths...), `{"host":"` + wkHost + `","paths":[{"path":"/","pathType":"Prefix","service":"web"}]}`}, issuer, "more than one ingress.hosts entry"},
		{"second entry of the same host routes auth-service again", 2, []string{wkHostValue(wkPath(wkJWKS, "Exact")), wkHostValue(wkPath(wkDiscovery, "Exact"))}, issuer, "more than one ingress.hosts entry"},
		{"use-regex annotation", 2, []string{wkHostValue(wkBothPaths...)}, append(append([]string{}, issuer...), "--set-string", `ingress.annotations.nginx\.ingress\.kubernetes\.io/use-regex=true`), "on an Ingress that routes to auth-service"},
		{"rewrite-target annotation", 2, []string{wkHostValue(wkBothPaths...)}, append(append([]string{}, issuer...), "--set-string", `ingress.annotations.nginx\.ingress\.kubernetes\.io/rewrite-target=/x`), "on an Ingress that routes to auth-service"},
		{"controller labels null", 2, []string{wkHostValue(wkBothPaths...)}, append(append([]string{}, issuer...), "--set-json", `authService.networkPolicy.ingressControllerPodLabels=null`), "ingressControllerPodLabels must not be empty"},
		{"controller labels empty map", 2, []string{wkHostValue(wkBothPaths...)}, append(append([]string{}, issuer...), "--set-json", `authService.networkPolicy.ingressControllerPodLabels={}`), "ingressControllerPodLabels must not be empty"},
		{"auth-service not running", 0, []string{wkHostValue(wkBothPaths...)}, issuer, "authService.phase >= 1"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, out, err := renderAuth(t, wkArgs(c.phase, c.hosts, c.extra...)...)
			if err == nil || !strings.Contains(out, c.want) {
				t.Errorf("want a render failure containing %q, got err=%v\n%.800s", c.want, err, out)
			}
		})
	}
}

func wkController(namespace string) netpolSource {
	src := controllerSourceFor(namespace)
	src.podLabels = map[string]string{"app.kubernetes.io/name": "ingress-nginx", "app.kubernetes.io/component": "controller"}
	return src
}

// One API-port rule carries every peer: the egress proof plan of the deploy repo reads at most one rule on the API port.
func TestWellKnownRouteSharesTheOneAPIPortRule(t *testing.T) {
	for _, routed := range []bool{false, true} {
		var hosts []string
		if routed {
			hosts = []string{wkHostValue(wkBothPaths...)}
		}
		policy := wkNetpol(t, renderOK(t, wkArgs(2, hosts, "--set", "authService.accessToken.issuerURL="+wkIssuer)))
		rules := 0
		for _, rule := range policy.Spec.Ingress {
			for _, p := range rule.Ports {
				if p.Port == 8095 {
					rules++
				}
			}
		}
		if rules != 1 {
			t.Errorf("routed=%v: %d ingress rules on the API port, want 1", routed, rules)
		}
	}
}
