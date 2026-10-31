package devhealth_test

import (
	"reflect"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
)

const (
	exchangeCA    = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
	exchangeToken = "/var/run/secrets/kubernetes.io/serviceaccount/token"
)

func exchangeSets(extra ...string) []string {
	return append(append(issuerSets("hs256_compat", hs256Issuer), exchangeInputs()...), extra...)
}

func exchangeInputs() []string {
	return []string{
		"--set", "authService.workloadExchange.enabled=true",
		"--set", "authService.workloadExchange.trustDomain=cluster.example",
		"--set", "authService.workloadExchange.clusterIssuer=https://kubernetes.default.svc",
		"--set-json", `authService.networkPolicy.egress.kubeAPI.to=[{"ipBlock":{"cidr":"10.43.0.1/32"}}]`,
	}
}

func kindDocs(docs []map[string]any, kind string) []map[string]any {
	var out []map[string]any
	for _, d := range docs {
		if d["kind"] == kind {
			out = append(out, d)
		}
	}
	return out
}

func TestWorkloadExchangeOffRendersNothing(t *testing.T) {
	docs := renderPhase(t, 2, issuerSets("hs256_compat", hs256Issuer)...)
	for _, kind := range []string{"ClusterRole", "ClusterRoleBinding"} {
		if n := len(kindDocs(docs, kind)); n != 0 {
			t.Errorf("exchange off renders %d %s", n, kind)
		}
	}
	svc := authDoc(docs, "Deployment", "a-dev-health-auth-service")
	for _, env := range []string{"AUTH_WORKLOAD_EXCHANGE", "AUTH_WORKLOAD_TRUST_DOMAIN", "AUTH_WORKLOAD_EXCHANGE_KUBE_API_URL"} {
		if _, set := containerEnvValue(t, svc, env); set {
			t.Errorf("exchange off renders %s", env)
		}
	}
	if at(svc, "spec", "template", "spec", "automountServiceAccountToken") != false {
		t.Error("exchange off mounts the auth-service ServiceAccount token")
	}
}

// The binding grants create on tokenreviews to the auth-service ServiceAccount alone.
func TestWorkloadExchangeBindsTokenReviewToAuthServiceOnly(t *testing.T) {
	docs := renderPhase(t, 2, exchangeSets()...)
	roles, bindings := kindDocs(docs, "ClusterRole"), kindDocs(docs, "ClusterRoleBinding")
	if len(roles) != 1 || len(bindings) != 1 {
		t.Fatalf("want one ClusterRole and one ClusterRoleBinding, got %d and %d", len(roles), len(bindings))
	}
	wantRules := []any{map[string]any{
		"apiGroups": []any{"authentication.k8s.io"}, "resources": []any{"tokenreviews"}, "verbs": []any{"create"},
	}}
	if got := at(roles[0], "rules"); !reflect.DeepEqual(got, wantRules) {
		t.Errorf("ClusterRole rules = %v, want exactly create on tokenreviews", got)
	}
	wantSubjects := []any{map[string]any{"kind": "ServiceAccount", "name": "a-dev-health-auth-service", "namespace": "default"}}
	if got := at(bindings[0], "subjects"); !reflect.DeepEqual(got, wantSubjects) {
		t.Errorf("binding subjects = %v, want the auth-service ServiceAccount alone", got)
	}
	if at(bindings[0], "roleRef", "name") != at(roles[0], "metadata", "name") || at(bindings[0], "roleRef", "kind") != "ClusterRole" {
		t.Errorf("roleRef = %v does not name the rendered ClusterRole", at(bindings[0], "roleRef"))
	}
}

// Only auth-service mounts a ServiceAccount token; it is the reviewer token.
func TestWorkloadExchangeMountsTheReviewerTokenForAuthServiceOnly(t *testing.T) {
	docs := renderPhase(t, 2, exchangeSets()...)
	for _, d := range append(kindDocs(docs, "Deployment"), kindDocs(docs, "StatefulSet")...) {
		name := toString(at(d, "metadata", "name"))
		got := at(d, "spec", "template", "spec", "automountServiceAccountToken")
		want := name == "a-dev-health-auth-service"
		if (got == true) != want {
			t.Errorf("%s automountServiceAccountToken = %v, want %v", name, got, want)
		}
	}
	for _, d := range kindDocs(docs, "ServiceAccount") {
		name := toString(at(d, "metadata", "name"))
		if got, want := at(d, "automountServiceAccountToken") == true, name == "a-dev-health-auth-service"; got != want {
			t.Errorf("ServiceAccount %s automountServiceAccountToken = %v, want %v", name, got, want)
		}
	}
}

// The rendered env starts auth-service with the exchange on, and refuses without the Kubernetes settings.
func TestWorkloadExchangeEnvStartsAuthService(t *testing.T) {
	docs := renderPhase(t, 2, exchangeSets()...)
	svc := authDoc(docs, "Deployment", "a-dev-health-auth-service")
	lookup := containerLookup(t, svc, "auth-service")
	for env, want := range map[string]string{
		"AUTH_WORKLOAD_EXCHANGE":                     "true",
		"AUTH_WORKLOAD_TRUST_DOMAIN":                 "cluster.example",
		"AUTH_WORKLOAD_EXCHANGE_CLUSTER_ISSUER":      "https://kubernetes.default.svc",
		"AUTH_WORKLOAD_EXCHANGE_KUBE_API_URL":        "https://kubernetes.default.svc",
		"AUTH_WORKLOAD_EXCHANGE_KUBE_CA_FILE":        exchangeCA,
		"AUTH_WORKLOAD_EXCHANGE_REVIEWER_TOKEN_FILE": exchangeToken,
	} {
		if got, _ := lookup(env); got != want {
			t.Errorf("%s = %q, want %q", env, got, want)
		}
	}
	cfg, err := authconfig.Load(authconfig.Spec{Overrides: authServiceOverrides(t, svc), LookupEnv: lookup})
	if err != nil {
		t.Fatalf("auth-service refuses the rendered exchange settings: %v", err)
	}
	want := authconfig.WorkloadExchange{
		Enabled: true, TrustDomain: "cluster.example", ClusterIssuer: "https://kubernetes.default.svc",
		KubeAPIURL: "https://kubernetes.default.svc", KubeCAFile: exchangeCA, ReviewerTokenFile: exchangeToken,
	}
	if cfg.WorkloadExchange != want {
		t.Errorf("the config the rendered env loads = %+v, want %+v", cfg.WorkloadExchange, want)
	}
}

func npDoc(docs []map[string]any) map[string]any {
	return authDoc(docs, "NetworkPolicy", "a-dev-health-auth-service")
}

func npRules(t *testing.T, np map[string]any, direction string) []any {
	t.Helper()
	if np == nil {
		t.Fatal("no auth-service NetworkPolicy")
	}
	rules, _ := at(np, "spec", direction).([]any)
	return rules
}

func hasPortRule(rules []any, port int) bool {
	for _, r := range rules {
		ports, _ := at(r, "ports").([]any)
		for _, p := range ports {
			if n, ok := at(p, "port").(int); ok && n == port && at(p, "protocol") != "UDP" {
				return true
			}
		}
	}
	return false
}

// The Kubernetes API is reachable only while the exchange is on, and only at the listed peers; go-api reaches the API port.
func TestWorkloadExchangeNetworkPolicy(t *testing.T) {
	off := npDoc(renderPhase(t, 2, issuerSets("hs256_compat", hs256Issuer)...))
	on := npDoc(renderPhase(t, 2, exchangeSets()...))
	if hasPortRule(npRules(t, off, "egress"), 443) {
		t.Error("exchange off opens egress on 443")
	}
	egress := npRules(t, on, "egress")
	var kube any
	for _, r := range egress {
		if reflect.DeepEqual(at(r, "to"), []any{map[string]any{"ipBlock": map[string]any{"cidr": "10.43.0.1/32"}}}) {
			kube = r
		}
	}
	if kube == nil || !hasPortRule([]any{kube}, 443) {
		t.Fatalf("no egress rule to the listed Kubernetes API peer on 443: %v", egress)
	}
	if len(egress) != len(npRules(t, off, "egress"))+1 {
		t.Errorf("exchange adds %d egress rules, want exactly 1", len(egress)-len(npRules(t, off, "egress")))
	}

	// Phase 1: go-api has no route to auth-service, but go-api presents its workload token to the exchange.
	p1 := npDoc(renderPhase(t, 1, append(issuerSets("", hs256Issuer), exchangeInputs()...)...))
	if !hasPortRule(npRules(t, p1, "ingress"), 8095) {
		t.Errorf("exchange on at phase 1 admits nobody on the API port: %v", npRules(t, p1, "ingress"))
	}
	if hasPortRule(npRules(t, npDoc(renderPhase(t, 1)), "ingress"), 8095) {
		t.Error("exchange off at phase 1 admits a caller on the API port")
	}
}

func TestWorkloadExchangeRefusals(t *testing.T) {
	cases := map[string]struct {
		args []string
		want string
	}{
		"no issuer":            {phaseArgs(2, "--set", "authService.workloadExchange.enabled=true", "--set", "authService.workloadExchange.trustDomain=c", "--set", "authService.workloadExchange.clusterIssuer=https://k.example"), "authService.accessToken.issuerURL"},
		"no trust domain":      {phaseArgs(2, withoutSet(exchangeSets(), "authService.workloadExchange.trustDomain=cluster.example")...), "authService.workloadExchange.trustDomain"},
		"no cluster issuer":    {phaseArgs(2, withoutSet(exchangeSets(), "authService.workloadExchange.clusterIssuer=https://kubernetes.default.svc")...), "authService.workloadExchange.clusterIssuer"},
		"no kube api peer":     {phaseArgs(2, withoutKubeAPI(exchangeSets())...), "authService.networkPolicy.egress.kubeAPI.to"},
		"enabled string":       {phaseArgs(2, exchangeSets("--set-string", "authService.workloadExchange.enabled=true")...), "authService.workloadExchange.enabled"},
		"extraEnv exchange":    {phaseArgs(2, exchangeSets("--set-json", `authService.extraEnv=[{"name":"AUTH_WORKLOAD_EXCHANGE","value":"true"}]`)...), "authService.extraEnv must not set AUTH_WORKLOAD_EXCHANGE"},
		"enabled empty string": {phaseArgs(2, append(withoutSet(exchangeSets(), "authService.workloadExchange.enabled=true"), "--set-string", "authService.workloadExchange.enabled=")...), "authService.workloadExchange.enabled"},
		"enabled zero":         {phaseArgs(2, append(withoutSet(exchangeSets(), "authService.workloadExchange.enabled=true"), "--set-json", "authService.workloadExchange.enabled=0")...), "authService.workloadExchange.enabled"},
		"no api url":           {phaseArgs(2, exchangeSets("--set-string", "authService.workloadExchange.kubeAPIURL=")...), "authService.workloadExchange.kubeAPIURL"},
		"empty peer":           {phaseArgs(2, exchangeSets("--set-json", `authService.networkPolicy.egress.kubeAPI.to=[{}]`)...), "authService.networkPolicy.egress.kubeAPI.to"},
		"scalar peer":          {phaseArgs(2, exchangeSets("--set-json", `authService.networkPolicy.egress.kubeAPI.to=["10.0.0.1"]`)...), "authService.networkPolicy.egress.kubeAPI.to"},
		"one empty peer among": {phaseArgs(2, exchangeSets("--set-json", `authService.networkPolicy.egress.kubeAPI.to=[{"ipBlock":{"cidr":"10.43.0.1/32"}},{}]`)...), "authService.networkPolicy.egress.kubeAPI.to"},
		"port zero":            {phaseArgs(2, exchangeSets("--set-json", "authService.networkPolicy.egress.kubeAPI.port=0")...), "authService.networkPolicy.egress.kubeAPI.port"},
		"port too big":         {phaseArgs(2, exchangeSets("--set-json", "authService.networkPolicy.egress.kubeAPI.port=65536")...), "authService.networkPolicy.egress.kubeAPI.port"},
		"port string":          {phaseArgs(2, exchangeSets("--set-string", "authService.networkPolicy.egress.kubeAPI.port=443")...), "authService.networkPolicy.egress.kubeAPI.port"},
		"port fraction":        {phaseArgs(2, exchangeSets("--set-json", "authService.networkPolicy.egress.kubeAPI.port=443.5")...), "authService.networkPolicy.egress.kubeAPI.port"},
		"phase 0":              {append(append([]string{}, authBase...), "--set", "authService.phase=0", "--set", "authService.workloadExchange.enabled=true"), "authService.workloadExchange"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			_, out, err := renderAuth(t, c.args...)
			if err == nil || !strings.Contains(out, c.want) {
				t.Fatalf("not refused with %q (err %v): %.300s", c.want, err, out)
			}
		})
	}
}

func withoutSet(args []string, drop string) []string {
	var out []string
	for i := 0; i < len(args); i++ {
		if args[i] == "--set" && i+1 < len(args) && args[i+1] == drop {
			i++
			continue
		}
		out = append(out, args[i])
	}
	return out
}

func withoutKubeAPI(args []string) []string {
	var out []string
	for i := 0; i < len(args); i++ {
		if args[i] == "--set-json" && i+1 < len(args) && strings.HasPrefix(args[i+1], "authService.networkPolicy.egress.kubeAPI.to=") {
			i++
			continue
		}
		out = append(out, args[i])
	}
	return out
}

// The chart keeps only the missing-input gate for the API origin: a malformed origin is refused by auth-service at
// start (D6465), so a form auth-service accepts must not be refused here.
func TestWorkloadExchangeLeavesTheAPIOriginToAuthService(t *testing.T) {
	for _, url := range []string{"https://kubernetes.default.svc/", "https://[fd00::1]:6443", "https://10.43.0.1:6443"} {
		t.Run(url, func(t *testing.T) {
			docs, out, err := renderAuth(t, phaseArgs(2, exchangeSets("--set-string", "authService.workloadExchange.kubeAPIURL="+url)...)...)
			if err != nil {
				t.Fatalf("the chart refuses %q: %v\n%.300s", url, err, out)
			}
			svc := authDoc(docs, "Deployment", "a-dev-health-auth-service")
			if got, _ := containerEnvValue(t, svc, "AUTH_WORKLOAD_EXCHANGE_KUBE_API_URL"); got != url {
				t.Errorf("AUTH_WORKLOAD_EXCHANGE_KUBE_API_URL = %q, want %q unchanged", got, url)
			}
		})
	}
}

// helm parses a plain --set number as an integer and --set-json or a values file as a float: both forms render.
func TestWorkloadExchangeAcceptsAFullPeerAndAnotherPort(t *testing.T) {
	for flag, set := range map[string]string{"--set": "--set", "--set-json": "--set-json"} {
		t.Run(flag, func(t *testing.T) {
			docs := renderPhase(t, 2, exchangeSets(set, `authService.networkPolicy.egress.kubeAPI.port=6443`)...)
			if !hasPortRule(npRules(t, npDoc(docs), "egress"), 6443) {
				t.Error("kubeAPI.port 6443 renders no egress rule on 6443")
			}
		})
	}
}
