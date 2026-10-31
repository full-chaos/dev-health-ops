package devhealth_test

import (
	"reflect"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
)

const hs256Issuer = "https://auth.example/v1"

func issuerSets(class, issuer string) []string {
	var out []string
	if class != "" {
		out = append(out, "--set", "authService.accessToken.class="+class)
	}
	if issuer != "" {
		out = append(out, "--set", "authService.accessToken.issuerURL="+issuer)
	}
	return out
}

// The workload exchange needs auth-service's AUTH_TOKEN_ISSUER_URL, and prod keeps hs256_compat web tokens: the issuer
// renders for auth-service under that class and changes nothing go-api or query-api verify.
func TestAuthServiceIssuerRendersUnderHS256Compat(t *testing.T) {
	envOf := func(docs []map[string]any, name, env string) (string, bool) {
		return containerEnvValue(t, authDoc(docs, "Deployment", name), env)
	}
	for _, class := range []string{"hs256_compat", ""} {
		t.Run("class="+class, func(t *testing.T) {
			without := renderPhase(t, 2, issuerSets(class, "")...)
			with := renderPhase(t, 2, issuerSets(class, hs256Issuer)...)
			if _, set := envOf(without, "a-dev-health-auth-service", "AUTH_TOKEN_ISSUER_URL"); set {
				t.Error("no issuer value renders AUTH_TOKEN_ISSUER_URL")
			}
			if v, _ := envOf(with, "a-dev-health-auth-service", "AUTH_TOKEN_ISSUER_URL"); v != hs256Issuer {
				t.Errorf("auth-service AUTH_TOKEN_ISSUER_URL = %q, want %q", v, hs256Issuer)
			}
			if _, set := envOf(with, "a-dev-health-auth-service", "AUTH_JWT_AUDIENCE"); set {
				t.Error("hs256_compat with an issuer renders AUTH_JWT_AUDIENCE: that is the eddsa audience")
			}
			for _, name := range []string{"a-dev-health-go-api", "a-dev-health-query-api"} {
				if _, set := envOf(with, name, "AUTH_ACCESS_TOKEN_ISSUER"); set {
					t.Errorf("%s gets AUTH_ACCESS_TOKEN_ISSUER under hs256_compat: web-token validation would switch", name)
				}
				a, b := authDoc(without, "Deployment", name), authDoc(with, "Deployment", name)
				if a == nil || !reflect.DeepEqual(a, b) {
					t.Errorf("%s Deployment differs when only the auth-service issuer is set", name)
				}
			}
		})
	}
}

func TestAuthServiceIssuerUnderEdDSAIsUnchanged(t *testing.T) {
	docs := renderPhase(t, 2, issuerSets("eddsa", hs256Issuer)...)
	for _, want := range []struct{ name, env string }{
		{"a-dev-health-auth-service", "AUTH_TOKEN_ISSUER_URL"},
		{"a-dev-health-auth-service", "AUTH_JWT_AUDIENCE"},
		{"a-dev-health-go-api", "AUTH_ACCESS_TOKEN_ISSUER"},
		{"a-dev-health-query-api", "AUTH_ACCESS_TOKEN_ISSUER"},
	} {
		if _, set := containerEnvValue(t, authDoc(docs, "Deployment", want.name), want.env); !set {
			t.Errorf("%s lost %s under eddsa", want.name, want.env)
		}
	}
}

func TestAuthServiceIssuerIsCheckedUnderEveryClass(t *testing.T) {
	for _, class := range []string{"hs256_compat", ""} {
		for _, issuer := range []string{"http://auth.example", "https://auth.example/", "https://Auth.example"} {
			_, out, err := renderAuth(t, phaseArgs(2, issuerSets(class, issuer)...)...)
			if err == nil || !strings.Contains(out, "needs authService.accessToken.issuerURL") {
				t.Errorf("class %q issuer %q: not refused (err %v)", class, issuer, err)
			}
		}
	}
}

// auth-service starts with AUTH_WORKLOAD_EXCHANGE=true on the rendered hs256_compat issuer, and refuses without it.
func TestAuthServiceStartsWithWorkloadExchangeOnHS256CompatIssuer(t *testing.T) {
	exchange := map[string]string{
		"AUTH_WORKLOAD_EXCHANGE":                     "true",
		"AUTH_WORKLOAD_TRUST_DOMAIN":                 "cluster.example",
		"AUTH_WORKLOAD_EXCHANGE_CLUSTER_ISSUER":      "https://kubernetes.default.svc",
		"AUTH_WORKLOAD_EXCHANGE_KUBE_API_URL":        "https://kubernetes.default.svc",
		"AUTH_WORKLOAD_EXCHANGE_KUBE_CA_FILE":        "/run/kube/ca.crt",
		"AUTH_WORKLOAD_EXCHANGE_REVIEWER_TOKEN_FILE": "/run/kube/token",
	}
	load := func(docs []map[string]any) error {
		service := authDoc(docs, "Deployment", "a-dev-health-auth-service")
		lookup := containerLookup(t, service, "auth-service")
		withExchange := func(name string) (string, bool) {
			if v, ok := exchange[name]; ok {
				return v, true
			}
			return lookup(name)
		}
		_, err := authconfig.Load(authconfig.Spec{Overrides: authServiceOverrides(t, service), LookupEnv: withExchange})
		return err
	}
	if err := load(renderPhase(t, 2, issuerSets("hs256_compat", hs256Issuer)...)); err != nil {
		t.Fatalf("auth-service refuses AUTH_WORKLOAD_EXCHANGE=true on the rendered issuer: %v", err)
	}
	err := load(renderPhase(t, 2, issuerSets("hs256_compat", "")...))
	if err == nil || !strings.Contains(err.Error(), "AUTH_WORKLOAD_EXCHANGE=true needs AUTH_TOKEN_ISSUER_URL") {
		t.Fatalf("without an issuer the start is not refused for it: %v", err)
	}
}
