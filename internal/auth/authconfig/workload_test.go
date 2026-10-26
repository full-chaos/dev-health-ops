package authconfig

import (
	"fmt"
	"log/slog"
	"strings"
	"testing"
)

func workloadEnv() map[string]string {
	env := baseEnv()
	env[EnvTokenIssuerURL] = "https://auth.example.com"
	env[EnvWorkloadExchange] = "true"
	env[EnvWorkloadTrustDomain] = "cluster.example.test"
	env[EnvWorkloadClusterIssuer] = "https://kubernetes.default.svc.cluster.local"
	env[EnvWorkloadKubeAPIURL] = "https://kubernetes.default.svc"
	env[EnvWorkloadKubeCAFile] = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
	env[EnvWorkloadReviewerTokenFile] = "/var/run/secrets/kubernetes.io/serviceaccount/token"
	return env
}

func TestTheWorkloadExchangeIsOffByDefault(t *testing.T) {
	env := baseEnv()
	env[EnvWorkloadTrustDomain] = "cluster.example.test"
	cfg, err := Load(Spec{LookupEnv: lookupFrom(env)})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.WorkloadExchange != (WorkloadExchange{}) {
		t.Fatalf("exchange settings %+v without %s=true", cfg.WorkloadExchange, EnvWorkloadExchange)
	}
	env[EnvWorkloadExchange] = "false"
	if cfg, err = Load(Spec{LookupEnv: lookupFrom(env)}); err != nil || cfg.WorkloadExchange.Enabled {
		t.Fatalf("false: err %v enabled %v", err, cfg.WorkloadExchange.Enabled)
	}
	env[EnvWorkloadExchange] = "yes"
	if _, err = Load(Spec{LookupEnv: lookupFrom(env)}); err == nil || !strings.Contains(err.Error(), EnvWorkloadExchange) {
		t.Fatalf("a typo was not refused: %v", err)
	}
}

func TestTheWorkloadExchangeLoadsEverySetting(t *testing.T) {
	for _, origin := range []string{"https://kubernetes.default.svc/", "https://kubernetes.default.svc:443", "https://10.43.0.1:6443"} {
		accepted := workloadEnv()
		accepted[EnvWorkloadKubeAPIURL] = origin
		if _, err := Load(Spec{LookupEnv: lookupFrom(accepted)}); err != nil {
			t.Fatalf("origin %s was refused: %v", origin, err)
		}
	}
	cfg, err := Load(Spec{LookupEnv: lookupFrom(workloadEnv())})
	if err != nil {
		t.Fatal(err)
	}
	want := WorkloadExchange{
		Enabled: true, TrustDomain: "cluster.example.test", ClusterIssuer: "https://kubernetes.default.svc.cluster.local",
		KubeAPIURL: "https://kubernetes.default.svc", KubeCAFile: "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt",
		ReviewerTokenFile: "/var/run/secrets/kubernetes.io/serviceaccount/token",
	}
	if cfg.WorkloadExchange != want {
		t.Fatalf("got %+v, want %+v", cfg.WorkloadExchange, want)
	}
	rendered := fmt.Sprint(slog.GroupValue(cfg.SafeAttrs()...))
	if !strings.Contains(rendered, "workload_exchange_enabled=true") {
		t.Fatalf("the startup line does not say the exchange is on: %s", rendered)
	}
	for _, path := range []string{want.KubeCAFile, want.ReviewerTokenFile} {
		if strings.Contains(rendered, path) {
			t.Fatal("the startup line carries a path")
		}
	}
}

// On, every setting is required: no trust domain, no cluster issuer, no API
// server, no CA, no reviewer token or no token issuer refuses startup, and the
// refusal names the variable, never its value.
func TestTheWorkloadExchangeRefusesToStartWithoutEverySetting(t *testing.T) {
	for _, name := range []string{
		EnvTokenIssuerURL, EnvWorkloadTrustDomain, EnvWorkloadClusterIssuer, EnvWorkloadKubeAPIURL,
		EnvWorkloadKubeCAFile, EnvWorkloadReviewerTokenFile,
	} {
		env := workloadEnv()
		delete(env, name)
		_, err := Load(Spec{LookupEnv: lookupFrom(env)})
		if err == nil || !strings.Contains(err.Error(), name) {
			t.Errorf("without %s: err %v, want a refusal naming it", name, err)
		}
	}
	for name, bad := range map[string]map[string]string{
		"trust domain with whitespace":              {EnvWorkloadTrustDomain: " cluster.example.test"},
		"trust domain too long":                     {EnvWorkloadTrustDomain: strings.Repeat("a", 254)},
		"plain http API server":                     {EnvWorkloadKubeAPIURL: "http://kubernetes.default.svc"},
		"API server with a path":                    {EnvWorkloadKubeAPIURL: "https://kubernetes.default.svc/apis"},
		"API server with credentials":               {EnvWorkloadKubeAPIURL: "https://user:secret-value-77@kubernetes.default.svc"},
		"API server with a bare query":              {EnvWorkloadKubeAPIURL: "https://kubernetes.default.svc?"},
		"API server with a bare fragment":           {EnvWorkloadKubeAPIURL: "https://kubernetes.default.svc#"},
		"API server with an empty port":             {EnvWorkloadKubeAPIURL: "https://kubernetes.default.svc:"},
		"API server with an empty port and a slash": {EnvWorkloadKubeAPIURL: "https://kubernetes.default.svc:/"},
		"relative CA path":                          {EnvWorkloadKubeCAFile: "ca.crt"},
		"unclean token path":                        {EnvWorkloadReviewerTokenFile: "/var/run/../token"},
	} {
		env := workloadEnv()
		for key, value := range bad {
			env[key] = value
		}
		_, err := Load(Spec{LookupEnv: lookupFrom(env)})
		if err == nil {
			t.Errorf("%s: accepted", name)
			continue
		}
		if strings.Contains(err.Error(), "secret-value-77") {
			t.Errorf("%s: the refusal echoes the value", name)
		}
	}
}
