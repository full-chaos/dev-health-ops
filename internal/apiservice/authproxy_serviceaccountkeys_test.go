package apiservice

import (
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/authprovision"
)

func TestServiceAccountKeysFlagNameAndDefault(t *testing.T) {
	if EnvAuthProxyServiceAccountKeys != "AUTH_PROXY_SERVICE_ACCOUNT_KEYS" {
		t.Fatalf("EnvAuthProxyServiceAccountKeys = %q", EnvAuthProxyServiceAccountKeys)
	}
	for name, env := range map[string]map[string]string{
		"unset":                                {},
		"empty":                                {EnvAuthProxyServiceAccountKeys: " "},
		"url only":                             {EnvAuthProxyURL: "http://auth-service:8095"},
		"explicit false":                       {EnvAuthProxyURL: "http://auth-service:8095", EnvAuthProxyServiceAccountKeys: "false"},
		"every aggregate on, this flag unset":  allOn("http://auth-service:8095"),
		"org provisioning on, this flag unset": {EnvAuthProxyURL: "http://auth-service:8095", EnvAuthProxyOrgProvisioning: "true"},
	} {
		if cfg := mustProxyConfig(t, env); cfg.ServiceAccountKeys || cfg.ServiceAccountKeysOn() {
			t.Errorf("%s: the switch is on", name)
		}
	}
	if (AuthProxyConfig{ServiceAccountKeys: true}).ServiceAccountKeysOn() {
		t.Error("the switch counts as on with no upstream URL")
	}
}

func TestServiceAccountKeysFlagIsStrictAndStandsAlone(t *testing.T) {
	for name, env := range map[string]map[string]string{
		"without a url": {EnvAuthProxyServiceAccountKeys: "true"},
		"not a boolean": {EnvAuthProxyURL: "http://auth-service:8095", EnvAuthProxyServiceAccountKeys: "yes"},
	} {
		if _, err := ParseAuthProxyConfig(lookupFrom(env)); err == nil {
			t.Errorf("%s: accepted %v", name, env)
		}
	}
	cfg := mustProxyConfig(t, map[string]string{EnvAuthProxyURL: "http://auth-service:8095", EnvAuthProxyServiceAccountKeys: "true"})
	if !cfg.ServiceAccountKeysOn() || cfg.OrgProvisioningOn() {
		t.Fatalf("keys on %v, org provisioning on %v; want only the keys switch", cfg.ServiceAccountKeysOn(), cfg.OrgProvisioningOn())
	}
	for _, aggregate := range AuthAggregates {
		if cfg.Enabled(aggregate) {
			t.Errorf("the key switch moved the %s routes", aggregate)
		}
	}
}

// Off builds no client (the routes answer 503 config_missing); on builds the
// key client over the workload credential, and a retired credential selector
// is a start-up error naming the switch.
func TestServiceAccountKeysClientFollowsTheSwitch(t *testing.T) {
	url := "http://auth-service:8095"
	off := mustProxyConfig(t, map[string]string{EnvAuthProxyURL: url})
	cred, _, err := newCredentials(off, issueEnv(workloadEnv(t, nil)))
	if err != nil {
		t.Fatal(err)
	}
	if keys, err := serviceAccountKeys(off, cred); err != nil || keys != nil {
		t.Fatalf("switch off: client %v err %v, want none", keys, err)
	}
	on := mustProxyConfig(t, map[string]string{EnvAuthProxyURL: url, EnvAuthProxyServiceAccountKeys: "true"})
	cred, _, err = newCredentials(on, issueEnv(workloadEnv(t, nil)))
	if err != nil {
		t.Fatal(err)
	}
	if cred.workload == nil {
		t.Fatal("switch on: no workload credential was made")
	}
	keys, err := serviceAccountKeys(on, cred)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := keys.(*authprovision.KeyClient); !ok {
		t.Fatalf("switch on: client %T, want *authprovision.KeyClient", keys)
	}
	_, _, err = newCredentials(on, issueEnv(workloadEnv(t, map[string]string{EnvAuthProvisioningCredential: credentialSessionCompat})))
	if err == nil || !strings.Contains(err.Error(), EnvAuthProxyServiceAccountKeys) {
		t.Fatalf("session credential selector: err %v, want a start-up error naming %s", err, EnvAuthProxyServiceAccountKeys)
	}
	if _, err := serviceAccountKeys(on, provisioningCredential{}); err == nil {
		t.Fatal("switch on with no credential built a client")
	}
}
