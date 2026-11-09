package apiservice

import (
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/workloadclient"
)

func TestOrgProvisioningFlagNameAndDefault(t *testing.T) {
	if EnvAuthProxyOrgProvisioning != "AUTH_PROXY_ORG_PROVISIONING" {
		t.Fatalf("EnvAuthProxyOrgProvisioning = %q", EnvAuthProxyOrgProvisioning)
	}
	for name, env := range map[string]map[string]string{
		"unset":                               {},
		"empty":                               {EnvAuthProxyOrgProvisioning: " "},
		"url only":                            {EnvAuthProxyURL: "http://auth-service:8095"},
		"explicit false":                      {EnvAuthProxyURL: "http://auth-service:8095", EnvAuthProxyOrgProvisioning: "false"},
		"every aggregate on, this flag unset": allOn("http://auth-service:8095"),
	} {
		cfg := mustProxyConfig(t, env)
		if cfg.OrgProvisioning || cfg.OrgProvisioningOn() {
			t.Errorf("%s: the switch is on", name)
		}
	}
	if (AuthProxyConfig{OrgProvisioning: true}).OrgProvisioningOn() {
		t.Error("the switch counts as on with no upstream URL")
	}
}

// The switch is strict: a bool, and never without the upstream URL. It moves
// no route, so it needs no sessions flag, and it is no aggregate.
func TestOrgProvisioningFlagIsStrictAndStandsAlone(t *testing.T) {
	for name, env := range map[string]map[string]string{
		"without a url":   {EnvAuthProxyOrgProvisioning: "true"},
		"not a boolean":   {EnvAuthProxyURL: "http://auth-service:8095", EnvAuthProxyOrgProvisioning: "yes"},
		"bad url":         {EnvAuthProxyURL: "http://auth-service:8095/x", EnvAuthProxyOrgProvisioning: "true"},
		"numeric garbage": {EnvAuthProxyURL: "http://auth-service:8095", EnvAuthProxyOrgProvisioning: "2"},
	} {
		if _, err := ParseAuthProxyConfig(lookupFrom(env)); err == nil {
			t.Errorf("%s: accepted %v", name, env)
		}
	}
	cfg := mustProxyConfig(t, map[string]string{EnvAuthProxyURL: "http://auth-service:8095", EnvAuthProxyOrgProvisioning: "true"})
	if !cfg.OrgProvisioningOn() {
		t.Fatal("the switch is off with the flag and the URL set")
	}
	for _, aggregate := range AuthAggregates {
		if cfg.Enabled(aggregate) {
			t.Errorf("the org provisioning switch moved the %s routes", aggregate)
		}
		if AuthProxyFlagEnv(aggregate) == EnvAuthProxyOrgProvisioning {
			t.Errorf("the switch is registered as the %s aggregate", aggregate)
		}
	}
}

// Each client follows the switch: none when off (with or without a credential), one from the workload credential when
// on, and a start-up error naming the switch when on with no workload credential (the session credential is retired).
func TestOrgClientsFollowTheSwitchAndNeedTheWorkloadCredential(t *testing.T) {
	on := mustProxyConfig(t, map[string]string{EnvAuthProxyURL: "http://auth-service:8095", EnvAuthProxyOrgProvisioning: "true"})
	off := mustProxyConfig(t, map[string]string{EnvAuthProxyURL: "http://auth-service:8095"})
	wc, _ := workloadclient.New(t)
	have := provisioningCredential{workload: wc}
	for name, cred := range map[string]provisioningCredential{"with a credential": have, "without": {}} {
		if r, err := orgReactivator(off, cred); r != nil || err != nil {
			t.Fatalf("reactivator, switch off %s: %v %v, want no client", name, r, err)
		}
		if p, err := orgProvisioner(off, cred); p != nil || err != nil {
			t.Fatalf("provisioner, switch off %s: %v %v, want no client", name, p, err)
		}
		if s, err := orgSuspender(off, cred); s != nil || err != nil {
			t.Fatalf("suspender, switch off %s: %v %v, want no client", name, s, err)
		}
	}
	if r, err := orgReactivator(on, have); r == nil || err != nil {
		t.Fatalf("reactivator, switch on with the workload credential: %v %v, want a client", r, err)
	}
	if p, err := orgProvisioner(on, have); p == nil || err != nil {
		t.Fatalf("provisioner, switch on with the workload credential: %v %v, want a client", p, err)
	}
	if s, err := orgSuspender(on, have); s == nil || err != nil {
		t.Fatalf("suspender, switch on with the workload credential: %v %v, want a client", s, err)
	}
	if r, err := orgReactivator(on, provisioningCredential{}); r != nil || err == nil || !strings.Contains(err.Error(), EnvAuthProxyOrgProvisioning) {
		t.Fatalf("reactivator, switch on with no credential: %v %v, want a start-up error naming the switch", r, err)
	}
	if p, err := orgProvisioner(on, provisioningCredential{}); p != nil || err == nil || !strings.Contains(err.Error(), EnvAuthProxyOrgProvisioning) {
		t.Fatalf("provisioner, switch on with no credential: %v %v, want a start-up error naming the switch", p, err)
	}
	if s, err := orgSuspender(on, provisioningCredential{}); s != nil || err == nil || !strings.Contains(err.Error(), EnvAuthProxyOrgProvisioning) {
		t.Fatalf("suspender, switch on with no credential: %v %v, want a start-up error naming the switch", s, err)
	}
}
