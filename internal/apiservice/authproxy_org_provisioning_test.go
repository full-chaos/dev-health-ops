package apiservice

import (
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/sessionissue"
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

func TestOrgReactivatorFollowsTheSwitchAndNeedsTheCredential(t *testing.T) {
	token := testIssueToken(t)
	on := mustProxyConfig(t, map[string]string{EnvAuthProxyURL: "http://auth-service:8095", EnvAuthProxyOrgProvisioning: "true"})
	off := mustProxyConfig(t, map[string]string{EnvAuthProxyURL: "http://auth-service:8095"})
	if r, err := orgReactivator(off, issueEnv(map[string]string{sessionissue.EnvToken: token})); r != nil || err != nil {
		t.Fatalf("switch off with a token: %v %v, want no client", r, err)
	}
	if r, err := orgReactivator(off, issueEnv(map[string]string{})); r != nil || err != nil {
		t.Fatalf("switch off with no token: %v %v, want no client and no error", r, err)
	}
	if r, err := orgReactivator(on, issueEnv(map[string]string{sessionissue.EnvToken: " " + token + "\n"})); r == nil || err != nil {
		t.Fatalf("switch on with a credential: %v %v, want a client", r, err)
	}
	for name, env := range map[string]map[string]string{
		"no credential":  {},
		"empty":          {sessionissue.EnvToken: ""},
		"acr credential": {sessionissue.EnvToken: "svc_acr_" + token[len("svc_authsess_"):]},
	} {
		r, err := orgReactivator(on, issueEnv(env))
		if err == nil || r != nil {
			t.Errorf("switch on, %s: %v %v, want a start-up error", name, r, err)
			continue
		}
		if !strings.Contains(err.Error(), EnvAuthProxyOrgProvisioning) || strings.Contains(err.Error(), token[len("svc_authsess_"):]) {
			t.Errorf("switch on, %s: error %q must name the switch and never the credential", name, err)
		}
	}
}

// The same switch builds the provisioning client the member, invite and
// delete routes call: none when off, one from the same credential when on.
func TestOrgProvisionerFollowsTheSwitchAndNeedsTheCredential(t *testing.T) {
	token := testIssueToken(t)
	on := mustProxyConfig(t, map[string]string{EnvAuthProxyURL: "http://auth-service:8095", EnvAuthProxyOrgProvisioning: "true"})
	off := mustProxyConfig(t, map[string]string{EnvAuthProxyURL: "http://auth-service:8095"})
	if p, err := orgProvisioner(off, issueEnv(map[string]string{sessionissue.EnvToken: token})); p != nil || err != nil {
		t.Fatalf("switch off with a token: %v %v, want no client", p, err)
	}
	if p, err := orgProvisioner(on, issueEnv(map[string]string{sessionissue.EnvToken: " " + token + "\n"})); p == nil || err != nil {
		t.Fatalf("switch on with a credential: %v %v, want a client", p, err)
	}
	for name, env := range map[string]map[string]string{
		"no credential":  {},
		"acr credential": {sessionissue.EnvToken: "svc_acr_" + token[len("svc_authsess_"):]},
	} {
		p, err := orgProvisioner(on, issueEnv(env))
		if err == nil || p != nil {
			t.Errorf("switch on, %s: %v %v, want a start-up error", name, p, err)
			continue
		}
		if !strings.Contains(err.Error(), EnvAuthProxyOrgProvisioning) || strings.Contains(err.Error(), token[len("svc_authsess_"):]) {
			t.Errorf("switch on, %s: error %q must name the switch and never the credential", name, err)
		}
	}
}

// The suspend client follows the same switch and needs the same credential as the reactivate client.
func TestOrgSuspenderFollowsTheSwitchAndNeedsTheCredential(t *testing.T) {
	token := testIssueToken(t)
	on := mustProxyConfig(t, map[string]string{EnvAuthProxyURL: "http://auth-service:8095", EnvAuthProxyOrgProvisioning: "true"})
	off := mustProxyConfig(t, map[string]string{EnvAuthProxyURL: "http://auth-service:8095"})
	if s, err := orgSuspender(off, issueEnv(map[string]string{sessionissue.EnvToken: token})); s != nil || err != nil {
		t.Fatalf("switch off with a token: %v %v, want no client", s, err)
	}
	if s, err := orgSuspender(off, issueEnv(map[string]string{})); s != nil || err != nil {
		t.Fatalf("switch off with no token: %v %v, want no client and no error", s, err)
	}
	if s, err := orgSuspender(on, issueEnv(map[string]string{sessionissue.EnvToken: " " + token + "\n"})); s == nil || err != nil {
		t.Fatalf("switch on with a credential: %v %v, want a client", s, err)
	}
	for name, env := range map[string]map[string]string{
		"no credential":  {},
		"empty":          {sessionissue.EnvToken: ""},
		"acr credential": {sessionissue.EnvToken: "svc_acr_" + token[len("svc_authsess_"):]},
	} {
		s, err := orgSuspender(on, issueEnv(env))
		if err == nil || s != nil {
			t.Errorf("switch on, %s: %v %v, want a start-up error", name, s, err)
			continue
		}
		if !strings.Contains(err.Error(), EnvAuthProxyOrgProvisioning) || strings.Contains(err.Error(), token[len("svc_authsess_"):]) {
			t.Errorf("switch on, %s: error %q must name the switch and never the credential", name, err)
		}
	}
}
