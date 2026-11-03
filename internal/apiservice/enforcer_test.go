package apiservice

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/authzenforce"
)

func enforceEnv(extra map[string]string) map[string]string {
	env := map[string]string{authzenforce.GoAPI.Env(): "billing,teams"}
	for k, v := range extra {
		env[k] = v
	}
	return env
}

func TestNoCellOnBuildsNoEnforcer(t *testing.T) {
	for name, env := range map[string]map[string]string{
		"unset": {},
		"blank": {authzenforce.GoAPI.Env(): "  "},
	} {
		enf, err := buildEnforcer(lookupFrom(env), quietLogger(), AuthProxyConfig{}, nil)
		if enf != nil || err != nil {
			t.Errorf("%s: enforcer %v err %v, want neither", name, enf, err)
		}
	}
}

func TestABadEnforceValueIsOffNeverAStartError(t *testing.T) {
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	enf, err := buildEnforcer(lookupFrom(map[string]string{authzenforce.GoAPI.Env(): "nope,*,Billing"}), logger, AuthProxyConfig{}, nil)
	if enf != nil || err != nil {
		t.Fatalf("a value with no accepted namespace: enforcer %v err %v, want neither", enf, err)
	}
	if !strings.Contains(logs.String(), "level=ERROR") || strings.Contains(logs.String(), "nope") {
		t.Fatalf("log %q: want one ERROR line that quotes no value", logs.String())
	}
}

func TestACellOnNeedsTheAuthServiceUpstream(t *testing.T) {
	_, err := buildEnforcer(lookupFrom(enforceEnv(nil)), quietLogger(), AuthProxyConfig{}, nil)
	if err == nil || !strings.Contains(err.Error(), "GO_API_AUTHZ_ENFORCE") || !strings.Contains(err.Error(), EnvAuthProxyURL) {
		t.Fatalf("err = %v, want one naming GO_API_AUTHZ_ENFORCE and %s", err, EnvAuthProxyURL)
	}
	if strings.Contains(err.Error(), "billing") {
		t.Fatalf("the error quotes the value: %v", err)
	}
}

func TestACellOnNeedsAWorkloadCredential(t *testing.T) {
	proxy := mustProxyConfig(t, map[string]string{EnvAuthProxyURL: "http://auth-service:8095"})
	_, err := buildEnforcer(lookupFrom(enforceEnv(nil)), quietLogger(), proxy, nil)
	if err == nil || !strings.Contains(err.Error(), "GO_API_AUTHZ_ENFORCE") || !strings.Contains(err.Error(), EnvAuthWorkloadSubjectFile) {
		t.Fatalf("err = %v, want one naming GO_API_AUTHZ_ENFORCE and %s", err, EnvAuthWorkloadSubjectFile)
	}
}

func TestACellOnBuildsTheEnforcerFromTheWorkloadInputs(t *testing.T) {
	proxy := mustProxyConfig(t, map[string]string{EnvAuthProxyURL: "http://auth-service:8095"})
	env := enforceEnv(map[string]string{EnvAuthWorkloadSubjectFile: subjectFile(t), EnvAuthWorkloadIssuer: testWorkloadIssuer})
	enf, err := buildEnforcer(lookupFrom(env), quietLogger(), proxy, nil)
	if err != nil || enf == nil {
		t.Fatalf("enforcer %v err %v, want an enforcer", enf, err)
	}
}

func TestACellOnReusesTheSharedWorkloadClient(t *testing.T) {
	proxy := mustProxyConfig(t, map[string]string{EnvAuthProxyURL: "http://auth-service:8095"})
	shared, err := newWorkloadClient(proxy, lookupFrom(map[string]string{EnvAuthWorkloadSubjectFile: subjectFile(t), EnvAuthWorkloadIssuer: testWorkloadIssuer}),
		[]workloadAsk{{EnvAuthProxyOrgProvisioning, EnvAuthProvisioningCredential}})
	if err != nil {
		t.Fatal(err)
	}
	enf, err := buildEnforcer(lookupFrom(enforceEnv(nil)), quietLogger(), proxy, shared)
	if err != nil || enf == nil {
		t.Fatalf("enforcer %v err %v, want one built over the shared client with no workload env", enf, err)
	}
}

func TestOnlyTheEnforcementCellsOfGoAPINeedTheCredential(t *testing.T) {
	env := map[string]string{authzenforce.QueryAPI.Env(): "billing"}
	if enf, err := buildEnforcer(lookupFrom(env), quietLogger(), AuthProxyConfig{}, nil); enf != nil || err != nil {
		t.Fatalf("query-api's variable reached go-api's builder: %v %v", enf, err)
	}
}
