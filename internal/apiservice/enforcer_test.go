package apiservice

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-go/pep"

	authv1 "github.com/full-chaos/dev-health-ops/contracts/auth/v1"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
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
		enf, err := buildEnforcer(lookupFrom(env), quietLogger(), newDecisionPoint(AuthProxyConfig{}, lookupFrom(env), nil))
		if enf != nil || err != nil {
			t.Errorf("%s: enforcer %v err %v, want neither", name, enf, err)
		}
	}
}

func TestABadEnforceValueIsOffNeverAStartError(t *testing.T) {
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	enf, err := buildEnforcer(lookupFrom(map[string]string{authzenforce.GoAPI.Env(): "nope,*,Billing"}), logger, newDecisionPoint(AuthProxyConfig{}, lookupFrom(nil), nil))
	if enf != nil || err != nil {
		t.Fatalf("a value with no accepted namespace: enforcer %v err %v, want neither", enf, err)
	}
	if !strings.Contains(logs.String(), "level=ERROR") || strings.Contains(logs.String(), "nope") {
		t.Fatalf("log %q: want one ERROR line that quotes no value", logs.String())
	}
}

func TestACellOnNeedsTheAuthServiceUpstream(t *testing.T) {
	_, err := buildEnforcer(lookupFrom(enforceEnv(nil)), quietLogger(), newDecisionPoint(AuthProxyConfig{}, lookupFrom(enforceEnv(nil)), nil))
	if err == nil || !strings.Contains(err.Error(), "GO_API_AUTHZ_ENFORCE") || !strings.Contains(err.Error(), EnvAuthProxyURL) {
		t.Fatalf("err = %v, want one naming GO_API_AUTHZ_ENFORCE and %s", err, EnvAuthProxyURL)
	}
	if strings.Contains(err.Error(), "billing") {
		t.Fatalf("the error quotes the value: %v", err)
	}
}

func TestACellOnNeedsAWorkloadCredential(t *testing.T) {
	proxy := mustProxyConfig(t, map[string]string{EnvAuthProxyURL: "http://auth-service:8095"})
	_, err := buildEnforcer(lookupFrom(enforceEnv(nil)), quietLogger(), newDecisionPoint(proxy, lookupFrom(enforceEnv(nil)), nil))
	if err == nil || !strings.Contains(err.Error(), "GO_API_AUTHZ_ENFORCE") || !strings.Contains(err.Error(), EnvAuthWorkloadSubjectFile) {
		t.Fatalf("err = %v, want one naming GO_API_AUTHZ_ENFORCE and %s", err, EnvAuthWorkloadSubjectFile)
	}
}

func TestACellOnBuildsTheEnforcerFromTheWorkloadInputs(t *testing.T) {
	proxy := mustProxyConfig(t, map[string]string{EnvAuthProxyURL: "http://auth-service:8095"})
	env := enforceEnv(map[string]string{EnvAuthWorkloadSubjectFile: subjectFile(t), EnvAuthWorkloadIssuer: testWorkloadIssuer})
	enf, err := buildEnforcer(lookupFrom(env), quietLogger(), newDecisionPoint(proxy, lookupFrom(env), nil))
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
	enf, err := buildEnforcer(lookupFrom(enforceEnv(nil)), quietLogger(), newDecisionPoint(proxy, lookupFrom(enforceEnv(nil)), shared))
	if err != nil || enf == nil {
		t.Fatalf("enforcer %v err %v, want one built over the shared client with no workload env", enf, err)
	}
}

func TestOnlyTheEnforcementCellsOfGoAPINeedTheCredential(t *testing.T) {
	env := map[string]string{authzenforce.QueryAPI.Env(): "billing"}
	if enf, err := buildEnforcer(lookupFrom(env), quietLogger(), newDecisionPoint(AuthProxyConfig{}, lookupFrom(env), nil)); enf != nil || err != nil {
		t.Fatalf("query-api's variable reached go-api's builder: %v %v", enf, err)
	}
}

func TestTheSharedWorkloadClientIsTheProvisioningOneThenTheSessionOne(t *testing.T) {
	proxy := mustProxyConfig(t, map[string]string{EnvAuthProxyURL: "http://auth-service:8095"})
	env := lookupFrom(map[string]string{EnvAuthWorkloadSubjectFile: subjectFile(t), EnvAuthWorkloadIssuer: testWorkloadIssuer})
	ask := []workloadAsk{{EnvAuthProxyOrgProvisioning, EnvAuthProvisioningCredential}}
	one, err := newWorkloadClient(proxy, env, ask)
	if err != nil {
		t.Fatal(err)
	}
	two, err := newWorkloadClient(proxy, env, ask)
	if err != nil {
		t.Fatal(err)
	}
	if got := sharedWorkloadClient(provisioningCredential{workload: one}, sessionCredential{workload: two}); got != one {
		t.Fatal("the provisioning client is not preferred")
	}
	if got := sharedWorkloadClient(provisioningCredential{}, sessionCredential{workload: two}); got != two {
		t.Fatal("the session client is not the fallback")
	}
	if got := sharedWorkloadClient(provisioningCredential{}, sessionCredential{}); got != nil {
		t.Fatal("a client appeared from nowhere")
	}
}

// Startup builds the Enforcer and installs it beside the guard: a source pin where no database runs.
func TestStartupInstallsTheEnforcerOnTheGuard(t *testing.T) {
	raw, err := os.ReadFile("service.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"buildEnforcer(envsecrets.ProcessLookup", "protected.guard.SetEnforcer(enforcer)"} {
		if !strings.Contains(string(raw), want) {
			t.Errorf("service.go does not contain %q", want)
		}
	}
}

// go-api asks the decision point through one client: the shadow and the Enforcer share the one this package builds.
func TestGoAPIBuildsOneDecisionPointClient(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	var builders []string
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		raw, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		if n := strings.Count(string(raw), "pep.New("); n > 0 {
			builders = append(builders, fmt.Sprintf("%s x%d", name, n))
		}
	}
	if len(builders) != 1 || !strings.HasSuffix(builders[0], " x1") {
		t.Fatalf("pep.New calls: %v, want one call in one file", builders)
	}
}

// The shadow and the Enforcer get the same client, so one workload exchange serves both.
func TestTheShadowAndTheEnforcerShareOneDecisionPointClient(t *testing.T) {
	pdp := newShadowPDPServer(t)
	proxy := mustProxyConfig(t, map[string]string{EnvAuthProxyURL: pdp.URL})
	dp := newDecisionPoint(proxy, lookupFrom(map[string]string{EnvAuthWorkloadBootstrapFile: bootstrapFile(t), EnvAuthWorkloadIssuer: testWorkloadIssuer}), nil)
	enforce, err := dp.decider(workloadAsk{flag: authzenforce.GoAPI.Env() + " lists a namespace"})
	if err != nil {
		t.Fatal(err)
	}
	shadow, err := dp.decider(workloadAsk{flag: policy.EnvShadowPDP, selector: "the shadow PDP credential"})
	if err != nil {
		t.Fatal(err)
	}
	if enforce != shadow {
		t.Fatal("the shadow and the Enforcer got two decision point clients")
	}
	rows, err := policy.ParseShadowRows(authv1.GoAPIEndpointProfiles, goAPIAccessService)
	if err != nil {
		t.Fatal(err)
	}
	row := rows[shadowRowKey(t)]
	request := pep.Request{PrincipalReference: shadowTestBearer, Action: row.Action, ResourceType: row.ResourceType,
		ResourceKey: "11111111-1111-4111-8111-111111111111", OrganizationID: "11111111-1111-4111-8111-111111111111"}
	for _, d := range []pep.Decider{enforce, shadow} {
		d.Decide(context.Background(), request)
	}
	if auths, _, _, exchanges := pdp.snapshot(); len(auths) != 2 || exchanges != 1 {
		t.Fatalf("PDP calls = %d, workload exchanges = %d; want 2 calls over 1 exchange", len(auths), exchanges)
	}
}

// A failed build is not kept: the next asker's error names its own switch.
func TestAFailedDecisionPointBuildNamesEachAsker(t *testing.T) {
	proxy := mustProxyConfig(t, map[string]string{EnvAuthProxyURL: "http://auth-service:8095"})
	dp := newDecisionPoint(proxy, lookupFrom(nil), nil)
	for _, flag := range []string{"FIRST_ASKER", "SECOND_ASKER"} {
		_, err := dp.decider(workloadAsk{flag: flag})
		if err == nil || !strings.HasPrefix(err.Error(), flag+": ") {
			t.Fatalf("err = %v, want one that starts with %s", err, flag)
		}
	}
}
