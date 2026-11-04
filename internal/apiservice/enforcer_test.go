package apiservice

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-go/pep"

	authv1 "github.com/full-chaos/dev-health-ops/contracts/auth/v1"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/authzenforce"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
)

func enforceEnv(extra map[string]string) map[string]string {
	env := map[string]string{authzenforce.GoAPI.Env(): "billing,teams"}
	for k, v := range extra {
		env[k] = v
	}
	return env
}

func recordConfigInvalid(t *testing.T) *[]bool {
	t.Helper()
	var got []bool
	previous := recordEnforceConfigInvalid
	recordEnforceConfigInvalid = func(_ context.Context, invalid bool) { got = append(got, invalid) }
	t.Cleanup(func() { recordEnforceConfigInvalid = previous })
	return &got
}

func TestNoCellOnBuildsNoEnforcer(t *testing.T) {
	for name, env := range map[string]map[string]string{
		"unset": {},
		"blank": {authzenforce.GoAPI.Env(): "  "},
	} {
		gauge := recordConfigInvalid(t)
		var logs bytes.Buffer
		enf, err := buildEnforcer(lookupFrom(env), slog.New(slog.NewTextHandler(&logs, nil)), newDecisionPoint(AuthProxyConfig{}, lookupFrom(env), nil))
		if enf != nil || err != nil {
			t.Errorf("%s: enforcer %v err %v, want neither", name, enf, err)
		}
		if logs.Len() != 0 || !slices.Equal(*gauge, []bool{false}) {
			t.Errorf("%s: log %q gauge %v, want no log and the config-invalid gauge at 0", name, logs.String(), *gauge)
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

// A cell on with no way to ask is not a start error: go-api starts, logs one ERROR naming the missing variable, sets
// the gauge, and its Enforcer refuses every request of the switched-on namespaces.
func assertStartsFailClosed(t *testing.T, proxy AuthProxyConfig, variable string) {
	t.Helper()
	gauge := recordConfigInvalid(t)
	var logs bytes.Buffer
	env := lookupFrom(enforceEnv(nil))
	enf, err := buildEnforcer(env, slog.New(slog.NewTextHandler(&logs, nil)), newDecisionPoint(proxy, env, nil))
	if err != nil || enf == nil {
		t.Fatalf("enforcer %v err %v, want a fail-closed enforcer and no start error", enf, err)
	}
	if n := strings.Count(logs.String(), "level=ERROR"); n != 1 || !strings.Contains(logs.String(), "variable="+variable) || !strings.Contains(logs.String(), "GO_API_AUTHZ_ENFORCE") {
		t.Fatalf("log %q: want one ERROR line naming GO_API_AUTHZ_ENFORCE and variable=%s", logs.String(), variable)
	}
	if strings.Contains(logs.String(), "billing") {
		t.Fatalf("the log quotes the value: %q", logs.String())
	}
	if !slices.Equal(*gauge, []bool{true}) {
		t.Fatalf("config-invalid gauge %v, want [true]", *gauge)
	}
}

func TestACellOnWithoutTheAuthServiceUpstreamStartsFailClosed(t *testing.T) {
	assertStartsFailClosed(t, AuthProxyConfig{}, EnvAuthProxyURL)
}

func TestACellOnWithoutAWorkloadCredentialStartsFailClosed(t *testing.T) {
	assertStartsFailClosed(t, mustProxyConfig(t, map[string]string{EnvAuthProxyURL: "http://auth-service:8095"}), EnvAuthWorkloadSubjectFile+","+EnvAuthWorkloadBootstrapFile)
}

// The start is not refused: the switch with no decision point inputs leaves the rest of go-api serving.
func TestConfigureACellOnWithoutDecisionPointInputsStillStarts(t *testing.T) {
	t.Setenv("ENVIRONMENT", "development")
	for _, key := range []string{EnvAuthProxyURL, EnvAuthWorkloadIssuer, EnvAuthWorkloadBootstrapFile, EnvAuthWorkloadSubjectFile, policy.EnvShadowPDP} {
		t.Setenv(key, "")
		os.Unsetenv(key)
	}
	t.Setenv(authzenforce.GoAPI.Env(), "teams")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	components, err := configureWith(ctx, reachableDatabase(config.Config{APIAddress: "127.0.0.1:0"}), health.NewRegistry(time.Second), quietLogger(), nil)
	closeComponents(components)
	if err != nil {
		t.Fatalf("start with a cell on and no decision point inputs: %v, want a start", err)
	}
}

func TestACellOnBuildsTheEnforcerFromTheWorkloadInputs(t *testing.T) {
	proxy := mustProxyConfig(t, map[string]string{EnvAuthProxyURL: "http://auth-service:8095"})
	env := enforceEnv(map[string]string{EnvAuthWorkloadSubjectFile: subjectFile(t), EnvAuthWorkloadIssuer: testWorkloadIssuer})
	gauge := recordConfigInvalid(t)
	enf, err := buildEnforcer(lookupFrom(env), quietLogger(), newDecisionPoint(proxy, lookupFrom(env), nil))
	if err != nil || enf == nil {
		t.Fatalf("enforcer %v err %v, want an enforcer", enf, err)
	}
	if !slices.Equal(*gauge, []bool{false}) {
		t.Fatalf("config-invalid gauge %v, want [false]", *gauge)
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

// Startup builds one decision point client, the Enforcer and the shadow over it, and installs both beside the guard:
// a source pin where no database runs.
func TestStartupInstallsTheEnforcerOnTheGuard(t *testing.T) {
	raw, err := os.ReadFile("service.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"newDecisionPoint(authProxy, envsecrets.ProcessLookup, sharedWorkloadClient(provisioningCred, sessionCred))",
		"buildEnforcer(envsecrets.ProcessLookup, logger, decisionPoint)", "protected.guard.SetEnforcer(enforcer)",
		"protected.guard.SetShadow(newShadow(decisionPoint, envsecrets.ProcessLookup, logger))",
	} {
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
