package devhealth_test

import (
	"reflect"
	"strings"
	"testing"
)

// CHAOS-13039: go-api presents its workload token on the session issue route, read from a bootstrap credential file that a Secret
// mounts. goApi.sessionIssue.workloadBootstrap is off by default, and off renders nothing. Values alone cannot carry it: with sessions
// on the template always renders AUTH_SESSION_ISSUE_TOKEN as a required secretKeyRef, so go-api would keep needing an svc_authsess_
// Secret. auth-service needs no template change: AUTH_WORKLOAD_BOOTSTRAP and AUTH_SESSION_ISSUE_WORKLOAD_SUBJECT pass through
// authService.extraEnv (the last cell).

const (
	wbIssuer  = "https://auth.workload-proof.example"
	wbSecret  = "go-api-workload"
	wbSubject = "prn_AAAAAAAAAAAAAAAAAAAAAA"
)

var wbOwnEnv = []string{"AUTH_SESSION_ISSUE_CREDENTIAL", "AUTH_WORKLOAD_BOOTSTRAP_FILE", "AUTH_WORKLOAD_ISSUER"}

func wbSessions() []string {
	return append([]string{"--set", "authService.phase=2"}, riverHook...)
}

func wbOn(extra ...string) []string {
	sets := append(wbSessions(), "--set", "authService.accessToken.issuerURL="+wbIssuer,
		"--set", "goApi.sessionIssue.workloadBootstrap.enabled=true", "--set", "goApi.sessionIssue.workloadBootstrap.secretName="+wbSecret)
	return append(sets, extra...)
}

func TestWorkloadBootstrapOffRendersNothing(t *testing.T) {
	docs, def, err := renderAuth(t, goAPIWithAuth(wbSessions()...)...)
	if err != nil {
		t.Fatalf("render failed: %v\n%.400s", err, def)
	}
	env := sessionsContainerEnv(t, docs, "Deployment", "a-dev-health-go-api")
	for _, name := range wbOwnEnv {
		if env[name] != nil {
			t.Errorf("the default render carries %s: %v", name, env[name])
		}
	}
	if env["AUTH_SESSION_ISSUE_TOKEN"] == nil {
		t.Error("the default render lost AUTH_SESSION_ISSUE_TOKEN")
	}
	if v := at(authDoc(docs, "Deployment", "a-dev-health-go-api"), "spec", "template", "spec", "volumes"); v != nil {
		t.Errorf("go-api pod volumes = %v, want no key", v)
	}
	if strings.Contains(def, "workload-bootstrap") {
		t.Error("the default render names the workload-bootstrap volume")
	}
	_, explicit, err := renderAuth(t, goAPIWithAuth(append(wbSessions(), "--set", "goApi.sessionIssue.workloadBootstrap.enabled=false",
		"--set", "goApi.sessionIssue.workloadBootstrap.secretName="+wbSecret)...)...)
	if err != nil {
		t.Fatalf("render failed: %v\n%.400s", err, explicit)
	}
	if def != explicit {
		t.Fatal("goApi.sessionIssue.workloadBootstrap.enabled=false changes the render")
	}
}

func TestWorkloadBootstrapOnRendersTheCredentialFileAndTheWorkloadSelector(t *testing.T) {
	docs, out, err := renderAuth(t, goAPIWithAuth(wbOn("--set", "goApi.sessionIssue.workloadBootstrap.secretKey=bootstrap.cred")...)...)
	if err != nil {
		t.Fatalf("render failed: %v\n%.400s", err, out)
	}
	env := sessionsContainerEnv(t, docs, "Deployment", "a-dev-health-go-api")
	want := map[string]string{
		"AUTH_SESSION_ISSUE_CREDENTIAL": "workload", "AUTH_WORKLOAD_BOOTSTRAP_FILE": "/run/workload/api.bootstrap", "AUTH_WORKLOAD_ISSUER": wbIssuer,
	}
	for name, value := range want {
		if got := at(env[name], "value"); got != value {
			t.Errorf("%s = %v, want %q", name, got, value)
		}
	}
	if env["AUTH_SESSION_ISSUE_TOKEN"] != nil {
		t.Errorf("workload mode still renders AUTH_SESSION_ISSUE_TOKEN: %v", env["AUTH_SESSION_ISSUE_TOKEN"])
	}
	if strings.Contains(out, "svc_authsess_") || strings.Contains(out, "AUTH_SESSION_ISSUE_TOKEN") {
		t.Error("workload mode leaves an svc_authsess_ credential reference in the render")
	}
	dep := authDoc(docs, "Deployment", "a-dev-health-go-api")
	vols, _ := at(dep, "spec", "template", "spec", "volumes").([]any)
	if len(vols) != 1 || at(vols[0], "name") != "workload-bootstrap" || at(vols[0], "secret", "secretName") != wbSecret {
		t.Fatalf("go-api pod volumes = %v, want the one workload-bootstrap Secret volume of %s", vols, wbSecret)
	}
	if !reflect.DeepEqual(at(vols[0], "secret", "defaultMode"), 292) {
		t.Errorf("defaultMode = %v, want 292 (0444): the pod runs as uid 65532 and the file is root-owned", at(vols[0], "secret", "defaultMode"))
	}
	items, _ := at(vols[0], "secret", "items").([]any)
	if len(items) != 1 || at(items[0], "key") != "bootstrap.cred" || at(items[0], "path") != "api.bootstrap" {
		t.Errorf("items = %v, want key bootstrap.cred at path api.bootstrap", items)
	}
	if at(vols[0], "secret", "optional") != nil {
		t.Errorf("the Secret volume is optional: %v", at(vols[0], "secret", "optional"))
	}
	mounts, _ := at(dep, "spec", "template", "spec", "containers", 0, "volumeMounts").([]any)
	if len(mounts) != 1 || at(mounts[0], "name") != "workload-bootstrap" || at(mounts[0], "mountPath") != "/run/workload" || at(mounts[0], "readOnly") != true {
		t.Errorf("go-api volumeMounts = %v, want the one read-only /run/workload mount", mounts)
	}
	if n := strings.Count(out, wbSecret); n != 1 {
		t.Errorf("the Secret name appears %d times, want 1 (go-api only)", n)
	}
}

func TestWorkloadBootstrapOnChangesOnlyTheGoAPIDeployment(t *testing.T) {
	base, out, err := renderAuth(t, goAPIWithAuth(wbSessions()...)...)
	if err != nil {
		t.Fatalf("render failed: %v\n%.400s", err, out)
	}
	on, out, err := renderAuth(t, goAPIWithAuth(wbOn()...)...)
	if err != nil {
		t.Fatalf("render failed: %v\n%.400s", err, out)
	}
	if len(base) != len(on) {
		t.Fatalf("document count %d -> %d", len(base), len(on))
	}
	for i := range base {
		meta, _ := base[i]["metadata"].(map[string]any)
		name, _ := meta["name"].(string)
		if base[i]["kind"] == "Deployment" && strings.HasSuffix(name, "-go-api") {
			continue
		}
		if !reflect.DeepEqual(base[i], on[i]) {
			t.Errorf("%v %s changed", base[i]["kind"], name)
		}
	}
}

func TestWorkloadBootstrapKeepsTheExtraVolumes(t *testing.T) {
	docs, out, err := renderAuth(t, goAPIWithAuth(append(wbOn(), goAPIExtraVolumeSets()[2:]...)...)...)
	if err != nil {
		t.Fatalf("render failed: %v\n%.400s", err, out)
	}
	dep := authDoc(docs, "Deployment", "a-dev-health-go-api")
	vols, _ := at(dep, "spec", "template", "spec", "volumes").([]any)
	mounts, _ := at(dep, "spec", "template", "spec", "containers", 0, "volumeMounts").([]any)
	if len(vols) != 2 || len(mounts) != 2 {
		t.Fatalf("volumes %d and mounts %d, want 2 and 2 (the bootstrap one and the extra one)", len(vols), len(mounts))
	}
	if at(vols[0], "name") != "workload-bootstrap" || at(vols[1], "name") != "workload-token" || at(mounts[1], "name") != "workload-token" {
		t.Errorf("volumes %v mounts %v: want the bootstrap entry first, the extra entry kept", vols, mounts)
	}
}

func TestWorkloadBootstrapRefusals(t *testing.T) {
	cases := map[string]struct {
		sets []string
		want string
	}{
		"sessions off": {append(append([]string{}, "--set", "goApi.sessionIssue.workloadBootstrap.enabled=true", "--set", "goApi.sessionIssue.workloadBootstrap.secretName="+wbSecret,
			"--set", "authService.accessToken.issuerURL="+wbIssuer), riverHook...), "goApi.sessionIssue.workloadBootstrap"},
		"no issuer": {append(wbSessions(), "--set", "goApi.sessionIssue.workloadBootstrap.enabled=true", "--set", "goApi.sessionIssue.workloadBootstrap.secretName="+wbSecret),
			"authService.accessToken.issuerURL"},
		"no secret name": {append(wbSessions(), "--set", "authService.accessToken.issuerURL="+wbIssuer, "--set", "goApi.sessionIssue.workloadBootstrap.enabled=true"),
			"goApi.sessionIssue.workloadBootstrap.secretName"},
		"bad secret key": {wbOn("--set", "goApi.sessionIssue.workloadBootstrap.secretKey=../x"), "secretKey"},
		"enabled not a bool": {append(wbSessions(), "--set", "authService.accessToken.issuerURL="+wbIssuer, "--set-string", "goApi.sessionIssue.workloadBootstrap.enabled=yes",
			"--set", "goApi.sessionIssue.workloadBootstrap.secretName="+wbSecret), "enabled"},
	}
	for _, name := range wbOwnEnv {
		cases["extraEnv "+name] = struct {
			sets []string
			want string
		}{wbOn("--set-json", `goApi.extraEnv=[{"name":"`+name+`","value":"x"}]`), "goApi.extraEnv must not set " + name}
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			_, out, err := renderAuth(t, goAPIWithAuth(c.sets...)...)
			if err == nil {
				t.Fatalf("the render succeeded, want a refusal naming %q", c.want)
			}
			if !strings.Contains(out, c.want) {
				t.Errorf("the refusal does not name %q:\n%.600s", c.want, out)
			}
		})
	}
}

func TestWorkloadBootstrapOffLeavesExtraEnvAlone(t *testing.T) {
	for _, name := range wbOwnEnv {
		docs, out, err := renderAuth(t, goAPIWithAuth(append(wbSessions(), "--set-json", `goApi.extraEnv=[{"name":"`+name+`","value":"x"}]`)...)...)
		if err != nil {
			t.Fatalf("extraEnv %s with the block off was refused: %v\n%.400s", name, err, out)
		}
		if at(sessionsContainerEnv(t, docs, "Deployment", "a-dev-health-go-api")[name], "value") != "x" {
			t.Errorf("extraEnv %s with the block off is not rendered", name)
		}
	}
}

func TestWorkloadBootstrapAuthServiceTakesItsEnvThroughExtraEnv(t *testing.T) {
	docs, out, err := renderAuth(t, goAPIWithAuth(append(wbSessions(), "--set", "authService.accessToken.issuerURL="+wbIssuer, "--set-json",
		`authService.extraEnv=[{"name":"AUTH_WORKLOAD_BOOTSTRAP","value":"true"},{"name":"AUTH_SESSION_ISSUE_WORKLOAD_SUBJECT","value":"`+wbSubject+`"}]`)...)...)
	if err != nil {
		t.Fatalf("render failed: %v\n%.400s", err, out)
	}
	env := sessionsContainerEnv(t, docs, "Deployment", "a-dev-health-auth-service")
	if at(env["AUTH_WORKLOAD_BOOTSTRAP"], "value") != "true" || at(env["AUTH_SESSION_ISSUE_WORKLOAD_SUBJECT"], "value") != wbSubject {
		t.Errorf("auth-service env: AUTH_WORKLOAD_BOOTSTRAP %v, AUTH_SESSION_ISSUE_WORKLOAD_SUBJECT %v", env["AUTH_WORKLOAD_BOOTSTRAP"], env["AUTH_SESSION_ISSUE_WORKLOAD_SUBJECT"])
	}
	if at(env["AUTH_TOKEN_ISSUER_URL"], "value") != wbIssuer {
		t.Errorf("auth-service AUTH_TOKEN_ISSUER_URL = %v, want %s (the workload token issuer go-api sends)", env["AUTH_TOKEN_ISSUER_URL"], wbIssuer)
	}
}
