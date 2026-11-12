package devhealth_test

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// go-api's credential on the session issue route: the workload token is the only mode. With sessions on the chart renders
// AUTH_SESSION_ISSUE_CREDENTIAL=workload; goApi.sessionIssue.credential ("workload" or "") and the retired secretName/secretKey
// keys change nothing.

const ignoredIssueSecret = "dev-health-auth-session-issue"

var (
	issueSessionsOn = []string{"--set", "authService.phase=2"}
	issueWorkload   = []string{"--set", "goApi.sessionIssue.credential=workload"}
)

func issueSets(extra ...string) []string {
	sets := append(append([]string{}, issueSessionsOn...), riverHook...)
	return append(sets, extra...)
}

func siGoAPIEnv(t *testing.T, docs []map[string]any) map[string]any {
	t.Helper()
	return sessionsContainerEnv(t, docs, "Deployment", "a-dev-health-go-api")
}

// sessionIssueNames are go-api's env names that start with AUTH_SESSION_ISSUE_, sorted.
func sessionIssueNames(env map[string]any) []string {
	var names []string
	for name := range env {
		if strings.HasPrefix(name, "AUTH_SESSION_ISSUE_") {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	return names
}

func TestSessionIssueSessionsOnRendersTheWorkloadCredentialOnly(t *testing.T) {
	t.Parallel()

	docs, out, err := renderAuth(t, goAPIWithAuth(issueSets()...)...)
	if err != nil {
		t.Fatalf("render failed: %v\n%.400s", err, out)
	}
	env := siGoAPIEnv(t, docs)
	if got := sessionIssueNames(env); len(got) != 1 || got[0] != "AUTH_SESSION_ISSUE_CREDENTIAL" {
		t.Errorf("go-api session issue env = %v, want only AUTH_SESSION_ISSUE_CREDENTIAL", got)
	}
	if at(env["AUTH_SESSION_ISSUE_CREDENTIAL"], "value") != "workload" {
		t.Errorf("AUTH_SESSION_ISSUE_CREDENTIAL = %v, want workload", env["AUTH_SESSION_ISSUE_CREDENTIAL"])
	}
	if n := strings.Count(out, "name: AUTH_SESSION_ISSUE_CREDENTIAL"); n != 1 {
		t.Errorf("AUTH_SESSION_ISSUE_CREDENTIAL rendered %d times, want 1", n)
	}
}

// Every way a values file can still write the credential key or the retired secretName/secretKey keys renders byte for byte
// the same as the keys absent, so a values file that sets them renders unchanged.
func TestSessionIssueValuesKeysRenderTheSameAsAbsent(t *testing.T) {
	t.Parallel()

	_, absent, err := renderAuth(t, goAPIWithAuth(issueSets()...)...)
	if err != nil {
		t.Fatalf("render failed: %v\n%.400s", err, absent)
	}
	for _, extra := range [][]string{
		issueWorkload,
		{"--set", "goApi.sessionIssue.credential="},
		{"--set-string", "goApi.sessionIssue.credential="},
		{"--set-json", `goApi.sessionIssue.credential=""`},
		{"-f", emptyCredentialValues(t)},
		{"--set", "goApi.sessionIssue.secretName=" + ignoredIssueSecret},
		{"--set", "goApi.sessionIssue.secretKey=custom"},
		{"--set", "goApi.sessionIssue.secretName=" + ignoredIssueSecret, "--set", "goApi.sessionIssue.secretKey=custom", "--set", "goApi.sessionIssue.credential="},
	} {
		_, out, err := renderAuth(t, goAPIWithAuth(issueSets(extra...)...)...)
		if err != nil {
			t.Errorf("%v was refused: %v\n%.400s", extra, err, out)
			continue
		}
		if out != absent {
			t.Errorf("%v renders differently from the keys absent", extra)
		}
		if strings.Contains(out, ignoredIssueSecret) || strings.Contains(out, "key: custom") {
			t.Errorf("%v: the retired secretName/secretKey reach the render", extra)
		}
	}
}

// The chart renders the name, so goApi.extraEnv cannot set it in any spelling, with sessions on or off (the goApiEnvNames rule).
func TestSessionIssueRefusesTheExtraEnvName(t *testing.T) {
	t.Parallel()

	for _, base := range [][]string{issueSets(), append([]string{}, riverHook...)} {
		for _, name := range []string{"AUTH_SESSION_ISSUE_CREDENTIAL", "auth_session_issue_credential", " AUTH_SESSION_ISSUE_CREDENTIAL"} {
			_, out, err := renderAuth(t, goAPIWithAuth(append(append([]string{}, base...), "--set-json", `goApi.extraEnv=[{"name":"`+name+`","value":"workload"}]`)...)...)
			if err == nil {
				t.Errorf("extraEnv %q rendered, want a refusal", name)
				continue
			}
			if !strings.Contains(out, "goApi.extraEnv must not set") {
				t.Errorf("extraEnv %q: the refusal does not name extraEnv:\n%.600s", name, out)
			}
		}
	}
}

func TestSessionIssueWithBootstrapRendersOneCredential(t *testing.T) {
	t.Parallel()

	docs, out, err := renderAuth(t, goAPIWithAuth(wbOn(issueWorkload...)...)...)
	if err != nil {
		t.Fatalf("render failed: %v\n%.400s", err, out)
	}
	env := siGoAPIEnv(t, docs)
	if at(env["AUTH_SESSION_ISSUE_CREDENTIAL"], "value") != "workload" || env["AUTH_WORKLOAD_BOOTSTRAP_FILE"] == nil {
		t.Errorf("bootstrap + credential render wrong: %v", env)
	}
	if got := sessionIssueNames(env); len(got) != 1 {
		t.Errorf("go-api session issue env = %v, want only AUTH_SESSION_ISSUE_CREDENTIAL", got)
	}
	if n := strings.Count(out, "name: AUTH_SESSION_ISSUE_CREDENTIAL"); n != 1 {
		t.Errorf("AUTH_SESSION_ISSUE_CREDENTIAL rendered %d times, want 1", n)
	}
}

func TestSessionIssueSchemaRefusesOtherValues(t *testing.T) {
	t.Parallel()

	for _, set := range []string{"goApi.sessionIssue.credential=session_compat", "goApi.sessionIssue.credential=token", "goApi.sessionIssue.credential=Workload"} {
		_, out, err := renderAuth(t, goAPIWithAuth(append([]string{"--set", set}, issueSessionsOn...)...)...)
		if err == nil {
			t.Errorf("%s was accepted", set)
			continue
		}
		if !strings.Contains(out, "credential") || !strings.Contains(out, "workload") {
			t.Errorf("%s: the refusal does not name the credential enum:\n%.400s", set, out)
		}
	}
	for _, set := range []string{"goApi.sessionIssue.extra=x", "goApi.sessionIssue.secretName=Bad_Name", "goApi.sessionIssue.secretKey=a b"} {
		if _, out, err := renderAuth(t, goAPIWithAuth(append([]string{"--set", set}, issueSessionsOn...)...)...); err == nil {
			t.Errorf("%s was accepted\n%.200s", set, out)
		}
	}
}

func TestSessionIssueSessionsOffRendersNoCredential(t *testing.T) {
	t.Parallel()

	off := append([]string{}, riverHook...)
	docs, def, err := renderAuth(t, goAPIWithAuth(off...)...)
	if err != nil {
		t.Fatalf("render failed: %v\n%.400s", err, def)
	}
	if got := sessionIssueNames(siGoAPIEnv(t, docs)); len(got) != 0 {
		t.Errorf("sessions off renders %v", got)
	}
	_, withKey, err := renderAuth(t, goAPIWithAuth(append(append([]string{}, off...), issueWorkload...)...)...)
	if err != nil {
		t.Fatalf("render failed: %v\n%.400s", err, withKey)
	}
	if def != withKey {
		t.Error("credential=workload with sessions off changes the render")
	}
}

// emptyCredentialValues is a values file that sets goApi.sessionIssue.credential to "", the form a venue values file uses.
func emptyCredentialValues(t *testing.T) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "empty-credential.yaml")
	if err := os.WriteFile(p, []byte("goApi:\n  sessionIssue:\n    credential: \"\"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}
