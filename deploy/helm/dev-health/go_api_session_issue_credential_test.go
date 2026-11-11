package devhealth_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

var issueWorkload = []string{"--set", "goApi.sessionIssue.credential=workload"}

func issueSets(extra ...string) []string {
	sets := append(append([]string{}, issueSessionsOn...), riverHook...)
	return append(sets, extra...)
}

func siGoAPIEnv(t *testing.T, docs []map[string]any) map[string]any {
	t.Helper()
	return sessionsContainerEnv(t, docs, "Deployment", "a-dev-health-go-api")
}

func TestSessionIssueCredentialWorkloadRendersNoToken(t *testing.T) {
	docs, out, err := renderAuth(t, goAPIWithAuth(issueSets(issueWorkload...)...)...)
	if err != nil {
		t.Fatalf("render failed: %v\n%.400s", err, out)
	}
	env := siGoAPIEnv(t, docs)
	if env["AUTH_SESSION_ISSUE_TOKEN"] != nil {
		t.Error("credential=workload still renders AUTH_SESSION_ISSUE_TOKEN")
	}
	if at(env["AUTH_SESSION_ISSUE_CREDENTIAL"], "value") != "workload" {
		t.Errorf("AUTH_SESSION_ISSUE_CREDENTIAL = %v, want workload", env["AUTH_SESSION_ISSUE_CREDENTIAL"])
	}
	if n := strings.Count(out, "AUTH_SESSION_ISSUE_CREDENTIAL"); n != 1 {
		t.Errorf("AUTH_SESSION_ISSUE_CREDENTIAL appears %d times in the render, want 1", n)
	}
}

func TestSessionIssueCredentialWorkloadIgnoresSecretName(t *testing.T) {
	docs, out, err := renderAuth(t, goAPIWithAuth(issueSets(append([]string{"--set", "goApi.sessionIssue.secretName=" + dedicatedIssueSecret}, issueWorkload...)...)...)...)
	if err != nil {
		t.Fatalf("render failed: %v\n%.400s", err, out)
	}
	if siGoAPIEnv(t, docs)["AUTH_SESSION_ISSUE_TOKEN"] != nil {
		t.Error("credential=workload with secretName still renders the token reference")
	}
	if n := strings.Count(out, dedicatedIssueSecret); n != 0 {
		t.Errorf("the dedicated Secret name appears %d times in the render, want 0", n)
	}
}

func TestSessionIssueCredentialWorkloadIgnoresSecretKey(t *testing.T) {
	docs, out, err := renderAuth(t, goAPIWithAuth(issueSets(append([]string{"--set", "goApi.sessionIssue.secretKey=custom"}, issueWorkload...)...)...)...)
	if err != nil {
		t.Fatalf("render failed: %v\n%.400s", err, out)
	}
	if siGoAPIEnv(t, docs)["AUTH_SESSION_ISSUE_TOKEN"] != nil || strings.Contains(out, "key: custom") {
		t.Error("credential=workload with secretKey still renders the token reference")
	}
}

func TestSessionIssueCredentialWorkloadRefusesExtraEnvName(t *testing.T) {
	_, out, err := renderAuth(t, goAPIWithAuth(issueSets(append([]string{"--set-json", `goApi.extraEnv=[{"name":"AUTH_SESSION_ISSUE_CREDENTIAL","value":"workload"}]`}, issueWorkload...)...)...)...)
	if err == nil {
		t.Fatal("the render succeeded, want a refusal")
	}
	if !strings.Contains(out, "goApi.extraEnv must not set AUTH_SESSION_ISSUE_CREDENTIAL") {
		t.Errorf("the refusal does not name the extraEnv entry:\n%.600s", out)
	}
}

func TestSessionIssueCredentialWorkloadChangesOnlyGoAPI(t *testing.T) {
	_, token, err := renderAuth(t, goAPIWithAuth(issueSets()...)...)
	if err != nil {
		t.Fatalf("render failed: %v\n%.400s", err, token)
	}
	_, work, err := renderAuth(t, goAPIWithAuth(issueSets(issueWorkload...)...)...)
	if err != nil {
		t.Fatalf("render failed: %v\n%.400s", err, work)
	}
	a, b := splitDocs(token), splitDocs(work)
	if len(a) != len(b) {
		t.Fatalf("document count %d vs %d", len(a), len(b))
	}
	changed := 0
	for i := range a {
		if a[i] != b[i] {
			changed++
			if !strings.Contains(a[i], "name: a-dev-health-go-api\n") || !strings.Contains(a[i], "kind: Deployment") {
				t.Errorf("a document other than the go-api Deployment changed:\n%.200s", a[i])
			}
		}
	}
	if changed != 1 {
		t.Errorf("%d documents changed, want 1", changed)
	}
}

func splitDocs(out string) []string { return strings.Split(out, "\n---\n") }

func TestSessionIssueCredentialWorkloadWithBootstrapRendersOneCredential(t *testing.T) {
	docs, out, err := renderAuth(t, goAPIWithAuth(wbOn(issueWorkload...)...)...)
	if err != nil {
		t.Fatalf("render failed: %v\n%.400s", err, out)
	}
	env := siGoAPIEnv(t, docs)
	if at(env["AUTH_SESSION_ISSUE_CREDENTIAL"], "value") != "workload" || env["AUTH_WORKLOAD_BOOTSTRAP_FILE"] == nil || env["AUTH_SESSION_ISSUE_TOKEN"] != nil {
		t.Errorf("bootstrap + credential render wrong: %v", env)
	}
	if n := strings.Count(out, "name: AUTH_SESSION_ISSUE_CREDENTIAL"); n != 1 {
		t.Errorf("AUTH_SESSION_ISSUE_CREDENTIAL rendered %d times, want 1", n)
	}
}

func TestSessionIssueCredentialSchemaRefusesOtherValues(t *testing.T) {
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
}

func TestSessionIssueCredentialWorkloadSessionsOffRendersNothing(t *testing.T) {
	off := append([]string{}, riverHook...)
	_, def, err := renderAuth(t, goAPIWithAuth(off...)...)
	if err != nil {
		t.Fatalf("render failed: %v\n%.400s", err, def)
	}
	_, withKey, err := renderAuth(t, goAPIWithAuth(append(append([]string{}, off...), issueWorkload...)...)...)
	if err != nil {
		t.Fatalf("render failed: %v\n%.400s", err, withKey)
	}
	if def != withKey {
		t.Error("credential=workload with sessions off changes the render")
	}
}

// credential "" is the key absent (CHAOS-13467: a venue that layers values.prod.yaml unsets it this way): the unset render is compared
// with the explicit-default block and with every way to write "", byte for byte, and checked structurally.
func TestSessionIssueCredentialUnsetRendersTokenMode(t *testing.T) {
	t.Parallel()
	explicit := []string{"--set", "goApi.sessionIssue.secretName=", "--set", "goApi.sessionIssue.secretKey=AUTH_SESSION_ISSUE_TOKEN"}
	cases := []struct {
		name  string
		sets  []string
		token bool
		cred  bool
	}{
		{"sessions-on-token", issueSets(), true, false},
		{"sessions-on-dedicated-secret", issueSets("--set", "goApi.sessionIssue.secretName="+dedicatedIssueSecret, "--set", "goApi.sessionIssue.secretKey=custom"), true, false},
		{"sessions-off", append([]string{}, riverHook...), false, false},
		{"bootstrap-on", wbOn(), false, true},
	}
	for _, c := range cases {
		docs, unset, err := renderAuth(t, goAPIWithAuth(c.sets...)...)
		if err != nil {
			t.Fatalf("%s: render failed: %v\n%.400s", c.name, err, unset)
		}
		_, explicitOut, err := renderAuth(t, goAPIWithAuth(append(append([]string{}, explicit...), c.sets...)...)...)
		if err != nil {
			t.Fatalf("%s: render failed: %v\n%.400s", c.name, err, explicitOut)
		}
		if unset != explicitOut {
			t.Errorf("%s: credential unset differs from the explicit default sessionIssue block", c.name)
		}
		env := siGoAPIEnv(t, docs)
		if (env["AUTH_SESSION_ISSUE_TOKEN"] != nil) != c.token {
			t.Errorf("%s: AUTH_SESSION_ISSUE_TOKEN present = %v, want %v", c.name, env["AUTH_SESSION_ISSUE_TOKEN"] != nil, c.token)
		}
		if (env["AUTH_SESSION_ISSUE_CREDENTIAL"] != nil) != c.cred {
			t.Errorf("%s: AUTH_SESSION_ISSUE_CREDENTIAL present = %v, want %v", c.name, env["AUTH_SESSION_ISSUE_CREDENTIAL"] != nil, c.cred)
		}
		for _, empty := range [][]string{
			{"--set", "goApi.sessionIssue.credential="},
			{"--set-string", "goApi.sessionIssue.credential="},
			{"--set-json", `goApi.sessionIssue.credential=""`},
			{"-f", emptyCredentialValues(t)},
		} {
			_, emptyOut, err := renderAuth(t, goAPIWithAuth(append(append([]string{}, c.sets...), empty...)...)...)
			if err != nil {
				t.Errorf("%s: credential \"\" (%s) was refused: %v\n%.400s", c.name, empty[0], err, emptyOut)
			} else if emptyOut != unset {
				t.Errorf("%s: credential \"\" (%s) differs from the key absent", c.name, empty[0])
			}
		}
	}
}

func TestSessionIssueCredentialWorkloadAcceptsOtherCaseSpellings(t *testing.T) {
	for _, name := range []string{"auth_session_issue_credential", "Auth_Session_Issue_Credential", " AUTH_SESSION_ISSUE_CREDENTIAL"} {
		_, out, err := renderAuth(t, goAPIWithAuth(issueSets(append([]string{"--set-json", `goApi.extraEnv=[{"name":"` + name + `","value":"x"}]`}, issueWorkload...)...)...)...)
		if err != nil {
			t.Errorf("extraEnv %q is a different variable in a container and was refused: %v\n%.400s", name, err, out)
		}
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

// The workload render is unchanged by the "" admission: the same env as before, one AUTH_SESSION_ISSUE_CREDENTIAL, no token reference.
func TestSessionIssueCredentialWorkloadRenderUnchangedByEmptyAdmission(t *testing.T) {
	docs, out, err := renderAuth(t, goAPIWithAuth(issueSets(issueWorkload...)...)...)
	if err != nil {
		t.Fatalf("render failed: %v\n%.400s", err, out)
	}
	env := siGoAPIEnv(t, docs)
	if at(env["AUTH_SESSION_ISSUE_CREDENTIAL"], "value") != "workload" || env["AUTH_SESSION_ISSUE_TOKEN"] != nil {
		t.Errorf("credential=workload render changed: %v", env)
	}
	if n := strings.Count(out, "name: AUTH_SESSION_ISSUE_CREDENTIAL"); n != 1 {
		t.Errorf("AUTH_SESSION_ISSUE_CREDENTIAL rendered %d times, want 1", n)
	}
	_, unset, err := renderAuth(t, goAPIWithAuth(issueSets()...)...)
	if err != nil {
		t.Fatalf("render failed: %v\n%.400s", err, unset)
	}
	if unset == out {
		t.Error("credential=workload renders the same as the key absent")
	}
}
