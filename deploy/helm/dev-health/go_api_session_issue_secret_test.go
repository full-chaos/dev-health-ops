package devhealth_test

import (
	"strings"
	"testing"
)

const dedicatedIssueSecret = "dev-health-auth-session-issue"

var issueSessionsOn = []string{"--set", "goApi.authProxy.sessions=true"}

func issueRef(t *testing.T, docs []map[string]any) any {
	t.Helper()
	token := sessionsContainerEnv(t, docs, "Deployment", "a-dev-health-go-api")["AUTH_SESSION_ISSUE_TOKEN"]
	if token == nil {
		t.Fatal("go-api has no AUTH_SESSION_ISSUE_TOKEN")
	}
	return at(token, "valueFrom", "secretKeyRef")
}

func TestSessionIssueDefaultRenderUnchanged(t *testing.T) {
	base := append(append([]string{}, issueSessionsOn...), riverHook...)
	docs, def, err := renderAuth(t, goAPIWithAuth(base...)...)
	if err != nil {
		t.Fatalf("render failed: %v\n%.400s", err, def)
	}
	ref := issueRef(t, docs)
	if at(ref, "name") != "a-dev-health-secrets" || at(ref, "key") != "AUTH_SESSION_ISSUE_TOKEN" || at(ref, "optional") != nil {
		t.Fatalf("default ref = %v, want the shared Secret, required", ref)
	}
	_, explicit, err := renderAuth(t, goAPIWithAuth(append(append([]string{}, base...),
		"--set", "goApi.sessionIssue.secretName=", "--set", "goApi.sessionIssue.secretKey=AUTH_SESSION_ISSUE_TOKEN")...)...)
	if err != nil {
		t.Fatalf("render failed: %v\n%.400s", err, explicit)
	}
	if def != explicit {
		t.Fatal("explicit empty goApi.sessionIssue changes the render")
	}
}

func TestSessionIssueOverrideNamesDedicatedSecret(t *testing.T) {
	sets := append(append([]string{"--set", "goApi.sessionIssue.secretName=" + dedicatedIssueSecret}, issueSessionsOn...), riverHook...)
	docs, out, err := renderAuth(t, goAPIWithAuth(sets...)...)
	if err != nil {
		t.Fatalf("render failed: %v\n%.400s", err, out)
	}
	ref := issueRef(t, docs)
	if at(ref, "name") != dedicatedIssueSecret || at(ref, "key") != "AUTH_SESSION_ISSUE_TOKEN" || at(ref, "optional") != nil {
		t.Fatalf("override ref = %v, want the dedicated Secret, required", ref)
	}
	if n := strings.Count(out, dedicatedIssueSecret); n != 1 {
		t.Fatalf("the dedicated Secret name appears %d times in the render, want 1 (go-api only)", n)
	}
	other := sessionsContainerEnv(t, docs, "Deployment", "a-dev-health-go-api")["AUTH_PROXY_URL"]
	if other == nil {
		t.Fatal("go-api lost AUTH_PROXY_URL")
	}
}

func TestSessionIssueOverrideSessionsOffRendersNothing(t *testing.T) {
	sets := append([]string{"--set", "goApi.sessionIssue.secretName=" + dedicatedIssueSecret}, riverHook...)
	docs, out, err := renderAuth(t, goAPIWithAuth(sets...)...)
	if err != nil {
		t.Fatalf("render failed: %v\n%.400s", err, out)
	}
	if strings.Contains(out, dedicatedIssueSecret) || sessionsContainerEnv(t, docs, "Deployment", "a-dev-health-go-api")["AUTH_SESSION_ISSUE_TOKEN"] != nil {
		t.Fatal("sessions off still renders the issue credential")
	}
}

func TestSessionIssueSchemaRefusesBadValues(t *testing.T) {
	for _, set := range []string{
		"goApi.sessionIssue.secretName=Bad_Name",
		"goApi.sessionIssue.secretName=a/b",
		"goApi.sessionIssue.secretKey=a b",
		"goApi.sessionIssue.secretKey=",
		"goApi.sessionIssue.extra=x",
	} {
		if _, out, err := renderAuth(t, goAPIWithAuth(append([]string{"--set", set}, issueSessionsOn...)...)...); err == nil {
			t.Errorf("%s was accepted\n%.200s", set, out)
		}
	}
}
