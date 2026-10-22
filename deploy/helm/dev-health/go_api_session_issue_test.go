package devhealth_test

import "testing"

// containerEnv is the env of the first container of the named object.
func sessionsContainerEnv(t *testing.T, docs []map[string]any, kind, name string) map[string]any {
	t.Helper()
	doc := authDoc(docs, kind, name)
	if doc == nil {
		t.Fatalf("no %s %s in the render", kind, name)
	}
	out := map[string]any{}
	containers, _ := at(doc, "spec", "template", "spec", "containers").([]any)
	if len(containers) == 0 {
		t.Fatalf("%s %s has no container", kind, name)
	}
	env, _ := at(containers[0], "env").([]any)
	for _, entry := range env {
		if name, ok := at(entry, "name").(string); ok {
			out[name] = entry
		}
	}
	return out
}

var riverHook = []string{
	"--set", "migrations.hook.riverMigrate.enabled=true",
	"--set", "migrations.hook.riverMigrate.image=ghcr.io/full-chaos/dho:sha-0123456789ab",
}

// AUTH_PROXY_SESSIONS on: go-api presents the auth-session-issuer credential
// from the release Secret, and every step that applies the api role's River
// grants (the migrate Job, the River hook) reads the same flag, so the
// refresh_tokens REVOKE follows it.
func TestSessionsOnWiresTheIssueCredentialAndTheGrantFlag(t *testing.T) {
	docs, out, err := renderAuth(t, goAPIWithAuth(append([]string{"--set", "goApi.authProxy.sessions=true"}, riverHook...)...)...)
	if err != nil {
		t.Fatalf("render failed: %v\n%.400s", err, out)
	}
	token := sessionsContainerEnv(t, docs, "Deployment", "a-dev-health-go-api")["AUTH_SESSION_ISSUE_TOKEN"]
	if token == nil {
		t.Fatal("go-api has no AUTH_SESSION_ISSUE_TOKEN with sessions on")
	}
	if at(token, "value") != nil || at(token, "valueFrom", "secretKeyRef", "key") != "AUTH_SESSION_ISSUE_TOKEN" ||
		at(token, "valueFrom", "secretKeyRef", "name") != "a-dev-health-secrets" || at(token, "valueFrom", "secretKeyRef", "optional") != nil {
		t.Fatalf("AUTH_SESSION_ISSUE_TOKEN = %v, want a required secretKeyRef to the release Secret", token)
	}
	for _, job := range []string{"a-dev-health-migrate", "a-dev-health-river-migrate"} {
		flag := sessionsContainerEnv(t, docs, "Job", job)["AUTH_PROXY_SESSIONS"]
		if at(flag, "value") != "true" {
			t.Errorf("Job %s: AUTH_PROXY_SESSIONS = %v, want \"true\"", job, flag)
		}
	}
}

func TestSessionsOffRendersNoIssueCredentialOrGrantFlag(t *testing.T) {
	docs, out, err := renderAuth(t, goAPIWithAuth(riverHook...)...)
	if err != nil {
		t.Fatalf("render failed: %v\n%.400s", err, out)
	}
	if token := sessionsContainerEnv(t, docs, "Deployment", "a-dev-health-go-api")["AUTH_SESSION_ISSUE_TOKEN"]; token != nil {
		t.Fatalf("go-api reads AUTH_SESSION_ISSUE_TOKEN with sessions off: %v", token)
	}
	for _, job := range []string{"a-dev-health-migrate", "a-dev-health-river-migrate"} {
		if flag := sessionsContainerEnv(t, docs, "Job", job)["AUTH_PROXY_SESSIONS"]; flag != nil {
			t.Errorf("Job %s sets AUTH_PROXY_SESSIONS with sessions off: %v", job, flag)
		}
	}
}
