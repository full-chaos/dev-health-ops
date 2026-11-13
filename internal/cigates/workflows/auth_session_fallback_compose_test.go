package workflows_test

import (
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/apiservice"
)

// The local go-api never mints its own HS256 sessions and never signs in with a service credential. go-api mints its
// own session pair whenever its session issuer is nil (apiservice.Deps.SessionIssuer: "nil mints them in go-api"),
// and sessionIssuer returns nil unless AuthProxyConfig.Enabled(AuthSessions): AUTH_PROXY_SESSIONS on AND
// AUTH_PROXY_URL set. This cell reads go-api's compose environment through the same parser go-api starts with, so a
// compose that leaves either one out is refused here. With the flag on, go-api presents its workload token on the
// session issue route (AUTH_SESSION_ISSUE_CREDENTIAL=workload): compose writes the bootstrap credential file with the
// auth-workload-bootstrap one-shot and the principal id auth-service accepts with auth-workload-subject, on every `up`
// and with no manual step. The retired session issuer credential and the one-shot that seeded its row are not part of
// compose (the retiredcredentialnames census keeps its name out of every tracked file).
func TestComposeGoAPIDoesNotMintItsOwnHS256Sessions(t *testing.T) {
	c := readComposeFile(t, "compose.yml")
	goAPI, ok := c.Services["go-api"]
	if !ok {
		t.Fatal("compose.yml has no service go-api")
	}
	env := map[string]string{}
	for k, v := range goAPI.Environment {
		if s, ok := v.(string); ok {
			env[k] = expandDefaults(s, nil)
		}
	}
	lookup := func(name string) (string, bool) { v, ok := env[name]; return v, ok }

	cfg, err := apiservice.ParseAuthProxyConfig(lookup)
	if err != nil {
		t.Fatalf("go-api's compose environment is refused by its auth proxy parser: %v", err)
	}
	if !cfg.Enabled(apiservice.AuthSessions) {
		t.Fatalf("go-api's compose environment leaves sessions in go-api (AUTH_PROXY_SESSIONS %q, AUTH_PROXY_URL %q): its session issuer is nil and it mints HS256 session pairs itself",
			env[apiservice.AuthProxyFlagEnv(apiservice.AuthSessions)], env[apiservice.EnvAuthProxyURL])
	}
	if got := env[apiservice.EnvAuthSessionIssueCredential]; got != "workload" {
		t.Errorf("go-api %s resolves to %q: local compose presents go-api's workload token, so it must be workload", apiservice.EnvAuthSessionIssueCredential, got)
	}
	for _, name := range []string{apiservice.EnvAuthWorkloadBootstrapFile, apiservice.EnvAuthWorkloadIssuer} {
		if strings.TrimSpace(env[name]) == "" {
			t.Errorf("go-api has no %s: the workload credential needs it and go-api refuses to start without it", name)
		}
	}
	if _, ok := c.Services["auth-session-issue-seed"]; ok {
		t.Error("compose.yml still has auth-session-issue-seed: it writes the row of the retired session issuer credential, which nothing presents")
	}
	if c.Services["go-api"].DependsOn["auth-workload-bootstrap"].Condition != "service_completed_successfully" {
		t.Error("go-api does not wait for auth-workload-bootstrap: it would start without its credential file")
	}
	if c.Services["auth-service"].DependsOn["auth-workload-subject"].Condition != "service_completed_successfully" {
		t.Error("auth-service does not wait for auth-workload-subject: it would start without the principal id it accepts")
	}
}
