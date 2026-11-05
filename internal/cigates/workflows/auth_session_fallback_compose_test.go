package workflows_test

import (
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/sessionissue"
	"github.com/full-chaos/dev-health-ops/internal/apiservice"
)

// CHAOS-13107: the real HS256 fallback of the local go-api. go-api mints its own HS256 session pair whenever its
// session issuer is nil (apiservice.Deps.SessionIssuer: "nil mints them in go-api"), and sessionIssuer returns nil
// unless AuthProxyConfig.Enabled(AuthSessions): AUTH_PROXY_SESSIONS on AND AUTH_PROXY_URL set. This cell reads
// go-api's compose environment through the same parser go-api starts with, so a compose that leaves either one out
// is refused here. With the flag on, go-api builds its session_compat client from AUTH_SESSION_ISSUE_TOKEN (acp
// ruling (b): local compose presents the svc_authsess_ credential, not the workload token); a token sessionissue.New
// refuses would stop go-api at start, and a token with no auth-session-issuer row would make every sign-in answer
// 503, so the seed one-shot must hash the same value.
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
	if sel := env[apiservice.EnvAuthSessionIssueCredential]; sel != "" && sel != "session_compat" {
		t.Errorf("go-api %s resolves to %q: local compose presents session_compat (acp ruling (b)), so it must be blank or session_compat",
			apiservice.EnvAuthSessionIssueCredential, sel)
	}
	token := env[sessionissue.EnvToken]
	if _, err := sessionissue.New(cfg.Upstream, token, 0); err != nil {
		t.Errorf("go-api's session issue client is refused (%v): go-api does not start", err)
	}

	seed, ok := c.Services["auth-session-issue-seed"]
	if !ok {
		t.Fatal("compose.yml has no service auth-session-issue-seed: no auth-session-issuer row holds go-api's token, and auth-service refuses every session issue")
	}
	seedToken, _ := envString(t, seed, sessionissue.EnvToken)
	if raw, _ := envString(t, goAPI, sessionissue.EnvToken); raw != seedToken {
		t.Errorf("go-api %s is %q, the seed one-shot hashes %q: the two must be the same compose value", sessionissue.EnvToken, raw, seedToken)
	}
	if !runsWherever(seed.Profiles, goAPI.Profiles) {
		t.Errorf("auth-session-issue-seed profiles = %v: it must start in every profile go-api starts in", seed.Profiles)
	}
}
