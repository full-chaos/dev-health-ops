//go:build integration

package sso_test

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/authprovision"
	"github.com/full-chaos/dev-health-ops/internal/api/licensing"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/sso"
	"github.com/full-chaos/dev-health-ops/internal/apiservice"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
)

// The OAuth provider create and patch routes are retired: over a real migrated Postgres, the real guard and real
// tokens, every caller (an entitled org's owner, an unentitled org's owner, a platform admin without a membership with
// and without an org in the token, no credential) gets go-api's not-found answer for a valid and an invalid body, with
// the provider switch off and on, and with a process license that grants sso_saml. No provider row changes.

// retiredServer is the SSO routes in go-api's own server (apiservice.NewServer, with the scope layers): a path no route
// takes is its not-found answer.
func retiredServer(t *testing.T, st *entitlementStack, providers authprovision.SSOProviders) *httptest.Server {
	t.Helper()
	logger := slog.New(slog.DiscardHandler)
	auth := st.tokens.Authenticator(t, policy.PGStore{Pool: st.pool}, logger)
	deps := sso.Deps{Pool: st.pool, Guard: policy.NewGuard(auth, logger), Logger: logger, Write: apiservice.WriteError}
	if providers != nil {
		deps.Providers = providers
	}
	cfg, err := config.Load(config.Spec{Service: config.APIServiceName, LookupEnv: func(string) (string, bool) { return "", false }})
	if err != nil {
		t.Fatal(err)
	}
	api, err := apiservice.NewServer(cfg, logger, sso.Routes(deps), policy.NewScope(auth, logger).Layers()...)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(api.Handler())
	t.Cleanup(server.Close)
	return server
}

func retiredDo(t *testing.T, server *httptest.Server, method, path, token, body string) (int, string) {
	t.Helper()
	request, err := http.NewRequest(method, server.URL+"/api/v1/auth"+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	if body != "" {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	raw, _ := io.ReadAll(response.Body)
	return response.StatusCode, string(raw)
}

func TestRetiredOAuthProviderRoutesAnswerNotFoundForEveryCaller(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	st := startEntitlementStack(t, ctx)
	existing := uuid.New()
	if _, err := st.pool.Exec(ctx, `INSERT INTO sso_providers (id, org_id, name, protocol, status, is_default, allow_idp_initiated,
	auto_provision_users, default_role, config, created_at, updated_at)
VALUES ($1, $2, 'Existing GitHub', 'oauth_github', 'active', false, false, false, 'member', '{"client_id":"i"}'::json, now(), now())`,
		existing, st.orgs["entitled"]); err != nil {
		t.Fatal(err)
	}
	snapshot := func() string {
		t.Helper()
		var rows string
		if err := st.pool.QueryRow(ctx, `SELECT coalesce(string_agg(id::text || '|' || name || '|' || status || '|' || config::text || '|' ||
	updated_at::text, ',' ORDER BY id), '') FROM sso_providers`).Scan(&rows); err != nil {
			t.Fatal(err)
		}
		return rows
	}
	before := snapshot()
	callers := map[string]string{
		"entitled owner":               st.token(t, st.owners["entitled"], st.orgs["entitled"].String()),
		"unentitled owner":             st.token(t, st.owners["community"], st.orgs["community"].String()),
		"platform admin, entitled org": st.token(t, st.superuser, st.orgs["entitled"].String()),
		"platform admin, no org":       st.token(t, st.superuser, ""),
		"no credential":                "",
	}
	steps := []struct{ method, path, body string }{
		{http.MethodPost, "/oauth/providers", `{"name":"g","provider_type":"google","oauth_config":{"client_id":"i","client_secret":"s"}}`},
		{http.MethodPost, "/oauth/providers", `{}`},
		{http.MethodPatch, "/oauth/providers/" + existing.String(), `{"name":"renamed"}`},
		{http.MethodPatch, "/oauth/providers/" + existing.String(), `{"oauth_config":"x"}`},
	}
	for _, license := range []*licensing.ProcessLicense{nil, {Tier: "enterprise", Features: map[string]bool{"sso_saml": true}}} {
		licensing.SetProcessLicense(license)
		t.Cleanup(func() { licensing.SetProcessLicense(nil) })
		for switchName, providers := range map[string]authprovision.SSOProviders{"switch off": nil, "switch on": st.providers} {
			server := retiredServer(t, st, providers)
			for callerName, token := range callers {
				for _, step := range steps {
					status, body := retiredDo(t, server, step.method, step.path, token, step.body)
					if status != http.StatusNotFound || body != `{"detail":"Not Found"}` {
						t.Errorf("license %v, %s, %s: %s %s %s = %d %s, want 404 {\"detail\":\"Not Found\"}", license, switchName, callerName,
							step.method, step.path, step.body, status, body)
					}
				}
			}
		}
	}
	if after := snapshot(); after != before {
		t.Errorf("sso_providers changed:\nbefore %s\nafter  %s", before, after)
	}
	if calls := st.providers.take(); len(calls) != 0 {
		t.Errorf("auth-service provider client was called %d times", len(calls))
	}
}

// An OAuth provider row that already exists is not changed by the retired routes, and its sign-in through go-api with the
// OIDC forward off answers 503 config_missing naming AUTH_PROXY_OIDC with no provider write: go-api has no own OAuth
// sign-in; auth-service serves it (CHAOS-14131).
func TestRetiredOAuthProviderRoutesLeaveAnExistingRowAndSignInIsForwardOnly(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	st := startStack(t, ctx)
	orgID := seedOrg(t, ctx, st.pool, "enterprise")
	gitlab := startFakeGitLab(t)
	providerID := seedProvider(t, ctx, st.pool, orgID, providerOpts{
		protocol: "oauth_gitlab", status: "active", config: gitlabConfig(gitlab.server.URL), autoProvision: true,
		clientSecretEncryptedFallback: "gitlab-secret",
	})

	for _, step := range []struct{ method, path, body string }{
		{http.MethodPost, "/api/v1/auth/oauth/providers", `{"name":"g","provider_type":"gitlab","oauth_config":{"client_id":"i"}}`},
		{http.MethodPatch, "/api/v1/auth/oauth/providers/" + providerID.String(), `{"name":"renamed"}`},
	} {
		request, err := http.NewRequest(step.method, st.server.URL+step.path, strings.NewReader(step.body))
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Content-Type", "application/json")
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		_ = response.Body.Close()
		if response.StatusCode != http.StatusNotFound {
			t.Errorf("%s %s = %d, want 404 (retired)", step.method, step.path, response.StatusCode)
		}
	}

	for _, verb := range []string{"authorize", "callback"} {
		status, body := postJSON(t, st.server, "/api/v1/auth/oauth/"+providerID.String()+"/"+verb, map[string]any{"code": "c", "state": "s"})
		detail, _ := body["detail"].(map[string]any)
		if status != http.StatusServiceUnavailable || detail["reason_code"] != "config_missing" || detail["message"] != "AUTH_PROXY_OIDC is not set" {
			t.Errorf("%s through the existing row: %d %v, want 503 config_missing AUTH_PROXY_OIDC", verb, status, body)
		}
	}
	var stamped int
	if err := st.pool.QueryRow(ctx, `SELECT count(*) FROM sso_providers WHERE id = $1 AND (last_login_at IS NOT NULL OR last_error IS NOT NULL)`,
		providerID).Scan(&stamped); err != nil {
		t.Fatal(err)
	}
	if stamped != 0 {
		t.Error("go-api recorded a sign-in or a failure on the provider row")
	}
	var name string
	if err := st.pool.QueryRow(ctx, `SELECT name FROM sso_providers WHERE id = $1`, providerID).Scan(&name); err != nil {
		t.Fatal(err)
	}
	if name == "renamed" {
		t.Error("the retired PATCH route renamed the provider row")
	}
}
