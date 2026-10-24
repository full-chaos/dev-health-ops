//go:build integration

package sso_test

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"testing"
	"time"
)

// While auth-service owns users, an SSO or OAuth sign-in of an account that
// already exists still succeeds and writes nothing to public.users: neither
// last_login_at nor the profile columns change.
func TestUsersOwnedSignInOfAnExistingAccountWritesNoUsersRow(t *testing.T) {
	t.Setenv("AUTH_USERS_OWNED", "true")

	t.Run("oidc", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		st := startStack(t, ctx)
		idp := startFakeIdP(t)
		orgID := seedOrg(t, ctx, st.pool, "enterprise")
		config := fmt.Sprintf(`{"client_id":"test-client","issuer":%q,"token_endpoint":%q,"jwks_uri":%q,"userinfo_endpoint":%q,"scopes":["openid","email"]}`,
			idp.server.URL, idp.server.URL+"/token", idp.server.URL+"/jwks", idp.server.URL+"/userinfo")
		providerID := seedProvider(t, ctx, st.pool, orgID, providerOpts{
			protocol: "oidc", status: "active", config: config, autoProvision: true,
			clientSecretEncryptedFallback: "test-client-secret",
		})
		const email = "existing.oidc@allowed.example"
		seedMember(t, ctx, st.pool, orgID, email, "oidc", "idp-subject-owned")
		before := usersRow(t, ctx, st, email)

		client := newCookieClient(t)
		status, authResp := postJSONWithClient(t, client, st.server, "/api/v1/auth/oidc/"+providerID.String()+"/authorize", map[string]any{})
		if status != http.StatusOK {
			t.Fatalf("authorize: %d %v", status, authResp)
		}
		parsed, err := url.Parse(authResp["authorization_url"].(string))
		if err != nil {
			t.Fatal(err)
		}
		idp.set(parsed.Query().Get("nonce"), email, "Changed Name", "idp-subject-owned")
		status, body := postJSONWithClient(t, client, st.server, "/api/v1/auth/oidc/"+providerID.String()+"/callback",
			map[string]any{"code": "test-auth-code", "state": authResp["state"]})
		if status != http.StatusOK || body["access_token"] == nil {
			t.Fatalf("sign-in with the switch on: %d %v", status, body)
		}
		if after := usersRow(t, ctx, st, email); after != before {
			t.Fatalf("public.users changed by the sign-in:\nbefore %s\nafter  %s", before, after)
		}
	})

	t.Run("oauth", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		st := startStack(t, ctx)
		orgID := seedOrg(t, ctx, st.pool, "enterprise")
		gitlab := startFakeGitLab(t)
		gitlab.email = "existing.gitlab@allowed.example"
		gitlab.avatarURL = "https://gitlab.example/new-avatar.png"
		seedMember(t, ctx, st.pool, orgID, gitlab.email, "gitlab", gitlab.userID)
		providerID := seedProvider(t, ctx, st.pool, orgID, providerOpts{
			protocol: "oauth_gitlab", status: "active", config: gitlabConfig(gitlab.server.URL), autoProvision: true,
			clientSecretEncryptedFallback: "gitlab-secret",
		})
		before := usersRow(t, ctx, st, gitlab.email)

		client := newCookieClient(t)
		status, authResp := postJSONWithClient(t, client, st.server, "/api/v1/auth/oauth/"+providerID.String()+"/authorize", map[string]any{})
		if status != http.StatusOK {
			t.Fatalf("authorize: %d %v", status, authResp)
		}
		status, body := postJSONWithClient(t, client, st.server, "/api/v1/auth/oauth/"+providerID.String()+"/callback",
			map[string]any{"code": "c", "state": authResp["state"]})
		if status != http.StatusOK || body["access_token"] == nil {
			t.Fatalf("sign-in with the switch on: %d %v", status, body)
		}
		if after := usersRow(t, ctx, st, gitlab.email); after != before {
			t.Fatalf("public.users changed by the sign-in:\nbefore %s\nafter  %s", before, after)
		}
	})
}

// usersRow is the whole row of one account as text, so any column change shows.
func usersRow(t *testing.T, ctx context.Context, st stack, email string) string {
	t.Helper()
	var row string
	if err := st.pool.QueryRow(ctx, `SELECT to_jsonb(u)::text FROM users u WHERE email = $1`, email).Scan(&row); err != nil {
		t.Fatalf("read the users row: %v", err)
	}
	return row
}
