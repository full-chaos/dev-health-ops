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

func wantJoinRefused(t *testing.T, ctx context.Context, st stack, email string, status int, body map[string]any) {
	t.Helper()
	detail, _ := body["detail"].(map[string]any)
	if status != http.StatusForbidden || detail["reason"] != "membership_required" {
		t.Fatalf("sign-in = %d %v, want 403 membership_required", status, body)
	}
	if n := countRows(t, ctx, st, `SELECT count(*) FROM memberships m JOIN users u ON u.id = m.user_id WHERE u.email = $1`, email); n != 0 {
		t.Fatalf("%d membership rows, want none", n)
	}
}

// While auth-service owns users, go-api joins nobody: a known account with no
// membership is refused as a row with auto-provisioning off refuses it.
func TestSSOJoinRefusedWhileUsersOwned(t *testing.T) {
	t.Setenv("AUTH_USERS_OWNED", "true")

	t.Run("saml", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		st := startStack(t, ctx)
		idp := newSAMLIdP(t)
		const sp, email = "https://sp.test/saml/metadata", "join.saml@allowed.example"
		providerID := seedSAMLOrgProvider(t, ctx, st, idp, sp)
		seedBoundAccount(t, ctx, st.pool, providerID, email, "saml", "join-saml-subject")
		status, body := samlSignIn(t, st, idp, providerID, sp, email, "join-saml-subject")
		wantJoinRefused(t, ctx, st, email, status, body)
	})

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
		const email = "join.oidc@allowed.example"
		seedBoundAccount(t, ctx, st.pool, providerID, email, "oidc", "join-oidc-subject")
		client := newCookieClient(t)
		status, authResp := postJSONWithClient(t, client, st.server, "/api/v1/auth/oidc/"+providerID.String()+"/authorize", map[string]any{})
		if status != http.StatusOK {
			t.Fatalf("authorize: %d %v", status, authResp)
		}
		parsed, err := url.Parse(authResp["authorization_url"].(string))
		if err != nil {
			t.Fatal(err)
		}
		idp.set(parsed.Query().Get("nonce"), email, "Join Oidc", "join-oidc-subject")
		status, body := postJSONWithClient(t, client, st.server, "/api/v1/auth/oidc/"+providerID.String()+"/callback",
			map[string]any{"code": "test-auth-code", "state": authResp["state"]})
		wantJoinRefused(t, ctx, st, email, status, body)
	})

	t.Run("oauth", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		st := startStack(t, ctx)
		orgID := seedOrg(t, ctx, st.pool, "enterprise")
		gitlab := startFakeGitLab(t)
		gitlab.email = "join.gitlab@allowed.example"
		seedAccount(t, ctx, st.pool, gitlab.email, "gitlab", gitlab.userID)
		providerID := seedProvider(t, ctx, st.pool, orgID, providerOpts{
			protocol: "oauth_gitlab", status: "active", config: gitlabConfig(gitlab.server.URL), autoProvision: true,
			clientSecretEncryptedFallback: "gitlab-secret",
		})
		client := newCookieClient(t)
		status, authResp := postJSONWithClient(t, client, st.server, "/api/v1/auth/oauth/"+providerID.String()+"/authorize", map[string]any{})
		if status != http.StatusOK {
			t.Fatalf("authorize: %d %v", status, authResp)
		}
		status, body := postJSONWithClient(t, client, st.server, "/api/v1/auth/oauth/"+providerID.String()+"/callback",
			map[string]any{"code": "c", "state": authResp["state"]})
		wantJoinRefused(t, ctx, st, gitlab.email, status, body)
	})
}

// With the switch off go-api's join is as it was.
func TestSSOJoinUnchangedSwitchOff(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	st := startStack(t, ctx)
	idp := newSAMLIdP(t)
	const sp, email = "https://sp.test/saml/metadata", "join.off@allowed.example"
	providerID := seedSAMLOrgProvider(t, ctx, st, idp, sp)
	seedBoundAccount(t, ctx, st.pool, providerID, email, "saml", "join-off-subject")
	status, body := samlSignIn(t, st, idp, providerID, sp, email, "join-off-subject")
	assertProvisionedMember(t, ctx, st, orgOf(t, ctx, st.pool, providerID), email, "saml", "join-off-subject", status, body)
}
