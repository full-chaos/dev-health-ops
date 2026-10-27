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

const (
	seededFormat    = "urn:oasis:names:tc:SAML:2.0:nameid-format:persistent"
	seededQualifier = "seeded-qualifier"
)

func seedParts(t *testing.T, ctx context.Context, st stack, email string) {
	t.Helper()
	if _, err := st.pool.Exec(ctx, `UPDATE users SET saml_nameid_format = $2, saml_sp_name_qualifier = $3 WHERE email = $1`,
		email, seededFormat, seededQualifier); err != nil {
		t.Fatalf("seed the NameID parts: %v", err)
	}
}

func requireParts(t *testing.T, ctx context.Context, st stack, email, step string) {
	t.Helper()
	got := readStoredParts(t, ctx, st, email)
	if got.format == nil || *got.format != seededFormat || got.spnq == nil || *got.spnq != seededQualifier {
		t.Fatalf("%s: the stored NameID parts changed: %+v", step, got)
	}
}

// An OIDC or OAuth sign-in leaves the stored SAML NameID parts as they are.
func TestNonSAMLSignInKeepsTheStoredNameIDParts(t *testing.T) {
	t.Run("oidc", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		st := startStack(t, ctx)
		idp := startFakeIdP(t)
		orgID := seedOrg(t, ctx, st.pool, "enterprise")
		config := fmt.Sprintf(`{"client_id":"test-client","issuer":%q,"token_endpoint":%q,"jwks_uri":%q,"userinfo_endpoint":%q,"scopes":["openid","email"]}`,
			idp.server.URL, idp.server.URL+"/token", idp.server.URL+"/jwks", idp.server.URL+"/userinfo")
		providerID := seedProvider(t, ctx, st.pool, orgID, providerOpts{
			protocol: "oidc", status: "active", config: config, autoProvision: false,
			clientSecretEncryptedFallback: "test-client-secret",
		})
		const email = "parts.oidc@allowed.example"
		seedMember(t, ctx, st.pool, orgID, email, "oidc", "idp-subject-parts")
		seedParts(t, ctx, st, email)

		client := newCookieClient(t)
		status, authResp := postJSONWithClient(t, client, st.server, "/api/v1/auth/oidc/"+providerID.String()+"/authorize", map[string]any{})
		if status != http.StatusOK {
			t.Fatalf("authorize: %d %v", status, authResp)
		}
		parsed, err := url.Parse(authResp["authorization_url"].(string))
		if err != nil {
			t.Fatal(err)
		}
		idp.set(parsed.Query().Get("nonce"), email, "Person", "idp-subject-parts")
		status, body := postJSONWithClient(t, client, st.server, "/api/v1/auth/oidc/"+providerID.String()+"/callback",
			map[string]any{"code": "test-auth-code", "state": authResp["state"]})
		if status != http.StatusOK || body["access_token"] == nil {
			t.Fatalf("sign-in: %d %v", status, body)
		}
		requireParts(t, ctx, st, email, "oidc sign-in")
	})

	t.Run("oauth", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		st := startStack(t, ctx)
		orgID := seedOrg(t, ctx, st.pool, "enterprise")
		gitlab := startFakeGitLab(t)
		gitlab.email = "parts.gitlab@allowed.example"
		seedMember(t, ctx, st.pool, orgID, gitlab.email, "gitlab", gitlab.userID)
		providerID := seedProvider(t, ctx, st.pool, orgID, providerOpts{
			protocol: "oauth_gitlab", status: "active", config: gitlabConfig(gitlab.server.URL), autoProvision: false,
			clientSecretEncryptedFallback: "gitlab-secret",
		})
		seedParts(t, ctx, st, gitlab.email)

		client := newCookieClient(t)
		status, authResp := postJSONWithClient(t, client, st.server, "/api/v1/auth/oauth/"+providerID.String()+"/authorize", map[string]any{})
		if status != http.StatusOK {
			t.Fatalf("authorize: %d %v", status, authResp)
		}
		status, body := postJSONWithClient(t, client, st.server, "/api/v1/auth/oauth/"+providerID.String()+"/callback",
			map[string]any{"code": "c", "state": authResp["state"]})
		if status != http.StatusOK || body["access_token"] == nil {
			t.Fatalf("sign-in: %d %v", status, body)
		}
		requireParts(t, ctx, st, gitlab.email, "oauth sign-in")
	})
}

// While auth-service owns users, a SAML sign-in of an existing account writes nothing to public.users, the NameID
// parts included, even when the assertion carries other parts.
func TestUsersOwnedSAMLSignInLeavesTheStoredNameIDPartsAlone(t *testing.T) {
	t.Setenv("AUTH_USERS_OWNED", "true")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	st := startStack(t, ctx)
	idp := newSAMLIdP(t)
	providerID := seedSAMLOrgProviderAuto(t, ctx, st, idp, nameIDSP, false)
	const email = "parts.saml.owned@allowed.example"
	seedBound(t, ctx, st.pool, providerID, "saml", email, "name-owned")
	seedParts(t, ctx, st, email)
	before := usersRow(t, ctx, st, email)

	status := samlSignInWithNameID(t, st, idp, providerID, email, "name-owned",
		"urn:oasis:names:tc:SAML:1.1:nameid-format:unspecified", "other-qualifier")
	if status != http.StatusOK {
		t.Fatalf("sign-in with the switch on: %d", status)
	}
	if after := usersRow(t, ctx, st, email); after != before {
		t.Fatalf("public.users changed by the sign-in:\nbefore %s\nafter  %s", before, after)
	}
	requireParts(t, ctx, st, email, "owned saml sign-in")
}
