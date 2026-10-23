//go:build integration

package sso_test

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// An OIDC or SAML account is identified by its provider and its subject: the same
// subject at another provider is another identity.

type scopeRig struct {
	protocol string
	// seed registers a provider of this protocol in a new organization.
	seed func(t *testing.T, ctx context.Context, st stack, auto bool) uuid.UUID
	// signIn runs one sign-in at the provider and returns the HTTP answer.
	signIn func(t *testing.T, st stack, providerID uuid.UUID, email, subject string) (int, map[string]any)
}

func newSAMLScopeRig() scopeRig {
	const sp = "https://sp.test/saml/metadata"
	idps := map[uuid.UUID]*samlIdP{}
	return scopeRig{
		protocol: "saml",
		seed: func(t *testing.T, ctx context.Context, st stack, auto bool) uuid.UUID {
			idp := newSAMLIdP(t)
			id := seedSAMLOrgProviderAuto(t, ctx, st, idp, sp, auto)
			idps[id] = idp
			return id
		},
		signIn: func(t *testing.T, st stack, providerID uuid.UUID, email, subject string) (int, map[string]any) {
			return samlSignIn(t, st, idps[providerID], providerID, sp, email, subject)
		},
	}
}

func newOIDCScopeRig() scopeRig {
	idps := map[uuid.UUID]*fakeIdP{}
	return scopeRig{
		protocol: "oidc",
		seed: func(t *testing.T, ctx context.Context, st stack, auto bool) uuid.UUID {
			idp := startFakeIdP(t)
			orgID := seedOrg(t, ctx, st.pool, "enterprise")
			config := fmt.Sprintf(`{"client_id":"test-client","issuer":%q,"token_endpoint":%q,"jwks_uri":%q,"userinfo_endpoint":%q,"scopes":["openid","email"]}`,
				idp.server.URL, idp.server.URL+"/token", idp.server.URL+"/jwks", idp.server.URL+"/userinfo")
			id := seedProvider(t, ctx, st.pool, orgID, providerOpts{
				protocol: "oidc", status: "active", config: config, autoProvision: auto,
				clientSecretEncryptedFallback: "test-client-secret",
			})
			idps[id] = idp
			return id
		},
		signIn: func(t *testing.T, st stack, providerID uuid.UUID, email, subject string) (int, map[string]any) {
			client := newCookieClient(t)
			status, authResp := postJSONWithClient(t, client, st.server, "/api/v1/auth/oidc/"+providerID.String()+"/authorize", map[string]any{})
			if status != http.StatusOK {
				t.Fatalf("authorize: %d %v", status, authResp)
			}
			parsed, err := url.Parse(authResp["authorization_url"].(string))
			if err != nil {
				t.Fatal(err)
			}
			idps[providerID].set(parsed.Query().Get("nonce"), email, "Person", subject)
			return postJSONWithClient(t, client, st.server, "/api/v1/auth/oidc/"+providerID.String()+"/callback",
				map[string]any{"code": "test-auth-code", "state": authResp["state"]})
		},
	}
}

func scopeRigs() []scopeRig { return []scopeRig{newOIDCScopeRig(), newSAMLScopeRig()} }

// seedBound is an account of a provider: its identity names the provider, and it is a
// member of the provider's organization.
func seedBound(t *testing.T, ctx context.Context, pool *pgxpool.Pool, providerID uuid.UUID, protocol, email, subject string) uuid.UUID {
	t.Helper()
	id := uuid.New()
	if _, err := pool.Exec(ctx, `INSERT INTO users (id, email, auth_provider, auth_provider_id, sso_provider_id, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, true, true, false, 0, now(), now())`, id, email, protocol, subject, providerID); err != nil {
		t.Fatalf("seed bound account: %v", err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO memberships (id, org_id, user_id, role, joined_at, created_at, updated_at)
SELECT $1, org_id, $2, 'member', now(), now(), now() FROM sso_providers WHERE id = $3`, uuid.New(), id, providerID); err != nil {
		t.Fatalf("seed membership: %v", err)
	}
	return id
}

func untouched(t *testing.T, ctx context.Context, st stack, id uuid.UUID) {
	t.Helper()
	var touched int
	if err := st.pool.QueryRow(ctx, `SELECT count(*) FROM users WHERE id = $1 AND last_login_at IS NULL AND full_name IS NULL`, id).Scan(&touched); err != nil || touched != 1 {
		t.Fatalf("the account %s was changed by another provider's sign-in (rows untouched=%d, err %v)", id, touched, err)
	}
}

func TestSSOIdentityIsScopedToItsProvider(t *testing.T) {
	for _, rig := range scopeRigs() {
		t.Run(rig.protocol, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()
			st := startStack(t, ctx)
			providerA := rig.seed(t, ctx, st, true)
			providerB := rig.seed(t, ctx, st, false)
			const subject = "subject-shared"
			const owner = "owner.person@allowed.example"
			ownerID := seedBound(t, ctx, st.pool, providerA, rig.protocol, owner, subject)
			before := snapshotRows(t, ctx, st)

			status, body := rig.signIn(t, st, providerB, "other.person@allowed.example", subject)
			if status < 400 || body["access_token"] != nil {
				t.Fatalf("another provider's assertion of the subject was accepted: %d %v", status, body)
			}
			untouched(t, ctx, st, ownerID)
			assertNoRowsWritten(t, ctx, st, before)

			status, body = rig.signIn(t, st, providerB, owner, subject)
			if status != http.StatusConflict {
				t.Fatalf("subject and email of an account of another provider: status=%d body=%v, want 409", status, body)
			}
			if detail, _ := body["detail"].(map[string]any); detail["reason"] != "identity_conflict" || body["access_token"] != nil {
				t.Fatalf("body=%v, want reason identity_conflict and no token", body)
			}
			untouched(t, ctx, st, ownerID)
			assertNoRowsWritten(t, ctx, st, before)
		})
	}
}

func TestSSOSameSubjectAtAnotherProviderIsAnotherAccount(t *testing.T) {
	for _, rig := range scopeRigs() {
		t.Run(rig.protocol, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()
			st := startStack(t, ctx)
			providerA := rig.seed(t, ctx, st, true)
			providerB := rig.seed(t, ctx, st, true)
			const subject = "subject-shared"
			ownerID := seedBound(t, ctx, st.pool, providerA, rig.protocol, "first.person@allowed.example", subject)

			status, body := rig.signIn(t, st, providerB, "second.person@allowed.example", subject)
			if status != http.StatusOK || body["access_token"] == nil {
				t.Fatalf("a new account of provider B: %d %v", status, body)
			}
			untouched(t, ctx, st, ownerID)
			var other uuid.UUID
			var bound uuid.UUID
			if err := st.pool.QueryRow(ctx, `SELECT id, sso_provider_id FROM users WHERE email = 'second.person@allowed.example'`).Scan(&other, &bound); err != nil {
				t.Fatal(err)
			}
			if other == ownerID || bound != providerB {
				t.Fatalf("account %s bound to %s, want a new account bound to %s", other, bound, providerB)
			}
		})
	}
}

func TestSSOSameProviderStillSignsIn(t *testing.T) {
	for _, rig := range scopeRigs() {
		t.Run(rig.protocol, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()
			st := startStack(t, ctx)
			providerA := rig.seed(t, ctx, st, false)
			const email = "member.person@allowed.example"
			id := seedBound(t, ctx, st.pool, providerA, rig.protocol, email, "subject-own")

			status, body := rig.signIn(t, st, providerA, email, "subject-own")
			if status != http.StatusOK || body["access_token"] == nil {
				t.Fatalf("sign-in at the account's own provider: %d %v", status, body)
			}
			var stamped int
			if err := st.pool.QueryRow(ctx, `SELECT count(*) FROM users WHERE id = $1 AND last_login_at IS NOT NULL`, id).Scan(&stamped); err != nil || stamped != 1 {
				t.Fatalf("last_login_at not stamped: %d (%v)", stamped, err)
			}
		})
	}
}

func TestSSOUnknownSubjectWithATakenEmailAnswers409(t *testing.T) {
	for _, rig := range scopeRigs() {
		t.Run(rig.protocol, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()
			st := startStack(t, ctx)
			providerA := rig.seed(t, ctx, st, true)
			const email = "taken.person@allowed.example"
			seedLocalUser(t, ctx, st.pool, email)

			status, body := rig.signIn(t, st, providerA, email, "subject-unknown")
			if status != http.StatusConflict {
				t.Fatalf("status=%d body=%v, want 409", status, body)
			}
			assertIdentityConflict(t, ctx, st, providerA, body)
		})
	}
}

func TestSSOJITStoresTheProvider(t *testing.T) {
	for _, rig := range scopeRigs() {
		t.Run(rig.protocol, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()
			st := startStack(t, ctx)
			providerA := rig.seed(t, ctx, st, true)
			const email = "new.person@allowed.example"

			status, body := rig.signIn(t, st, providerA, email, "subject-new")
			if status != http.StatusOK || body["access_token"] == nil {
				t.Fatalf("auto-provision: %d %v", status, body)
			}
			var bound *uuid.UUID
			if err := st.pool.QueryRow(ctx, `SELECT sso_provider_id FROM users WHERE email = $1 AND auth_provider = $2 AND auth_provider_id = 'subject-new'`,
				email, rig.protocol).Scan(&bound); err != nil || bound == nil || *bound != providerA {
				t.Fatalf("sso_provider_id = %v (err %v), want %s", bound, err, providerA)
			}
			if status, body = rig.signIn(t, st, providerA, email, "subject-new"); status != http.StatusOK {
				t.Fatalf("second sign-in: %d %v", status, body)
			}
		})
	}
}

// A row that holds the subject but no provider (it predates the column and could not be
// bound to exactly one provider) is no identity of any provider.
func TestSSOUnboundLegacyRowIsRefused(t *testing.T) {
	for _, rig := range scopeRigs() {
		t.Run(rig.protocol, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()
			st := startStack(t, ctx)
			providerA := rig.seed(t, ctx, st, true)
			const email = "legacy.person@allowed.example"
			seedMember(t, ctx, st.pool, orgOf(t, ctx, st.pool, providerA), email, rig.protocol, "subject-legacy")
			var id uuid.UUID
			if err := st.pool.QueryRow(ctx, `SELECT id FROM users WHERE email = $1`, email).Scan(&id); err != nil {
				t.Fatal(err)
			}
			before := snapshotRows(t, ctx, st)

			status, body := rig.signIn(t, st, providerA, email, "subject-legacy")
			if status != http.StatusConflict {
				t.Fatalf("status=%d body=%v, want 409", status, body)
			}
			assertIdentityConflict(t, ctx, st, providerA, body)
			untouched(t, ctx, st, id)
			assertNoRowsWritten(t, ctx, st, before)
		})
	}
}

// OAuth providers (github, gitlab, google) are the public identity providers: their subject
// is global per type, so the key stays (type, subject) and an account without a provider
// column value still signs in.
func TestOAuthIdentityStaysKeyedByTypeAndSubject(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	st := startStack(t, ctx)
	orgID := seedOrg(t, ctx, st.pool, "enterprise")
	gitlab := startFakeGitLab(t)
	gitlab.email = "oauth.member@allowed.example"
	gitlab.userID = "gl-global"
	providerID := seedProvider(t, ctx, st.pool, orgID, providerOpts{
		protocol: "oauth_gitlab", status: "active", config: gitlabConfig(gitlab.server.URL), autoProvision: false,
		clientSecretEncryptedFallback: "gitlab-secret",
	})
	seedMember(t, ctx, st.pool, orgID, gitlab.email, "gitlab", gitlab.userID)

	client := newCookieClient(t)
	status, authResp := postJSONWithClient(t, client, st.server, "/api/v1/auth/oauth/"+providerID.String()+"/authorize", map[string]any{})
	if status != http.StatusOK {
		t.Fatalf("authorize: %d %v", status, authResp)
	}
	status, body := postJSONWithClient(t, client, st.server, "/api/v1/auth/oauth/"+providerID.String()+"/callback",
		map[string]any{"code": "c", "state": authResp["state"]})
	if status != http.StatusOK || body["access_token"] == nil {
		t.Fatalf("callback: %d %v", status, body)
	}
}
