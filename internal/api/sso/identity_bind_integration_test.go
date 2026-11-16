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

type identityRow struct {
	provider, subject *string
	fullName          *string
}

func readIdentity(t *testing.T, ctx context.Context, pool *pgxpool.Pool, email string) (identityRow, int) {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM users WHERE email = $1`, email).Scan(&n); err != nil {
		t.Fatal(err)
	}
	var out identityRow
	if n == 1 {
		if err := pool.QueryRow(ctx, `SELECT auth_provider, auth_provider_id, full_name FROM users WHERE email = $1`, email).
			Scan(&out.provider, &out.subject, &out.fullName); err != nil {
			t.Fatal(err)
		}
	}
	return out, n
}

func seedLocalUser(t *testing.T, ctx context.Context, pool *pgxpool.Pool, email string) {
	t.Helper()
	if _, err := pool.Exec(ctx, `INSERT INTO users (id, email, auth_provider, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, 'local', true, true, false, 0, now(), now())`, uuid.New(), email); err != nil {
		t.Fatalf("seed user: %v", err)
	}
}

func assertIdentityConflict(t *testing.T, ctx context.Context, st stack, providerID uuid.UUID, body map[string]any) {
	t.Helper()
	detail, _ := body["detail"].(map[string]any)
	if detail["message"] != "This sign-in could not be completed. Contact your administrator if this continues." || len(detail) != 1 || body["access_token"] != nil {
		t.Fatalf("body=%v, want the one generic message, no reason and no token", body)
	}
	var providerStatus string
	var audits int
	if err := st.pool.QueryRow(ctx, `SELECT status FROM sso_providers WHERE id = $1`, providerID).Scan(&providerStatus); err != nil {
		t.Fatal(err)
	}
	if err := st.pool.QueryRow(ctx, `SELECT count(*) FROM audit_logs WHERE resource_id = $1 AND action = 'sso_login' AND status = 'failure' AND error_message = 'identity conflict'`,
		providerID.String()).Scan(&audits); err != nil {
		t.Fatal(err)
	}
	if providerStatus != "active" || audits != 1 {
		t.Fatalf("provider status=%q failure audits=%d, want active and 1 audit row naming the identity conflict", providerStatus, audits)
	}
}

func orgOf(t *testing.T, ctx context.Context, pool *pgxpool.Pool, providerID uuid.UUID) uuid.UUID {
	t.Helper()
	var id uuid.UUID
	if err := pool.QueryRow(ctx, `SELECT org_id FROM sso_providers WHERE id = $1`, providerID).Scan(&id); err != nil {
		t.Fatal(err)
	}
	return id
}

func seedAccount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, email, authProvider, subject string) {
	t.Helper()
	if _, err := pool.Exec(ctx, `INSERT INTO users (id, email, auth_provider, auth_provider_id, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, $3, $4, true, true, false, 0, now(), now())`, uuid.New(), email, authProvider, subject); err != nil {
		t.Fatalf("seed account: %v", err)
	}
	if authProvider == "gitlab" {
		bindSelfHostedGitLabAccounts(t, ctx, pool)
	}
}

func assertMembershipRequired(t *testing.T, ctx context.Context, st stack, providerID uuid.UUID, status int, body map[string]any) {
	t.Helper()
	detail, _ := body["detail"].(map[string]any)
	if status != http.StatusForbidden || detail["reason"] != "membership_required" || body["access_token"] != nil {
		t.Fatalf("status=%d body=%v, want 403 membership_required and no token", status, body)
	}
	var sessions, audits int
	var providerStatus string
	if err := st.pool.QueryRow(ctx, `SELECT count(*) FROM refresh_tokens`).Scan(&sessions); err != nil {
		t.Fatal(err)
	}
	if err := st.pool.QueryRow(ctx, `SELECT status FROM sso_providers WHERE id = $1`, providerID).Scan(&providerStatus); err != nil {
		t.Fatal(err)
	}
	if err := st.pool.QueryRow(ctx, `SELECT count(*) FROM audit_logs WHERE resource_id = $1 AND action = 'sso_login' AND status = 'failure'`,
		providerID.String()).Scan(&audits); err != nil {
		t.Fatal(err)
	}
	var touched int
	if err := st.pool.QueryRow(ctx, `SELECT count(*) FROM users WHERE last_login_at IS NOT NULL OR full_name IS NOT NULL`).Scan(&touched); err != nil {
		t.Fatal(err)
	}
	if touched != 0 {
		t.Fatalf("%d users rows were updated by a refused sign-in", touched)
	}
	if sessions != 0 || providerStatus != "active" || audits != 1 {
		t.Fatalf("refresh rows=%d provider status=%q failure audits=%d, want 0, active, 1", sessions, providerStatus, audits)
	}
}

// seedBoundAccount is an OIDC account that holds the identity of one provider and no membership.
func seedBoundAccount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, providerID uuid.UUID, email, authProvider, subject string) {
	t.Helper()
	seedAccount(t, ctx, pool, email, authProvider, subject)
	if _, err := pool.Exec(ctx, `UPDATE users SET sso_provider_id = $2 WHERE email = $1`, email, providerID); err != nil {
		t.Fatalf("bind account: %v", err)
	}
}

// seedMember is a member of the organization; an OIDC account holds the identity of the
// organization's provider of that protocol.
func seedMember(t *testing.T, ctx context.Context, pool *pgxpool.Pool, orgID uuid.UUID, email, authProvider, subject string) {
	t.Helper()
	seedAccount(t, ctx, pool, email, authProvider, subject)
	if _, err := pool.Exec(ctx, `UPDATE users SET sso_provider_id = (SELECT id FROM sso_providers WHERE org_id = $2 AND protocol = $3 ORDER BY id LIMIT 1) WHERE email = $1 AND $3 = 'oidc'`,
		email, orgID, authProvider); err != nil {
		t.Fatalf("bind member: %v", err)
	}
	var userID uuid.UUID
	if err := pool.QueryRow(ctx, `SELECT id FROM users WHERE email = $1`, email).Scan(&userID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO memberships (id, org_id, user_id, role, joined_at, created_at, updated_at)
VALUES ($1, $2, $3, 'member', now(), now(), now())`, uuid.New(), orgID, userID); err != nil {
		t.Fatalf("seed membership: %v", err)
	}
}

func TestOAuthSignInKeepsAnExistingAccountsIdentity(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	st := startStack(t, ctx)
	orgID := seedOrg(t, ctx, st.pool, "enterprise")
	gitlab := startFakeGitLab(t)
	gitlab.email = "existing.gitlab@allowed.example"
	gitlab.userID = "gl-1"
	providerID := seedProvider(t, ctx, st.pool, orgID, providerOpts{
		protocol: "oauth_gitlab", status: "active", config: gitlabConfig(gitlab.server.URL), autoProvision: true,
		clientSecretEncryptedFallback: "gitlab-secret",
	})
	seedLocalUser(t, ctx, st.pool, gitlab.email)

	client := newCookieClient(t)
	status, authResp := postJSONWithClient(t, client, st.server, "/api/v1/auth/oauth/"+providerID.String()+"/authorize", map[string]any{})
	if status != http.StatusOK {
		t.Fatalf("authorize: %d %v", status, authResp)
	}
	status, body := postJSONWithClient(t, client, st.server, "/api/v1/auth/oauth/"+providerID.String()+"/callback",
		map[string]any{"code": "c", "state": authResp["state"]})
	if status != http.StatusConflict {
		t.Fatalf("callback: status=%d body=%v, want 409", status, body)
	}
	assertIdentityConflict(t, ctx, st, providerID, body)
	got, n := readIdentity(t, ctx, st.pool, gitlab.email)
	if n != 1 || got.provider == nil || *got.provider != "local" || got.subject != nil {
		t.Fatalf("account changed: rows=%d %+v", n, got)
	}
}

func TestOIDCSignInWithoutMembershipAnswers403(t *testing.T) {
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
	const email = "oidc.nonmember@allowed.example"
	seedBoundAccount(t, ctx, st.pool, providerID, email, "oidc", "idp-subject-nm")

	client := newCookieClient(t)
	status, authResp := postJSONWithClient(t, client, st.server, "/api/v1/auth/oidc/"+providerID.String()+"/authorize", map[string]any{})
	if status != http.StatusOK {
		t.Fatalf("authorize: %d %v", status, authResp)
	}
	parsed, err := url.Parse(authResp["authorization_url"].(string))
	if err != nil {
		t.Fatal(err)
	}
	idp.set(parsed.Query().Get("nonce"), email, "Non Member", "idp-subject-nm")
	status, body := postJSONWithClient(t, client, st.server, "/api/v1/auth/oidc/"+providerID.String()+"/callback",
		map[string]any{"code": "test-auth-code", "state": authResp["state"]})
	assertMembershipRequired(t, ctx, st, providerID, status, body)
}

func TestOAuthSignInWithoutMembershipAnswers403(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	st := startStack(t, ctx)
	orgID := seedOrg(t, ctx, st.pool, "enterprise")
	gitlab := startFakeGitLab(t)
	gitlab.email = "oauth.nonmember@allowed.example"
	gitlab.userID = "gl-nm"
	providerID := seedProvider(t, ctx, st.pool, orgID, providerOpts{
		protocol: "oauth_gitlab", status: "active", config: gitlabConfig(gitlab.server.URL), autoProvision: false,
		clientSecretEncryptedFallback: "gitlab-secret",
	})
	seedAccount(t, ctx, st.pool, gitlab.email, "gitlab", gitlab.userID)

	client := newCookieClient(t)
	status, authResp := postJSONWithClient(t, client, st.server, "/api/v1/auth/oauth/"+providerID.String()+"/authorize", map[string]any{})
	if status != http.StatusOK {
		t.Fatalf("authorize: %d %v", status, authResp)
	}
	status, body := postJSONWithClient(t, client, st.server, "/api/v1/auth/oauth/"+providerID.String()+"/callback",
		map[string]any{"code": "c", "state": authResp["state"]})
	assertMembershipRequired(t, ctx, st, providerID, status, body)
}

func countRows(t *testing.T, ctx context.Context, st stack, query string, args ...any) int {
	t.Helper()
	var n int
	if err := st.pool.QueryRow(ctx, query, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

type rowCounts struct{ users, memberships, refresh int }

func snapshotRows(t *testing.T, ctx context.Context, st stack) rowCounts {
	t.Helper()
	return rowCounts{
		users:       countRows(t, ctx, st, `SELECT count(*) FROM users`),
		memberships: countRows(t, ctx, st, `SELECT count(*) FROM memberships`),
		refresh:     countRows(t, ctx, st, `SELECT count(*) FROM refresh_tokens`),
	}
}

func assertNoRowsWritten(t *testing.T, ctx context.Context, st stack, before rowCounts) {
	t.Helper()
	if after := snapshotRows(t, ctx, st); after != before {
		t.Fatalf("rows changed: before %+v, after %+v", before, after)
	}
}

func installMembershipInsertFailure(t *testing.T, ctx context.Context, st stack) {
	t.Helper()
	for _, statement := range []string{
		`CREATE FUNCTION fail_membership_insert() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'membership insert refused'; END $$`,
		`CREATE TRIGGER fail_membership_insert BEFORE INSERT ON memberships FOR EACH ROW EXECUTE FUNCTION fail_membership_insert()`,
	} {
		if _, err := st.pool.Exec(ctx, statement); err != nil {
			t.Fatalf("install failure trigger: %v", err)
		}
	}
}

func assertProvisionedMember(t *testing.T, ctx context.Context, st stack, orgID uuid.UUID, email, provider, subject string, status int, body map[string]any) {
	t.Helper()
	if status != http.StatusOK || body["access_token"] == nil || body["org_id"] != orgID.String() {
		t.Fatalf("status=%d body=%v, want 200 with a session in org %s", status, body, orgID)
	}
	if n := countRows(t, ctx, st, `SELECT count(*) FROM users u JOIN memberships m ON m.user_id = u.id AND m.org_id = $4
WHERE u.email = $1 AND u.auth_provider = $2 AND u.auth_provider_id = $3 AND m.role = 'member'`, email, provider, subject, orgID); n != 1 {
		t.Fatalf("account with identity and membership rows = %d, want 1", n)
	}
	if n := countRows(t, ctx, st, `SELECT count(*) FROM refresh_tokens WHERE org_id = $1`, orgID); n != 0 {
		t.Fatalf("refresh rows in the org = %d, want 0: auth-service owns sessions", n)
	}
}

func TestOIDCAutoProvisionCreatesAccountIdentityMembershipAndSession(t *testing.T) {
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
	client := newCookieClient(t)
	status, authResp := postJSONWithClient(t, client, st.server, "/api/v1/auth/oidc/"+providerID.String()+"/authorize", map[string]any{})
	if status != http.StatusOK {
		t.Fatalf("authorize: %d %v", status, authResp)
	}
	parsed, err := url.Parse(authResp["authorization_url"].(string))
	if err != nil {
		t.Fatal(err)
	}
	const email = "oidc.jit@allowed.example"
	idp.set(parsed.Query().Get("nonce"), email, "Oidc Jit", "oidc-jit-subject")
	status, body := postJSONWithClient(t, client, st.server, "/api/v1/auth/oidc/"+providerID.String()+"/callback",
		map[string]any{"code": "test-auth-code", "state": authResp["state"]})
	assertProvisionedMember(t, ctx, st, orgID, email, "oidc", "oidc-jit-subject", status, body)
}

func TestOIDCAutoProvisionOffRefusesANewAccountWithoutWrites(t *testing.T) {
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
	client := newCookieClient(t)
	status, authResp := postJSONWithClient(t, client, st.server, "/api/v1/auth/oidc/"+providerID.String()+"/authorize", map[string]any{})
	if status != http.StatusOK {
		t.Fatalf("authorize: %d %v", status, authResp)
	}
	parsed, err := url.Parse(authResp["authorization_url"].(string))
	if err != nil {
		t.Fatal(err)
	}
	before := snapshotRows(t, ctx, st)
	idp.set(parsed.Query().Get("nonce"), "oidc.off@allowed.example", "Oidc Off", "oidc-off-subject")
	status, body := postJSONWithClient(t, client, st.server, "/api/v1/auth/oidc/"+providerID.String()+"/callback",
		map[string]any{"code": "test-auth-code", "state": authResp["state"]})
	if status < 400 || body["access_token"] != nil {
		t.Fatalf("status=%d body=%v, want a refusal", status, body)
	}
	assertNoRowsWritten(t, ctx, st, before)
}

func oauthJITStack(t *testing.T, ctx context.Context, auto bool, email, subject string) (stack, *fakeGitLab, uuid.UUID, uuid.UUID) {
	t.Helper()
	st := startStack(t, ctx)
	orgID := seedOrg(t, ctx, st.pool, "enterprise")
	gitlab := startFakeGitLab(t)
	gitlab.email = email
	gitlab.userID = subject
	providerID := seedProvider(t, ctx, st.pool, orgID, providerOpts{
		protocol: "oauth_gitlab", status: "active", config: gitlabConfig(gitlab.server.URL), autoProvision: auto,
		clientSecretEncryptedFallback: "gitlab-secret",
	})
	return st, gitlab, orgID, providerID
}

func oauthSignIn(t *testing.T, st stack, providerID uuid.UUID) (int, map[string]any) {
	t.Helper()
	client := newCookieClient(t)
	status, authResp := postJSONWithClient(t, client, st.server, "/api/v1/auth/oauth/"+providerID.String()+"/authorize", map[string]any{})
	if status != http.StatusOK {
		t.Fatalf("authorize: %d %v", status, authResp)
	}
	return postJSONWithClient(t, client, st.server, "/api/v1/auth/oauth/"+providerID.String()+"/callback",
		map[string]any{"code": "c", "state": authResp["state"]})
}

func TestOAuthAutoProvisionCreatesAccountIdentityMembershipAndSession(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	st, _, orgID, providerID := oauthJITStack(t, ctx, true, "oauth.jit@allowed.example", "gl-jit")
	status, body := oauthSignIn(t, st, providerID)
	assertProvisionedMember(t, ctx, st, orgID, "oauth.jit@allowed.example", "gitlab", "gl-jit", status, body)
}

func TestOAuthAutoProvisionOffRefusesANewAccountWithoutWrites(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	st, _, _, providerID := oauthJITStack(t, ctx, false, "oauth.off@allowed.example", "gl-off")
	before := snapshotRows(t, ctx, st)
	status, body := oauthSignIn(t, st, providerID)
	if status < 400 || body["access_token"] != nil {
		t.Fatalf("status=%d body=%v, want a refusal", status, body)
	}
	assertNoRowsWritten(t, ctx, st, before)
}

func TestOAuthAutoProvisionFailureLeavesNoRows(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	st, _, _, providerID := oauthJITStack(t, ctx, true, "oauth.fail@allowed.example", "gl-fail")
	installMembershipInsertFailure(t, ctx, st)
	before := snapshotRows(t, ctx, st)
	status, body := oauthSignIn(t, st, providerID)
	if status < 400 || body["access_token"] != nil {
		t.Fatalf("status=%d body=%v, want a failure", status, body)
	}
	assertNoRowsWritten(t, ctx, st, before)
}

func TestOAuthAutoProvisionJoinsAnAccountWithNoMembership(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	st, _, orgID, providerID := oauthJITStack(t, ctx, true, "oauth.orphan@allowed.example", "gl-orphan")
	seedAccount(t, ctx, st.pool, "oauth.orphan@allowed.example", "gitlab", "gl-orphan")
	status, body := oauthSignIn(t, st, providerID)
	assertProvisionedMember(t, ctx, st, orgID, "oauth.orphan@allowed.example", "gitlab", "gl-orphan", status, body)
}
