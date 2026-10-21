//go:build integration

package sso_test

import (
	"context"
	"fmt"
	"net/http"
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
	if detail["reason"] != "identity_conflict" || body["access_token"] != nil {
		t.Fatalf("body=%v, want reason identity_conflict and no token", body)
	}
	var providerStatus string
	var audits int
	if err := st.pool.QueryRow(ctx, `SELECT status FROM sso_providers WHERE id = $1`, providerID).Scan(&providerStatus); err != nil {
		t.Fatal(err)
	}
	if err := st.pool.QueryRow(ctx, `SELECT count(*) FROM audit_logs WHERE resource_id = $1 AND action = 'sso_login' AND status = 'failure'`,
		providerID.String()).Scan(&audits); err != nil {
		t.Fatal(err)
	}
	if providerStatus != "active" || audits != 1 {
		t.Fatalf("provider status=%q failure audits=%d, want active and 1", providerStatus, audits)
	}
}

func seedSAMLOrgProvider(t *testing.T, ctx context.Context, st stack, idp *samlIdP, spEntityID string) uuid.UUID {
	t.Helper()
	orgID := seedOrg(t, ctx, st.pool, "enterprise")
	config := fmt.Sprintf(`{"entity_id":%q,"sso_url":"https://idp.test/sso","certificate":%q,"sp_entity_id":%q,"sp_acs_url":%q}`,
		idp.idp.MetadataURL.String(), idp.certConfigValue(), spEntityID, "https://sp.test/api/v1/auth/saml/{provider_id}/acs")
	return seedProvider(t, ctx, st.pool, orgID, providerOpts{protocol: "saml", status: "active", config: config, autoProvision: true})
}

func samlSignIn(t *testing.T, st stack, idp *samlIdP, providerID uuid.UUID, spEntityID, email, nameID string) (int, map[string]any) {
	t.Helper()
	response := idp.signedSAMLResponse(t, samlResponseOpts{
		spEntityID: spEntityID, acsURL: samlACSOverrideFor(providerID), email: email, fullName: "Person", nameID: nameID,
	})
	return postJSON(t, st.server, "/api/v1/auth/saml/"+providerID.String()+"/acs", map[string]any{"SAMLResponse": response})
}

func TestSAMLSignInKeepsAnExistingAccountsIdentity(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	st := startStack(t, ctx)
	idp := newSAMLIdP(t)
	const sp = "https://sp.test/saml/metadata"
	providerID := seedSAMLOrgProvider(t, ctx, st, idp, sp)
	const email = "existing.person@allowed.example"
	seedLocalUser(t, ctx, st.pool, email)

	status, body := samlSignIn(t, st, idp, providerID, sp, email, "subject-1")
	if status != http.StatusConflict {
		t.Fatalf("status=%d body=%v, want 409", status, body)
	}
	assertIdentityConflict(t, ctx, st, providerID, body)
	got, n := readIdentity(t, ctx, st.pool, email)
	if n != 1 || got.provider == nil || *got.provider != "local" || got.subject != nil || got.fullName != nil {
		t.Fatalf("account changed: rows=%d %+v", n, got)
	}
}

func TestSAMLSignInFromASecondOrgDoesNotRebindAnAccount(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	st := startStack(t, ctx)
	const sp = "https://sp.test/saml/metadata"
	const email = "shared.person@allowed.example"
	idpA, idpB := newSAMLIdP(t), newSAMLIdP(t)
	providerA := seedSAMLOrgProvider(t, ctx, st, idpA, sp)
	providerB := seedSAMLOrgProvider(t, ctx, st, idpB, sp)

	if status, body := samlSignIn(t, st, idpA, providerA, sp, email, "subject-a"); status != http.StatusOK {
		t.Fatalf("first sign-in: %d %v", status, body)
	}
	status, body := samlSignIn(t, st, idpB, providerB, sp, email, "subject-b")
	if status != http.StatusConflict {
		t.Fatalf("second org: status=%d body=%v, want 409", status, body)
	}
	assertIdentityConflict(t, ctx, st, providerB, body)
	got, n := readIdentity(t, ctx, st.pool, email)
	if n != 1 || got.subject == nil || *got.subject != "subject-a" {
		t.Fatalf("account rebound: rows=%d %+v", n, got)
	}
	if status, body := samlSignIn(t, st, idpA, providerA, sp, email, "subject-a"); status != http.StatusOK {
		t.Fatalf("same identity again: %d %v", status, body)
	}
}

func TestSAMLACSRefusesAResponseAddressedToAnotherRow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	st := startStack(t, ctx)
	const sp = "https://sp.test/saml/metadata"
	idp := newSAMLIdP(t)
	providerA := seedSAMLOrgProvider(t, ctx, st, idp, sp)
	orgB := seedOrg(t, ctx, st.pool, "enterprise")
	configB := fmt.Sprintf(`{"entity_id":%q,"sso_url":"https://idp.test/sso","certificate":%q,"sp_entity_id":%q,"sp_acs_url":%q}`,
		idp.idp.MetadataURL.String(), idp.certConfigValue(), sp, samlACSOverrideFor(providerA))
	providerB := seedProvider(t, ctx, st.pool, orgB, providerOpts{protocol: "saml", status: "active", config: configB, autoProvision: true})

	const email = "bound.person@allowed.example"
	status, body := samlSignIn(t, st, idp, providerA, sp, email, "subject-1")
	if status != http.StatusOK {
		t.Fatalf("row A: %d %v", status, body)
	}
	response := idp.signedSAMLResponse(t, samlResponseOpts{
		spEntityID: sp, acsURL: samlACSOverrideFor(providerA), email: "other." + email, fullName: "Person", nameID: "subject-2",
	})
	status, body = postJSON(t, st.server, "/api/v1/auth/saml/"+providerB.String()+"/acs", map[string]any{"SAMLResponse": response})
	if status < 400 || body["access_token"] != nil {
		t.Fatalf("row B accepted a response addressed to row A: %d %v", status, body)
	}
	if _, n := readIdentity(t, ctx, st.pool, "other."+email); n != 0 {
		t.Fatalf("row B provisioned an account")
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

func TestSAMLSignInMatchesAnExistingEmailIgnoringCase(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	st := startStack(t, ctx)
	idp := newSAMLIdP(t)
	const sp = "https://sp.test/saml/metadata"
	providerID := seedSAMLOrgProvider(t, ctx, st, idp, sp)
	seedLocalUser(t, ctx, st.pool, "Mixed.Case@allowed.example")

	status, body := samlSignIn(t, st, idp, providerID, sp, "mixed.case@allowed.example", "subject-1")
	if status != http.StatusConflict {
		t.Fatalf("status=%d body=%v, want 409", status, body)
	}
	assertIdentityConflict(t, ctx, st, providerID, body)
	var n int
	if err := st.pool.QueryRow(ctx, `SELECT count(*) FROM users WHERE lower(email) = 'mixed.case@allowed.example'`).Scan(&n); err != nil || n != 1 {
		t.Fatalf("users with this email: %d (err %v), want 1", n, err)
	}
}
