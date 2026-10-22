//go:build integration

package authruntime

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
)

// issuerCredential writes an internal_service_credentials row as the
// service-credentials verbs do and returns its token, built at run time.
func (e *routesEnv) issuerCredential(t *testing.T, ctx context.Context, service, scopes string, revoked, expired bool) (string, uuid.UUID) {
	t.Helper()
	random := make([]byte, 32)
	if _, err := rand.Read(random); err != nil {
		t.Fatal(err)
	}
	token := "svc_authsess_" + base64.RawURLEncoding.EncodeToString(random)
	sum := sha256.Sum256([]byte(token))
	id := uuid.New()
	var revokedAt, expiresAt any
	if revoked {
		revokedAt = time.Now().Add(-time.Minute)
	}
	if expired {
		expiresAt = time.Now().Add(-time.Minute)
	}
	if _, err := e.admin.Exec(ctx, `INSERT INTO internal_service_credentials
		(id, service_name, token_hash, token_prefix, scopes, expires_at, revoked_at, created_at)
		VALUES ($1, $2, $3, $4, $5::json, $6, $7, now())`,
		id, service, hex.EncodeToString(sum[:]), token[:16], scopes, expiresAt, revokedAt); err != nil {
		t.Fatalf("seed service credential: %v", err)
	}
	return token, id
}

// The session issue route through configure, as the runtime role: a live
// auth-session-issuer credential gets a pinned session with its
// auth.session.issued row, the end user's client and agent; every other
// credential is refused and creates nothing.
func TestIssueSessionRouteThroughConfigure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	e := newRoutesEnv(t, ctx)
	user, org := e.seed(t, ctx)
	client := map[string]string{"X-Forwarded-For": "198.51.100.9", "Content-Type": "application/json", "User-Agent": "issue-route-test/1.0"}
	body := `{"principal_id":"` + user.String() + `","organization_id":"` + org.String() + `","amr":["pwd"]}`

	for name, tc := range map[string]struct {
		service, scopes  string
		revoked, expired bool
	}{
		"another service": {"acr", `["entitlements:read"]`, false, false},
		"revoked":         {"auth-session-issuer", `["sessions:issue"]`, true, false},
		"expired":         {"auth-session-issuer", `["sessions:issue"]`, false, true},
		"no issue scope":  {"auth-session-issuer", `[]`, false, false},
	} {
		token, _ := e.issuerCredential(t, ctx, tc.service, tc.scopes, tc.revoked, tc.expired)
		header := map[string]string{"Authorization": "Bearer " + token}
		for k, v := range client {
			header[k] = v
		}
		if got := e.doWith(t, "/internal/v1/sessions", body, header); got.status != http.StatusUnauthorized {
			t.Fatalf("%s: %d %s, want 401", name, got.status, got.body)
		}
	}
	var before int
	if err := e.admin.QueryRow(ctx, `SELECT count(*) FROM auth.sessions WHERE principal_id = $1`, user).Scan(&before); err != nil || before != 0 {
		t.Fatalf("%d sessions after refused credentials (%v), want 0", before, err)
	}

	token, credentialID := e.issuerCredential(t, ctx, "auth-session-issuer", `["sessions:issue"]`, false, false)
	header := map[string]string{"Authorization": "Bearer " + token}
	for k, v := range client {
		header[k] = v
	}
	got := e.doWith(t, "/internal/v1/sessions", body, header)
	if got.status != http.StatusOK {
		t.Fatalf("issue = %d %s", got.status, got.body)
	}
	issued := object(t, got)
	claims, err := e.edge.Verify(ctx, issued["access_token"].(string))
	if err != nil || claims.OrgID != org.String() {
		t.Fatalf("query-api's edge verifier: %+v %v", claims, err)
	}
	var pinned int
	if err := e.admin.QueryRow(ctx, `SELECT count(*) FROM auth.sessions s JOIN public.users u ON u.id = s.principal_id
		WHERE s.principal_id = $1 AND s.organization_id = $2 AND s.amr = ARRAY['pwd'] AND s.legacy_token_version = u.token_version
		AND host(s.client_ip) = '198.51.100.9' AND s.user_agent = 'issue-route-test/1.0'`, user, org).Scan(&pinned); err != nil || pinned != 1 {
		t.Fatalf("%d pinned sessions from the client (%v), want 1", pinned, err)
	}
	var source, credential, amr, ip, agent string
	if err := e.admin.QueryRow(ctx, `SELECT attributes->>'source', attributes->>'credential_id', attributes->>'amr',
		host(client_ip), user_agent FROM auth.security_audit_events
		WHERE event_type = 'auth.session.issued' AND subject_principal_id = $1 AND organization_id = $2`, user, org).
		Scan(&source, &credential, &amr, &ip, &agent); err != nil {
		t.Fatalf("auth.session.issued row: %v", err)
	}
	if source != "go-api-delegated" || credential != credentialID.String() || amr != `["pwd"]` || ip != "198.51.100.9" || agent != "issue-route-test/1.0" {
		t.Fatalf("auth.session.issued row: source %s credential %s amr %s ip %s agent %s", source, credential, amr, ip, agent)
	}
	refreshed := e.doWith(t, "/api/v1/auth/refresh", `{"refresh_token":"`+issued["refresh_token"].(string)+`"}`, client)
	if refreshed.status != http.StatusOK {
		t.Fatalf("the issued credential does not refresh: %d %s", refreshed.status, refreshed.body)
	}
}
