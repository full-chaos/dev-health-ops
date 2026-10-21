//go:build integration

package authruntime

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/passwordhash"
	"github.com/full-chaos/dev-health-ops/internal/auth/requestid"
)

// compatRoutesMarker is logged once the compatibility routes were driven
// through configure against a live PostgreSQL.
const compatRoutesMarker = "AUTH_COMPAT_ROUTES_EXECUTED"

func (e *routesEnv) doWith(t *testing.T, path, body string, header map[string]string) answer {
	t.Helper()
	request, err := http.NewRequest(http.MethodPost, e.front.URL+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	for key, value := range header {
		request.Header.Set(key, value)
	}
	response, err := e.client.Do(request)
	if err != nil {
		t.Fatalf("POST %s: %v", path, err)
	}
	defer response.Body.Close()
	raw, _ := io.ReadAll(response.Body)
	return answer{status: response.StatusCode, body: raw, header: response.Header}
}

func object(t *testing.T, got answer) map[string]any {
	t.Helper()
	var out map[string]any
	if err := json.Unmarshal(got.body, &out); err != nil {
		t.Fatalf("not a JSON object (%d): %s", got.status, got.body)
	}
	return out
}

// auditRow is one security_audit_events row as these tests read it.
type auditRow struct {
	requestID, clientIP, org, subject, reason *string
}

func (e *routesEnv) auditOf(t *testing.T, ctx context.Context, eventType string, requestWire string) auditRow {
	t.Helper()
	var row auditRow
	err := e.admin.QueryRow(ctx, `SELECT request_id, host(client_ip), organization_id::text, subject_principal_id::text,
		attributes->>'reason_code' FROM auth.security_audit_events WHERE event_type = $1 ORDER BY id DESC LIMIT 1`, eventType).
		Scan(&row.requestID, &row.clientIP, &row.org, &row.subject, &row.reason)
	if err != nil {
		t.Fatalf("audit row %s: %v", eventType, err)
	}
	if row.requestID == nil {
		t.Fatalf("%s row has no request id", eventType)
	}
	id, err := uuid.Parse(*row.requestID)
	if err != nil || requestid.WireOf(id) != requestWire {
		t.Fatalf("%s row request id %v is not the answer's %s", eventType, *row.requestID, requestWire)
	}
	return row
}

func text(p *string) string {
	if p == nil {
		return "<nil>"
	}
	return *p
}

// TestCompatRoutesThroughConfigure drives go-api's login, refresh and logout
// wire through the handler configure builds, as the runtime role on a live
// PostgreSQL, behind a trusted proxy (the test server's loopback peer) that
// names the client in X-Forwarded-For: the session and every audit row carry
// that client, not the proxy.
func TestCompatRoutesThroughConfigure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	e := newRoutesEnv(t, ctx)
	user, org := e.seed(t, ctx)
	hash, err := passwordhash.Hash("compat-route-password")
	if err != nil {
		t.Fatal(err)
	}
	// go-api's LoginRequest refuses a special-use domain (example.test), so the
	// login address is on example.com.
	email := "compat-" + user.String()[:8] + "@example.com"
	if _, err := e.admin.Exec(ctx, `UPDATE users SET password_hash = $2, email = $3 WHERE id = $1`, user, hash, email); err != nil {
		t.Fatal(err)
	}
	client := map[string]string{"X-Forwarded-For": "198.51.100.7", "Content-Type": "application/json", "User-Agent": "compat-route-test/1.0"}

	// A wrong password: go-api's 401, audited for the user at their org, from the client.
	got := e.doWith(t, "/api/v1/auth/login", `{"email":"`+email+`","password":"wrong"}`, client)
	if got.status != http.StatusUnauthorized || strings.TrimSpace(string(got.body)) != `{"detail":{"message":"Invalid credentials"}}` {
		t.Fatalf("wrong password = %d %s", got.status, got.body)
	}
	failed := e.auditOf(t, ctx, "auth.login.failed", got.header.Get("X-Auth-Request-ID"))
	if text(failed.clientIP) != "198.51.100.7" || text(failed.org) != org.String() || text(failed.subject) != user.String() ||
		text(failed.reason) != "invalid_credentials" {
		t.Fatalf("failed-login row: ip %s org %s subject %s reason %s", text(failed.clientIP), text(failed.org), text(failed.subject), text(failed.reason))
	}

	// Sign-in: go-api's LoginResponse; the session and its audit row commit together.
	got = e.doWith(t, "/api/v1/auth/login", `{"email":"`+email+`","password":"compat-route-password"}`, client)
	if got.status != http.StatusOK {
		t.Fatalf("login = %d %s", got.status, got.body)
	}
	login := object(t, got)
	userInfo := login["user"].(map[string]any)
	if userInfo["org_id"] != org.String() || userInfo["role"] != "owner" || login["needs_onboarding"] != false || login["token_type"] != "bearer" {
		t.Fatalf("login body %s", got.body)
	}
	claims, err := e.edge.Verify(ctx, login["access_token"].(string))
	if err != nil || claims.OrgID != org.String() {
		t.Fatalf("query-api's edge verifier: %+v %v", claims, err)
	}
	succeeded := e.auditOf(t, ctx, "auth.login.succeeded", got.header.Get("X-Auth-Request-ID"))
	if text(succeeded.clientIP) != "198.51.100.7" || text(succeeded.org) != org.String() || text(succeeded.subject) != user.String() {
		t.Fatalf("sign-in row: ip %s org %s subject %s", text(succeeded.clientIP), text(succeeded.org), text(succeeded.subject))
	}
	var sessions int
	if err := e.admin.QueryRow(ctx, `SELECT count(*) FROM auth.sessions WHERE principal_id = $1 AND organization_id = $2
		AND host(client_ip) = '198.51.100.7' AND amr = ARRAY['pwd'] AND user_agent = 'compat-route-test/1.0'`, user, org).Scan(&sessions); err != nil || sessions != 1 {
		t.Fatalf("%d sessions for the sign-in (%v), want 1 at the org from the client with its user agent", sessions, err)
	}

	// Refresh: go-api's wire with the rotated opaque credential.
	got = e.doWith(t, "/api/v1/auth/refresh", `{"refresh_token":"`+login["refresh_token"].(string)+`"}`, client)
	if got.status != http.StatusOK {
		t.Fatalf("refresh = %d %s", got.status, got.body)
	}
	refreshed := object(t, got)
	if refreshed["refresh_token"] == login["refresh_token"] || refreshed["user"].(map[string]any)["org_id"] != org.String() {
		t.Fatalf("refresh body %s", got.body)
	}

	// Logout ends the session; its credential is then refused with go-api's text.
	got = e.doWith(t, "/api/v1/auth/logout", `{"refresh_token":"`+refreshed["refresh_token"].(string)+`"}`, client)
	if got.status != http.StatusOK || strings.TrimSpace(string(got.body)) != `{"message":"Logout successful"}` {
		t.Fatalf("logout = %d %s", got.status, got.body)
	}
	revoked := e.auditOf(t, ctx, "auth.session.revoked", got.header.Get("X-Auth-Request-ID"))
	if text(revoked.clientIP) != "198.51.100.7" {
		t.Fatalf("revocation row client %s", text(revoked.clientIP))
	}
	got = e.doWith(t, "/api/v1/auth/refresh", `{"refresh_token":"`+refreshed["refresh_token"].(string)+`"}`, client)
	if got.status != http.StatusUnauthorized || strings.TrimSpace(string(got.body)) != `{"detail":{"message":"Invalid or expired refresh token"}}` {
		t.Fatalf("refresh after logout = %d %s", got.status, got.body)
	}

	// A forwarded chain is not one proxy's answer: the peer is the client.
	got = e.doWith(t, "/api/v1/auth/login", `{"email":"`+email+`","password":"wrong"}`,
		map[string]string{"X-Forwarded-For": "203.0.113.5, 198.51.100.7", "Content-Type": "application/json"})
	chained := e.auditOf(t, ctx, "auth.login.failed", got.header.Get("X-Auth-Request-ID"))
	if text(chained.clientIP) != "127.0.0.1" {
		t.Fatalf("a forwarded chain recorded %s, want the peer", text(chained.clientIP))
	}
	t.Logf("%s login=200 wrong=401 refresh=200 logout=200 after_logout=401 audit_client_ip=forwarded chain=peer", compatRoutesMarker)
}
