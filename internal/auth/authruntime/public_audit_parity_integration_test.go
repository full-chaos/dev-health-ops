//go:build integration

package authruntime

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/passwordhash"
)

// CHAOS-11400 (acp scope growth, PLAN-11400-2/-3): go-api forwards refresh and logout to auth-service at phase 2
// (AUTH_PROXY_SESSIONS) and login at phase 3 (AUTH_PROXY_LOCAL_LOGIN); the forward replaces go-api's handler, so the
// public.audit_logs row go-api's own handler wrote (session/login.go, refresh.go, routes.go) must come from
// auth-service, in the transaction of the auth event it goes with: login with auth.login.succeeded or
// auth.login.failed, refresh with auth.session.refreshed (new), logout with auth.session.revoked. created_at is that
// event's occurred_at; request_metadata.request_id is the X-Request-Id the go-api proxy forwards.

const (
	compatForwardedID     = "venue-oracle-req-1"
	compatParityMetadata  = `{"ip_address": "203.0.113.7", "user_agent": "venue-oracle/1", "request_id": "venue-oracle-req-1"}`
	compatRefreshedEvent  = "auth.session.refreshed"
	compatParitySignIn    = "compat-parity-signin"
	compatParityForwarded = "203.0.113.7"
)

// compatPublicRow is one public.audit_logs row, every column go-api writes.
type compatPublicRow struct {
	id                               uuid.UUID
	org, user                        *string
	action, resourceType, resourceID string
	description, changes, metadata   *string
	status                           string
	errorMessage                     *string
	createdAt                        time.Time
}

func compatStr(s string) *string { return &s }

func compatClient() map[string]string {
	return map[string]string{"X-Forwarded-For": compatParityForwarded, "Content-Type": "application/json",
		"User-Agent": "venue-oracle/1", "X-Request-Id": compatForwardedID}
}

func (e *routesEnv) compatPublicRows(t *testing.T, ctx context.Context, action, description string) []compatPublicRow {
	t.Helper()
	rows, err := e.admin.Query(ctx, `SELECT id, org_id::text, user_id::text, action, resource_type, resource_id, description,
	changes::text, request_metadata::text, status, error_message, created_at
FROM public.audit_logs WHERE action = $1 AND description = $2 ORDER BY created_at, id`, action, description)
	if err != nil {
		t.Fatalf("read public.audit_logs: %v", err)
	}
	defer rows.Close()
	var out []compatPublicRow
	for rows.Next() {
		var r compatPublicRow
		if err := rows.Scan(&r.id, &r.org, &r.user, &r.action, &r.resourceType, &r.resourceID, &r.description,
			&r.changes, &r.metadata, &r.status, &r.errorMessage, &r.createdAt); err != nil {
			t.Fatalf("scan public.audit_logs: %v", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read public.audit_logs: %v", err)
	}
	return out
}

func (e *routesEnv) countRows(t *testing.T, ctx context.Context, query string, args ...any) int {
	t.Helper()
	var n int
	if err := e.admin.QueryRow(ctx, query, args...).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	return n
}

// requireCompatRow asserts exactly one row of (want.action, want.description), every column of it, and created_at
// equal to the occurred_at of the latest eventType event of the user: one instant for both rows.
func (e *routesEnv) requireCompatRow(t *testing.T, ctx context.Context, step string, want compatPublicRow, eventType string, user uuid.UUID) {
	t.Helper()
	got := e.compatPublicRows(t, ctx, want.action, *want.description)
	if len(got) != 1 {
		t.Errorf("%s: public.audit_logs rows of %q/%q = %d, want exactly go-api's 1", step, want.action, *want.description, len(got))
		return
	}
	row := got[0]
	check := func(column, got, want string) {
		if got != want {
			t.Errorf("%s: %s = %q, want %q", step, column, got, want)
		}
	}
	if row.id == uuid.Nil {
		t.Errorf("%s: id is the nil uuid", step)
	}
	check("org_id", text(row.org), text(want.org))
	check("user_id", text(row.user), text(want.user))
	check("resource_type", row.resourceType, want.resourceType)
	check("resource_id", row.resourceID, want.resourceID)
	check("changes", text(row.changes), text(want.changes))
	check("request_metadata", text(row.metadata), text(want.metadata))
	check("status", row.status, want.status)
	check("error_message", text(row.errorMessage), text(want.errorMessage))
	var at time.Time
	if err := e.admin.QueryRow(ctx, `SELECT occurred_at FROM auth.security_audit_events WHERE event_type = $1 AND subject_principal_id = $2
		ORDER BY occurred_at DESC, id DESC LIMIT 1`, eventType, user).Scan(&at); err != nil {
		t.Errorf("%s: the %s event: %v", step, eventType, err)
		return
	}
	if !row.createdAt.Equal(at) {
		t.Errorf("%s: created_at = %s, want the %s event's occurred_at %s", step, row.createdAt, eventType, at)
	}
}

// parityUser seeds a member of one organization with a password on an example.com address.
func (e *routesEnv) parityUser(t *testing.T, ctx context.Context) (user, org uuid.UUID, email string) {
	t.Helper()
	user, org = e.seed(t, ctx)
	hash, err := passwordhash.Hash(compatParitySignIn)
	if err != nil {
		t.Fatal(err)
	}
	email = "parity-" + user.String()[:8] + "@example.com"
	if _, err := e.admin.Exec(ctx, `UPDATE users SET password_hash = $2, email = $3 WHERE id = $1`, user, hash, email); err != nil {
		t.Fatal(err)
	}
	return user, org, email
}

func (e *routesEnv) refusePublicAuditInserts(t *testing.T, ctx context.Context, on bool) {
	t.Helper()
	statements := []string{
		`CREATE OR REPLACE FUNCTION public.chaos11400_refuse_audit_insert() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'chaos11400: injected public.audit_logs insert failure'; END $$`,
		`CREATE TRIGGER chaos11400_refuse_audit_insert BEFORE INSERT ON public.audit_logs
FOR EACH ROW EXECUTE FUNCTION public.chaos11400_refuse_audit_insert()`,
	}
	if !on {
		statements = []string{`DROP TRIGGER chaos11400_refuse_audit_insert ON public.audit_logs`}
	}
	for _, statement := range statements {
		if _, err := e.admin.Exec(ctx, statement); err != nil {
			t.Fatalf("switch the insert refusal: %v", err)
		}
	}
}

// TestCompatRoutesWriteGoAPIsPublicAuditRows: go-api's row for each compatibility route, from the client go-api's
// golden recorded (203.0.113.7, venue-oracle/1, behind the trusted loopback proxy):
//
//	login_failed/session "Login failed: invalid credentials" (login.go:251): user = resource = the user, org = the
//	  user's first membership, changes {"email": <normalized>}, status failure, error "Invalid credentials";
//	login/session "User logged in" (login.go:363): org = the issued session's, changes {};
//	login/session "Access token refreshed" (refresh.go:190): org = the session's, changes {};
//	logout/session "User logged out" (routes.go:340): user and org of the revoked session (server-derived).
func TestCompatRoutesWriteGoAPIsPublicAuditRows(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	e := newRoutesEnv(t, ctx)
	user, org, email := e.parityUser(t, ctx)
	client := compatClient()
	userID, orgID := user.String(), org.String()

	got := e.doWith(t, "/api/v1/auth/login", `{"email":"`+email+`","password":"wrong"}`, client)
	if got.status != http.StatusUnauthorized {
		t.Fatalf("wrong password = %d %s", got.status, got.body)
	}
	e.requireCompatRow(t, ctx, "wrong password", compatPublicRow{
		org: &orgID, user: &userID, action: "login_failed", resourceType: "session", resourceID: userID,
		description: compatStr("Login failed: invalid credentials"), changes: compatStr(`{"email": "` + email + `"}`),
		metadata: compatStr(compatParityMetadata), status: "failure", errorMessage: compatStr("Invalid credentials"),
	}, "auth.login.failed", user)

	got = e.doWith(t, "/api/v1/auth/login", `{"email":"`+email+`","password":"`+compatParitySignIn+`"}`, client)
	if got.status != http.StatusOK {
		t.Fatalf("login = %d %s", got.status, got.body)
	}
	login := object(t, got)
	e.requireCompatRow(t, ctx, "login", compatPublicRow{
		org: &orgID, user: &userID, action: "login", resourceType: "session", resourceID: userID,
		description: compatStr("User logged in"), changes: compatStr("{}"),
		metadata: compatStr(compatParityMetadata), status: "success",
	}, "auth.login.succeeded", user)

	got = e.doWith(t, "/api/v1/auth/refresh", `{"refresh_token":"`+login["refresh_token"].(string)+`"}`, client)
	if got.status != http.StatusOK {
		t.Fatalf("refresh = %d %s", got.status, got.body)
	}
	refreshed := object(t, got)
	if n := e.countRows(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE event_type = $1 AND subject_principal_id = $2
		AND outcome = 'allowed' AND attributes->>'reason_code' = 'accepted' AND attributes->>'target_kind' = 'session'`, compatRefreshedEvent, user); n != 1 {
		t.Errorf("%s events = %d, want 1 for the one refresh", compatRefreshedEvent, n)
	}
	e.requireCompatRow(t, ctx, "refresh", compatPublicRow{
		org: &orgID, user: &userID, action: "login", resourceType: "session", resourceID: userID,
		description: compatStr("Access token refreshed"), changes: compatStr("{}"),
		metadata: compatStr(compatParityMetadata), status: "success",
	}, compatRefreshedEvent, user)

	got = e.doWith(t, "/api/v1/auth/logout", `{"refresh_token":"`+refreshed["refresh_token"].(string)+`"}`, client)
	if got.status != http.StatusOK {
		t.Fatalf("logout = %d %s", got.status, got.body)
	}
	e.requireCompatRow(t, ctx, "logout", compatPublicRow{
		org: &orgID, user: &userID, action: "logout", resourceType: "session", resourceID: userID,
		description: compatStr("User logged out"), changes: compatStr("{}"),
		metadata: compatStr(compatParityMetadata), status: "success",
	}, "auth.session.revoked", user)
}

// A logout or refresh of a dead or unknown credential changes nothing: no auth event and no public row (acp ruling
// 5; go-api's refusal rows of an unknown credential name no existing organization).
func TestCompatRoutesWriteNoRowsForADeadCredential(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	e := newRoutesEnv(t, ctx)
	user, _, email := e.parityUser(t, ctx)
	client := compatClient()
	got := e.doWith(t, "/api/v1/auth/login", `{"email":"`+email+`","password":"`+compatParitySignIn+`"}`, client)
	if got.status != http.StatusOK {
		t.Fatalf("login = %d %s", got.status, got.body)
	}
	credential := object(t, got)["refresh_token"].(string)
	if got := e.doWith(t, "/api/v1/auth/logout", `{"refresh_token":"`+credential+`"}`, client); got.status != http.StatusOK {
		t.Fatalf("first logout = %d %s", got.status, got.body)
	}
	revoked := e.countRows(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE event_type = 'auth.session.revoked' AND subject_principal_id = $1`, user)
	public := e.countRows(t, ctx, `SELECT count(*) FROM public.audit_logs`)
	for _, body := range []string{`{"refresh_token":"` + credential + `"}`, `{"refresh_token":"not-a-live-credential"}`} {
		if got := e.doWith(t, "/api/v1/auth/logout", body, client); got.status != http.StatusOK {
			t.Fatalf("logout of a dead credential = %d %s", got.status, got.body)
		}
	}
	if got := e.doWith(t, "/api/v1/auth/refresh", `{"refresh_token":"not-a-live-credential"}`, client); got.status != http.StatusUnauthorized {
		t.Fatalf("refresh of an unknown credential = %d %s", got.status, got.body)
	}
	if n := e.countRows(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE event_type = 'auth.session.revoked' AND subject_principal_id = $1`, user); n != revoked {
		t.Errorf("auth.session.revoked events = %d after logouts of a dead session, want %d", n, revoked)
	}
	if n := e.countRows(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE event_type = $1`, compatRefreshedEvent); n != 0 {
		t.Errorf("%s events = %d, want 0", compatRefreshedEvent, n)
	}
	if n := e.countRows(t, ctx, `SELECT count(*) FROM public.audit_logs`); n != public {
		t.Errorf("public.audit_logs rows = %d after dead-credential calls, want %d", n, public)
	}
}

// A failed public insert aborts the flow's commit: the login issues no session, the refresh does not rotate (the old
// credential still works once the insert works again), the logout does not end the session.
func TestCompatPublicAuditInsertFailureAbortsTheFlow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	e := newRoutesEnv(t, ctx)
	user, _, email := e.parityUser(t, ctx)
	client := compatClient()
	got := e.doWith(t, "/api/v1/auth/login", `{"email":"`+email+`","password":"`+compatParitySignIn+`"}`, client)
	if got.status != http.StatusOK {
		t.Fatalf("login = %d %s", got.status, got.body)
	}
	credential := object(t, got)["refresh_token"].(string)
	sessions := e.countRows(t, ctx, `SELECT count(*) FROM auth.sessions WHERE principal_id = $1`, user)
	succeeded := e.countRows(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE event_type = 'auth.login.succeeded' AND subject_principal_id = $1`, user)

	e.refusePublicAuditInserts(t, ctx, true)
	if got := e.doWith(t, "/api/v1/auth/login", `{"email":"`+email+`","password":"`+compatParitySignIn+`"}`, client); got.status == http.StatusOK {
		t.Errorf("login with a failing public audit insert = 200; want a failure")
	}
	if n := e.countRows(t, ctx, `SELECT count(*) FROM auth.sessions WHERE principal_id = $1`, user); n != sessions {
		t.Errorf("sessions = %d after an aborted login, want %d", n, sessions)
	}
	if n := e.countRows(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE event_type = 'auth.login.succeeded' AND subject_principal_id = $1`, user); n != succeeded {
		t.Errorf("auth.login.succeeded events = %d after an aborted login, want %d", n, succeeded)
	}
	if got := e.doWith(t, "/api/v1/auth/refresh", `{"refresh_token":"`+credential+`"}`, client); got.status == http.StatusOK {
		t.Errorf("refresh with a failing public audit insert = 200; want a failure")
	}
	if got := e.doWith(t, "/api/v1/auth/logout", `{"refresh_token":"`+credential+`"}`, client); got.status == http.StatusOK {
		t.Errorf("logout with a failing public audit insert = 200; want a failure")
	}
	if n := e.countRows(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE event_type IN ('auth.session.refreshed', 'auth.session.revoked') AND subject_principal_id = $1`, user); n != 0 {
		t.Errorf("refresh/revoke events = %d after aborted calls, want 0", n)
	}
	e.refusePublicAuditInserts(t, ctx, false)
	if got := e.doWith(t, "/api/v1/auth/refresh", `{"refresh_token":"`+credential+`"}`, client); got.status != http.StatusOK {
		t.Errorf("refresh with the old credential after the aborted refresh and logout = %d %s; want 200 (nothing was committed)", got.status, got.body)
	}
}
