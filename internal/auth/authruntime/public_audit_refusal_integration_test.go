//go:build integration

package authruntime

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
)

// CHAOS-11400 (acp answers-11400-impl-1, G1 and G2): a refused compat refresh writes go-api's login_failed row
// (internal/api/session/refresh.go:150, 240, 265, 308). A refusal that revokes commits the row with the revocation and
// its auth.session.revoked row; one that changes no auth state writes the row alone. Every refused refresh, the go-api
// refresh token's exchange included, stops before the rotation: no session, no auth.session.refreshed row.

// refusalRow is go-api's row of one refresh refusal: resource session/<sub>, changes {}, status failure.
func refusalRow(org, user uuid.UUID, withUser bool, description, message string) compatPublicRow {
	orgID, userID := org.String(), user.String()
	row := compatPublicRow{org: &orgID, action: "login_failed", resourceType: "session", resourceID: userID,
		description: compatStr(description), changes: compatStr("{}"), metadata: compatStr(compatParityMetadata),
		status: "failure", errorMessage: compatStr(message)}
	if withUser {
		row.user = &userID
	}
	return row
}

// loginCredential signs the parity user in and returns its refresh credential.
func (e *routesEnv) loginCredential(t *testing.T, email string) string {
	t.Helper()
	got := e.doWith(t, "/api/v1/auth/login", `{"email":"`+email+`","password":"`+compatParitySignIn+`"}`, compatClient())
	if got.status != http.StatusOK {
		t.Fatalf("login = %d %s", got.status, got.body)
	}
	return object(t, got)["refresh_token"].(string)
}

func (e *routesEnv) refreshAs(t *testing.T, token string) answer {
	t.Helper()
	return e.doWith(t, "/api/v1/auth/refresh", `{"refresh_token":"`+token+`"}`, compatClient())
}

func (e *routesEnv) refreshedCount(t *testing.T, ctx context.Context, user uuid.UUID) int {
	t.Helper()
	return e.countRows(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE event_type = $1 AND subject_principal_id = $2`, compatRefreshedEvent, user)
}

// refusal.go:150: the user is gone. go-api names no user_id (it has none), the token's sub as the resource.
func TestCompatRefreshOfAGoneUserWritesGoAPIsRefusalRow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	e := newRoutesEnv(t, ctx)
	user, org, email := e.parityUser(t, ctx)
	credential := e.loginCredential(t, email)
	if _, err := e.admin.Exec(ctx, `DELETE FROM public.users WHERE id = $1`, user); err != nil {
		t.Fatal(err)
	}
	if got := e.refreshAs(t, credential); got.status != http.StatusUnauthorized {
		t.Fatalf("refresh of a gone user = %d %s", got.status, got.body)
	}
	if n := e.refreshedCount(t, ctx, user); n != 0 {
		t.Errorf("%s events = %d, want 0: a refused refresh does not rotate", compatRefreshedEvent, n)
	}
	e.requireCompatRow(t, ctx, "gone user", refusalRow(org, user, false, "Token refresh failed: user not found", "User not found"),
		"auth.session.revoked", user)
}

// refresh.go:265: the user is deactivated: the session is revoked and the row commits with the revocation.
func TestCompatRefreshOfADeactivatedUserWritesGoAPIsRefusalRow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	e := newRoutesEnv(t, ctx)
	user, org, email := e.parityUser(t, ctx)
	credential := e.loginCredential(t, email)
	if _, err := e.admin.Exec(ctx, `UPDATE public.users SET is_active = false WHERE id = $1`, user); err != nil {
		t.Fatal(err)
	}
	if got := e.refreshAs(t, credential); got.status != http.StatusUnauthorized {
		t.Fatalf("refresh of a deactivated user = %d %s", got.status, got.body)
	}
	if n := e.refreshedCount(t, ctx, user); n != 0 {
		t.Errorf("%s events = %d, want 0", compatRefreshedEvent, n)
	}
	e.requireCompatRow(t, ctx, "deactivated", refusalRow(org, user, true, "Token refresh failed: user deactivated", "Account is disabled"),
		"auth.session.revoked", user)
}

// refresh.go:308: the membership the session was minted for is gone.
func TestCompatRefreshOfARemovedMemberWritesGoAPIsRefusalRow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	e := newRoutesEnv(t, ctx)
	user, org, email := e.parityUser(t, ctx)
	credential := e.loginCredential(t, email)
	if _, err := e.admin.Exec(ctx, `DELETE FROM public.memberships WHERE user_id = $1 AND org_id = $2`, user, org); err != nil {
		t.Fatal(err)
	}
	if got := e.refreshAs(t, credential); got.status != http.StatusUnauthorized {
		t.Fatalf("refresh of a removed member = %d %s", got.status, got.body)
	}
	if n := e.refreshedCount(t, ctx, user); n != 0 {
		t.Errorf("%s events = %d, want 0", compatRefreshedEvent, n)
	}
	e.requireCompatRow(t, ctx, "removed member", refusalRow(org, user, true, "Token refresh failed: not a member", "Not a member of the organization"),
		"auth.session.revoked", user)
}

// forgedRefresh is a go-api refresh JWT for sub and org whose signature does not verify.
func forgedRefresh(sub, org string) string {
	enc := base64.RawURLEncoding.EncodeToString
	return enc([]byte(`{"alg":"HS256","typ":"JWT"}`)) + "." +
		enc([]byte(`{"sub":"`+sub+`","org_id":"`+org+`","type":"refresh","jti":"forged-jti"}`)) + "." + enc([]byte("not-the-signature"))
}

// refresh.go:240: a go-api refresh token that does not verify, whose unverified org claim names an existing
// organization: the row alone (no auth state changed, so no auth event), with the unverified sub.
func TestCompatRefreshOfAnInvalidTokenWritesGoAPIsRefusalRow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	e := newRoutesEnv(t, ctx)
	user, org, _ := e.parityUser(t, ctx)
	events := e.countRows(t, ctx, `SELECT count(*) FROM auth.security_audit_events`)
	before := time.Now()
	if got := e.refreshAs(t, forgedRefresh(user.String(), org.String())); got.status != http.StatusUnauthorized {
		t.Fatalf("refresh of a forged token = %d %s", got.status, got.body)
	}
	if n := e.countRows(t, ctx, `SELECT count(*) FROM auth.security_audit_events`); n != events {
		t.Errorf("auth events %d -> %d for a refused token: no auth state changed", events, n)
	}
	want := refusalRow(org, user, true, "Refresh token validation failed", "Invalid or expired refresh token")
	got := e.compatPublicRows(t, ctx, want.action, *want.description)
	if len(got) != 1 {
		t.Fatalf("public.audit_logs rows of %q = %d, want go-api's 1", *want.description, len(got))
	}
	row := got[0]
	if text(row.org) != text(want.org) || text(row.user) != text(want.user) || row.resourceType != want.resourceType ||
		row.resourceID != want.resourceID || text(row.changes) != text(want.changes) || text(row.metadata) != text(want.metadata) ||
		row.status != want.status || text(row.errorMessage) != text(want.errorMessage) {
		t.Errorf("row %+v, want %+v", row, want)
	}
	if row.createdAt.Before(before.Add(-5*time.Second)) || row.createdAt.After(time.Now().Add(5*time.Second)) {
		t.Errorf("created_at = %s, want the request's time", row.createdAt)
	}
}

// G1: the go-api refresh token of a deactivated user is refused before its exchange creates a session: no session,
// no auth.session.refreshed row, and go-api's deactivated row once.
func TestCompatRefreshOfALegacyTokenIsGuardedBeforeTheExchange(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	e := newRoutesEnv(t, ctx)
	user, org, _ := e.parityUser(t, ctx)
	signer, err := edgetoken.NewSigner(testAccessSecret, "dev-health-ops", "dev-health-api")
	if err != nil {
		t.Fatal(err)
	}
	jti, family := uuid.NewString(), uuid.New()
	token, err := signer.Refresh(edgetoken.RefreshClaims{UserID: user.String(), OrgID: org.String(), FamilyID: family.String()}, time.Now(), jti)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256([]byte(jti))
	if _, err := e.admin.Exec(ctx, `INSERT INTO public.refresh_tokens (id, user_id, org_id, token_hash, family_id, expires_at, created_at)
		VALUES (gen_random_uuid(), $1, $2, $3, $4, now() + interval '1 day', now())`, user, org, hex.EncodeToString(digest[:]), family); err != nil {
		t.Fatal(err)
	}
	if _, err := e.admin.Exec(ctx, `UPDATE public.users SET is_active = false WHERE id = $1`, user); err != nil {
		t.Fatal(err)
	}
	sessions := e.countRows(t, ctx, `SELECT count(*) FROM auth.sessions WHERE principal_id = $1`, user)
	if got := e.refreshAs(t, token); got.status != http.StatusUnauthorized {
		t.Fatalf("refresh of a deactivated user's go-api token = %d %s", got.status, got.body)
	}
	if n := e.countRows(t, ctx, `SELECT count(*) FROM auth.sessions WHERE principal_id = $1`, user); n != sessions {
		t.Errorf("sessions %d -> %d: the refused go-api token was exchanged before the guard", sessions, n)
	}
	if n := e.refreshedCount(t, ctx, user); n != 0 {
		t.Errorf("%s events = %d, want 0", compatRefreshedEvent, n)
	}
	want := refusalRow(org, user, true, "Token refresh failed: user deactivated", "Account is disabled")
	if got := e.compatPublicRows(t, ctx, want.action, *want.description); len(got) != 1 || text(got[0].org) != org.String() || text(got[0].user) != user.String() {
		t.Errorf("deactivated rows = %+v, want go-api's one row for the user in the token's organization", got)
	}
}

// legacyRefreshToken mints a go-api refresh JWT for the user in org and seeds its public.refresh_tokens row.
func (e *routesEnv) legacyRefreshToken(t *testing.T, ctx context.Context, user, org uuid.UUID) string {
	t.Helper()
	signer, err := edgetoken.NewSigner(testAccessSecret, "dev-health-ops", "dev-health-api")
	if err != nil {
		t.Fatal(err)
	}
	jti, family := uuid.NewString(), uuid.New()
	token, err := signer.Refresh(edgetoken.RefreshClaims{UserID: user.String(), OrgID: org.String(), FamilyID: family.String()}, time.Now(), jti)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256([]byte(jti))
	if _, err := e.admin.Exec(ctx, `INSERT INTO public.refresh_tokens (id, user_id, org_id, token_hash, family_id, expires_at, created_at)
		VALUES (gen_random_uuid(), $1, $2, $3, $4, now() + interval '1 day', now())`, user, org, hex.EncodeToString(digest[:]), family); err != nil {
		t.Fatal(err)
	}
	return token
}

// G1, the guard's own refusal: a go-api refresh token of a user no longer a member of the token's organization is
// refused by the refresh guard inside the exchange, before the exchange commits a session (the store itself would
// exchange it); go-api's not-a-member row once.
func TestCompatRefreshOfALegacyTokenOfARemovedMemberIsGuardedBeforeTheExchange(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	e := newRoutesEnv(t, ctx)
	user, org, _ := e.parityUser(t, ctx)
	token := e.legacyRefreshToken(t, ctx, user, org)
	if _, err := e.admin.Exec(ctx, `DELETE FROM public.memberships WHERE user_id = $1 AND org_id = $2`, user, org); err != nil {
		t.Fatal(err)
	}
	sessions := e.countRows(t, ctx, `SELECT count(*) FROM auth.sessions WHERE principal_id = $1`, user)
	if got := e.refreshAs(t, token); got.status != http.StatusUnauthorized {
		t.Fatalf("refresh of a removed member's go-api token = %d %s", got.status, got.body)
	}
	if n := e.countRows(t, ctx, `SELECT count(*) FROM auth.sessions WHERE principal_id = $1`, user); n != sessions {
		t.Errorf("sessions %d -> %d: the refused go-api token was exchanged before the guard", sessions, n)
	}
	if n := e.refreshedCount(t, ctx, user); n != 0 {
		t.Errorf("%s events = %d, want 0", compatRefreshedEvent, n)
	}
	want := refusalRow(org, user, true, "Token refresh failed: not a member", "Not a member of the organization")
	if got := e.compatPublicRows(t, ctx, want.action, *want.description); len(got) != 1 || text(got[0].org) != org.String() || text(got[0].user) != user.String() {
		t.Errorf("not-a-member rows = %+v, want go-api's one row for the user in the token's organization", got)
	}
}

// disablePrincipal disables the user's auth principal as an audited write does: the commit guard is armed for the
// transaction (a guarded column is written only inside audit.Commit).
func (e *routesEnv) disablePrincipal(t *testing.T, ctx context.Context, user uuid.UUID) {
	t.Helper()
	tx, err := e.admin.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SELECT set_config('dev_health.audit_txid', pg_current_xact_id()::text, true),
		set_config('dev_health.audit_changed', pg_current_xact_id()::text || ':0', true)`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `UPDATE auth.principals SET disabled_at = now() WHERE id = $1`, user); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
}

// acp answers-11400-impl-2 Q2: an auth-service credential whose principal is disabled is refused by the store under
// the session lock; go-api's deactivated row (refresh.go:265) names the locked session's user and organization, in a
// transaction of its own (no auth state changes, so no auth event).
func TestCompatRefreshOfADisabledPrincipalsCredentialWritesGoAPIsRefusalRow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	e := newRoutesEnv(t, ctx)
	user, org, email := e.parityUser(t, ctx)
	credential := e.loginCredential(t, email)
	e.disablePrincipal(t, ctx, user)
	events := e.countRows(t, ctx, `SELECT count(*) FROM auth.security_audit_events`)
	before := time.Now()
	if got := e.refreshAs(t, credential); got.status != http.StatusUnauthorized {
		t.Fatalf("refresh of a disabled principal's credential = %d %s", got.status, got.body)
	}
	if n := e.countRows(t, ctx, `SELECT count(*) FROM auth.security_audit_events`); n != events {
		t.Errorf("auth events %d -> %d: the refusal changes no auth state", events, n)
	}
	want := refusalRow(org, user, true, "Token refresh failed: user deactivated", "Account is disabled")
	got := e.compatPublicRows(t, ctx, want.action, *want.description)
	if len(got) != 1 {
		t.Fatalf("public.audit_logs rows of %q = %d, want go-api's 1", *want.description, len(got))
	}
	row := got[0]
	if text(row.org) != text(want.org) || text(row.user) != text(want.user) || row.resourceType != want.resourceType ||
		row.resourceID != want.resourceID || text(row.changes) != text(want.changes) || text(row.metadata) != text(want.metadata) ||
		row.status != want.status || text(row.errorMessage) != text(want.errorMessage) {
		t.Errorf("row %+v, want %+v", row, want)
	}
	if row.createdAt.Before(before.Add(-5*time.Second)) || row.createdAt.After(time.Now().Add(5*time.Second)) {
		t.Errorf("created_at = %s, want the request's time", row.createdAt)
	}
}

// acp answers-11400-impl-2 Q1: a deactivated principal's first go-api exchange revokes the token's family in the
// store's own transaction and writes go-api's row in a transaction after it. When that row write fails the answer is
// 500 and the family stays revoked.
func TestCompatRefreshOfADeactivatedLegacyTokenWithAFailedRowAnswers500AndKeepsTheRevoke(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	e := newRoutesEnv(t, ctx)
	user, org, _ := e.parityUser(t, ctx)
	token := e.legacyRefreshToken(t, ctx, user, org)
	if _, err := e.admin.Exec(ctx, `UPDATE public.users SET is_active = false WHERE id = $1`, user); err != nil {
		t.Fatal(err)
	}
	e.refusePublicAuditInserts(t, ctx, true)
	if got := e.refreshAs(t, token); got.status != http.StatusInternalServerError {
		t.Fatalf("refresh with a failing row write = %d %s, want go-api's 500", got.status, got.body)
	}
	if n := e.countRows(t, ctx, `SELECT count(*) FROM public.refresh_tokens WHERE user_id = $1 AND revoked_at IS NULL`, user); n != 0 {
		t.Errorf("live go-api refresh tokens of the user = %d, want 0: the family revoke stands", n)
	}
	if n := e.countRows(t, ctx, `SELECT count(*) FROM public.audit_logs`); n != 0 {
		t.Errorf("public.audit_logs rows = %d, want 0", n)
	}
}
