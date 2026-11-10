//go:build integration

package session_test

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/authmail"
	"github.com/full-chaos/dev-health-ops/internal/api/sessionissue"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/edtokens"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pinrig"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/sessionscenario"
)

const (
	changeAlice = "10000000-0000-4000-8000-000000000001"
	changeBob   = "10000000-0000-4000-8000-000000000002"
	changeFrank = "10000000-0000-4000-8000-000000000006"
	newPassword = "Brand-new-horse-42"
)

func changeRequest(t *testing.T, ctx context.Context, st stack, method, path, token, body string) (int, string) {
	t.Helper()
	request, err := http.NewRequestWithContext(ctx, method, st.server.URL+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Forwarded-For", "203.0.113.9")
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	raw, _ := io.ReadAll(response.Body)
	return response.StatusCode, string(raw)
}

// changeToken is a routed user access token (EdDSA) of userID in orgID ("" = none), at token_version version.
// The routed api reads the email and the role from the rows, never from the token: changeToken fails unless the
// fixture holds exactly that email and, with an organization, that role.
func changeToken(t *testing.T, st stack, userID, email, orgID, role string, version int64) string {
	t.Helper()
	id, err := uuid.Parse(userID)
	if err != nil || id.String() != userID {
		t.Fatalf("changeToken: user id %q is not a canonical uuid (a routed token's sub has one spelling)", userID)
	}
	var stored string
	if err := st.pool.QueryRow(context.Background(), `SELECT email FROM users WHERE id = $1`, id).Scan(&stored); err != nil || stored != email {
		t.Fatalf("changeToken: users row of %s has email %q (%v), want %q", userID, stored, err, email)
	}
	if orgID != "" {
		var held string
		if err := st.pool.QueryRow(context.Background(), `SELECT role FROM memberships WHERE user_id = $1 AND org_id = $2::uuid`, id, orgID).
			Scan(&held); err != nil || held != role {
			t.Fatalf("changeToken: membership of %s in %s has role %q (%v), want %q", userID, orgID, held, err, role)
		}
	}
	return st.access.MustMint(t, edtokens.Claims{UserID: id, OrgID: orgID, TokenVersion: version})
}

func passwordHashOf(t *testing.T, ctx context.Context, st stack, id string) string {
	t.Helper()
	var hash string
	if err := st.pool.QueryRow(ctx, `SELECT password_hash FROM users WHERE id = $1::uuid`, id).Scan(&hash); err != nil {
		t.Fatal(err)
	}
	return hash
}

func changeBody(current, next string) string {
	return `{"current_password": "` + current + `", "new_password": "` + next + `"}`
}

// TestChangePasswordRefusals pins every refusal: fixed text, no echo of the
// body, and the stored hash untouched.
func TestChangePasswordRefusals(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	t.Setenv("TRUSTED_PROXIES", "127.0.0.1,::1")
	st := startStack(t, ctx)
	orgA := st.specs["alice"]["org_id"].(string)
	alice := changeToken(t, st, changeAlice, "alice@example.com", orgA, "owner", 0)
	before := passwordHashOf(t, ctx, st, changeAlice)
	const marker = "zzMarker-9Qx"

	for _, tc := range []struct {
		name, token, body string
		status            int
		must              string
	}{
		{"anonymous", "", changeBody(sessionscenario.Password, newPassword), http.StatusUnauthorized, ""},
		{"wrong current", alice, changeBody("wrong-"+marker, newPassword), http.StatusForbidden, "Current password verification failed"},
		{"policy", alice, changeBody(sessionscenario.Password, "Short-pw-1x"), http.StatusUnprocessableEntity, "violations"},
		{"too short new", alice, changeBody(sessionscenario.Password, marker[:5]), http.StatusUnprocessableEntity, "Invalid password change request"},
		{"missing current", alice, `{"new_password": "` + newPassword + `"}`, http.StatusUnprocessableEntity, "Invalid password change request"},
	} {
		status, body := changeRequest(t, ctx, st, http.MethodPost, "/api/v1/auth/change-password", tc.token, tc.body)
		if status != tc.status {
			t.Errorf("%s: status %d, want %d (%s)", tc.name, status, tc.status, body)
		}
		if tc.must != "" && !strings.Contains(body, tc.must) {
			t.Errorf("%s: body lacks %q: %s", tc.name, tc.must, body)
		}
		for _, leak := range []string{marker, marker[:5], newPassword, sessionscenario.Password, "Short-pw-1x"} {
			if strings.Contains(body, leak) {
				t.Errorf("%s: response echoes a submitted value", tc.name)
			}
		}
	}
	if passwordHashOf(t, ctx, st, changeAlice) != before {
		t.Error("a refused change moved the password hash")
	}

	// An account without a local password (github) is refused generically. The routed api reads frank's
	// role from the membership row, so the fixture holds the one the token used to claim.
	if _, err := st.pool.Exec(ctx, `INSERT INTO memberships (id, user_id, org_id, role, joined_at, created_at)
		VALUES (gen_random_uuid(), $1::uuid, $2::uuid, 'admin', now(), now())`, changeFrank, orgA); err != nil {
		t.Fatalf("seed frank's membership: %v", err)
	}
	frank := changeToken(t, st, changeFrank, "frank@example.com", orgA, "admin", 0)
	status, body := changeRequest(t, ctx, st, http.MethodPost, "/api/v1/auth/change-password", frank, changeBody(sessionscenario.Password, newPassword))
	if status != http.StatusForbidden || !strings.Contains(body, "managed by your sign-in provider") {
		t.Errorf("managed account: %d %s", status, body)
	}
}

// TestChangePasswordSuccess pins the state the route exists to reach: the
// hash changed, the old password stops working, every other session is
// dead, the caller's fresh pair works, and no other user is touched. It runs
// on the sessions-owned posture production runs (AUTH_PROXY_SESSIONS: every
// pair comes from auth-service's issue route as a real auth-service session and
// a routed user access token naming it), so a session is dead when its
// token_version pin is behind the user's: go-api's session check refuses it and
// auth-service's refresh guard refuses its refresh (compat_guard.go guardRefresh).
func TestChangePasswordSuccess(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	t.Setenv("TRUSTED_PROXIES", "127.0.0.1,::1")
	access := edtokens.New(t)
	login := &pinnedLoginIssuer{tokens: access}
	st := startStackRouted(t, ctx, authmail.Config{}, login, nil, nil, access)
	login.pool, login.rig = st.pool, pinrig.NewPinRig(t, ctx, st.pool, st.pg)
	post := func(path, token, body string) (int, string) {
		return changeRequest(t, ctx, st, http.MethodPost, path, token, body)
	}
	alice := uuid.MustParse(changeAlice)
	live := func(name string, sid uuid.UUID, want bool) {
		t.Helper()
		err := login.rig.Check(ctx, alice, sid)
		if refused := pinrig.Refused(err); refused == want || (!refused && err != nil) {
			t.Errorf("%s: session check err = %v, want live = %v", name, err, want)
		}
	}

	// Two sessions of alice: "other" (a second device) and "current".
	status, loginBody := post("/api/v1/auth/login", "", `{"email": "alice@example.com", "password": "`+sessionscenario.Password+`"}`)
	if status != http.StatusOK {
		t.Fatalf("login other: %d %s", status, loginBody)
	}
	otherAccess, otherSession := sessionscenario.Field(loginBody, "access_token"), login.last(t)
	status, loginBody = post("/api/v1/auth/login", "", `{"email": "alice@example.com", "password": "`+sessionscenario.Password+`"}`)
	if status != http.StatusOK {
		t.Fatalf("login current: %d %s", status, loginBody)
	}
	currentAccess, currentSession := sessionscenario.Field(loginBody, "access_token"), login.last(t)
	// Both sessions are live before the change.
	live("other session before the change", otherSession, true)
	live("current session before the change", currentSession, true)
	if status, b := changeRequest(t, ctx, st, http.MethodGet, "/api/v1/auth/me", otherAccess, ""); status != http.StatusOK {
		t.Fatalf("other access token before the change: %d %s", status, b)
	}
	// The session continues in the organization it signed in with.
	_, meBefore := changeRequest(t, ctx, st, http.MethodGet, "/api/v1/auth/me", currentAccess, "")
	sessionOrg := sessionscenario.Field(meBefore, "org_id")
	if sessionOrg == "" {
		t.Fatalf("me answered no org_id: %s", meBefore)
	}
	bobBefore := passwordHashOf(t, ctx, st, changeBob)
	aliceBefore := passwordHashOf(t, ctx, st, changeAlice)

	status, body := post("/api/v1/auth/change-password", currentAccess, changeBody(sessionscenario.Password, newPassword))
	if status != http.StatusOK {
		t.Fatalf("change: %d %s", status, body)
	}
	newAccess, newRefresh, freshSession := sessionscenario.Field(body, "access_token"), sessionscenario.Field(body, "refresh_token"), login.last(t)
	if newAccess == "" || newRefresh == "" {
		t.Fatalf("change answered no token pair: %s", body)
	}
	if strings.Contains(body, newPassword) || strings.Contains(body, sessionscenario.Password) {
		t.Error("success body echoes a password")
	}
	if passwordHashOf(t, ctx, st, changeAlice) == aliceBefore {
		t.Error("the password hash did not change")
	}
	if passwordHashOf(t, ctx, st, changeBob) != bobBefore {
		t.Error("another user's hash changed")
	}

	if status, _ := post("/api/v1/auth/login", "", `{"email": "alice@example.com", "password": "`+sessionscenario.Password+`"}`); status != http.StatusUnauthorized {
		t.Errorf("old password login: %d, want 401", status)
	}
	if status, b := post("/api/v1/auth/login", "", `{"email": "alice@example.com", "password": "`+newPassword+`"}`); status != http.StatusOK {
		t.Errorf("new password login: %d %s", status, b)
	}
	// Other sessions: the auth-service session is dead (its refresh is refused), the access token stale.
	live("other session after the change", otherSession, false)
	live("pre-change current session after the change", currentSession, false)
	for name, token := range map[string]string{"other": otherAccess, "pre-change current": currentAccess} {
		if status, _ := changeRequest(t, ctx, st, http.MethodGet, "/api/v1/auth/me", token, ""); status != http.StatusUnauthorized {
			t.Errorf("%s access token after change: %d, want 401", name, status)
		}
	}
	// The caller's fresh pair keeps working, in the same organization.
	status, me := changeRequest(t, ctx, st, http.MethodGet, "/api/v1/auth/me", newAccess, "")
	if status != http.StatusOK || sessionscenario.Field(me, "org_id") != sessionOrg {
		t.Errorf("fresh access token: %d %s", status, me)
	}
	live("fresh session after the change", freshSession, true)

	var audits int
	if err := st.pool.QueryRow(ctx, `SELECT count(*) FROM audit_logs WHERE action = 'password_changed' AND user_id = $1::uuid`, changeAlice).Scan(&audits); err != nil {
		t.Fatal(err)
	}
	if audits != 1 {
		t.Errorf("password_changed audit rows: %d, want 1", audits)
	}
	var changes []byte
	_ = st.pool.QueryRow(ctx, `SELECT coalesce(changes::text, '') FROM audit_logs WHERE action = 'password_changed' AND user_id = $1::uuid`, changeAlice).Scan(&changes)
	if bytes.Contains(changes, []byte(newPassword)) {
		t.Error("audit row carries the password")
	}
}

// pinnedLoginIssuer is auth-service's session issue route as go-api's sign-in routes call it with sessions
// owned by auth-service: each pair is a new auth-service session (pinned to the principal's token_version
// now) and a routed user access token (EdDSA) of tokens naming that session.
type pinnedLoginIssuer struct {
	tokens *edtokens.Issuer
	pool   *pgxpool.Pool
	rig    *pinrig.PinRig
	mu     sync.Mutex
	issued []uuid.UUID
}

func (i *pinnedLoginIssuer) Issue(ctx context.Context, _ *http.Request, req sessionissue.Request) (sessionissue.Pair, error) {
	var version int64
	if err := i.pool.QueryRow(ctx, `SELECT token_version FROM users WHERE id = $1`, req.PrincipalID).Scan(&version); err != nil {
		return sessionissue.Pair{}, err
	}
	if _, err := i.pool.Exec(ctx, `INSERT INTO auth.principals (id, kind, display_name) VALUES ($1, 'user', 'pin-rig') ON CONFLICT (id) DO NOTHING`, req.PrincipalID); err != nil {
		return sessionissue.Pair{}, err
	}
	created, err := i.rig.Sessions().Create(ctx, req.PrincipalID, nil, []string{"pwd"})
	if err != nil {
		return sessionissue.Pair{}, err
	}
	sid := created.Session.ID
	claims := edtokens.Claims{UserID: req.PrincipalID, TokenVersion: version, SessionID: &sid}
	if req.OrganizationID != nil {
		claims.OrgID = req.OrganizationID.String()
	}
	access, err := i.tokens.Mint(claims)
	if err != nil {
		return sessionissue.Pair{}, err
	}
	i.mu.Lock()
	i.issued = append(i.issued, sid)
	i.mu.Unlock()
	return sessionissue.Pair{Access: access, Refresh: "dhr1_" + uuid.NewString(), ExpiresIn: 3600}, nil
}

// last is the session of the newest pair.
func (i *pinnedLoginIssuer) last(t *testing.T) uuid.UUID {
	t.Helper()
	i.mu.Lock()
	defer i.mu.Unlock()
	if len(i.issued) == 0 {
		t.Fatal("the issue route was not called")
	}
	return i.issued[len(i.issued)-1]
}

// TestChangePasswordRateLimit pins the per-user budget: 5 an hour.
func TestChangePasswordRateLimit(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	t.Setenv("TRUSTED_PROXIES", "127.0.0.1,::1")
	st := startStack(t, ctx)
	alice := changeToken(t, st, changeAlice, "alice@example.com", st.specs["alice"]["org_id"].(string), "owner", 0)
	var last int
	for i := 0; i < 6; i++ {
		last, _ = changeRequest(t, ctx, st, http.MethodPost, "/api/v1/auth/change-password", alice, changeBody("wrong-current-pw", newPassword))
		if i < 5 && last != http.StatusForbidden {
			t.Fatalf("attempt %d: %d, want 403", i+1, last)
		}
	}
	if last != http.StatusTooManyRequests {
		t.Errorf("sixth attempt: %d, want 429", last)
	}
}

// TestChangePasswordLimitSharesOneBucketPerUser: the budget is the user's, not the session's: a token of
// another session of the same user counts against the same per-user budget. A routed token's sub has one
// spelling, so another spelling of the subject (braces) is refused at the token contract, before the limit.
func TestChangePasswordLimitSharesOneBucketPerUser(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	t.Setenv("TRUSTED_PROXIES", "127.0.0.1,::1")
	st := startStack(t, ctx)
	org := st.specs["alice"]["org_id"].(string)
	plain := changeToken(t, st, changeAlice, "alice@example.com", org, "owner", 0)
	otherSession := changeToken(t, st, changeAlice, "alice@example.com", org, "owner", 0)
	braced := st.access.MustMint(t, edtokens.Claims{UserID: uuid.MustParse(changeAlice), OrgID: org, Subject: "{" + changeAlice + "}"})
	if status, body := changeRequest(t, ctx, st, http.MethodPost, "/api/v1/auth/change-password", braced, changeBody("wrong-current-pw", newPassword)); status != http.StatusUnauthorized ||
		!strings.Contains(body, "Invalid or expired token") {
		t.Errorf("braced subject: %d %s, want the 401 token refusal", status, body)
	}
	for i := 0; i < 5; i++ {
		if status, _ := changeRequest(t, ctx, st, http.MethodPost, "/api/v1/auth/change-password", plain, changeBody("wrong-current-pw", newPassword)); status != http.StatusForbidden {
			t.Fatalf("attempt %d: %d, want 403 (the refused braced token spent no budget)", i+1, status)
		}
	}
	if status, _ := changeRequest(t, ctx, st, http.MethodPost, "/api/v1/auth/change-password", otherSession, changeBody("wrong-current-pw", newPassword)); status != http.StatusTooManyRequests {
		t.Errorf("another session's sixth attempt: %d, want 429", status)
	}
}

// TestChangePasswordKeepsOnboardingFlag: a user with no organization still needs onboarding
// after the change, as after a login.
func TestChangePasswordKeepsOnboardingFlag(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	t.Setenv("TRUSTED_PROXIES", "127.0.0.1,::1")
	st := startStack(t, ctx)
	// Heidi (seeded, local, no membership).
	const heidi = "10000000-0000-4000-8000-000000000008"
	token := changeToken(t, st, heidi, "heidi@example.com", "", "member", 0)
	status, body := changeRequest(t, ctx, st, http.MethodPost, "/api/v1/auth/change-password", token, changeBody(sessionscenario.Password, newPassword))
	if status != http.StatusOK {
		t.Fatalf("change: %d %s", status, body)
	}
	if !strings.Contains(body, `"needs_onboarding":true`) {
		t.Errorf("orgless user lost needs_onboarding: %s", body)
	}
}
