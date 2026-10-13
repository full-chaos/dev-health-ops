//go:build integration

package session_test

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
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

func changeToken(t *testing.T, st stack, userID, email, orgID, role string, version int64) string {
	t.Helper()
	token, err := st.signer.Access(edgetoken.AccessClaims{UserID: userID, Email: email, OrgID: orgID, Role: role, TokenVersion: version},
		time.Now(), "00000000-0000-4000-8000-0000000000aa")
	if err != nil {
		t.Fatal(err)
	}
	return token
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

	// An account without a local password (github) is refused generically.
	frank := changeToken(t, st, changeFrank, "frank@example.com", st.specs["alice"]["org_id"].(string), "admin", 0)
	status, body := changeRequest(t, ctx, st, http.MethodPost, "/api/v1/auth/change-password", frank, changeBody(sessionscenario.Password, newPassword))
	if status != http.StatusForbidden || !strings.Contains(body, "managed by your sign-in provider") {
		t.Errorf("managed account: %d %s", status, body)
	}
}

// TestChangePasswordSuccess pins the state the route exists to reach: the
// hash changed, the old password stops working, every other session is
// dead, the caller's fresh pair works, and no other user is touched.
func TestChangePasswordSuccess(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	t.Setenv("TRUSTED_PROXIES", "127.0.0.1,::1")
	st := startStack(t, ctx)
	post := func(path, token, body string) (int, string) {
		return changeRequest(t, ctx, st, http.MethodPost, path, token, body)
	}

	// Two sessions of alice: "other" (a second device) and "current".
	status, loginBody := post("/api/v1/auth/login", "", `{"email": "alice@example.com", "password": "`+sessionscenario.Password+`"}`)
	if status != http.StatusOK {
		t.Fatalf("login other: %d %s", status, loginBody)
	}
	otherAccess, otherRefresh := sessionscenario.Field(loginBody, "access_token"), sessionscenario.Field(loginBody, "refresh_token")
	status, loginBody = post("/api/v1/auth/login", "", `{"email": "alice@example.com", "password": "`+sessionscenario.Password+`"}`)
	if status != http.StatusOK {
		t.Fatalf("login current: %d %s", status, loginBody)
	}
	currentAccess := sessionscenario.Field(loginBody, "access_token")
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
	newAccess, newRefresh := sessionscenario.Field(body, "access_token"), sessionscenario.Field(body, "refresh_token")
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
	// Other sessions: refresh revoked, access token stale.
	if status, _ := post("/api/v1/auth/refresh", "", `{"refresh_token": "`+otherRefresh+`"}`); status != http.StatusUnauthorized {
		t.Errorf("other session refresh: %d, want 401", status)
	}
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
	if status, b := post("/api/v1/auth/refresh", "", `{"refresh_token": "`+newRefresh+`"}`); status != http.StatusOK {
		t.Errorf("fresh refresh token: %d %s", status, b)
	}

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
