//go:build integration

package session_test

import (
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

// TestChangePasswordLimitSharesOneBucketPerUser: an equivalent spelling of the subject UUID
// (braces) counts against the same per-user budget.
func TestChangePasswordLimitSharesOneBucketPerUser(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	t.Setenv("TRUSTED_PROXIES", "127.0.0.1,::1")
	st := startStack(t, ctx)
	org := st.specs["alice"]["org_id"].(string)
	plain := changeToken(t, st, changeAlice, "alice@example.com", org, "owner", 0)
	braced := changeToken(t, st, "{"+changeAlice+"}", "alice@example.com", org, "owner", 0)
	for i := 0; i < 5; i++ {
		if status, _ := changeRequest(t, ctx, st, http.MethodPost, "/api/v1/auth/change-password", plain, changeBody("wrong-current-pw", newPassword)); status != http.StatusForbidden {
			t.Fatalf("attempt %d: %d, want 403", i+1, status)
		}
	}
	if status, _ := changeRequest(t, ctx, st, http.MethodPost, "/api/v1/auth/change-password", braced, changeBody("wrong-current-pw", newPassword)); status != http.StatusTooManyRequests {
		t.Errorf("braced subject sixth attempt: %d, want 429", status)
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
