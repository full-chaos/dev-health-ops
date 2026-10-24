//go:build integration

package session_test

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestSocialLoginNeverLinksAnAccountByEmail(t *testing.T) {
	t.Setenv("TRUSTED_PROXIES", "127.0.0.1,::1")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	st := startStack(t, ctx)
	post := func(provider, token string) (int, string) {
		request, err := http.NewRequestWithContext(ctx, http.MethodPost, st.server.URL+"/api/v1/auth/social-login",
			strings.NewReader(`{"provider": "`+provider+`", "provider_access_token": "`+token+`"}`))
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("X-Forwarded-For", "203.0.113.9")
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		raw, _ := io.ReadAll(response.Body)
		return response.StatusCode, string(raw)
	}
	providerID := func(email string) (provider, id string) {
		var p, i *string
		if err := st.pool.QueryRow(ctx, `SELECT auth_provider, auth_provider_id FROM users WHERE email = $1`, email).Scan(&p, &i); err != nil {
			t.Fatal(err)
		}
		if p != nil {
			provider = *p
		}
		if i != nil {
			id = *i
		}
		return provider, id
	}
	loginCount := func(email string) int {
		var n int
		if err := st.pool.QueryRow(ctx, `SELECT count(*) FROM users WHERE email = $1 AND last_login_at IS NOT NULL`, email).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}

	for _, tc := range []struct {
		name, token, email, wantProvider, wantID string
	}{
		{"password account, new provider identity", "gh-alice", "alice@example.com", "local", ""},
		{"same provider, other subject", "gh-frank-new-id", "frank@example.com", "github", "100"},
		{"same provider, no stored subject", "gh-olga", "olga@example.com", "github", ""},
	} {
		status, body := post("github", tc.token)
		if status != http.StatusConflict {
			t.Errorf("%s: status %d %s, want 409", tc.name, status, body)
		}
		if strings.Contains(body, "access_token") || strings.Contains(body, "refresh_token") {
			t.Errorf("%s: the refusal carries a token: %s", tc.name, body)
		}
		if body != `{"detail":{"message":"An account with this email already exists. Please sign in with your existing sign-in method."}}` || strings.Contains(body, "existing_provider") {
			t.Errorf("%s: body %s, want the one answer that names no sign-in method", tc.name, body)
		}
		if provider, id := providerID(tc.email); provider != tc.wantProvider || id != tc.wantID {
			t.Errorf("%s: row is now %q/%q, want %q/%q", tc.name, provider, id, tc.wantProvider, tc.wantID)
		}
	}
	if loginCount("olga@example.com") != 0 {
		t.Error("a refused social sign-in recorded a login")
	}

	status, body := post("github", "gh-frank")
	if status != http.StatusOK || !strings.Contains(body, `"is_new_user":false`) || !strings.Contains(body, "frank@example.com") {
		t.Errorf("a linked identity no longer signs in: %d %s", status, body)
	}
}

func TestSocialLoginRefusesAnUnverifiedProviderEmailAlike(t *testing.T) {
	t.Setenv("TRUSTED_PROXIES", "127.0.0.1,::1")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	st := startStack(t, ctx)
	post := func(token string) (int, string) {
		request, err := http.NewRequestWithContext(ctx, http.MethodPost, st.server.URL+"/api/v1/auth/social-login",
			strings.NewReader(`{"provider": "gitlab", "provider_access_token": "`+token+`"}`))
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("X-Forwarded-For", "203.0.113.9")
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		raw, _ := io.ReadAll(response.Body)
		return response.StatusCode, string(raw)
	}
	var usersBefore int
	if err := st.pool.QueryRow(ctx, `SELECT count(*) FROM users`).Scan(&usersBefore); err != nil {
		t.Fatal(err)
	}
	existingStatus, existingBody := post("gl-unverified-frank")
	newStatus, newBody := post("gl-unverified-new")
	if existingStatus != http.StatusForbidden || newStatus != http.StatusForbidden {
		t.Fatalf("unverified e-mail: existing account %d %s, no account %d %s, want 403 both", existingStatus, existingBody, newStatus, newBody)
	}
	if existingBody != newBody {
		t.Errorf("the answer depends on whether the account exists:\n existing %s\n none     %s", existingBody, newBody)
	}
	if strings.Contains(existingBody, "github") || strings.Contains(existingBody, "existing_provider") {
		t.Errorf("the refusal names the existing sign-in method: %s", existingBody)
	}
	var usersAfter, frankIDs int
	if err := st.pool.QueryRow(ctx, `SELECT count(*) FROM users`).Scan(&usersAfter); err != nil {
		t.Fatal(err)
	}
	if err := st.pool.QueryRow(ctx, `SELECT count(*) FROM users WHERE email = 'frank@example.com' AND auth_provider = 'github' AND auth_provider_id = '100'`).Scan(&frankIDs); err != nil {
		t.Fatal(err)
	}
	if usersAfter != usersBefore || frankIDs != 1 {
		t.Errorf("a refused sign-in wrote: users %d -> %d, frank row intact %d", usersBefore, usersAfter, frankIDs)
	}
}

func TestSocialConflictAnswerNamesNoSignInMethod(t *testing.T) {
	t.Setenv("TRUSTED_PROXIES", "127.0.0.1,::1")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	st := startStack(t, ctx)
	post := func(token string) (int, string) {
		request, err := http.NewRequestWithContext(ctx, http.MethodPost, st.server.URL+"/api/v1/auth/social-login",
			strings.NewReader(`{"provider": "github", "provider_access_token": "`+token+`"}`))
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("X-Forwarded-For", "203.0.113.9")
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		raw, _ := io.ReadAll(response.Body)
		return response.StatusCode, string(raw)
	}
	// A password account, a github account with another subject and one with no subject.
	var first string
	for index, token := range []string{"gh-alice", "gh-frank-new-id", "gh-olga"} {
		status, body := post(token)
		if status != http.StatusConflict {
			t.Fatalf("%s: %d %s, want 409", token, status, body)
		}
		for _, leak := range []string{"existing_provider", "local", "github", "password"} {
			if strings.Contains(strings.ToLower(strings.ReplaceAll(body, "this email already exists", "")), leak) {
				t.Errorf("%s: the answer names %q: %s", token, leak, body)
			}
		}
		if index == 0 {
			first = body
		} else if body != first {
			t.Errorf("%s: answer differs by the account that holds the email:\n %s\n %s", token, first, body)
		}
	}
}
