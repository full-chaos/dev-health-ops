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
		status, body := post(tc.token)
		if status != http.StatusConflict {
			t.Errorf("%s: status %d %s, want 409", tc.name, status, body)
		}
		if strings.Contains(body, "access_token") || strings.Contains(body, "refresh_token") {
			t.Errorf("%s: the refusal carries a token: %s", tc.name, body)
		}
		if !strings.Contains(body, `"existing_provider":"`+tc.wantProvider+`"`) {
			t.Errorf("%s: body %s, want existing_provider %q", tc.name, body, tc.wantProvider)
		}
		if provider, id := providerID(tc.email); provider != tc.wantProvider || id != tc.wantID {
			t.Errorf("%s: row is now %q/%q, want %q/%q", tc.name, provider, id, tc.wantProvider, tc.wantID)
		}
	}
	if loginCount("olga@example.com") != 0 {
		t.Error("a refused social sign-in recorded a login")
	}

	status, body := post("gh-frank")
	if status != http.StatusOK || !strings.Contains(body, `"is_new_user":false`) || !strings.Contains(body, "frank@example.com") {
		t.Errorf("a linked identity no longer signs in: %d %s", status, body)
	}
}
