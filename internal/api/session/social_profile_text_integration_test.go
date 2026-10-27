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

// CHAOS-11678: a lone surrogate in the free text of a social profile. The name is stored with U+FFFD; the email
// is no usable address: the unverified-email answer, nothing written.
func TestSocialLoginWithASurrogateInTheProfileText(t *testing.T) {
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
	count := func(query string, args ...any) int {
		t.Helper()
		var n int
		if err := st.pool.QueryRow(ctx, query, args...).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	usersBefore := count(`SELECT count(*) FROM users`)

	status, body := post("gh-surrogate-email")
	if status != http.StatusForbidden || body != `{"detail":{"message":"The provider has not verified this email address"}}` {
		t.Errorf("surrogate email: %d %s, want the 403 unverified-email answer", status, body)
	}
	if n := count(`SELECT count(*) FROM users`); n != usersBefore {
		t.Errorf("a refused sign-in wrote users: %d -> %d", usersBefore, n)
	}

	status, body = post("gh-surrogate-name")
	if status != http.StatusOK || !strings.Contains(body, `"is_new_user":true`) {
		t.Fatalf("surrogate name: %d %s, want a 200 new user", status, body)
	}
	var name, username *string
	if err := st.pool.QueryRow(ctx, `SELECT full_name, username FROM users WHERE email = 'sur-name@example.com'`).Scan(&name, &username); err != nil {
		t.Fatal(err)
	}
	if name == nil || *name != "Sur�" || username == nil || *username != "sur�" {
		t.Errorf("stored name %v username %v, want the surrogates replaced by U+FFFD", name, username)
	}
}
