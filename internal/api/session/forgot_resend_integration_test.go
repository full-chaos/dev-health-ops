//go:build integration

package session_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/authmail"
)

func postFrom(t *testing.T, ctx context.Context, st stack, path, body, from string) (int, string) {
	t.Helper()
	status, text, _ := postFromWithHeaders(t, ctx, st, path, body, from)
	return status, text
}

// postFromWithHeaders also returns the response headers a caller can compare:
// the per-request ones (date, request id) are left out.
func postFromWithHeaders(t *testing.T, ctx context.Context, st stack, path, body, from string) (int, string, map[string]string) {
	t.Helper()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, st.server.URL+path, bytes.NewBufferString(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Origin", "http://localhost:3000")
	request.Header.Set("X-Forwarded-For", from)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	raw, _ := io.ReadAll(response.Body)
	headers := map[string]string{}
	for name, values := range response.Header {
		switch strings.ToLower(name) {
		case "date", "x-request-id":
			continue
		}
		headers[strings.ToLower(name)] = strings.Join(values, ",")
	}
	return response.StatusCode, string(raw), headers
}

func TestForgotAndResendAnswerKnownAndUnknownAlike(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	t.Setenv("TRUSTED_PROXIES", "127.0.0.1,::1")
	capture := &captureMail{}
	st := startStackWithMail(t, ctx, authmail.Config{Mail: capture, AppBaseURL: "https://app.example.test", AppBaseURLSet: true})
	pendingPassword := fmt.Sprintf("Pw-%d-xYz", time.Now().UnixNano())
	if status, body := registerPost(t, ctx, st, fmt.Sprintf(`{"email": "pending.person@example.com", "password": %q}`, pendingPassword), "203.0.113.50"); status != http.StatusAccepted {
		t.Fatalf("register: %d %s", status, body)
	}
	capture.waitFor(t, "pending.person@example.com", "Verify your email address")

	ip := 0
	answer := func(path, email string) (int, string, map[string]string) {
		ip++
		return postFromWithHeaders(t, ctx, st, path, fmt.Sprintf(`{"email": %q}`, email), fmt.Sprintf("203.0.113.%d", 100+ip))
	}
	for _, path := range []string{"/api/v1/auth/resend-verification", "/api/v1/auth/forgot-password"} {
		var first struct {
			status  int
			body    string
			headers map[string]string
		}
		for index, email := range []string{"pending.person@example.com", "nobody.here@example.com", "alice@example.com"} {
			status, body, headers := answer(path, email)
			if status != http.StatusOK {
				t.Fatalf("%s %s: %d %s, want 200", path, email, status, body)
			}
			if index == 0 {
				first.status, first.body, first.headers = status, body, headers
			} else if status != first.status || body != first.body || !reflect.DeepEqual(headers, first.headers) {
				t.Errorf("%s %s answers %d %s %v, the first address %d %s %v: want one answer, headers included", path, email, status, body, headers, first.status, first.body, first.headers)
			}
		}
	}
	// Only an account the route acts on gets mail and a token row; the unknown address gets nothing.
	capture.waitFor(t, "pending.person@example.com", "Reset your password")
	capture.waitFor(t, "alice@example.com", "Reset your password")
	time.Sleep(time.Second)
	if n := capture.count("nobody.here@example.com"); n != 0 {
		t.Errorf("an unknown address got %d mails", n)
	}
	var aliceTokens, nobodyTokens int
	if err := st.pool.QueryRow(ctx, `SELECT count(*) FROM password_reset_tokens t JOIN users u ON u.id = t.user_id WHERE u.email = 'alice@example.com'`).Scan(&aliceTokens); err != nil {
		t.Fatal(err)
	}
	if err := st.pool.QueryRow(ctx, `SELECT count(*) FROM password_reset_tokens t JOIN users u ON u.id = t.user_id WHERE u.email = 'nobody.here@example.com'`).Scan(&nobodyTokens); err != nil {
		t.Fatal(err)
	}
	if aliceTokens != 1 || nobodyTokens != 0 {
		t.Errorf("reset tokens: alice %d (want 1), unknown %d (want 0)", aliceTokens, nobodyTokens)
	}
	if n := capture.count("alice@example.com"); n != 1 {
		t.Errorf("alice (verified) got %d mails, want only the reset mail", n)
	}
}
