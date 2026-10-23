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

	"github.com/full-chaos/dev-health-ops/internal/api/authmail"
	"github.com/full-chaos/dev-health-ops/internal/mail"
)

type captureMail struct {
	mu   sync.Mutex
	sent []mail.Message
}

func (c *captureMail) Name() string { return "capture" }

func (c *captureMail) Send(_ context.Context, message mail.Message) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sent = append(c.sent, message)
	return nil
}

func (c *captureMail) waitFor(t *testing.T, to, subject string) mail.Message {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		c.mu.Lock()
		for _, message := range c.sent {
			if message.To == to && message.Subject == subject {
				c.mu.Unlock()
				return message
			}
		}
		c.mu.Unlock()
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("no %q mail to %s", subject, to)
	return mail.Message{}
}

func (c *captureMail) count(to string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, message := range c.sent {
		if message.To == to {
			n++
		}
	}
	return n
}

func registerPost(t *testing.T, ctx context.Context, st stack, body string) (int, string) {
	t.Helper()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, st.server.URL+"/api/v1/auth/register", bytes.NewBufferString(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Origin", "http://localhost:3000")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	raw, _ := io.ReadAll(response.Body)
	return response.StatusCode, string(raw)
}

func TestRegisterExistingEmailAnswersLikeNewEmail(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	capture := &captureMail{}
	st := startStackWithMail(t, ctx, authmail.Config{Mail: capture, AppBaseURL: "https://app.example.test", AppBaseURLSet: true})
	count := func(table string) (n int) {
		if err := st.pool.QueryRow(ctx, `SELECT count(*) FROM `+table).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	usersBefore, orgsBefore := count("users"), count("organizations")

	newStatus, newBody := registerPost(t, ctx, st, `{"email": "fresh.person@example.com", "password": "Fresh-pass-2026"}`)
	st.openWindows()
	oldStatus, oldBody := registerPost(t, ctx, st, `{"email": "ALICE@example.com", "password": "Another-pass-9"}`)

	if newStatus != http.StatusAccepted || oldStatus != http.StatusAccepted || newBody != oldBody {
		t.Fatalf("new %d %s, existing %d %s: want the same 202 answer", newStatus, newBody, oldStatus, oldBody)
	}
	for _, leak := range []string{"user_id", "org_id", "already", "alice"} {
		if strings.Contains(strings.ToLower(oldBody), leak) {
			t.Errorf("answer carries %q: %s", leak, oldBody)
		}
	}
	if count("users") != usersBefore+1 || count("organizations") != orgsBefore+1 {
		t.Errorf("rows: users %d->%d orgs %d->%d: only the new address may add a user and an org", usersBefore, count("users"), orgsBefore, count("organizations"))
	}
	capture.waitFor(t, "fresh.person@example.com", "Verify your email address")
	notice := capture.waitFor(t, "alice@example.com", "You already have an account")
	if strings.Contains(notice.HTML, "Another-pass-9") || strings.Contains(notice.HTML, "token=") {
		t.Errorf("notice carries the submitted password or a link token: %s", notice.HTML)
	}
	if capture.count("alice@example.com") != 1 {
		t.Errorf("existing owner got %d mails, want 1", capture.count("alice@example.com"))
	}
}
