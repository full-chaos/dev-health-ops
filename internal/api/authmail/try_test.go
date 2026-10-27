package authmail

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/mail"
)

type countingSender struct{ sent atomic.Int32 }

func (s *countingSender) Name() string { return "counting" }
func (s *countingSender) Send(context.Context, mail.Message) error {
	s.sent.Add(1)
	return nil
}

func TestTryHonoursItsContextAndSendDoesNot(t *testing.T) {
	sender := &countingSender{}
	config := Config{Mail: sender}
	values := map[string]string{"full_name": "x", "verification_url": "https://example.test/v"}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := config.Try(cancelled, "a@example.com", "s", "verification", values); err == nil || sender.sent.Load() != 0 {
		t.Fatalf("Try on a cancelled context: err %v, sent %d, want an error and no send", err, sender.sent.Load())
	}
	config.Send(cancelled, slog.New(slog.NewTextHandler(io.Discard, nil)), "f", "a@example.com", "s", "verification", values)
	if sender.sent.Load() != 1 {
		t.Errorf("Send must outlive a cancelled request: sent %d, want 1", sender.sent.Load())
	}
}

type capturingSender struct{ got []mail.Message }

func (s *capturingSender) Name() string { return "capturing" }
func (s *capturingSender) Send(_ context.Context, m mail.Message) error {
	s.got = append(s.got, m)
	return nil
}

// The go-api mail path (register, forgot, resend, invite) escapes a display name in the body and leaves the subject alone.
func TestTryEscapesTheDisplayNameInTheBody(t *testing.T) {
	sender := &capturingSender{}
	config := Config{Mail: sender}
	values := map[string]string{"full_name": `<b onclick="x()">Pat & 'Co'</b>`, "verification_url": "https://example.test/v?a=1&b=2"}
	if err := config.Try(context.Background(), "a@example.com", "Verify your email address", "verification", values); err != nil {
		t.Fatal(err)
	}
	if len(sender.got) != 1 {
		t.Fatalf("sent %d, want 1", len(sender.got))
	}
	body := sender.got[0].HTML
	if strings.Contains(body, "<b onclick") || !strings.Contains(body, "&lt;b onclick=&#34;x()&#34;&gt;Pat &amp; &#39;Co&#39;&lt;/b&gt;") {
		t.Fatalf("name not escaped:\n%s", body)
	}
	if sender.got[0].Subject != "Verify your email address" {
		t.Fatalf("subject changed: %q", sender.got[0].Subject)
	}
}
