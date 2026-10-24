package authmail

import (
	"context"
	"io"
	"log/slog"
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
