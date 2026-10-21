package apiservice

import (
	"context"
	"io"
	"log/slog"
	"os"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/mail"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/proxycapture"
)

func TestTheInviteMailClientNeverDialsTheEnvironmentProxy(t *testing.T) {
	proxycapture.Check(t, func(ctx context.Context) {
		for name, value := range map[string]string{
			"EMAIL_PROVIDER": "resend", "EMAIL_FROM_ADDRESS": "noreply@example.com",
			"EMAIL_API_KEY": proxycapture.Canary, "RESEND_API_BASE_URL": "http://upstream.invalid",
		} {
			if err := os.Setenv(name, value); err != nil {
				panic(err)
			}
		}
		invites := inviteConfig(config.Config{}, slog.New(slog.NewTextHandler(io.Discard, nil)), func(string) (string, bool) { return "", false })
		_ = invites.Mail.Send(ctx, mail.Message{To: "someone@example.com", Subject: "s", HTML: "<p>h</p>"})
	}).AssertNothingReached(t)
}
