package mail

import (
	"context"
	"os"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/proxycapture"
)

// The child sets the provider variables itself: the parent's environment is not changed.
func TestTheDefaultResendClientNeverDialsTheEnvironmentProxy(t *testing.T) {
	proxycapture.Check(t, func(ctx context.Context) {
		for name, value := range map[string]string{
			"EMAIL_PROVIDER": "resend", "EMAIL_FROM_ADDRESS": "noreply@example.com",
			"EMAIL_API_KEY": proxycapture.Canary, "RESEND_API_BASE_URL": "http://upstream.invalid",
		} {
			if err := os.Setenv(name, value); err != nil {
				panic(err)
			}
		}
		sender, err := NewSenderFromEnv(nil)
		if err != nil {
			panic(err)
		}
		_ = sender.Send(ctx, Message{To: "someone@example.com", Subject: "s", HTML: "<p>h</p>"})
	}).AssertNothingReached(t)
}
