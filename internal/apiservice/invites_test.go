package apiservice

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/mail"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

func inviteLookup(values map[string]string) func(string) (string, bool) {
	return func(name string) (string, bool) { value, ok := values[name]; return value, ok }
}

func TestInviteConfigAppBaseURLSetVersusUnset(t *testing.T) {
	t.Setenv("EMAIL_PROVIDER", "console")
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	unset := inviteConfig(config.Config{}, logger, inviteLookup(nil))
	if unset.AppBaseURLSet {
		t.Error("an unset APP_BASE_URL reported as set")
	}
	empty := inviteConfig(config.Config{}, logger, inviteLookup(map[string]string{"APP_BASE_URL": ""}))
	if !empty.AppBaseURLSet || empty.AppBaseURL != "" {
		t.Errorf("a set-but-empty APP_BASE_URL: set=%v value=%q", empty.AppBaseURLSet, empty.AppBaseURL)
	}
}

// A mail misconfiguration must not stop the api: the sender is replaced by
// one that fails every send with the cause, so each invite logs a real reason.
func TestInviteConfigUnusableProviderDoesNotFailStartup(t *testing.T) {
	t.Setenv("EMAIL_PROVIDER", "resend")
	t.Setenv("EMAIL_API_KEY", "")
	t.Setenv("RESEND_API_KEY", "")
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	got := inviteConfig(config.Config{}, logger, inviteLookup(nil))
	if got.Mail == nil {
		t.Fatal("no sender at all: invites would silently skip the email instead of logging why")
	}
	if err := got.Mail.Send(context.Background(), mail.Message{To: "a@b.test", Subject: "s", HTML: "x"}); err == nil {
		t.Fatal("an unusable provider's sender accepted a message")
	}
}

func TestInviteConfigCarriesTheLinkSecrets(t *testing.T) {
	t.Setenv("EMAIL_PROVIDER", "console")
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := config.Config{
		APIJWTSecret: secrets.NewValue("jwt-key"), LinkTokenSecret: secrets.NewValue("link-new"),
		LinkTokenSecretPrevious: secrets.NewValue("link-old"),
	}
	got := inviteConfig(cfg, logger, inviteLookup(nil))
	if got.Secret() != "link-new" {
		t.Errorf("signing secret %q, want the link secret", got.Secret())
	}
	if accepts := got.AcceptSecrets(); len(accepts) != 2 || accepts[0] != "link-new" || accepts[1] != "link-old" {
		t.Errorf("accepted secrets %v, want link-new, link-old", accepts)
	}
}
