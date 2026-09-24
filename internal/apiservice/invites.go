package apiservice

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/apiservice/admin"
	"github.com/full-chaos/dev-health-ops/internal/mail"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
)

// inviteMailHTTPTimeout bounds the Resend transport's HTTP calls; the SMTP
// transport is bounded by the caller's context.
const inviteMailHTTPTimeout = 30 * time.Second

// inviteConfig builds create_org_invite's configuration from the process
// environment the way invites.py reads it at send time: the token secret is
// JWT_SECRET_KEY, else SETTINGS_ENCRYPTION_KEY (else the route's own
// last-resort default); the accept link base is APP_BASE_URL; the email
// sender is chosen by EMAIL_PROVIDER exactly as the worker's billing sender
// is (internal/mail).
//
// A mail misconfiguration must NOT stop the api from starting -- Python
// resolved its provider per send and logged a failure without surfacing it --
// so an unusable sender is logged once here and every invite then logs its
// own send failure, never failing the request.
func inviteConfig(cfg config.Config, logger *slog.Logger, lookup func(string) (string, bool)) admin.InviteConfig {
	out := admin.InviteConfig{}
	if secret := cfg.APIJWTSecret.Reveal(); secret != "" {
		out.TokenSecret = secret
	} else if key := cfg.SettingsEncryptionKey.Reveal(); key != "" {
		out.TokenSecret = key
	}
	out.AppBaseURL, out.AppBaseURLSet = lookup("APP_BASE_URL")
	sender, err := mail.NewSenderFromEnv(&http.Client{Timeout: inviteMailHTTPTimeout})
	if err != nil {
		logger.Error("api: invite email provider is unusable; invites will be created but not emailed",
			"error", err)
		out.Mail = unusableSender{err: err}
		return out
	}
	out.Mail = sender
	return out
}

// unusableSender fails every send with the error that made the configured
// provider unusable, so each invite logs the real cause.
type unusableSender struct{ err error }

func (s unusableSender) Name() string { return "unusable" }

func (s unusableSender) Send(context.Context, mail.Message) error {
	return errors.Join(errors.New("email provider is unusable"), s.err)
}
