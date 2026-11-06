package apiservice

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/session"
	"github.com/full-chaos/dev-health-ops/internal/apiservice/admin"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/httpguard"
	"github.com/full-chaos/dev-health-ops/internal/mail"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
)

// inviteMailHTTPTimeout bounds the Resend transport's HTTP calls; the SMTP
// transport is bounded by the caller's context.
const inviteMailHTTPTimeout = 30 * time.Second

// inviteConfig builds create_org_invite's configuration from the process
// environment: LINK_TOKEN_SECRET signs new links and, with
// LINK_TOKEN_SECRET_PREVIOUS, is the only key that accepts one (JWT_SECRET_KEY
// and SETTINGS_ENCRYPTION_KEY never sign or verify a link). Unset, the process
// starts, logs once, and the link routes answer 503 config_missing. The accept
// link base is APP_BASE_URL; the email sender is chosen by EMAIL_PROVIDER
// exactly as the worker's billing sender is (internal/mail).
//
// A mail misconfiguration must NOT stop the api from starting -- Python
// resolved its provider per send and logged a failure without surfacing it --
// so an unusable sender is logged once here and every invite then logs its
// own send failure, never failing the request.
func inviteConfig(cfg config.Config, logger *slog.Logger, lookup func(string) (string, bool)) admin.InviteConfig {
	out := admin.InviteConfig{}
	out.LinkSecret = cfg.LinkTokenSecret.Reveal()
	out.PreviousLinkSecret = cfg.LinkTokenSecretPrevious.Reveal()
	if !out.Configured() {
		logger.Warn("api: LINK_TOKEN_SECRET is not set; the link routes (register, verify, resend-verification, forgot-password, reset-password, accept-invite, onboard join, create invite) answer 503 config_missing")
	}
	out.AppBaseURL, out.AppBaseURLSet = lookup("APP_BASE_URL")
	sender, err := mail.NewSenderFromEnv(&http.Client{Timeout: inviteMailHTTPTimeout, Transport: httpguard.SharedTransport()})
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

// registerLimit is rate_limit.py's AUTH_REGISTER_LIMIT, read once at
// startup as Python reads it at import: unset is the route's default
// (3/hour); a value this api cannot serve the same way stops startup.
func registerLimit(lookup func(string) (string, bool)) (httpapi.Limit, error) {
	text, set := lookup("AUTH_REGISTER_LIMIT")
	if !set {
		return session.DefaultRegisterLimit, nil
	}
	return httpapi.ParseLimit(session.RegisterLimitID, text)
}
