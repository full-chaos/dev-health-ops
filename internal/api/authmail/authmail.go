// Package authmail is what the auth flows that e-mail a signed link share:
// the org invite (admin), e-mail verification and password reset (session).
// Each Python service reads the same three things at send time --
// _token_secret(), APP_BASE_URL and the configured e-mail provider -- and
// sends best effort, logging a failure and never surfacing it.
package authmail

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/auth/signedtoken"
	"github.com/full-chaos/dev-health-ops/internal/mail"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// SendTimeout bounds one send. Python awaits the send with no bound of its
// own; a relay that stalls must not hold the request open until the client
// gives up, so the send gets its own deadline (a failure is logged and never
// surfaced, exactly as in Python).
const SendTimeout = 30 * time.Second

// Config is the process's link-mail configuration. The zero value is a
// working default for a test that does not care: no mail is sent, the token
// secret is the Python last-resort fallback, and links point at the local web
// app.
type Config struct {
	// Mail sends the message. Nil means no message is sent (the failure is
	// logged like any other send failure).
	Mail mail.Sender
	// TokenSecret is the HMAC key link tokens are signed with:
	// _token_secret(), JWT_SECRET_KEY, else SETTINGS_ENCRYPTION_KEY, else
	// the fallback. The caller resolves that chain; "" here also falls back
	// to the last resort.
	TokenSecret string
	// LinkSecret is the link-token signing key (LINK_TOKEN_SECRET). Set, it
	// signs every new link; unset, links are signed with the TokenSecret chain.
	LinkSecret string
	// PreviousLinkSecret (LINK_TOKEN_SECRET_PREVIOUS) is a retired signing key
	// that is still accepted while links signed with it are in flight.
	PreviousLinkSecret string
	// AppBaseURL is APP_BASE_URL as the process sees it, "" when unset
	// (os.getenv("APP_BASE_URL", "http://localhost:3000") -- an unset
	// variable takes the default, a set-but-empty one does not, which is why
	// the caller passes AppBaseURLSet).
	AppBaseURL    string
	AppBaseURLSet bool
}

// Secret is the signing key for a new link token: the link secret when set,
// else the TokenSecret chain with its last-resort default.
func (c Config) Secret() string {
	if c.LinkSecret != "" {
		return c.LinkSecret
	}
	return signedtoken.Secret(c.TokenSecret, "")
}

// AcceptSecrets are the keys a presented link token is checked against, signing
// key first: the signing key, the previous link secret, and the TokenSecret
// chain value while one is configured (so links sent before the link secret
// was set stay valid; removing that key ends their validity). Blanks and
// repeats are left out.
func (c Config) AcceptSecrets() []string {
	out := []string{c.Secret()}
	for _, secret := range []string{c.PreviousLinkSecret, c.TokenSecret} {
		if secret != "" && !slices.Contains(out, secret) {
			out = append(out, secret)
		}
	}
	return out
}

// BaseURL is the web app origin links start with, trailing "/" stripped.
func (c Config) BaseURL() string {
	base := "http://localhost:3000"
	if c.AppBaseURLSet {
		base = c.AppBaseURL
	}
	return strings.TrimRight(base, "/")
}

// Link is f"{base_url}{path}?token={quote(token)}".
func (c Config) Link(path, token string) string {
	return c.BaseURL() + path + "?token=" + pythonparity.Quote(token, "/")
}

// Send renders template with values and sends it to to, best effort: any
// failure is logged with attrs (never the address) and swallowed. The send
// outlives a cancelled request, as the Python await does, up to SendTimeout.
func (c Config) Send(ctx context.Context, logger *slog.Logger, failure, to, subject, template string, values map[string]string, attrs ...any) {
	html, err := mail.RenderTemplate(template, values)
	if err == nil {
		if c.Mail == nil {
			err = errors.New("no mail sender is configured")
		} else {
			sendCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), SendTimeout)
			defer cancel()
			err = c.Mail.Send(sendCtx, mail.Message{To: to, Subject: subject, HTML: html})
		}
	}
	if err != nil {
		logger.ErrorContext(ctx, failure, append(attrs, "error", err)...)
	}
}
