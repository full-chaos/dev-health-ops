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
	"net/http"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/mail"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// SendTimeout bounds one send. Python awaits the send with no bound of its
// own; a relay that stalls must not hold the request open until the client
// gives up, so the send gets its own deadline (a failure is logged and never
// surfaced, exactly as in Python).
const SendTimeout = 30 * time.Second

// Config is the process's link-mail configuration. The zero value sends no
// mail, has no link secret (every link route refuses) and points links at the
// local web app.
type Config struct {
	// Mail sends the message. Nil means no message is sent (the failure is
	// logged like any other send failure).
	Mail mail.Sender
	// LinkSecret is the link-token signing key (LINK_TOKEN_SECRET). It signs
	// every new link and is the first key a presented link is checked against.
	// Unset, no link is signed or accepted (RefuseUnconfigured).
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

// Secret is the signing key for a new link token: the link secret, "" when unset.
func (c Config) Secret() string { return c.LinkSecret }

// AcceptSecrets are the keys a presented link token is checked against, signing
// key first: the link secret and the previous link secret. A previous secret
// alone accepts nothing. No other key ever verifies a link.
func (c Config) AcceptSecrets() []string {
	if c.LinkSecret == "" {
		return nil
	}
	out := []string{c.LinkSecret}
	if c.PreviousLinkSecret != "" && c.PreviousLinkSecret != c.LinkSecret {
		out = append(out, c.PreviousLinkSecret)
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
	if err := c.Try(context.WithoutCancel(ctx), to, subject, template, values); err != nil {
		logger.ErrorContext(ctx, failure, append(attrs, "error", err)...)
	}
}

// Try is Send without the logging and without detaching: it returns the failure and stops when ctx ends.
func (c Config) Try(ctx context.Context, to, subject, template string, values map[string]string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	html, err := mail.RenderTemplate(template, values)
	if err == nil {
		if c.Mail == nil {
			err = errors.New("no mail sender is configured")
		} else {
			sendCtx, cancel := context.WithTimeout(ctx, SendTimeout)
			defer cancel()
			err = c.Mail.Send(sendCtx, mail.Message{To: to, Subject: subject, HTML: html})
		}
	}
	return err
}

// Configured reports whether link tokens can be signed and accepted.
func (c Config) Configured() bool { return c.LinkSecret != "" }

// ConfigMissingDetail is the 503 body detail of a link route with no link secret.
const ConfigMissingDetail = "config_missing: LINK_TOKEN_SECRET"

// RefuseUnconfigured answers 503 config_missing and reports true while no link
// secret is set; a link route calls it before its first query.
func (c Config) RefuseUnconfigured(w http.ResponseWriter) bool {
	if c.Configured() {
		return false
	}
	policy.WriteDetail(w, http.StatusServiceUnavailable, ConfigMissingDetail, nil)
	return true
}
