package billing

import (
	"log/slog"
	"sync"

	"github.com/full-chaos/dev-health-ops/internal/api/standinkey"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

// webhookCredential is the Stripe signing secret and the verification key derived from it once, when
// the handlers are built (startup). An absent secret derives the stand-in key (standinkey): the
// webhook then verifies every delivery against a key no caller knows, after the same work as a
// configured one. The secret itself stays a secrets.Value, so it stays in the log redaction registry.
type webhookCredential struct {
	value secrets.Value
	key   standinkey.Key
}

func newWebhookCredential(value secrets.Value) webhookCredential {
	return webhookCredential{value: value, key: standinkey.New(value.Reveal())}
}

// Reveal is the configured secret ("" when absent).
func (c webhookCredential) Reveal() string { return c.value.Reveal() }

var startupLogOnce sync.Once

// LogUnconfigured writes one line for each billing credential that is absent. It is called once at
// startup (a process that mounts the webhook twice logs once); no request path logs it.
func LogUnconfigured(logger *slog.Logger, webhookSecret, stripeKey secrets.Value) {
	startupLogOnce.Do(func() {
		if logger == nil {
			logger = slog.Default()
		}
		if webhookSecret.Reveal() == "" {
			logger.Warn("billing webhook has no verification credential; every delivery to it is refused", "setting", "STRIPE_WEBHOOK_SECRET")
		}
		if stripeKey.Reveal() == "" {
			logger.Warn("billing has no API client credential; the routes that call the provider are unavailable", "setting", "STRIPE_SECRET_KEY")
		}
	})
}
