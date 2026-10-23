package webhookintake

import (
	"crypto/sha256"
	"io"
	"log/slog"
	"sync/atomic"

	"github.com/full-chaos/dev-health-ops/internal/api/standinkey"
)

// The verification material and the stand-in key for an absent credential live in
// internal/api/standinkey (one rule for every intake route); these are the names this package's
// handlers and tests use.

// unverifiableKey is the process-wide stand-in key.
var unverifiableKey = standinkey.StandIn()

func mustRandomKey(source io.Reader) string { return standinkey.MustRandomKey(source) }

// verifyingSecret is the key a delivery is checked against: the configured one, else the
// stand-in.
func verifyingSecret(configured string) string { return standinkey.Secret(configured) }

// The credentials stay in the log redaction registry (they are read with GetenvSecret): a credential
// echoed into a log line by mistake is blanked. They are read once, when each handler is built.

// verifyingKey is the fixed-size material a handler verifies with, made once when the handler is
// built (startup): see standinkey.Key.
type verifyingKey struct {
	hmacKey []byte
	digest  [sha256.Size]byte
}

// keyDerivationCount counts newVerifyingKey calls; a test pins that no request derives one.
type keyDerivationCount struct{ n atomic.Int64 }

func (c *keyDerivationCount) Load() int64 { return c.n.Load() }

var keyDerivations keyDerivationCount

func newVerifyingKey(configured string) verifyingKey {
	keyDerivations.n.Add(1)
	key := standinkey.New(configured)
	return verifyingKey{hmacKey: key.HMACKey, digest: key.Digest}
}

// LogUnconfiguredSecrets writes one line for each provider with no verification credential. It is
// called once at startup; no request path logs it.
func LogUnconfiguredSecrets(logger *slog.Logger) {
	if logger == nil {
		logger = slog.Default()
	}
	secrets := envSecrets()
	for _, provider := range []struct{ name, env, value string }{
		{"github", "GITHUB_WEBHOOK_SECRET", secrets.GitHub},
	} {
		if provider.value == "" {
			logger.Warn("webhook intake has no verification credential; every delivery to it is refused", "provider", provider.name, "env", provider.env)
		}
	}
}
