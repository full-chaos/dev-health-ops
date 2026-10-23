package webhookintake

import (
	"crypto/rand"
	"crypto/sha256"
	"io"
	"log/slog"
)

// unverifiableKey stands in for a provider whose verification credential is not set: no caller
// can know it, so every delivery to that provider fails verification after the same work as a
// delivery to a configured one. It is made once at startup; a failed random read stops the
// process, it never yields an empty or guessable key (an empty key would let anyone sign).
var unverifiableKey = mustRandomKey(rand.Reader)

const unverifiableKeyLength = 32

func mustRandomKey(source io.Reader) string {
	key := make([]byte, unverifiableKeyLength)
	if _, err := io.ReadFull(source, key); err != nil {
		panic("webhookintake: no random bytes for the stand-in verification key: " + err.Error())
	}
	return string(key)
}

// verifyingSecret is the key a delivery is checked against: the configured one, else the
// stand-in.
func verifyingSecret(configured string) string {
	if configured != "" {
		return configured
	}
	return unverifiableKey
}

// verifyingKey is the fixed-size material a handler verifies with, made once when the handler is
// built (startup): the per-request work then does not depend on the length of the provider
// credential or on whether one is set. hmacKey is at most one SHA-256 block (a longer key is
// hashed first, which is what HMAC does with it), so every HMAC pays the same two key blocks;
// digest is the SHA-256 of the credential for a plain token compare.
type verifyingKey struct {
	hmacKey []byte
	digest  [sha256.Size]byte
}

const sha256BlockSize = 64

func newVerifyingKey(configured string) verifyingKey {
	secret := []byte(verifyingSecret(configured))
	key := secret
	if len(key) > sha256BlockSize {
		hashed := sha256.Sum256(key)
		key = hashed[:]
	}
	return verifyingKey{hmacKey: key, digest: sha256.Sum256(secret)}
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
		{"gitlab", "GITLAB_WEBHOOK_TOKEN", secrets.GitLab},
		{"jira", "JIRA_WEBHOOK_SECRET", secrets.Jira},
	} {
		if provider.value == "" {
			logger.Warn("webhook intake has no verification credential; every delivery to it is refused", "provider", provider.name, "env", provider.env)
		}
	}
}
