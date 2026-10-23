package authconfig

import (
	"errors"
	"net/url"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

// Settings of the federated sign-in routes (OIDC, OAuth and SAML). Each
// protocol mounts only when all of its values are present.
const (
	// EnvSSOStateKey is the key the OIDC/OAuth state is derived from. Secret:
	// environment or _FILE only.
	EnvSSOStateKey = "AUTH_SSO_STATE_KEY"
	// EnvAppBaseURL is go-api's public base URL (APP_BASE_URL): the IdP
	// registrations carry it, so the callback and ACS URLs are built from it.
	EnvAppBaseURL = "APP_BASE_URL"
	// EnvSettingsEncryptionKey and EnvSettingsEncryptionSalt are go-api's
	// provider-secret key and salt (SETTINGS_ENCRYPTION_KEY/_SALT). The key is
	// a secret: environment or _FILE only.
	EnvSettingsEncryptionKey  = "SETTINGS_ENCRYPTION_KEY"
	EnvSettingsEncryptionSalt = "SETTINGS_ENCRYPTION_SALT"
)

var errBaseURL = errors.New(EnvAppBaseURL + " must be an absolute http(s) URL")

// SSO holds the federated sign-in settings.
type SSO struct {
	StateKey       secrets.Value
	AppBaseURL     string
	EncryptionKey  secrets.Value
	EncryptionSalt string
}

// OIDCEnabled is set when the OIDC and OAuth routes have all three values.
func (s SSO) OIDCEnabled() bool {
	return s.StateKey.Configured() && s.AppBaseURL != "" && s.EncryptionKey.Configured()
}

// SAMLEnabled is set when the SAML routes have the base URL and the key.
func (s SSO) SAMLEnabled() bool { return s.AppBaseURL != "" && s.EncryptionKey.Configured() }

func ssoSettings(lookup secrets.LookupEnv) (SSO, error) {
	state, _, err := secrets.ResolveSecret(EnvSSOStateKey, lookup)
	if err != nil {
		return SSO{}, err
	}
	key, _, err := secrets.ResolveSecret(EnvSettingsEncryptionKey, lookup)
	if err != nil {
		return SSO{}, err
	}
	out := SSO{StateKey: state, EncryptionKey: key, EncryptionSalt: strings.TrimSpace(valueOf(lookup, EnvSettingsEncryptionSalt))}
	if base := strings.TrimRight(strings.TrimSpace(valueOf(lookup, EnvAppBaseURL)), "/"); base != "" {
		parsed, parseErr := url.Parse(base)
		if parseErr != nil || (parsed.Scheme != "https" && parsed.Scheme != "http") || parsed.Host == "" {
			return SSO{}, errBaseURL
		}
		out.AppBaseURL = base
	}
	return out, nil
}
