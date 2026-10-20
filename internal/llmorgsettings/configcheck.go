package llmorgsettings

import "context"

// ConfigReason is why a saved BYO configuration can or cannot serve a call.
// The org status route, the explain model gate and the provider builders all
// ask this one question.
type ConfigReason string

const (
	ConfigNotConfigured      ConfigReason = "not_configured"
	ConfigUnknownProvider    ConfigReason = "unknown_provider"
	ConfigMissingCredentials ConfigReason = "missing_credentials"
	ConfigInvalidBaseURL     ConfigReason = "invalid_base_url"
	ConfigActive             ConfigReason = "active"
	// ConfigUnreadableRow: a stored setting exists and could not be read back (not decryptable). Gate-only: the status
	// route answers this shape with an error.
	ConfigUnreadableRow ConfigReason = "unreadable_row"
)

// CheckProvider is the no-I/O half: the normalized provider name and, when it
// cannot serve, why.
func CheckProvider(provider string) (string, ConfigReason) {
	name := normalizeProvider(provider)
	switch name {
	case "", "auto", "mock", "none":
		return name, ConfigNotConfigured
	}
	if !isKnownProvider(name) {
		return name, ConfigUnknownProvider
	}
	return name, ConfigActive
}

// CheckCredentials is the credentials half for a known provider. A base_url
// split error is an error, not a reason.
func CheckCredentials(ctx context.Context, provider, apiKey, baseURL string) (ConfigReason, error) {
	if apiKey == "" && baseURL == "" {
		return ConfigMissingCredentials, nil
	}
	if !credentialsComplete(provider, apiKey) {
		return ConfigMissingCredentials, nil
	}
	ok, _, err := ValidateBaseURLChecked(ctx, baseURL)
	if err != nil {
		return "", err
	}
	if !ok {
		return ConfigInvalidBaseURL, nil
	}
	return ConfigActive, nil
}

// CheckConfig answers whether a saved provider, api_key and base_url can
// serve a call: ConfigActive, or the reason it cannot.
func CheckConfig(ctx context.Context, provider, apiKey, baseURL string) (ConfigReason, error) {
	name, reason := CheckProvider(provider)
	if reason != ConfigActive {
		return reason, nil
	}
	return CheckCredentials(ctx, name, apiKey, baseURL)
}
