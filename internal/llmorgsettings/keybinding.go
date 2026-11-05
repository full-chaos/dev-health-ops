package llmorgsettings

import "time"

// KeyBound is whether an api_key row written at keyAt may be served with its
// scope's provider row written at providerAt. The provider row moves only when
// its value changes and writers write it before the key, so an older key was
// written for an earlier provider (CHAOS-13096).
func KeyBound(keyAt, providerAt time.Time) bool { return !keyAt.Before(providerAt) }

// KeyScope is the provider row and the api_key row of one stored config.
type KeyScope struct{ ProviderKey, APIKeyKey string }

// ScopeOf is the scope of one purpose override; "" is the default row.
func ScopeOf(purpose Purpose) KeyScope {
	if purpose == "" {
		return KeyScope{keyProvider, keyAPIKey}
	}
	return KeyScope{PurposeKey(purpose, keyProvider), PurposeKey(purpose, keyAPIKey)}
}

// KeyScopes are the default row and every purpose override, categorize included.
func KeyScopes() []KeyScope {
	scopes := []KeyScope{ScopeOf("")}
	for _, purpose := range append(append([]Purpose{}, Purposes...), PurposeCategorize) {
		scopes = append(scopes, ScopeOf(purpose))
	}
	return scopes
}

// UnboundKeys are the api_key rows, among the stored rows' updated_at, that are
// older than their scope's provider row.
func UnboundKeys(updatedAt map[string]time.Time) []string {
	var out []string
	for _, scope := range KeyScopes() {
		keyAt, hasKey := updatedAt[scope.APIKeyKey]
		providerAt, hasProvider := updatedAt[scope.ProviderKey]
		if hasKey && hasProvider && !KeyBound(keyAt, providerAt) {
			out = append(out, scope.APIKeyKey)
		}
	}
	return out
}

// unboundMarkPrefix starts the reserved entry that marks an unbound key in the
// loaded settings; no stored key can start with a NUL byte. The key itself stays
// loaded so the base_url key-leak checks still see it.
const unboundMarkPrefix = "\x00unbound:"

func markUnboundKeys(settings orgSettings, updatedAt map[string]time.Time) {
	for _, key := range UnboundKeys(updatedAt) {
		settings[unboundMarkPrefix+key] = "1"
	}
}

// servedKey is the stored api_key under key, "" when it is not bound.
func servedKey(settings orgSettings, key string) string {
	if settings[unboundMarkPrefix+key] != "" {
		return ""
	}
	return settings[key]
}
