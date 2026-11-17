package llmorgsettings

import (
	"fmt"
	"log/slog"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

func (c Credentials) redacted() any {
	type plain Credentials
	return plain(c)
}

// Format and LogValue keep the org's BYO API key out of every printed form.
func (c Credentials) Format(state fmt.State, verb rune) {
	secrets.FormatRedacted(state, verb, c.redacted())
}
func (c Credentials) LogValue() slog.Value { return secrets.LogRedacted(c.redacted()) }

// orgSettings is an org's decrypted LLM settings rows, api_key included. Its
// print and log forms show the api_key as the redaction marker.
type orgSettings map[string]string

// redacted hides every api_key row (the default's and each purpose override's)
// and any base_url row that carries a loaded key, and drops the reserved
// key-in-base_url mark so it never prints as a key. The categorize base_url is
// also hidden when its key is not in the map (a non-categorize store skips that
// key undecrypted), since it cannot be checked there.
func (s orgSettings) redacted() map[string]string {
	marked := keyInBaseURLMarked(s)
	keys := loadedAPIKeys(s)
	categorize := ScopeOf(PurposeCategorize)
	out := make(map[string]string, len(s))
	for key, value := range s {
		switch {
		case key == keyInBaseURLMark:
			continue
		case isAPIKeyRow(key):
			value = secrets.RedactString(value)
		case key == keyBaseURL || strings.HasSuffix(key, ":"+keyBaseURL):
			if marked || carriesAnyKey(value, keys) || (key == categorize.BaseURLKey && s[categorize.APIKeyKey] == "") {
				value = secrets.RedactString(value)
			}
		}
		out[key] = value
	}
	return out
}

func isAPIKeyRow(key string) bool {
	return key == keyAPIKey || strings.HasSuffix(key, ":"+keyAPIKey)
}

// loadedAPIKeys are the values of every api_key row in the map, the default's
// and each purpose override's, categorize included (not the unbound marks).
func loadedAPIKeys(s orgSettings) []string {
	var keys []string
	for key, value := range s {
		if isAPIKeyRow(key) && value != "" && !strings.HasPrefix(key, unboundMarkPrefix) {
			keys = append(keys, value)
		}
	}
	return keys
}

func carriesAnyKey(baseURL string, keys []string) bool {
	if baseURL == "" {
		return false
	}
	for _, key := range keys {
		if BaseURLCarriesKey(baseURL, key) {
			return true
		}
	}
	return false
}

func (s orgSettings) Format(state fmt.State, verb rune) {
	secrets.FormatRedacted(state, verb, s.redacted())
}
func (s orgSettings) LogValue() slog.Value { return secrets.LogRedacted(s.redacted()) }
