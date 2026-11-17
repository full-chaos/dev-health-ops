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

// redacted hides every api_key row and every base_url row (the default's and
// each purpose override's) and drops the reserved key-in-base_url mark. A
// base_url is hidden whatever it holds: no print can tell a clean url from one
// that carries a key the map does not hold (skipped or projected away).
func (s orgSettings) redacted() map[string]string {
	out := make(map[string]string, len(s))
	for key, value := range s {
		switch {
		case key == keyInBaseURLMark:
			continue
		case key == keyAPIKey || strings.HasSuffix(key, ":"+keyAPIKey),
			key == keyBaseURL || strings.HasSuffix(key, ":"+keyBaseURL):
			value = secrets.RedactString(value)
		}
		out[key] = value
	}
	return out
}

func (s orgSettings) Format(state fmt.State, verb rune) {
	secrets.FormatRedacted(state, verb, s.redacted())
}
func (s orgSettings) LogValue() slog.Value { return secrets.LogRedacted(s.redacted()) }
