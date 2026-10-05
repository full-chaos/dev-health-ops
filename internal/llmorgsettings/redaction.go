package llmorgsettings

import (
	"fmt"
	"log/slog"

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

func (s orgSettings) redacted() map[string]string {
	out := make(map[string]string, len(s))
	for key, value := range s {
		if key == keyAPIKey {
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
