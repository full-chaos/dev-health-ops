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
