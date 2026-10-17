package categorize

import (
	"fmt"
	"log/slog"
	"net/http"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

// Every printed form (fmt verbs, slog) of a provider config or provider hides
// the API key. The providers keep their config in an unexported field, which
// fmt would otherwise reflect over, so they redact through their config.

func (c SharedProviderConfig) redacted() any {
	type plain SharedProviderConfig
	return plain(c)
}

func (c SharedProviderConfig) Format(state fmt.State, verb rune) {
	secrets.FormatRedacted(state, verb, c.redacted())
}
func (c SharedProviderConfig) LogValue() slog.Value { return secrets.LogRedacted(c.redacted()) }

// The shared llm client is left out: it holds the key as an llm.Secret, which
// fmt reflects over when it is reached through an unexported field.
func (p SharedProvider) redacted() any {
	return struct{ Cfg SharedProviderConfig }{p.cfg}
}

func (p SharedProvider) Format(state fmt.State, verb rune) {
	secrets.FormatRedacted(state, verb, p.redacted())
}
func (p SharedProvider) LogValue() slog.Value { return secrets.LogRedacted(p.redacted()) }

func (c OllamaProviderConfig) redacted() any {
	type plain OllamaProviderConfig
	return plain(c)
}

func (c OllamaProviderConfig) Format(state fmt.State, verb rune) {
	secrets.FormatRedacted(state, verb, c.redacted())
}
func (c OllamaProviderConfig) LogValue() slog.Value { return secrets.LogRedacted(c.redacted()) }

func (p OllamaProvider) redacted() any {
	return struct {
		Cfg    OllamaProviderConfig
		Client *http.Client
	}{p.cfg, p.client}
}

func (p OllamaProvider) Format(state fmt.State, verb rune) {
	secrets.FormatRedacted(state, verb, p.redacted())
}
func (p OllamaProvider) LogValue() slog.Value { return secrets.LogRedacted(p.redacted()) }

func (c TypeSafeClientConfig) redacted() any {
	type plain TypeSafeClientConfig
	return plain(c)
}

func (c TypeSafeClientConfig) Format(state fmt.State, verb rune) {
	secrets.FormatRedacted(state, verb, c.redacted())
}
func (c TypeSafeClientConfig) LogValue() slog.Value { return secrets.LogRedacted(c.redacted()) }

func (c TypeSafeClient) redacted() any {
	return struct {
		Cfg    TypeSafeClientConfig
		Client *http.Client
	}{c.cfg, c.client}
}

func (c TypeSafeClient) Format(state fmt.State, verb rune) {
	secrets.FormatRedacted(state, verb, c.redacted())
}
func (c TypeSafeClient) LogValue() slog.Value { return secrets.LogRedacted(c.redacted()) }
