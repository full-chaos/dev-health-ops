package admin

import (
	"fmt"
	"log/slog"
	"math/big"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

// The structs below carry a BYO LLM API key. Every printed form (fmt verbs,
// slog) hides it.

func (c readinessBYOConfig) redacted() any {
	return struct {
		Provider, Model string
		APIKey          secrets.Hidden
		BaseURL         string
	}{c.provider, c.model, c.apiKey, c.baseURL}
}

func (c readinessBYOConfig) Format(state fmt.State, verb rune) {
	secrets.FormatRedacted(state, verb, c.redacted())
}
func (c readinessBYOConfig) LogValue() slog.Value { return secrets.LogRedacted(c.redacted()) }

// redactedOptional keeps an optional string readable: its value, or nil.
func redactedOptional(value *string) any {
	if value == nil {
		return nil
	}
	return *value
}

func (u llmUpsert) redacted() any {
	return struct {
		Provider      string
		Model         any
		APIKey        any
		BaseURL       any
		Concurrency   *big.Int
		BudgetLimitMu *big.Int
	}{u.provider, redactedOptional(u.model), redactedOptional(secrets.RedactPtr(u.apiKey)),
		redactedOptional(u.baseURL), u.concurrency, u.budgetLimitMu}
}

func (u llmUpsert) Format(state fmt.State, verb rune) {
	secrets.FormatRedacted(state, verb, u.redacted())
}
func (u llmUpsert) LogValue() slog.Value { return secrets.LogRedacted(u.redacted()) }

func (in LLMSettingsInput) redacted() any {
	return struct {
		Provider string
		Model    any
		APIKey   any
		BaseURL  any
	}{in.Provider, redactedOptional(in.Model), redactedOptional(secrets.RedactPtr(in.APIKey)), redactedOptional(in.BaseURL)}
}

func (in LLMSettingsInput) Format(state fmt.State, verb rune) {
	secrets.FormatRedacted(state, verb, in.redacted())
}
func (in LLMSettingsInput) LogValue() slog.Value { return secrets.LogRedacted(in.redacted()) }
