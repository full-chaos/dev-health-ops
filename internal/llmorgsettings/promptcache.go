package llmorgsettings

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// KeyPromptCache is the llm settings row an org admin writes to opt the org's
// saved BYO configuration in to the Anthropic prompt cache ("true"). No row, or
// any other value, reads as off.
const KeyPromptCache = "prompt_cache"

// promptCacheOptIn is whether the org opted in AND its effective provider is
// anthropic: the cache marker exists on that provider only.
func promptCacheOptIn(settings orgSettings, provider string) bool {
	return settings[KeyPromptCache] == "true" && provider == "anthropic"
}

// PromptCacheEnabled reads the org's opt-in: true only when the row says "true".
func PromptCacheEnabled(ctx context.Context, queryer SavedQueryer, orgID string) (bool, error) {
	var value *string
	err := queryer.QueryRow(ctx, `SELECT value FROM settings WHERE org_id = $1 AND category = $2 AND key = $3`,
		orgID, settingsCategoryLLM, KeyPromptCache).Scan(&value)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("llmorgsettings: prompt cache opt-in for org %q: %w", orgID, err)
	}
	return value != nil && *value == "true", nil
}

// InvestigateProviderIsAnthropic reports whether the saved config Ask Dev uses
// (the investigate override when one is stored, else the default row) is on the
// anthropic provider. The writer stores the provider in plain text and the
// byo_llm gate is not applied, as in HasSavedBYO.
func InvestigateProviderIsAnthropic(ctx context.Context, queryer SavedQueryer, orgID string) (bool, error) {
	override := PurposeKey(PurposeInvestigate, keyProvider)
	var defaultProvider, overrideProvider string
	err := queryer.QueryRow(ctx, `
SELECT coalesce(max(value) FILTER (WHERE key = $3), ''), coalesce(max(value) FILTER (WHERE key = $4), '')
FROM settings WHERE org_id = $1 AND category = $2 AND key = ANY($5)`,
		orgID, settingsCategoryLLM, keyProvider, override, []string{keyProvider, override}).Scan(&defaultProvider, &overrideProvider)
	if err != nil {
		return false, fmt.Errorf("llmorgsettings: saved investigate provider for org %q: %w", orgID, err)
	}
	return effectiveProvider(defaultProvider, overrideProvider) == "anthropic", nil
}

// effectiveProvider is the investigate override's provider when one is stored,
// else the default row's: an override is whole or absent.
func effectiveProvider(defaultProvider, overrideProvider string) string {
	if overrideProvider != "" {
		return normalizeProvider(overrideProvider)
	}
	return normalizeProvider(defaultProvider)
}
