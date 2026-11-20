package admin

import (
	"context"

	"github.com/full-chaos/dev-health-ops/internal/llmorgsettings"
)

// llmLeakScopes are the default row ("") and every purpose override, categorize included.
func llmLeakScopes() []llmorgsettings.Purpose {
	return append(append([]llmorgsettings.Purpose{""}, llmorgsettings.Purposes...), llmorgsettings.PurposeCategorize)
}

// storedLeakKeys is every stored api key of the org, the default row's and each
// purpose override's, for the save-time base_url check. An undecryptable key
// reads as "".
func (h *handlers) storedLeakKeys(ctx context.Context, orgID string) ([]string, error) {
	stored, err := readableStoredKey(h.storedLLMKey(ctx, orgID))
	if err != nil {
		return nil, &llmStepError{"read stored llm key", err}
	}
	keys := []string{stored}
	for _, purpose := range llmLeakScopes()[1:] {
		key, err := readableStoredKey(h.storedPurposeAPIKey(ctx, orgID, purpose))
		if err != nil {
			return nil, &llmStepError{"read stored llm purpose key", err}
		}
		keys = append(keys, key)
	}
	return keys, nil
}

// readerLeakKeys are the keys a default-row reader checks its stored base_url
// against: the keys it holds and, for a non-empty url, every stored key of the
// org (storedLeakKeys), so a url that carries another scope's key is refused
// before any lookup.
func (h *handlers) readerLeakKeys(ctx context.Context, orgID, baseURL string, held ...string) ([]string, error) {
	keys := append([]string{}, held...)
	if baseURL == "" {
		return keys, nil
	}
	stored, err := h.storedLeakKeys(ctx, orgID)
	if err != nil {
		return nil, err
	}
	return append(keys, stored...), nil
}

func (h *handlers) storedScopeBaseURLOf(ctx context.Context, orgID string, purpose llmorgsettings.Purpose) (string, error) {
	if h.storedScopeBaseURL != nil {
		return h.storedScopeBaseURL(ctx, orgID, purpose)
	}
	scoped, err := h.scopeKey(ctx, h.store.Pool, orgID, purpose, false, false)
	return scoped.BaseURL, err
}

// otherBaseURLCarriesKey is whether a stored base_url of any scope but own
// carries apiKey. The own scope is skipped: the write replaces its base_url
// with the body's, which is checked against apiKey on its own. The default
// row counts for every purpose write: a purpose with no base_url inherits it.
func (h *handlers) otherBaseURLCarriesKey(ctx context.Context, orgID string, own llmorgsettings.Purpose, apiKey *string) (bool, error) {
	if apiKey == nil || *apiKey == "" {
		return false, nil
	}
	for _, scope := range llmLeakScopes() {
		if scope == own {
			continue
		}
		baseURL, err := h.storedScopeBaseURLOf(ctx, orgID, scope)
		if err != nil {
			return false, &llmStepError{"read stored llm base url", err}
		}
		if llmorgsettings.BaseURLCarriesKey(baseURL, *apiKey) {
			return true, nil
		}
	}
	return false, nil
}
