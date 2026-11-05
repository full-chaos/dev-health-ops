package llmorgsettings

import "sort"

// ProviderTypeSafe is the provider name of TypeSafe System One (jev), a
// SystemOne decision model, when an org runs it on its own TypeSafe key. Like
// clef it writes no text, so it is known to the categorize purpose only
// (CheckProviderFor). It equals llmbudget.DecisionProviderTypeSafe, the name of
// the platform jev backend.
const ProviderTypeSafe = "typesafe"

// TypeSafePostURL is the one URL an org's TypeSafe request is sent to. The
// endpoint is fixed in code: a saved base_url is refused, never read.
const TypeSafePostURL = "https://api.typesafe.ai/v1/systemone"

// typeSafeModels is the closed list of jev models an org may save: pinned,
// versioned ids only, each with a price row. The moving aliases jev-latest and
// jev-preview are never listed.
var typeSafeModels = map[string]struct{}{"jev-1.13.0": {}}

// TypeSafeModelAllowed reports a saved model the list holds, byte for byte.
func TypeSafeModelAllowed(model string) bool {
	_, ok := typeSafeModels[model]
	return ok
}

// TypeSafeModels is the list, sorted.
func TypeSafeModels() []string {
	out := make([]string, 0, len(typeSafeModels))
	for model := range typeSafeModels {
		out = append(out, model)
	}
	sort.Strings(out)
	return out
}

// ProviderChangeNeedsKey is the key rule of a categorize override written with
// no api_key: when an override key is stored and the saved provider is not the
// new one, the stored key belongs to the other vendor and must not be sent to
// the new one, so the write needs a new key. Other purposes keep their rule.
func ProviderChangeNeedsKey(purpose Purpose, keyStored bool, storedProvider *string, newProvider string) bool {
	if purpose != PurposeCategorize || !keyStored {
		return false
	}
	return storedProvider == nil || normalizeProvider(*storedProvider) != normalizeProvider(newProvider)
}
