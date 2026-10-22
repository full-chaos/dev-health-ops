package llmbudget

import "sort"

var catalogProviders = []string{"anthropic", "gemini", "openai", "qwen"}

// CatalogProviders are the providers the model catalog covers, sorted.
func CatalogProviders() []string { return append([]string(nil), catalogProviders...) }

// CatalogModels is the sorted, unique list of model names the price book
// prices for a provider: the platform catalog a BYO model select shows with no
// key saved. A price row added to the book adds its model here; a provider with
// no priced model has no catalog.
func CatalogModels(provider string) []string {
	seen := map[string]struct{}{}
	var out []string
	add := func(model string) {
		if _, dup := seen[model]; !dup {
			seen[model] = struct{}{}
			out = append(out, model)
		}
	}
	if provider == "openai" {
		for _, key := range platformPriceKeys {
			if key != scriptedModel {
				add(key)
			}
		}
	}
	for _, row := range listPrices {
		if row.provider == provider {
			add(row.model)
		}
	}
	sort.Strings(out)
	return out
}
