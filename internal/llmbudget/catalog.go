package llmbudget

import (
	"sort"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

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

// unsupportedRequestParams are the request parameters a model family refuses,
// by provider and model-name prefix. A model with no row accepts what its
// caller sends today.
var unsupportedRequestParams = []struct {
	provider, prefix string
	params           []string
}{
	{"openai", "gpt-6", []string{"temperature"}},
}

// onlyReasoningEffort is the one reasoning_effort value a model family accepts,
// by provider and model-name prefix. A model with no row has no known limit.
var onlyReasoningEffort = []struct{ provider, prefix, effort string }{
	{"openai", "gpt-6", "none"},
}

// OnlyReasoningEffort is the one reasoning_effort value the model is known to
// accept, or "" when no limit is known.
func OnlyReasoningEffort(provider, model string) string {
	p := pythonparity.Lower(pythonparity.Strip(provider))
	m := pythonparity.Lower(pythonparity.Strip(model))
	for _, row := range onlyReasoningEffort {
		if row.provider == p && strings.HasPrefix(m, row.prefix) {
			return row.effort
		}
	}
	return ""
}

// UnsupportedRequestParams are the request parameters the model is known to
// refuse, so a caller leaves them off its first request.
func UnsupportedRequestParams(provider, model string) []string {
	p := pythonparity.Lower(pythonparity.Strip(provider))
	m := pythonparity.Lower(pythonparity.Strip(model))
	var out []string
	for _, row := range unsupportedRequestParams {
		if row.provider == p && strings.HasPrefix(m, row.prefix) {
			out = append(out, row.params...)
		}
	}
	return out
}
