package llmbudget

import (
	"reflect"
	"slices"
	"sort"
	"testing"
)

func TestCatalogModelsAreTheBookPricedModels(t *testing.T) {
	for _, provider := range CatalogProviders() {
		got := CatalogModels(provider)
		if len(got) == 0 {
			t.Errorf("%s: empty catalog", provider)
		}
		if !sort.StringsAreSorted(got) || len(slices.Compact(slices.Clone(got))) != len(got) {
			t.Errorf("%s: catalog is not sorted and unique: %v", provider, got)
		}
		for _, model := range got {
			if model == scriptedModel {
				t.Errorf("%s: the scripted test model is in the catalog", provider)
			}
		}
	}
	for _, row := range listPrices {
		if !slices.Contains(CatalogModels(row.provider), row.model) {
			t.Errorf("%s/%s is priced but not in the catalog", row.provider, row.model)
		}
	}
	for _, key := range platformPriceKeys {
		if key != scriptedModel && !slices.Contains(CatalogModels("openai"), key) {
			t.Errorf("openai/%s is priced but not in the catalog", key)
		}
	}
}

func TestCatalogHoldsTheAnthropicFiveFiveModels(t *testing.T) {
	want := []string{"claude-haiku-5-5", "claude-opus-5-5", "claude-sonnet-5-5"}
	if got := CatalogModels("anthropic"); !reflect.DeepEqual(got, want) {
		t.Fatalf("anthropic catalog %v, want %v", got, want)
	}
}

func TestCatalogHasNoModelForAnUnpricedProvider(t *testing.T) {
	for _, provider := range []string{"", "ollama", "azure-openai"} {
		if got := CatalogModels(provider); len(got) != 0 {
			t.Errorf("%q: %v", provider, got)
		}
	}
	list := CatalogModels("qwen")
	list[0] = "changed"
	if CatalogModels("qwen")[0] == "changed" {
		t.Error("CatalogModels returned shared storage")
	}
}
