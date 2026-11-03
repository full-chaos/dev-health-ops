package llmbudget

import (
	"reflect"
	"testing"
)

func TestCatalogHoldsTheCurrentOpenAIFamily(t *testing.T) {
	want := []string{
		"gpt-4.1", "gpt-4.1-mini", "gpt-4.1-nano",
		"gpt-5", "gpt-5-mini", "gpt-5-nano", "gpt-5.1", "gpt-5.2",
		"gpt-5.4", "gpt-5.4-mini", "gpt-5.4-nano", "gpt-5.5",
		"gpt-5.6-luna", "gpt-5.6-sol", "gpt-5.6-terra",
		"gpt-6-luna",
		"o1", "o3", "o3-mini", "o4-mini",
	}
	if got := CatalogModels("openai"); !reflect.DeepEqual(got, want) {
		t.Fatalf("openai catalog %v, want %v", got, want)
	}
}
