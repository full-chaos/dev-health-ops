package categorize

import (
	"encoding/json"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/units"
)

func TestCategorizationJSONSchemaMarshalsAndCoversEverySubcategory(t *testing.T) {
	schema := categorizationJSONSchema()

	encoded, err := json.Marshal(schema)
	if err != nil {
		t.Fatalf("schema did not marshal to JSON: %v", err)
	}

	var decoded map[string]any
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("marshalled schema did not round-trip: %v", err)
	}

	properties, ok := decoded["properties"].(map[string]any)
	if !ok {
		t.Fatal("top-level properties missing or not an object")
	}
	subcategoriesSchema, ok := properties["subcategories"].(map[string]any)
	if !ok {
		t.Fatal("properties.subcategories missing or not an object")
	}
	subcategoryProperties, ok := subcategoriesSchema["properties"].(map[string]any)
	if !ok {
		t.Fatal("properties.subcategories.properties missing or not an object")
	}

	if len(subcategoryProperties) != len(units.SortedSubcategories) {
		t.Fatalf("schema has %d subcategory properties, want %d (units.SortedSubcategories)",
			len(subcategoryProperties), len(units.SortedSubcategories))
	}
	for _, key := range units.SortedSubcategories {
		if _, ok := subcategoryProperties[key]; !ok {
			t.Errorf("schema missing subcategory property %q", key)
		}
		if !units.IsSubcategory(key) {
			t.Errorf("schema subcategory %q is not a recognised canonical subcategory", key)
		}
	}
}

func TestCategorizationJSONSchemaFreshEachCall(t *testing.T) {
	first := categorizationJSONSchema()
	first["type"] = "mutated"

	second := categorizationJSONSchema()
	if second["type"] == "mutated" {
		t.Fatal("categorizationJSONSchema returned a shared, mutable map across calls")
	}
}
