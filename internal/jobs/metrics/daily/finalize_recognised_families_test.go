package daily

import (
	"slices"
	"testing"
)

// A finalize family of families.json that SetNativeFinalizeFamilies does not
// recognise makes the setter refuse the whole map, and every finalize run then
// fails with ErrFinalizeFamilyIncomplete; a recognised name with no registry
// row has no contract. The two lists are one set.
func TestRecognisedFinalizeFamiliesAreTheRegistrysFinalizeFamilies(t *testing.T) {
	registry, err := LoadRegistry()
	if err != nil {
		t.Fatal(err)
	}
	var finalize []string
	for _, family := range registry.Families {
		if family.Phase == "finalize" {
			finalize = append(finalize, family.Name)
		}
	}
	recognised := slices.Clone(pythonRecognisedFinalizeFamilies)
	slices.Sort(finalize)
	slices.Sort(recognised)
	if len(finalize) == 0 || !slices.Equal(finalize, recognised) {
		t.Fatalf("families.json finalize families %v, recognised finalize families %v", finalize, recognised)
	}
}
