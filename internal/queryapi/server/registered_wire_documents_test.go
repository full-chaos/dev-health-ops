package server

import "testing"

// The smoke's legacy texts (CHAOS-9146) are the registry's: for every operation the smoke checks, the
// legacy texts it lists hash to exactly legacyDigestsByOperation[operation], in order, so a legacy text
// added to one list and not the other fails here and the smoke never accepts a text the edge refuses
// (or refuses one the edge accepts).
func TestWebPathSmokeLegacyTextsAreTheRegistryLegacyDigests(t *testing.T) {
	for operation := range webPathSmokeOperations {
		documents, ok := WebPathSmokeRegisteredDocuments(operation)
		if !ok || len(documents) == 0 || documents[0].Legacy {
			t.Fatalf("%s: documents %d ok %t: want the current text first", operation, len(documents), ok)
		}
		if documents[0].Text != webPathSmokeOperations[operation] {
			t.Errorf("%s: the first document is not the current registered text", operation)
		}
		want := legacyDigestsByOperation[operation]
		got := documents[1:]
		if len(got) != len(want) {
			t.Errorf("%s: the smoke lists %d legacy texts, the registry %d", operation, len(got), len(want))
			continue
		}
		for index, document := range got {
			if !document.Legacy || digestHex(document.Text) != want[index] {
				t.Errorf("%s: legacy text %d does not hash to legacyDigestsByOperation[%s][%d]", operation, index, operation, index)
			}
		}
	}
	if _, ok := WebPathSmokeRegisteredDocuments("noSuchOperation"); ok {
		t.Error("an operation the smoke does not check must be unknown")
	}
}
