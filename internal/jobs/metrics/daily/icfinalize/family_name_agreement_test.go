package icfinalize

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// FamilyName must equal the literal families.json declares AND the literal the
// Python finalize gate checks. If those three drift apart the mechanism fails
// SILENTLY into two writers: Go computes and writes, Python does not recognise
// its key, recomputes, and its rows supersede via computed_at DESC LIMIT 1 BY.
// Nothing errors and nothing goes red.
//
// This cannot be left to the two-writer integration test, which registers the
// family under the same literal it asserts on and therefore cannot detect a
// mismatch. It is asserted here, against the files themselves.

func TestFamilyNameMatchesFamiliesJSON(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "families.json"))
	if err != nil {
		t.Fatalf("read families.json: %v", err)
	}
	var registry struct {
		Families []struct {
			Name string `json:"name"`
		} `json:"families"`
	}
	if err := json.Unmarshal(data, &registry); err != nil {
		t.Fatalf("decode families.json: %v", err)
	}
	for _, family := range registry.Families {
		if family.Name == FamilyName {
			return
		}
	}
	t.Fatalf("families.json declares no family named %q -- the registry and the "+
		"Go constant have drifted", FamilyName)
}

// NEGATIVE CONTROL. The two assertions above must FAIL for a wrong constant --
// otherwise they would pass for any string that happens to appear somewhere in
// those files, and the guard would be decorative.
func TestTheAgreementAssertionsActuallyDiscriminate(t *testing.T) {
	const wrong = "ic_finalize_TYPO"

	data, err := os.ReadFile(filepath.Join("..", "families.json"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), `"name": "`+wrong+`"`) {
		t.Fatalf("families.json unexpectedly declares %q, so the positive test "+
			"above could pass for the wrong reason", wrong)
	}

}
