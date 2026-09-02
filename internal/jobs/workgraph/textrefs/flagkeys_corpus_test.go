package textrefs

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

type flagKeyCase struct {
	ID        string   `json:"id"`
	Text      string   `json:"text"`
	Keys      []string `json:"keys"`
	MinLength int      `json:"min_length"`
	Expect    []struct {
		FlagKey  string `json:"flag_key"`
		RawMatch string `json:"raw_match"`
	} `json:"extract_flag_key_refs"`
}

// TestFlagKeyRefsMatchFrozenPython drives ExtractFlagKeyRefs over the flag-key
// axis, which lives in its own list because each case carries a REGISTRY as well
// as a text and does not fit the shape every other case has.
func TestFlagKeyRefsMatchFrozenPython(t *testing.T) {
	path := filepath.Join("..", "..", "..", "..", "tests", "fixtures", "text_reference_parity.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read corpus: %v", err)
	}
	var doc struct {
		Schema string        `json:"schema"`
		Cases  []flagKeyCase `json:"flag_key_cases"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("decode corpus: %v", err)
	}
	if len(doc.Cases) == 0 {
		t.Fatal("no flag-key cases; every assertion below would be vacuous")
	}

	// A corpus of all-empty expectations would pass against an extractor that
	// returns nothing, so the non-empty count is asserted rather than assumed.
	nonEmpty := 0
	for _, c := range doc.Cases {
		if len(c.Expect) > 0 {
			nonEmpty++
		}
	}
	if nonEmpty < 10 {
		t.Fatalf("only %d flag-key cases have a non-empty expectation; the axis "+
			"cannot discriminate an extractor that returns nothing", nonEmpty)
	}

	pass, fail := 0, 0
	for _, c := range doc.Cases {
		// The corpus carries the min_length each case was generated with. Passing
		// FlagKeyMinLength unconditionally is what hid the sentinel defect: every
		// row used the default, so no row could see an override.
		got := ExtractFlagKeyRefs(c.Text, c.Keys, c.MinLength)
		type pair struct{ FlagKey, RawMatch string }
		gotPairs := []pair{}
		for _, r := range got {
			gotPairs = append(gotPairs, pair{r.FlagKey, r.RawMatch})
		}
		wantPairs := []pair{}
		for _, r := range c.Expect {
			wantPairs = append(wantPairs, pair{r.FlagKey, r.RawMatch})
		}
		g, _ := json.Marshal(gotPairs)
		w, _ := json.Marshal(wantPairs)
		if string(g) == string(w) {
			pass++
			continue
		}
		fail++
		t.Errorf("%s: keys=%q\n  got:  %s\n  want: %s", c.ID, c.Keys, g, w)
	}
	t.Logf("extract_flag_key_refs        %d/%d PASS (%d non-empty expectations)",
		pass, pass+fail, nonEmpty)
}
