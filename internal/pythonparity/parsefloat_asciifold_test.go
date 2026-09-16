package pythonparity

import (
	"strings"
	"testing"
	"unicode"
)

// TestAsciiFoldIsSpecificToTheKeywordSetItFolds turns the reasoning in
// asciiFold's doc comment into an executable claim.
//
// The comment says strings.EqualFold happens to agree with ASCII folding for
// inf/infinity/nan, because the only ASCII letters reachable by a non-ASCII
// simple fold are 'k' (U+212A) and 's' (U+017F), and neither appears in those
// words. That is true today and it is a property of the WORDS, not of the
// folding -- so it stops holding the moment a keyword containing an s or a k
// is added to this set.
//
// Prose cannot notice that. This test can, and it is the difference between a
// comment that ages into a lie and one that fails.
func TestAsciiFoldIsSpecificToTheKeywordSetItFolds(t *testing.T) {
	keywords := []string{"inf", "infinity", "nan"}

	// The two ASCII letters a non-ASCII simple fold can produce. Derived here
	// rather than hard-coded, so a Unicode table change is caught rather than
	// silently invalidating the argument.
	reachable := map[rune]rune{}
	for codePoint := rune(0x80); codePoint <= 0x10FFFF; codePoint++ {
		if codePoint >= 0xD800 && codePoint <= 0xDFFF {
			continue
		}
		for folded := unicode.SimpleFold(codePoint); folded != codePoint; folded = unicode.SimpleFold(folded) {
			if folded >= 'a' && folded <= 'z' {
				reachable[folded] = codePoint
			}
		}
	}

	if len(reachable) == 0 {
		t.Fatal("no ASCII letter is reachable by a non-ASCII simple fold; the " +
			"derivation has broken and this test would pass vacuously")
	}

	// The claim: no keyword letter is reachable, therefore EqualFold cannot
	// differ from ASCII folding on these words.
	for _, keyword := range keywords {
		for _, letter := range keyword {
			if source, ok := reachable[letter]; ok {
				t.Errorf(
					"keyword %q contains %q, which U+%04X simple-folds into. "+
						"strings.EqualFold now over-accepts for this keyword set, so "+
						"asciiFold's doc comment is WRONG and the two relations must "+
						"be distinguished explicitly rather than described as equivalent",
					keyword, string(letter), source,
				)
			}
		}
	}

	// And the control: the equivalence really is set-specific. A keyword
	// containing 's' IS over-accepted by EqualFold, which is why asciiFold is
	// used rather than EqualFold even though they agree today.
	if !strings.EqualFold("falſe", "false") {
		t.Error("expected strings.EqualFold to accept \"fal\\u017fe\" as \"false\"; " +
			"if it no longer does, Go's folding has changed and the whole argument " +
			"in asciiFold's comment needs re-deriving")
	}
	if asciiFold("falſe") == "false" {
		t.Error("asciiFold must NOT fold U+017F to 's' -- it folds ASCII only")
	}
}
