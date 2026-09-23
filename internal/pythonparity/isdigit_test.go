package pythonparity

import "testing"

// TestIsDigitMatchesLivePython pins IsDigit against str.isdigit(), verified
// live against /usr/bin/python3 (and the repo .venv's pydantic-carrying
// interpreter): ASCII/Unicode decimal digits, the superscript/subscript/
// circled/parenthesized "Digit but not Decimal" set IsDigit adds on top of
// the Nd table, and negative controls -- letters, space, and vulgar
// fractions (½, ¼, ⅓: category No like the superscripts, but NOT digits --
// the case a coarser "any No codepoint" rule would get wrong).
func TestIsDigitMatchesLivePython(t *testing.T) {
	cases := []struct {
		r    rune
		want bool
	}{
		{'0', true}, {'9', true},
		{'²', true}, // U+00B2 SUPERSCRIPT TWO -- the live round's repro
		{'³', true}, // U+00B3 SUPERSCRIPT THREE
		{'¹', true}, // U+00B9 SUPERSCRIPT ONE
		{'⁰', true}, // U+2070 SUPERSCRIPT ZERO
		{'₅', true}, // U+2085 SUBSCRIPT FIVE
		{'①', true}, // U+2460 CIRCLED DIGIT ONE
		{'⑤', true}, // U+2464 CIRCLED DIGIT FIVE
		{'༠', true}, // U+0F20 TIBETAN DIGIT ZERO (Nd)
		{'a', false}, {'A', false}, {' ', false},
		{'½', false}, // U+00BD VULGAR FRACTION ONE HALF -- category No, not a digit
		{'¼', false}, // U+00BC
		{'↉', false}, // U+2189 VULGAR FRACTION ZERO THIRDS
	}
	for _, c := range cases {
		if got := IsDigit(c.r); got != c.want {
			t.Errorf("IsDigit(%q U+%04X) = %v, want %v", c.r, c.r, got, c.want)
		}
	}
}
