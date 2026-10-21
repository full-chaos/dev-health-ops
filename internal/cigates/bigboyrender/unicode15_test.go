package bigboyrender

import (
	"regexp"
	"testing"
	"unicode/utf8"
)

// The regexp class strings and the rune tables are two spellings of one set each; every code point must agree.
func TestRegexpClassesMatchTheirTables(t *testing.T) {
	for name, pair := range map[string]struct {
		class string
		in    func(rune) bool
	}{
		"pyWord":  {pyWord, func(r rune) bool { return inRanges(pyWordRanges, r) }},
		"pySpace": {pySpace, isPySpace},
	} {
		re := regexp.MustCompile(`^[` + pair.class + `]$`)
		bad := 0
		for r := rune(0); r <= utf8.MaxRune; r++ {
			if r >= 0xd800 && r <= 0xdfff {
				continue // not encodable in UTF-8: no string can hold it
			}
			if got, want := re.MatchString(string(r)), pair.in(r); got != want {
				if bad < 5 {
					t.Errorf("%s: U+%04X regexp %v, table %v", name, r, got, want)
				}
				bad++
			}
		}
		if bad > 0 {
			t.Errorf("%s: %d code points disagree", name, bad)
		}
	}
}
