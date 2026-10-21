package workflows_test

import (
	"strings"
	"unicode"
)

// The python guards these tests port used `re` on str, where \s, \w, \d and \b are Unicode-aware; Go's are ASCII. These
// fragments give the same classes, so a workflow or script text holding a non-breaking space, an accented letter or a
// non-ASCII digit is classified as the python guard classified it.
const (
	pySpace     = `\s\x{1C}-\x{1F}\x{85}\x{A0}\x{1680}\x{2000}-\x{200A}\x{2028}\x{2029}\x{202F}\x{205F}\x{3000}`
	pyS         = `[` + pySpace + `]`
	pyNotS      = `[^` + pySpace + `]`
	pyWordSet   = `\p{L}\p{N}_`
	pyW         = `[` + pyWordSet + `]`
	pyD         = `\p{Nd}`
	pyBefore    = `(?:^|[^` + pyWordSet + `])`
	pyAfterLine = `(?:[^` + pyWordSet + `\n]|$)`
)

// pyWordBounded reports whether word occurs in text bounded by non-word characters on both sides (python \bword\b).
func pyWordBounded(text, word string) bool {
	for from := 0; ; {
		i := strings.Index(text[from:], word)
		if i < 0 {
			return false
		}
		i += from
		j := i + len(word)
		before, after := true, true
		if i > 0 {
			r := []rune(text[:i])
			before = !isPyWordRune(r[len(r)-1])
		}
		if j < len(text) {
			after = !isPyWordRune([]rune(text[j:])[0])
		}
		if before && after {
			return true
		}
		from = i + 1
	}
}

func isPyWordRune(r rune) bool {
	return r == '_' || (r >= '0' && r <= '9') || (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z') ||
		(r > 127 && (isLetterOrNumber(r)))
}

func isLetterOrNumber(r rune) bool {
	return unicode.IsLetter(r) || unicode.IsNumber(r)
}

// pyAfterAny and the no-newline form: python \b after a word, where the next character may be a newline (existence checks)
// or must stay on the line (a following [^\n]* part).
const pyAfterAny = `(?:[^` + pyWordSet + `]|$)`

func isPySpace(r rune) bool { return unicode.IsSpace(r) || (r >= 0x1C && r <= 0x1F) }

// pyStrip is python str.strip(): Unicode whitespace, including U+001C..U+001F.
func pyStrip(s string) string { return strings.TrimFunc(s, isPySpace) }

// pyAtoi is python int() on a run of decimal digits of any script.
func pyAtoi(s string) (int, bool) {
	if s == "" {
		return 0, false
	}
	n := 0
	for _, r := range s {
		v := -1
		for _, rg := range unicode.Nd.R16 {
			if r >= rune(rg.Lo) && r <= rune(rg.Hi) && rg.Stride == 1 {
				v = int(r-rune(rg.Lo)) % 10
			}
		}
		for _, rg := range unicode.Nd.R32 {
			if r >= rune(rg.Lo) && r <= rune(rg.Hi) && rg.Stride == 1 {
				v = int(r-rune(rg.Lo)) % 10
			}
		}
		if v < 0 {
			return 0, false
		}
		n = n*10 + v
	}
	return n, true
}
