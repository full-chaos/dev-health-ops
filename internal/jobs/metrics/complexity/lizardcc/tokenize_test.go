package lizardcc

import "testing"

// pythonWhitespaceRunes is the FULL set of code points Python's
// str.isspace()/re `\s` (under Unicode mode) recognises as whitespace,
// verified directly on bigboy against a live Python interpreter rather
// than assumed from documentation -- see this package's tokenize.go for
// which of these `\p{Z}` and unicode.IsSpace already cover on their own
// (all but U+001C-U+001F and U+0085) and which need the explicit
// pythonExtraWhitespaceRunes addition.
var pythonWhitespaceRunes = []rune{
	'\t', '\n', '\v', '\f', '\r', ' ',
	0x1c, 0x1d, 0x1e, 0x1f, // FS, GS, RS, US
	0x85,   // NEL
	0xa0,   // NBSP
	0x1680, // OGHAM SPACE MARK
	0x2000, 0x2001, 0x2002, 0x2003, 0x2004, 0x2005, 0x2006, 0x2007, 0x2008, 0x2009, 0x200a,
	0x2028, // LINE SEPARATOR
	0x2029, // PARAGRAPH SEPARATOR
	0x202f, // NARROW NO-BREAK SPACE
	0x205f, // MEDIUM MATHEMATICAL SPACE
	0x3000, // IDEOGRAPHIC SPACE
}

// TestPythonWhitespaceSetIsPinned closes codex round r3's whitespace
// finding (#2253) comprehensively: rather than fixing only the single
// reported codepoint (U+001C), this enumerates EVERY character Python's
// str.isspace() recognises and asserts isAllSpace agrees on each one --
// pinned so a future change to pythonExtraWhitespaceRunes (or a
// regression in unicode.IsSpace's own coverage, which can't happen, but
// the guard costs nothing) is caught immediately rather than only on the
// one codepoint a codex round happened to construct.
func TestPythonWhitespaceSetIsPinned(t *testing.T) {
	for _, r := range pythonWhitespaceRunes {
		if !isAllSpace(string(r)) {
			t.Errorf("isAllSpace(%U) = false, want true (Python str.isspace() is true for this code point)", r)
		}
	}
}

// TestPythonExtraWhitespaceRunesIsPinnedExactly guards the named set
// itself: adding or removing an entry is a reviewed code change, not a
// silent one -- if this fails, whoever changed the set must update the
// pin (and, per this set's own doc, explain what `\p{Z}`/unicode.IsSpace
// newly cover or newly miss).
func TestPythonExtraWhitespaceRunesIsPinnedExactly(t *testing.T) {
	want := []rune{0x1c, 0x1d, 0x1e, 0x1f, 0x85}
	got := pythonExtraWhitespaceRunes
	if len(got) != len(want) {
		t.Fatalf("pythonExtraWhitespaceRunes = %U, want %U", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("pythonExtraWhitespaceRunes = %U, want %U", got, want)
		}
	}
}

// TestTokenizerGluesEveryPythonWhitespaceCharacter is the tokenizer-level
// companion to TestPythonWhitespaceSetIsPinned: isAllSpace agreeing is
// necessary but not sufficient -- the REGEX must actually glue each
// character into a whitespace-run token in the first place (the bug
// codex found was exactly a character isAllSpace would have classified
// correctly if it ever SAW a token containing only that character, but
// the regex never produced one, so it fell to the single-char catch-all
// instead). For each whitespace character, tokenizing `a<char>b` must
// produce exactly the three tokens ["a", "<char>", "b"] with the middle
// one satisfying isAllSpace -- proving the regex and isAllSpace agree on
// every member of the pinned set, not just in isolation.
func TestTokenizerGluesEveryPythonWhitespaceCharacter(t *testing.T) {
	for _, r := range pythonWhitespaceRunes {
		src := "a" + string(r) + "b"
		toks := cLikeTokenPattern.FindAllString(src, -1)
		if len(toks) != 3 {
			t.Errorf("%U: got tokens %q, want exactly 3 (\"a\", whitespace, \"b\")", r, toks)
			continue
		}
		if !isAllSpace(toks[1]) {
			t.Errorf("%U: middle token %q is not classified as whitespace", r, toks[1])
		}
	}
}
