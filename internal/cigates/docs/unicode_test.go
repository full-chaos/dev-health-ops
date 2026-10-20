package docs_test

import (
	"strings"
	"testing"
)

// Unicode sweep of every regular expression and string function ported from Python (Python re and str are Unicode, Go
// regexp \s \w \d \b are ASCII): each row is a planted non-ASCII input that Python treats one way.
//
//   rawCSSValue  Python `#[0-9a-fA-F]{3,8}\b`   -> Unicode word boundary written out (`[^\p{L}\p{N}_]|$`)
//   cssToken     Python `(--[A-Za-z0-9_-]+)\s*:\s*([^;]+);` -> `\s` widened to `pySpaceClass` (Python isspace: `\t\n\v\f\r`, U+001C..U+001F, U+0085, `\p{Z}`); the name class is ASCII in both
//   TrimLeft     Python `lstrip()`, `strip()`   -> trimLeftPy, trimPy (isPySpace)
//   casefold     Python `casefold()`            -> cases.Fold
//   splitlines   Python `splitlines()`          -> splitLines
//   split        Python `split()`               -> strings.FieldsFunc(isPySpace)

func TestRawCSSValueFollowsPythonUnicodeWordBoundaries(t *testing.T) {
	for _, c := range []struct{ name, css, want string }{
		{"ascii", ".x { color: #badc0d; }", "#badc0d"},
		{"end of line", ".x { color: #abc", "#abc"},
		{"no-break space after", ".x { color: #abc\u00a0}", "#abc"},
		{"file separator after (python non-word)", ".x { color: #abc\x1f}", "#abc"},
		{"unit separator after (python non-word)", ".x { color: #abc\x1c}", "#abc"},
		{"declared token after a U+001C indent is allowed", ":root {\n\x1c\x1c--fc-void: #08080A;\n}", ""},
		{"declared token after a U+001F indent is allowed", ":root {\n\x1f--fc-void: #08080A;\n}", ""},
		{"letter after (cafe)", ".x { color: #abc\u00e9; }", ""},
		{"non-ASCII digit after", ".x { color: #abc\u0663; }", ""},
		{"non-ASCII letter before is irrelevant", ".caf\u00e9 { color: #bad; }", "#bad"},
		{"nine hex digits never match", ".x { color: #abcdef012; }", ""},
		{"declared token in :root is allowed", ":root {\n  --fc-void: #08080A;\n}", ""},
		{"declared token after a no-break-space indent is allowed", ":root {\n\u00a0\u00a0--fc-void: #08080A;\n}", ""},
		{"crlf line ends are line ends", ":root {\r\n  --fc-void: #08080A;\r\n}\r\n.a { color: var(--fc-void); }", ""},
		{"a raw value after a unicode line separator is found", ".a { }\u2028.b { color: #fff; }", "#fff"},
	} {
		if got := undeclaredRawCSSValue(c.css); got != c.want {
			t.Errorf("%s: undeclaredRawCSSValue = %q, want %q", c.name, got, c.want)
		}
	}
}

func TestPaletteTokensFollowPythonUnicodeWhitespaceAndAsciiNames(t *testing.T) {
	for _, c := range []struct{ name, css, token, want string }{
		{"ascii", "--fc-void: #08080A;", "--fc-void", "#08080A"},
		{"no-break space after the colon", "--fc-void:\u00a0#08080A;", "--fc-void", "#08080A"},
		{"no-break space before the colon", "--fc-ink\u00a0: #15171A;", "--fc-ink", "#15171A"},
		{"U+001C after the colon and U+001C before it", "--fc-void\x1c:\x1c#08080A;", "--fc-void", "#08080A"},
		{"U+001F and vertical tab around the colon", "--fc-void\x1f:\v#08080A;", "--fc-void", "#08080A"},
		{"value padded by U+001C and U+001F is stripped", "--fc-aqua: \x1c#04B7C4\x1f;", "--fc-aqua", "#04B7C4"},
		{"ideographic space and next line", "--fc-gold\u3000:\u0085#FFDB4B;", "--fc-gold", "#FFDB4B"},
		{"value padded by no-break spaces is stripped", "--fc-aqua: \u00a0#04B7C4\u00a0;", "--fc-aqua", "#04B7C4"},
		{"a letter ends the ASCII name, so there is no token", "--caf\u00e9: #fff;", "--caf", ""},
		{"a non-ASCII digit ends the ASCII name, so there is no token", "--x\u0663: #fff;", "--x", ""},
	} {
		if got := paletteTokens(c.css)[c.token]; got != c.want {
			t.Errorf("%s: token %s = %q, want %q", c.name, c.token, got, c.want)
		}
	}
}

func TestFoldAndNormalizedFollowPythonCasefoldAndSplit(t *testing.T) {
	for _, c := range []struct{ name, got, want string }{
		{"casefold sharp s", fold("Stra\u00dfe"), "strasse"},
		{"casefold capital sharp s", fold("\u1e9e"), "ss"},
		{"casefold final sigma", fold("\u039f\u03a3"), "\u03bf\u03c3"},
		{"normalized collapses no-break spaces", normalized("not\u00a0a  measure\u3000of\nperformance"), "not a measure of performance"},
		{"normalized keeps non-ASCII words", normalized("Caf\u00e9  AU\u00dfen"), "caf\u00e9 aussen"},
		{"normalized splits on U+001C", normalized("a\x1cb"), "a b"},
		{"normalized splits on U+001F", normalized("a\x1fb"), "a b"},
		{"normalized splits on vertical tab", normalized("a\vb"), "a b"},
	} {
		if c.got != c.want {
			t.Errorf("%s: got %q, want %q", c.name, c.got, c.want)
		}
	}
	if !strings.Contains(normalized("An association can\u00a0appear"), "an association can appear") {
		t.Error("a no-break space inside a required phrase must still match it")
	}
}

func TestPythonWhitespaceSetIsOneDefinition(t *testing.T) {
	// Python str.isspace (and so str.strip, str.split and re \s) is the set of Unicode white space plus U+001C..U+001F.
	for _, r := range []rune{'\t', '\n', '\v', '\f', '\r', 0x1c, 0x1d, 0x1e, 0x1f, ' ', 0x85, 0xa0, 0x1680, 0x2000, 0x200a, 0x2028, 0x2029, 0x202f, 0x205f, 0x3000} {
		if !isPySpace(r) {
			t.Errorf("U+%04X is Python white space and isPySpace refuses it", r)
		}
	}
	for _, r := range []rune{'a', '0', '_', 0x00e9, 0x0663, 0x200b, 0x1d, 'x'} {
		if r == 0x1d {
			continue
		}
		if isPySpace(r) {
			t.Errorf("U+%04X is not Python white space and isPySpace accepts it", r)
		}
	}
	if got := trimLeftPy("\x1c\x1f \u00a0--fc-"); got != "--fc-" {
		t.Errorf("trimLeftPy = %q", got)
	}
	if got := trimPy("\x1f\x1c a b \u00a0\x1c"); got != "a b" {
		t.Errorf("trimPy = %q", got)
	}
}
