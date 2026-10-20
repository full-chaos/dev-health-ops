package docs_test

import (
	"strings"
	"testing"
)

// Unicode sweep of every regular expression and string function ported from Python (Python re and str are Unicode, Go
// regexp \s \w \d \b are ASCII): each row is a planted non-ASCII input that Python treats one way.
//
//   rawCSSValue  Python `#[0-9a-fA-F]{3,8}\b`   -> Unicode word boundary written out (`[^\p{L}\p{N}_]|$`)
//   cssToken     Python `(--[A-Za-z0-9_-]+)\s*:\s*([^;]+);` -> `\s` widened to `[\s\p{Z}\x{0085}]`; the name class is ASCII in both
//   TrimLeft     Python `lstrip()`              -> unicode.IsSpace
//   casefold     Python `casefold()`            -> cases.Fold
//   splitlines   Python `splitlines()`          -> splitLines
//   split        Python `split()`               -> strings.Fields (Unicode white space in both)

func TestRawCSSValueFollowsPythonUnicodeWordBoundaries(t *testing.T) {
	for _, c := range []struct{ name, css, want string }{
		{"ascii", ".x { color: #badc0d; }", "#badc0d"},
		{"end of line", ".x { color: #abc", "#abc"},
		{"no-break space after", ".x { color: #abc\u00a0}", "#abc"},
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
	} {
		if c.got != c.want {
			t.Errorf("%s: got %q, want %q", c.name, c.got, c.want)
		}
	}
	if !strings.Contains(normalized("An association can\u00a0appear"), "an association can appear") {
		t.Error("a no-break space inside a required phrase must still match it")
	}
}
