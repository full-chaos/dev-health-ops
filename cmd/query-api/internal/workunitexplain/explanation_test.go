package workunitexplain

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

// lineSeparator/paragraphSeparator are built from code points rather than
// written as literals: a source file carrying a RAW U+2028 is invisible in
// review, and these tests turn on telling the raw character apart from its
// escaped spelling.
const (
	lineSeparator      = "\u2028"
	paragraphSeparator = "\u2029"
)

var separatorHex = map[string]string{lineSeparator: "2028", paragraphSeparator: "2029"}

// encoderEscape is the spelling the encoder emits for a separator: one
// backslash, then "u", then the code point's hex digits.
func encoderEscape(separator string) string {
	return "\\" + "u" + separatorHex[separator]
}

func encodeForTest(t *testing.T, value any) string {
	t.Helper()
	var buffer bytes.Buffer
	if err := WriteJSON(&buffer, value); err != nil {
		t.Fatalf("WriteJSON: %v", err)
	}
	return buffer.String()
}

// TestWriteJSONLeavesHTMLCharactersRaw pins the half of the reference's
// encoding this path DOES reproduce: it escapes none of `<`, `>` or `&`.
func TestWriteJSONLeavesHTMLCharactersRaw(t *testing.T) {
	body := encodeForTest(t, map[string]string{"detail": "a <tag> & more"})
	if !strings.Contains(body, "<tag> & more") {
		t.Errorf("body escaped an HTML character the reference leaves raw: %s", body)
	}
}

// TestWriteJSONEscapesTheSeparatorsAndSaysSo pins the half it does NOT
// reproduce, as a property rather than as a comment.
//
// SetEscapeHTML governs only the HTML trio; U+2028 and U+2029 are governed
// by a separate option this encoder type exposes no setter for, so they
// leave escaped where the reference writes their raw bytes. That is an
// accepted difference -- both spellings decode to the same string -- and
// this test exists so a reader finds the difference stated and measured
// instead of discovering it from a byte comparison somewhere downstream.
func TestWriteJSONEscapesTheSeparatorsAndSaysSo(t *testing.T) {
	value := map[string]string{"detail": "before " + lineSeparator + " and " + paragraphSeparator + " after"}
	body := encodeForTest(t, value)

	for _, separator := range []string{lineSeparator, paragraphSeparator} {
		if !strings.Contains(body, encoderEscape(separator)) {
			t.Errorf("body does not carry the escaped spelling of %q: %q", separator, body)
		}
		if strings.Contains(body, separator) {
			t.Errorf("body carries the raw bytes of %q; this path cannot emit them: %q", separator, body)
		}
	}

	// The difference is confined to the ENCODING: what a client decodes is
	// the original string, separators included.
	var decoded map[string]string
	if err := json.Unmarshal([]byte(body), &decoded); err != nil {
		t.Fatalf("decode own output: %v", err)
	}
	if decoded["detail"] != value["detail"] {
		t.Errorf("decoded detail = %q, want %q", decoded["detail"], value["detail"])
	}
}

// TestWriteJSONAppendsExactlyOneNewline pins the trailing byte, which is
// insignificant JSON whitespace and the same byte every sibling route's
// body carries.
func TestWriteJSONAppendsExactlyOneNewline(t *testing.T) {
	body := encodeForTest(t, map[string]string{"detail": "plain"})
	if !strings.HasSuffix(body, "}\n") || strings.HasSuffix(body, "}\n\n") {
		t.Errorf("want exactly one trailing newline, got %q", body)
	}
}
