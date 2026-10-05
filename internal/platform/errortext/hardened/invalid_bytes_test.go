package hardened

import (
	"strings"
	"testing"
	"unicode/utf8"
)

// CHAOS-8404 (declared difference of CHAOS-7947): an invalid UTF-8 byte in an error text. Main's sync chain (Go's regexp) kept every byte it did not
// replace; the engine reads the text as runes, so each invalid byte becomes ONE U+FFFD, as the recorded Python answer and the parity entry do. This is
// the behaviour of record: every sink of the sync path either rejects an invalid byte (a Postgres text column: SQLSTATE 22021) or replaces it (encoding/json,
// the JSON log handler) or escapes it (the text log handler), so a kept byte had no reader and made the text-column write fail. The test pins:
// (1) each invalid byte becomes exactly one U+FFFD, (2) the credential redaction is equal to the redaction of the same text with the bytes already replaced,
// (3) the result is valid UTF-8. RED if the bytes are kept again or dropped.
func TestSyncPathReplacesEachInvalidByteWithOneReplacementRuneAndRedactsAsBefore(t *testing.T) {
	rows := []struct{ name, in, want string }{
		{"stray byte before a credential", "prefix-\xff-Bearer abc", "prefix-�-[REDACTED]"},
		{"stray byte after a credential", "Bearer abc \xfe tail", "[REDACTED] � tail"},
		{"truncated multi-byte sequence", "caf\xc3 token=abc", "caf� [REDACTED]"},
		{"two stray bytes in a row are two replacement runes", "\x80\x81 Bearer abc", "�� [REDACTED]"},
		{"no credential at all", "plain \xff text", "plain � text"},
		{"stray byte inside the secret value", "token=ab\xffcd tail", "[REDACTED] tail"},
		{"valid multi-byte text is untouched", "café Bearer abc", "café [REDACTED]"},
		{"a three-byte prefix of a four-byte rune is three replacement runes", "\xf0\x9f\x98 token=abc", "��� [REDACTED]"},
	}
	for _, row := range rows {
		got := SyncWriters(row.in)
		if got != row.want {
			t.Errorf("%s: SyncWriters(%q) = %q, want %q", row.name, row.in, got, row.want)
		}
		if !utf8.ValidString(got) {
			t.Errorf("%s: SyncWriters(%q) = %q is not valid UTF-8", row.name, row.in, got)
		}
		if same := SyncWriters(eachInvalidByteAsReplacementRune(row.in)); same != got {
			t.Errorf("%s: redaction differs from the same text with the bytes already replaced: %q vs %q", row.name, got, same)
		}
	}
}

// eachInvalidByteAsReplacementRune writes U+FFFD for every byte that is not part of a valid rune.
func eachInvalidByteAsReplacementRune(text string) string {
	var out strings.Builder
	for index := 0; index < len(text); {
		r, size := utf8.DecodeRuneInString(text[index:])
		if r == utf8.RuneError && size == 1 {
			out.WriteRune('�')
		} else {
			out.WriteString(text[index : index+size])
		}
		index += size
	}
	return out.String()
}
