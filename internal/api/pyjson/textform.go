package pyjson

import (
	"unicode/utf8"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// TextClass names the kind of text a field holds; each class refuses its own set of forms.
type TextClass int

const (
	// ClassEmail is an email address: U+FFFD is refused with the other forms.
	ClassEmail TextClass = iota + 1
	// ClassDisplayText is a person or organization name: a literal U+FFFD is storable text and is kept.
	ClassDisplayText
	// ClassURL is a URL held as text: U+FFFD is refused with the other forms.
	ClassURL
)

// TextForm names a form of text a field refuses before any write; TextFormNone means the text is acceptable.
type TextForm string

const (
	TextFormNone            TextForm = ""
	TextFormReplacementChar TextForm = "replacement_char"
	TextFormLoneSurrogate   TextForm = "lone_surrogate"
	TextFormNUL             TextForm = "nul"
	TextFormControl         TextForm = "control"
	TextFormInvalidUTF8     TextForm = "invalid_utf8"
)

// BadTextForm returns the first refused form in text (WTF-8, as the body decoder keeps it), or TextFormNone. It is
// the one input rule go-api and auth-service share for email, display text and URL fields.
func BadTextForm(class TextClass, text string) TextForm {
	if class == ClassEmail {
		// The address validator trims whitespace-like characters at either edge (Python str.strip), so a character
		// trimmed there never reaches a stored address.
		text = pythonparity.Strip(text)
	}
	if HasSurrogate(text) {
		return TextFormLoneSurrogate
	}
	for index := 0; index < len(text); {
		r, size := utf8.DecodeRuneInString(text[index:])
		index += size
		switch {
		case r == utf8.RuneError && size <= 1:
			return TextFormInvalidUTF8
		case r == 0:
			return TextFormNUL
		case r < 0x20, r >= 0x7f && r <= 0x9f:
			return TextFormControl
		case r == utf8.RuneError && class != ClassDisplayText:
			return TextFormReplacementChar
		}
	}
	return TextFormNone
}
