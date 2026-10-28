package pyjson

import "testing"

func TestBadTextFormNamesTheFirstRefusedFormPerClass(t *testing.T) {
	for _, tc := range []struct {
		name  string
		text  string
		class TextClass
		want  TextForm
	}{
		{"plain email", "a@example.com", ClassEmail, TextFormNone},
		{"plain name with emoji", "Ann \U0001F600", ClassDisplayText, TextFormNone},
		{"empty", "", ClassURL, TextFormNone},
		{"edge controls on an email are trimmed", "\t\x1c a@example.com\n\u0085", ClassEmail, TextFormNone},
		{"control inside an email is refused", "a\tb@example.com", ClassEmail, TextFormControl},
		{"edge controls on a name are refused", "\tAnn", ClassDisplayText, TextFormControl},
		{"replacement char in email", "a�b@example.com", ClassEmail, TextFormReplacementChar},
		{"replacement char in url", "https://e.test/�", ClassURL, TextFormReplacementChar},
		{"replacement char in a name is kept", "A�B", ClassDisplayText, TextFormNone},
		{"lone high surrogate", FromRunes([]rune{'a', 0xd800}), ClassDisplayText, TextFormLoneSurrogate},
		{"lone low surrogate", FromRunes([]rune{0xdc00}), ClassEmail, TextFormLoneSurrogate},
		{"nul", "A\x00B", ClassDisplayText, TextFormNUL},
		{"c0 control", "A\x01B", ClassURL, TextFormControl},
		{"tab", "A\tB", ClassDisplayText, TextFormControl},
		{"line feed", "A\nB", ClassURL, TextFormControl},
		{"carriage return", "A\rB", ClassEmail, TextFormControl},
		{"del", "A\x7fB", ClassDisplayText, TextFormControl},
		{"c1 control", "A\u0085B", ClassDisplayText, TextFormControl},
		{"c1 upper edge", "A\u009fB", ClassDisplayText, TextFormControl},
		{"first printable after c1", "A B", ClassDisplayText, TextFormNone},
		{"invalid utf-8 byte", "A\xffB", ClassDisplayText, TextFormInvalidUTF8},
		{"truncated sequence", "A\xe2\x82", ClassURL, TextFormInvalidUTF8},
	} {
		if got := BadTextForm(tc.class, tc.text); got != tc.want {
			t.Errorf("%s: %q, want %q", tc.name, got, tc.want)
		}
	}
}
