package mail

import (
	"errors"
	"regexp"
	"strings"
	"testing"
)

func TestEncodeHeaderValue(t *testing.T) {
	for _, test := range []struct {
		name, value, want string
	}{
		{"ascii is verbatim", "Hello, World", "Hello, World"},
		{"empty", "", ""},
		{"ascii control chars pass through", "a\x00b\tc", "a\x00b\tc"},
		{"ascii that looks like an encoded word", "=?utf-8?q?x?=", "=?utf-8?q?x?="},
		// 3 bytes -> base64 4 chars, quoted-printable "Caf=C3=A9" is longer
		{"base64 when shorter", "Caf\u00e9 invite \u2603", "=?utf-8?b?Q2Fmw6kgaW52aXRlIOKYgw==?="},
		{"quoted-printable when shorter", "Caf\u00e9 au lait invite", "=?utf-8?q?Caf=C3=A9_au_lait_invite?="},
		// abcdef + e-acute (2 bytes): base64 = 12, quoted-printable = 6+6 = 12; a tie takes quoted-printable.
		{"a tie takes quoted-printable", "abcdef\u00e9", "=?utf-8?q?abcdef=C3=A9?="},
		{"quoted-printable keeps only RFC 2047's safe set", "\u00e9-!*+/ Az09", "=?utf-8?q?=C3=A9-!*+/_Az09?="},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := encodeHeaderValue(test.value)
			if err != nil {
				t.Fatal(err)
			}
			if got != test.want {
				t.Fatalf("got %q, want %q", got, test.want)
			}
		})
	}
}

// TestComposeRefusesLineBreaksInHeaderValues: a value that reaches a header
// from user input (an organization name in an invite subject) must never be
// able to start another header. Python refuses an ASCII value with a line
// break (HeaderWriteError/HeaderParseError) and the live oracle proves Go
// does too; for a NON-ASCII value with a line break Python instead folds it
// into a continuation line, and Go refuses -- the one place Go is stricter.
func TestComposeRefusesLineBreaksInHeaderValues(t *testing.T) {
	for _, test := range []struct {
		name          string
		from, to, sub string
	}{
		{"LF in subject", "a@x.test", "b@x.test", "A\nBcc: evil@x.test"},
		{"CR in subject", "a@x.test", "b@x.test", "A\rBcc: evil@x.test"},
		{"CRLF in subject", "a@x.test", "b@x.test", "A\r\nB"},
		{"non-ascii subject with a line break (Python folds; Go refuses)", "a@x.test", "b@x.test", "Caf\u00e9\nBcc: evil"},
		{"LF in from", "a@x.test\nBcc: evil@x.test", "b@x.test", "s"},
		{"LF in to", "a@x.test", "b@x.test\nBcc: evil@x.test", "s"},
	} {
		t.Run(test.name, func(t *testing.T) {
			raw, err := composeMIME(test.from, test.to, test.sub, "<p>x</p>")
			if !errors.Is(err, errUnsafeHeader) {
				t.Fatalf("composeMIME error = %v, want errUnsafeHeader (message bytes: %q)", err, raw)
			}
		})
	}
}

func TestPythonBoundaryHasPythonsShape(t *testing.T) {
	shape := regexp.MustCompile(`^={15}\d{19}==$`)
	seen := map[string]bool{}
	for i := 0; i < 50; i++ {
		boundary, err := pythonBoundary("")
		if err != nil {
			t.Fatal(err)
		}
		if !shape.MatchString(boundary) {
			t.Fatalf("boundary %q does not match ={15}\\d{19}==", boundary)
		}
		seen[boundary] = true
	}
	if len(seen) < 49 {
		t.Fatalf("only %d distinct boundaries in 50 draws", len(seen))
	}
}

func TestComposeUsesEveryLineBreakAsCRLFAndEndsWithOne(t *testing.T) {
	raw, err := composeMIME("a@x.test", "b@x.test", "s", "<p>x</p>\r\nline\rlast")
	if err != nil {
		t.Fatal(err)
	}
	text := string(raw)
	if strings.Contains(strings.ReplaceAll(text, "\r\n", ""), "\n") || strings.Contains(strings.ReplaceAll(text, "\r\n", ""), "\r") {
		t.Fatalf("message has a bare CR or LF: %q", text)
	}
	if !strings.HasSuffix(text, "--\r\n") {
		t.Fatalf("message does not end with the closing boundary and CRLF: %q", text[len(text)-20:])
	}
}

func TestEnvelopeAddress(t *testing.T) {
	for _, test := range []struct{ in, want string }{
		{"a@example.test", "a@example.test"},
		{"Dev Health <a@example.test>", "a@example.test"},
		{`"Health, Dev" <a@example.test>`, "a@example.test"},
		{"D\u00e9v <a@example.test>", "a@example.test"},
		{"<a@example.test>", "a@example.test"},
		{"Owner+Tag@Example.TEST", "Owner+Tag@Example.TEST"},
	} {
		if got := envelopeAddress(test.in); got != test.want {
			t.Errorf("envelopeAddress(%q) = %q, want %q", test.in, got, test.want)
		}
	}
}
