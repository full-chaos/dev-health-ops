package httpapi

import (
	"net/url"
	"reflect"
	"testing"
)

// CHAOS-14073, ruling B: ParseQuery reads a RAW (not percent-encoded) byte >= 0x80 as UTF-8, with one U+FFFD for each maximal
// ill-formed subpart, as it reads a percent-encoded byte. Starlette decodes the raw query as latin-1 before parse_qsl, which
// reads each raw byte as its own character (raw UTF-8 "é" reads as "Ã©"). That is a decoding bug of the Python side and not a
// contract the Go port copies: this divergence is declared here and pinned by the literals below.
//
// The Starlette columns were recorded from starlette 1.10.0 QueryParams(raw bytes) on CPython 3.14.7 (cell log sha256
// ecabca7b3aac8328ceaf9f6e6a7e46cc3075ef82edb6355b49944c4c1badc3f8, CHAOS-14073). A percent-encoded input reads the same in both.
func TestParseQueryRawNonASCIIDivergesFromStarlette(t *testing.T) {
	for _, tc := range []struct {
		raw                          string
		starletteKey, starletteValue string
		goKey, goValue               string
	}{
		{"k=\xc3\xa9", "k", "Ã©", "k", "é"},
		{"k=\xff", "k", "ÿ", "k", "�"},
		{"k=\xc3%A9", "k", "Ã�", "k", "é"},
		{"\xc3\xa9=v", "Ã©", "v", "é", "v"},
		{"k=\xe2\x82\xac", "k", "â\u0082¬", "k", "€"},
		{"k=\xf0\x9f\x98\x80", "k", "ð\u009f\u0098\u0080", "k", "\U0001f600"},
		{"k=\xc3", "k", "Ã", "k", "�"},
		{"k=\xe0\x80\x80", "k", "à\u0080\u0080", "k", "���"},
		{"k=\x80", "k", "\u0080", "k", "�"},
		{"k=a+b\xc3\xa9", "k", "a bÃ©", "k", "a bé"},
	} {
		got := ParseQuery(tc.raw)
		if want := (url.Values{tc.goKey: {tc.goValue}}); !reflect.DeepEqual(got, want) {
			t.Errorf("ParseQuery(%q) = %q, want %q", tc.raw, got, want)
		}
		if reflect.DeepEqual(got, url.Values{tc.starletteKey: {tc.starletteValue}}) {
			t.Errorf("ParseQuery(%q) reads as Starlette does; the declared divergence is gone, remove its row", tc.raw)
		}
	}
}

// The percent-encoded forms of the same bytes read the same in Go and in Starlette (recorded from starlette 1.10.0).
func TestParseQueryPercentEncodedNonASCIIMatchesStarlette(t *testing.T) {
	for _, tc := range []struct {
		raw, key, value string
	}{
		{"k=%C3%A9", "k", "é"},
		{"k=%FF", "k", "�"},
		{"k=a%C3b", "k", "a�b"},
		{"k=%E2%82%AC", "k", "€"},
	} {
		if got, want := ParseQuery(tc.raw), (url.Values{tc.key: {tc.value}}); !reflect.DeepEqual(got, want) {
			t.Errorf("ParseQuery(%q) = %q, want %q", tc.raw, got, want)
		}
	}
}
