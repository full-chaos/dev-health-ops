package httpapi

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http/httptest"
	"net/url"
	"reflect"
	"sort"
	"strings"
	"testing"
	"unicode/utf8"
)

func TestParseQueryIsCPythonParseQsl(t *testing.T) {
	for _, tc := range []struct {
		raw  string
		want url.Values
	}{
		// recorded from CPython 3.12.3 parse_qsl(raw, keep_blank_values=True)
		{"", url.Values{}},
		{"name=a;b", url.Values{"name": {"a;b"}}},
		{"name=a&name=b;c", url.Values{"name": {"a", "b;c"}}},
		{"a=1;b=2&c", url.Values{"a": {"1;b=2"}, "c": {""}}},
		{"team_id=jira%3AENG%zz", url.Values{"team_id": {"jira:ENG%zz"}}},
		{"team_id=jira:ENG%zz+Platform", url.Values{"team_id": {"jira:ENG%zz Platform"}}},
		{"team_id=jira%3AENG%FF", url.Values{"team_id": {"jira:ENG�"}}},
		{"k=%E2%82A", url.Values{"k": {"�A"}}},
		{"k=%F0%9F%98", url.Values{"k": {"�"}}},
		{"k=%ED%A0%80", url.Values{"k": {"���"}}},
		{"k=%C0%AF", url.Values{"k": {"��"}}},
		{"k=%E0%80%80", url.Values{"k": {"���"}}},
		{"k=%F4%90%80%80", url.Values{"k": {"����"}}},
		{"k=%EF%BF%BD", url.Values{"k": {"�"}}},
		{"k=%%41", url.Values{"k": {"%A"}}},
		{"k=%2B+x", url.Values{"k": {"+ x"}}},
		{"k=%2", url.Values{"k": {"%2"}}},
		{"k=%", url.Values{"k": {"%"}}},
		{"k=%4", url.Values{"k": {"%4"}}},
		{"k=%zz%41", url.Values{"k": {"%zzA"}}},
		{"flag", url.Values{"flag": {""}}},
		{"=v", url.Values{"": {"v"}}},
		{"=", url.Values{"": {""}}},
		{"&&a=1&", url.Values{"a": {"1"}}},
		{"a=1=2", url.Values{"a": {"1=2"}}},
		{"k%3Bx=1", url.Values{"k;x": {"1"}}},
		{"k%zz=1", url.Values{"k%zz": {"1"}}},
		{"k=%C3%A9", url.Values{"k": {"é"}}},
		{"k=%c3%a9", url.Values{"k": {"é"}}},
		{"k=café", url.Values{"k": {"café"}}},
		{"a=1&b=&a=2", url.Values{"a": {"1", "2"}, "b": {""}}},
		{"sid=x;y=1&sid=z", url.Values{"sid": {"x;y=1", "z"}}},
		{";", url.Values{";": {""}}},
		{";=;", url.Values{";": {";"}}},
		// Go only (a Python str cannot hold a raw invalid byte): it reads as U+FFFD like an escaped one.
		{"k=\xff", url.Values{"k": {"�"}}},
	} {
		if got := ParseQuery(tc.raw); !reflect.DeepEqual(got, tc.want) {
			t.Errorf("ParseQuery(%q) = %q, want %q", tc.raw, got, tc.want)
		}
	}
}

func TestQueryReadsTheRequestRawQuery(t *testing.T) {
	r := httptest.NewRequest("GET", "/x?team_id=jira:ENG;x&team_id=b%zz", nil)
	if got, want := Query(r), (url.Values{"team_id": {"jira:ENG;x", "b%zz"}}); !reflect.DeepEqual(got, want) {
		t.Fatalf("Query = %q, want %q", got, want)
	}
}

var oracleBoundaryBytes = []byte{0x00, 0x41, 0x7F, 0x80, 0x8F, 0x90, 0x9F, 0xA0, 0xBF, 0xC0, 0xC1, 0xC2, 0xDF,
	0xE0, 0xE1, 0xEC, 0xED, 0xEE, 0xEF, 0xF0, 0xF1, 0xF3, 0xF4, 0xF5, 0xFF}

func escapeBytes(bs ...byte) string {
	var b strings.Builder
	for _, c := range bs {
		fmt.Fprintf(&b, "%%%02X", c)
	}
	return b.String()
}

// Every escaped 2-byte sequence and every 3- and 4-byte sequence over the
// UTF-8 boundary bytes, as one value; the digest of the decoded values was
// recorded from CPython 3.12.3 parse_qsl over the same corpus in this order.
func TestParseQueryDecodesBytesLikeCPython(t *testing.T) {
	h := sha256.New()
	n := 0
	add := func(escaped string) {
		values := ParseQuery("k=" + escaped)["k"]
		if len(values) != 1 {
			t.Fatalf("k=%s: %d values", escaped, len(values))
		}
		h.Write([]byte(values[0]))
		h.Write([]byte{0})
		n++
	}
	for a := 0; a < 256; a++ {
		for b := 0; b < 256; b++ {
			add(escapeBytes(byte(a), byte(b)))
		}
	}
	for _, a := range oracleBoundaryBytes {
		for _, b := range oracleBoundaryBytes {
			for _, c := range oracleBoundaryBytes {
				add(escapeBytes(a, b, c))
			}
		}
	}
	for _, a := range oracleBoundaryBytes {
		for _, b := range oracleBoundaryBytes {
			for _, c := range oracleBoundaryBytes {
				for _, d := range oracleBoundaryBytes {
					add(escapeBytes(a, b, c, d))
				}
			}
		}
	}
	const want = "d59ef9509f663db561ea1b93710153469833fa1cb516d125daa268ca11d19289"
	if got := hex.EncodeToString(h.Sum(nil)); n != 471786 || got != want {
		t.Fatalf("%d values, digest %s; CPython: 471786 values, digest %s", n, got, want)
	}
}

// Every query of 1 to 4 tokens over separators, escapes and bad escapes; per
// query the keys sorted by UTF-8 bytes, each with its values in order. The
// digest was recorded from CPython 3.12.3 parse_qsl over the same corpus.
func TestParseQuerySplitsPairsLikeCPython(t *testing.T) {
	tokens := []string{"a", "=", "&", ";", "+", "%", "%2", "%zz", "%41", "%e9", "%C3%A9", "%FF", "~"}
	h := sha256.New()
	n := 0
	var walk func(prefix string, depth int)
	walk = func(prefix string, depth int) {
		if depth == 0 {
			values := ParseQuery(prefix)
			keys := make([]string, 0, len(values))
			for key := range values {
				keys = append(keys, key)
			}
			sort.Strings(keys)
			for _, key := range keys {
				h.Write([]byte(key))
				h.Write([]byte{1})
				for _, value := range values[key] {
					h.Write([]byte(value))
					h.Write([]byte{2})
				}
			}
			h.Write([]byte{0})
			n++
			return
		}
		for _, token := range tokens {
			walk(prefix+token, depth-1)
		}
	}
	for length := 1; length <= 4; length++ {
		walk("", length)
	}
	const want = "01b45ca95bac44fb5c4df2f81d932d5bac91007058001eb1d3891d155ffd9bd1"
	if got := hex.EncodeToString(h.Sum(nil)); n != 30940 || got != want {
		t.Fatalf("%d queries, digest %s; CPython: 30940 queries, digest %s", n, got, want)
	}
}

// For a query url.ParseQuery reads without error and whose decoded keys and
// values are valid UTF-8, ParseQuery answers the same values: the change is
// only for a ";", a malformed escape or invalid UTF-8.
func FuzzParseQueryMatchesNetURLOnWellFormedInput(f *testing.F) {
	for _, seed := range []string{"", "a=1", "a=1&a=2", "a=&b", "q=a+b%20c", "k=%C3%A9", "x=%2B%26%3D", "=v&&",
		"team_id=jira%3AENG", "limit=5&offset=10&org_id=00000000-0000-0000-0000-000000000001", "a=1;b", "k=%zz", "k=%FF"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, raw string) {
		want, err := url.ParseQuery(raw)
		got := ParseQuery(raw)
		if err != nil || !allValidUTF8(want) {
			return
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("ParseQuery(%q) = %q, url.ParseQuery = %q", raw, got, want)
		}
	})
}

func allValidUTF8(values url.Values) bool {
	for key, list := range values {
		if !utf8.ValidString(key) {
			return false
		}
		for _, value := range list {
			if !utf8.ValidString(value) {
				return false
			}
		}
	}
	return true
}

func TestParseQueryNeverDropsAPair(t *testing.T) {
	for _, raw := range []string{"a=1;b", "k=%zz", "k=%FF", "%zz=1", "a;=1&b=2"} {
		segments := 0
		for _, pair := range strings.Split(raw, "&") {
			if pair != "" {
				segments++
			}
		}
		got := 0
		for _, list := range ParseQuery(raw) {
			got += len(list)
		}
		if got != segments {
			t.Errorf("ParseQuery(%q): %d values for %d pairs", raw, got, segments)
		}
	}
}
