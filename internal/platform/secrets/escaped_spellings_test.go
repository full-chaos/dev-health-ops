package secrets

import (
	"errors"
	"net/url"
	"strings"
	"testing"
)

type spellingCase struct{ name, secret, text string }

// escapedSecret holds a space, @, a quote, & and a backslash, so every
// spelling below differs from the raw value and from every other spelling.
// controlSecret holds & and a control byte, so the JSON spellings with HTML
// escaping off differ from the quoted ones.
const (
	escapedSecret = `ab @c"d&e\f`
	controlSecret = "pw&\x01z"
	hexSecret     = "ab /:@z"
)

var spellingCases = []spellingCase{
	{"query escape", escapedSecret, `ab+%40c%22d%26e%5Cf`},
	{"path escape", escapedSecret, `ab%20@c%22d&e%5Cf`},
	{"userinfo escape", escapedSecret, `ab%20%40c%22d&e%5Cf`},
	{"strconv quote", escapedSecret, `ab @c\"d&e\\f`},
	{"strconv quote control byte", controlSecret, `pw&\x01z`},
	{"json escape", escapedSecret, `ab @c\"d\u0026e\\f`},
	{"json of quote", escapedSecret, `ab @c\\\"d\u0026e\\\\f`},
	{"json html off", controlSecret, `pw&\u0001z`},
	{"json of quote html off", controlSecret, `pw&\\x01z`},
	{"query escape lower hex", hexSecret, `ab+%2f%3a%40z`},
	{"path escape lower hex", hexSecret, `ab%20%2f:@z`},
	{"userinfo escape mixed hex", hexSecret, `ab%20%2f%3A%40z`},
}

func spellingSources() map[string]func(secret, text string) string {
	return map[string]func(string, string) string{
		"URL DSN": func(secret, text string) string {
			return NewBoundary((&url.URL{Scheme: "postgres", User: url.UserPassword("lg1", secret), Host: "h", Path: "/db"}).String()).RedactText(text)
		},
		"key=value DSN": func(secret, text string) string {
			quoted := strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(secret)
			return NewBoundary("host=h user=lg1 password='" + quoted + "' dbname=db").RedactText(text)
		},
		"resolved": func(secret, text string) string { return NewBoundaryWith("postgres://h/db", secret).RedactText(text) },
		"unparsable DSN": func(secret, text string) string {
			return NewUnparsableBoundary("host=h password='" + strings.NewReplacer(`\`, `\\`).Replace(secret) + "' %zz").RedactText(text)
		},
		"redacted cause": func(secret, text string) string {
			return WithRedactedCauseAlso(errors.New("s"), "postgres://h/db", errors.New(text), secret).Error()
		},
	}
}

func TestBoundaryRedactsEachEscapedSpelling(t *testing.T) {
	for source, redact := range spellingSources() {
		for _, c := range spellingCases {
			text := c.text
			t.Run(source+"/"+c.name, func(t *testing.T) {
				got := redact(c.secret, "failed for "+text+" end")
				if strings.Contains(got, text) || !strings.Contains(got, RedactedMarker) {
					t.Errorf("%s spelling left in %q", c.name, got)
				}
			})
		}
	}
}

func TestBoundaryRedactsAnEscapedLogin(t *testing.T) {
	b := NewBoundary((&url.URL{Scheme: "postgres", User: url.UserPassword(`lg @1`, "pw-x1"), Host: "h", Path: "/db"}).String())
	if got := b.RedactText("echo lg%20%401 here"); strings.Contains(got, "lg%20%401") {
		t.Errorf("escaped login left in %q", got)
	}
}

func TestShortValuesKeepOnlyTheirRawRedaction(t *testing.T) {
	if got := NewBoundaryWith("", "a@").RedactText("x a@ y a%40 z"); got != "x [REDACTED] y a%40 z" {
		t.Errorf("2-char value: got %q", got)
	}
	if got := NewBoundaryWith("", "a@b").RedactText("a%40b"); got != RedactedMarker {
		t.Errorf("3-char value spelling: got %q", got)
	}
}

// The gate counts characters: a 1- or 2-character value of multi-byte
// characters has no derived spelling; three characters do.
func TestShortValueGateCountsCharactersNotBytes(t *testing.T) {
	for value, escaped := range map[string]string{"李": "%E6%9D%8E", "李雷": "%E6%9D%8E%E9%9B%B7", "é": "%C3%A9"} {
		got := NewBoundaryWith("", value).RedactText("a " + escaped + " b " + value)
		if got != "a "+escaped+" b [REDACTED]" {
			t.Errorf("value %q: got %q", value, got)
		}
	}
	if got := NewBoundaryWith("", "李雷们").RedactText("%E6%9D%8E%E9%9B%B7%E4%BB%AC"); got != RedactedMarker {
		t.Errorf("3-character value: got %q", got)
	}
}

// A keyword fragment of a URL DSN query is a literal, not a decoded value:
// "?password=a%40" holds the 2-character password "a@", with no derived forms.
func TestURLQueryFragmentGetsNoDerivedSpellings(t *testing.T) {
	for name, dsn := range map[string]string{
		"query":            "postgres://acct@h/db?password=a%40",
		"query and other":  "postgres://acct@h/db?password=a%40&application_name=ops",
		"userinfo and key": "postgres://acct:a%40@h/db",
	} {
		if got := NewBoundary(dsn).RedactText("see a%2540 and a@"); got != "see a%2540 and [REDACTED]" {
			t.Errorf("%s: got %q", name, got)
		}
	}
	if got := NewBoundary("host=h password=pw-k1 user=lg1").RedactText("pw%2Dk1"); got != "pw%2Dk1" {
		t.Errorf("keyword DSN: got %q", got)
	}
	if got := NewBoundary("host=h password='a b' user=lg1").RedactText("a+b"); got != RedactedMarker {
		t.Errorf("keyword DSN decoded value keeps derived spellings: got %q", got)
	}
}

func TestSpellingsEqualToTheRawValueAreDeduped(t *testing.T) {
	if plain, escaped := credentialSpellings("plainvalue1"); len(plain) != 1 || len(escaped) != 0 {
		t.Errorf("credentialSpellings of a plain value = %q %q, want one entry", plain, escaped)
	}
	plain, escaped := credentialSpellings(escapedSecret)
	seen := map[string]bool{}
	for _, value := range append(append([]string{}, plain...), escaped...) {
		if seen[value] {
			t.Errorf("duplicate spelling %q in %q %q", value, plain, escaped)
		}
		seen[value] = true
	}
	if len(seen) != 8 {
		t.Errorf("got %d spellings %q %q, want 8", len(seen), plain, escaped)
	}
}

// The shape the reviewer named: the escaped login "svc:" ends where the raw
// password "A7kZ2" starts, so one replacement must not hide the password's
// first byte and leave the rest.
func TestOverlappingValuesAreRedactedAsOneUnion(t *testing.T) {
	b := NewBoundary("postgres://svc%3A:A7kZ2@h/db")
	if got := b.RedactText("echo svc%3A7kZ2 end"); got != "echo [REDACTED] end" {
		t.Errorf("got %q", got)
	}
	if got := NewBoundaryWith("", "E", "A7kZ2").RedactText("A7kZ2"); got != RedactedMarker {
		t.Errorf("marker corrupted: got %q", got)
	}
	if got := RedactValues("abcdef", "abcd", "cdef"); got != RedactedMarker {
		t.Errorf("RedactValues overlap: got %q", got)
	}
	if got := RedactValues("aXa", "a"); got != "[REDACTED]X[REDACTED]" {
		t.Errorf("RedactValues repeats: got %q", got)
	}
	if got := RedactValues("pw:pw:pw", "pw:pw"); got != RedactedMarker {
		t.Errorf("self overlap: got %q", got)
	}
}

// mainRedactedBytes is main's redactor: its value set (the DSN components and
// the resolved values, raw) and sequential strings.ReplaceAll, run on a slice
// that carries each byte's index in the original text (-1 for marker bytes).
func mainRedactedBytes(text string, values []string) map[int]bool {
	type cell struct {
		b   byte
		idx int
	}
	cells := make([]cell, len(text))
	for i := range text {
		cells[i] = cell{text[i], i}
	}
	covered := map[int]bool{}
	for _, value := range values {
		if value == "" {
			continue
		}
		var next []cell
		for i := 0; i < len(cells); {
			if i+len(value) <= len(cells) && matchesAt(cells[i:i+len(value)], value, func(c cell) byte { return c.b }) {
				for _, c := range cells[i : i+len(value)] {
					if c.idx >= 0 {
						covered[c.idx] = true
					}
				}
				for j := 0; j < len(RedactedMarker); j++ {
					next = append(next, cell{RedactedMarker[j], -1})
				}
				i += len(value)
				continue
			}
			next = append(next, cells[i])
			i++
		}
		cells = next
	}
	return covered
}

func matchesAt[T any](cells []T, value string, get func(T) byte) bool {
	for i := range cells {
		if get(cells[i]) != value[i] {
			return false
		}
	}
	return true
}

// newRedactedBytes is the bytes of the ORIGINAL text the boundary hides: every
// match of every value found on the original text, and the check that the
// boundary's output is exactly the original with those bytes removed (and no
// other) and a marker in their place. The property texts never hold a marker.
func newRedactedBytes(t *testing.T, b Boundary, text string) map[int]bool {
	t.Helper()
	spans := matchSpans(nil, text, b.values)
	spans = matchSpans(spans, foldPercentHex(text), b.escaped)
	covered := map[int]bool{}
	for _, span := range spans {
		for i := span[0]; i < span[1]; i++ {
			covered[i] = true
		}
	}
	var kept strings.Builder
	for i := 0; i < len(text); i++ {
		if !covered[i] {
			kept.WriteByte(text[i])
		}
	}
	out := b.RedactText(text)
	if got := strings.ReplaceAll(out, RedactedMarker, ""); got != kept.String() {
		t.Fatalf("text %q: output %q keeps %q, want the uncovered bytes %q", text, out, got, kept.String())
	}
	return covered
}

// Every byte main's redactor hides in a text, the union redactor hides too, for
// each pair of credentials (login/password/resolved, with overlapping and
// prefix/suffix-sharing values) against texts built from every spelling of
// every value, alone and joined at each overlap.
func TestEveryByteMainRedactsIsRedactedByTheUnion(t *testing.T) {
	type pair struct{ dsn, resolved string }
	pairs := []pair{
		{"postgres://svc%3A:A7kZ2@h/db", ""},
		{"postgres://svcOperatorsA:A7kZ2@h/db", ""},
		{"postgres://pw:pw@h/db", ""},
		{"postgres://acct@h/db", "abcdefghijklmnopqrst:p"},
		{"postgres://acct:abcd@h/db", "cdef"},
		{"host=h user=svc: password=A7kZ2", ""},
		{"", "E"},
		{"", "A7kZ2"},
		{"postgres://lg1:ab%20%40c@h/db?user=c%40x", "x@y"},
		{"postgres://lg1:a%40@h/db?password=a%40", "a@"},
	}
	for _, p := range pairs {
		mainValues := append(CredentialComponents(p.dsn), p.resolved)
		b := NewBoundaryWith(p.dsn, p.resolved)
		var pieces []string
		pieces = append(pieces, p.dsn, "svc%3A7kZ2", "a%2540", "x", "-")
		for _, v := range mainValues {
			plain, escaped := credentialSpellings(v)
			pieces = append(pieces, plain...)
			pieces = append(pieces, escaped...)
		}
		var texts []string
		for _, a := range pieces {
			if a == "" {
				continue
			}
			texts = append(texts, a, "e "+a+" e")
			for _, c := range pieces {
				if c == "" {
					continue
				}
				texts = append(texts, a+c)
				for k := 1; k < len(a) && k < len(c); k++ {
					if strings.HasSuffix(a, c[:k]) {
						texts = append(texts, a+c[k:])
					}
				}
			}
		}
		for _, text := range texts {
			if strings.Contains(text, RedactedMarker) {
				continue
			}
			want := mainRedactedBytes(text, mainValues)
			got := newRedactedBytes(t, b, text)
			for i := range want {
				if !got[i] {
					t.Fatalf("pair %v text %q: main redacts byte %d, the union does not (out %q)", p, text, i, b.RedactText(text))
				}
			}
		}
	}
}
