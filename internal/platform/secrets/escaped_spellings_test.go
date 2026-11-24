package secrets

import (
	"errors"
	"net/url"
	"strings"
	"testing"
)

// escapedSecret holds a space, @, a quote, & and a backslash, so every
// spelling below differs from the raw value and from every other spelling.
const escapedSecret = `ab @c"d&e\f`

var escapedSpellings = []struct{ name, text string }{
	{"query escape", `ab+%40c%22d%26e%5Cf`},
	{"path escape", `ab%20@c%22d&e%5Cf`},
	{"userinfo escape", `ab%20%40c%22d&e%5Cf`},
	{"strconv quote", `ab @c\"d&e\\f`},
	{"json escape", `ab @c\"d\u0026e\\f`},
	{"json of quote", `ab @c\\\"d\u0026e\\\\f`},
}

func escapedSources() map[string]func(text string) string {
	return map[string]func(string) string{
		"URL DSN": func(text string) string {
			return NewBoundary((&url.URL{Scheme: "postgres", User: url.UserPassword("lg1", escapedSecret), Host: "h", Path: "/db"}).String()).RedactText(text)
		},
		"key=value DSN": func(text string) string {
			return NewBoundary(`host=h user=lg1 password='ab @c"d&e\\f' dbname=db`).RedactText(text)
		},
		"resolved": func(text string) string { return NewBoundaryWith("postgres://h/db", escapedSecret).RedactText(text) },
		"unparsable DSN": func(text string) string {
			return NewUnparsableBoundary(`host=h password='ab @c"d&e\\f' %zz`).RedactText(text)
		},
		"redacted cause": func(text string) string {
			return WithRedactedCauseAlso(errors.New("s"), "postgres://h/db", errors.New(text), escapedSecret).Error()
		},
	}
}

func TestBoundaryRedactsEachEscapedSpelling(t *testing.T) {
	for source, redact := range escapedSources() {
		for _, spelling := range escapedSpellings {
			t.Run(source+"/"+spelling.name, func(t *testing.T) {
				got := redact("failed for " + spelling.text + " end")
				if strings.Contains(got, spelling.text) || !strings.Contains(got, RedactedMarker) {
					t.Errorf("%s spelling left in %q", spelling.name, got)
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
	const short = "a@"
	if got := NewBoundaryWith("", short).RedactText("x a@ y a%40 z"); got != "x [REDACTED] y a%40 z" {
		t.Errorf("2-char value: got %q", got)
	}
	if got := NewBoundaryWith("", "a@b").RedactText("a%40b"); got != RedactedMarker {
		t.Errorf("3-char value spelling: got %q", got)
	}
}

func TestSpellingsEqualToTheRawValueAreDeduped(t *testing.T) {
	if got := credentialSpellings("plainvalue1"); len(got) != 1 {
		t.Errorf("credentialSpellings of a plain value = %q, want one entry", got)
	}
	got := credentialSpellings(escapedSecret)
	seen := map[string]bool{}
	for _, value := range got {
		if seen[value] {
			t.Errorf("duplicate spelling %q in %q", value, got)
		}
		seen[value] = true
	}
	if len(got) != 1+len(escapedSpellings) {
		t.Errorf("got %d spellings %q, want %d", len(got), got, 1+len(escapedSpellings))
	}
}
