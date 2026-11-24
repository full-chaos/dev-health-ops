package secrets

import (
	"context"
	"errors"
	"math/rand"
	"net/url"
	"strconv"
	"strings"
	"testing"
)

// These cells use the package's public functions only, so they also run against
// a tree that has no union redactor.

// mainRedactedBytes is the redactor of main: its value set (the DSN components
// and the resolved values, raw) and sequential strings.ReplaceAll, run on a
// slice that carries each byte's index in the original text (-1 for marker
// bytes). It returns the original bytes it hides.
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
			if i+len(value) <= len(cells) && func() bool {
				for j := 0; j < len(value); j++ {
					if cells[i+j].b != value[j] {
						return false
					}
				}
				return true
			}() {
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

func isSubsequence(small, big string) bool {
	i := 0
	for j := 0; j < len(big) && i < len(small); j++ {
		if small[i] == big[j] {
			i++
		}
	}
	return i == len(small)
}

// The overlap of an escaped login with a raw password: one replacement must not
// hide the password's first byte and leave the rest.
func TestOverlappingValuesAreRedactedAsOneUnion(t *testing.T) {
	if got := NewBoundary("postgres://svc%3A:A7kZ2@h/db").RedactText("echo svc%3A7kZ2 end"); got != "echo [REDACTED] end" {
		t.Errorf("svc: / A7kZ2: got %q", got)
	}
	if got := NewBoundaryWith("", "svc:", "A7kZ2").RedactText("svc%3A7kZ2"); got != RedactedMarker {
		t.Errorf("resolved login and password: got %q", got)
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

// A value that holds another: the longer one is hidden whole whatever the order.
func TestAValueContainingAnotherIsRedactedWhole(t *testing.T) {
	for name, b := range map[string]Boundary{
		"short first": NewBoundaryWith("", "app_login", "app_login-s3cret"),
		"long first":  NewBoundaryWith("", "app_login-s3cret", "app_login"),
	} {
		if got := b.RedactText("pw app_login-s3cret"); got != "pw [REDACTED]" {
			t.Errorf("%s: got %q", name, got)
		}
	}
}

// The raw value is redacted at every length; an empty value is a no-op.
func TestRawValueIsRedactedAtEveryLengthAndEmptyIsANoOp(t *testing.T) {
	for _, value := range []string{"a", "a@", "李", "李雷", "a@b", "abcdefghijklmnop"} {
		if got := NewBoundaryWith("", value).RedactText("x " + value + " y"); got != "x [REDACTED] y" {
			t.Errorf("value %q: got %q", value, got)
		}
	}
	if got := NewBoundaryWith("", "", "").RedactText("x y"); got != "x y" {
		t.Errorf("empty resolved values: got %q", got)
	}
	if got := RedactValues("x y", "", ""); got != "x y" {
		t.Errorf("RedactValues empty: got %q", got)
	}
}

// The output is never searched again: a value that is part of the marker
// matches the text only, not the marker a first match wrote.
func TestMarkerBytesAreNeverRematched(t *testing.T) {
	text := "a RED b ACT c D] d [R e"
	want := "a [REDACTED] b [REDACTED] c [REDACTED] d [REDACTED] e"
	if got := NewBoundaryWith("", "RED", "ACT", "D]", "[R").RedactText(text); got != want {
		t.Errorf("Boundary: got %q", got)
	}
	if got := RedactValues(text, "RED", "ACT", "D]", "[R"); got != want {
		t.Errorf("RedactValues: got %q", got)
	}
	if got := RedactValues("[REDACTED]", RedactedMarker, "RED"); got != RedactedMarker {
		t.Errorf("a value equal to the marker: got %q", got)
	}
}

// Seeded corpus: logins, passwords and resolved values over a small alphabet
// (so they overlap and share prefixes and suffixes), texts joined from the raw
// and escaped spellings of each. Every byte main hides, the boundary hides:
// the bytes the boundary leaves are a subsequence of the bytes main leaves.
func TestSeededOverlapCorpusHidesEveryByteMainHides(t *testing.T) {
	rng := rand.New(rand.NewSource(14566))
	alphabet := []string{"A", "7", "k", "%", "3", ":", "-"}
	word := func(min, spread int) string {
		var b strings.Builder
		for n := min + rng.Intn(spread); n > 0; n-- {
			b.WriteString(alphabet[rng.Intn(len(alphabet))])
		}
		return b.String()
	}
	hidMore := 0
	for iteration := 0; iteration < 6000; iteration++ {
		login, password, resolved := word(1, 4), word(1, 5), word(0, 5)
		dsn := (&url.URL{Scheme: "postgres", User: url.UserPassword(login, password), Host: "h", Path: "/db"}).String()
		var pieces []string
		for _, value := range []string{login, password, resolved} {
			quoted := strconv.Quote(value)
			pieces = append(pieces, value, url.QueryEscape(value), url.PathEscape(value),
				(&url.URL{User: url.UserPassword("", value)}).String()[3:len((&url.URL{User: url.UserPassword("", value)}).String())-1], quoted[1:len(quoted)-1])
		}
		pieces = append(pieces, word(1, 3), dsn)
		var text strings.Builder
		for n := 2 + rng.Intn(4); n > 0; n-- {
			text.WriteString(pieces[rng.Intn(len(pieces))])
		}
		original := text.String()
		hidden := mainRedactedBytes(original, append(CredentialComponents(dsn), resolved))
		var mainKept strings.Builder
		for i := 0; i < len(original); i++ {
			if !hidden[i] {
				mainKept.WriteByte(original[i])
			}
		}
		out := NewBoundaryWith(dsn, resolved).RedactText(original)
		kept := strings.ReplaceAll(out, RedactedMarker, "")
		if !isSubsequence(kept, mainKept.String()) {
			t.Fatalf("login %q password %q resolved %q text %q: main keeps %q, the boundary keeps %q (out %q)", login, password, resolved, original, mainKept.String(), kept, out)
		}
		if len(kept) < mainKept.Len() {
			hidMore++
		}
	}
	if hidMore == 0 {
		t.Error("the corpus never made the boundary hide more than main: it proves nothing")
	}
}

// Join is one Boundary: the union runs over every part's values at once.
func TestJoinRedactsTheUnionAcrossParts(t *testing.T) {
	sink := NewBoundary("clickhouse://svc%3A:zz9q@c/d")
	database := NewBoundary("postgres://acct:A7kZ2@h/db")
	const text = "echo svc%3A7kZ2 end"
	if got := Join(sink, database).RedactText(text); got != "echo [REDACTED] end" {
		t.Errorf("Join RedactText: got %q", got)
	}
	if got := Join(sink, database).Redact(errors.New(text)).Error(); got != "echo [REDACTED] end" {
		t.Errorf("Join Redact: got %q", got)
	}
	if got := Join(database, sink).RedactText(text); got != "echo [REDACTED] end" {
		t.Errorf("Join in the other order: got %q", got)
	}
	if got := Join().RedactText(text); got != text {
		t.Errorf("Join of nothing: got %q", got)
	}
}

// One unparsable part makes the joined Boundary unparsable: Redact fails closed.
func TestJoinIsUnparsableWhenAnyPartIs(t *testing.T) {
	parsable := NewBoundary("postgres://acct:A7kZ2@h/db")
	unparsable := NewUnparsableBoundary("host=h password=Bq4rT %zz")
	for name, b := range map[string]Boundary{"last": Join(parsable, unparsable), "first": Join(unparsable, parsable)} {
		if got := b.Redact(errors.New("boom")); got == nil || got.Error() != UnparsableMarker {
			t.Errorf("%s: got %v, want %q", name, got, UnparsableMarker)
		}
		if got := b.Redact(context.Canceled); !errors.Is(got, context.Canceled) {
			t.Errorf("%s: a cancellation lost its identity: %v", name, got)
		}
		if got := b.RedactText("x Bq4rT y"); got != "x [REDACTED] y" {
			t.Errorf("%s: RedactText keeps the credential redaction: got %q", name, got)
		}
	}
	if got := Join(parsable, parsable).Redact(errors.New("boom")); got.Error() != "boom" {
		t.Errorf("parsable parts: got %v", got)
	}
}
