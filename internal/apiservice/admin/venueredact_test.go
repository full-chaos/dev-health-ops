package admin_test

import (
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// redactVolatileText blanks the string value of every member named in keys,
// at any depth, in the raw response text (venueoracle.RedactJSON), and changes
// nothing else: the two planes' bodies are then compared as the raw text they
// wrote, escaping, key order and number spelling included. Decoding and
// re-encoding the body first (the previous helper) would hide exactly those
// differences. A member whose value is not a string is left alone and so is
// still compared.
func redactVolatileText(body string, keys ...string) string {
	return venueoracle.RedactJSON(body, func(path []string, raw string) (string, bool) {
		if len(path) == 0 || !strings.HasPrefix(raw, `"`) {
			return "", false
		}
		for _, key := range keys {
			if path[len(path)-1] == key {
				return `""`, true
			}
		}
		return "", false
	})
}

func TestRedactVolatileTextChangesOnlyTheNamedValues(t *testing.T) {
	body := `{"id":"a1","name":"tab\there","created_at":"2026-01-01T00:00:00Z","nested":[{"id":"b2","x":1.50}],"note":"has \"id\":\"z\" inside"}`
	got := redactVolatileText(body, "id", "created_at")
	want := `{"id":"","name":"tab\there","created_at":"","nested":[{"id":"","x":1.50}],"note":"has \"id\":\"z\" inside"}`
	if got != want {
		t.Fatalf("redacted body\n got:  %s\n want: %s", got, want)
	}
}

// The point of the helper: bodies that differ only in escaping stay different
// after redaction (a decode-and-re-encode comparison made them equal).
func TestRedactVolatileTextKeepsEscapingDifferences(t *testing.T) {
	escaped := `{"id":"a","path":"x\/y"}`
	literal := `{"id":"b","path":"x/y"}`
	if redactVolatileText(escaped, "id") == redactVolatileText(literal, "id") {
		t.Fatal("bodies that differ in escaping compare equal after redaction")
	}
	if got := redactVolatileText(`{"id":"a","x":[1,2]}`, "id"); got != `{"id":"","x":[1,2]}` {
		t.Fatalf("got %s", got)
	}
	// Key order, spacing and number spelling are part of what is compared.
	if redactVolatileText(`{"a":1,"b":2}`, "id") == redactVolatileText(`{"b":2,"a":1}`, "id") {
		t.Fatal("reordered keys compare equal")
	}
	if redactVolatileText(`{"a":1.0}`, "id") == redactVolatileText(`{"a":1}`, "id") {
		t.Fatal("different number spellings compare equal")
	}
}
