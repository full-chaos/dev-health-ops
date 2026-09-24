package venueoracle

import (
	"strings"
	"testing"
)

// TestRedactJSONKeepsEveryOtherByte pins RedactJSON: the chosen values are
// replaced by path, and every other byte (float spelling, spacing, key
// order) stays as written; text that is not JSON is returned unchanged.
func TestRedactJSONKeepsEveryOtherByte(t *testing.T) {
	body := `{"id":"a1","n":1e-07, "items":[{"id":"x","v":0.00001},{"id":"y"}],"nested":{"id":"keep"}}`
	got := RedactJSON(body, func(path []string, raw string) (string, bool) {
		key := strings.Join(path, ".")
		if key == "id" || key == "items.[].id" {
			return `"<id>"`, true
		}
		return "", false
	})
	want := `{"id":"<id>","n":1e-07, "items":[{"id":"<id>","v":0.00001},{"id":"<id>"}],"nested":{"id":"keep"}}`
	if got != want {
		t.Fatalf("got  %s\nwant %s", got, want)
	}
	for _, text := range []string{`{"id":`, `not json`, ``, `{"id":"a"} x`} {
		if got := RedactJSON(text, func([]string, string) (string, bool) { return `""`, true }); got != text {
			t.Errorf("%q: got %q, want it unchanged", text, got)
		}
	}
	if got := RedactJSON(`[1, 2.50, true, null]`, func(path []string, raw string) (string, bool) { return "", false }); got != `[1, 2.50, true, null]` {
		t.Errorf("a walk with no replacement changed the text: %s", got)
	}
}
