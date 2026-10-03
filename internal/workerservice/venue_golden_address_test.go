package workerservice

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// venueGoldenFiles are the goldens of the venue tests of this package. Each
// was recorded by a program that got the address of a database of one run: no
// part of such an address is to be in the file.
var venueGoldenFiles = []string{
	"venue-team-catalog-fingerprint.golden.json",
}

// addressShapes are the texts of an address: a locator with a user part, a
// locator of a store, and a host (a name of this machine or a numeric one)
// with its port.
var addressShapes = []struct {
	what  string
	shape *regexp.Regexp
}{
	{"a locator with a user part", regexp.MustCompile(`[A-Za-z][A-Za-z0-9+.-]*://[^\s"/]*@`)},
	{"the locator of a store", regexp.MustCompile(`(?i)\b(postgres(ql)?(\+[a-z0-9]+)?|clickhouse|redis|rediss|valkey)://`)},
	{"a host with its port", regexp.MustCompile(`(?i)(^|[^A-Za-z0-9.])(localhost|host\.docker\.internal|\d{1,3}(\.\d{1,3}){3}|\[[0-9a-f:]+\]):\d{2,5}\b`)},
}

// addressErr is an error when text holds the shape of an address. The message
// names the shape and does not print the text.
func addressErr(text string) error {
	for _, shape := range addressShapes {
		if shape.shape.MatchString(text) {
			return fmt.Errorf("holds %s", shape.what)
		}
	}
	return nil
}

func TestAddressShapesAreFound(t *testing.T) {
	at := string(rune(64))
	for _, row := range []struct {
		name, text string
		found      bool
	}{
		{"a fingerprint pair", `[["77c563903f5eaa1294cc0691427b896c7b4a4dee", "credential"]]`, false},
		{"a row with a time and a JSON value", `id | jira | 2026-09-25 09:30:00.123456+00 | {"jira_project_id": "10001"}`, false},
		{"a version", "idna 3.20 at 1.2.3.4", false},
		{"a web locator with no user", "https://example.atlassian.net/rest", false},
		{"a store locator with a user", "postgresql+psycopg2://admin:secret" + at + "db.internal/venue", true},
		{"a store locator with no user", "postgres://db.internal/venue", true},
		{"a web locator with a user", "https://user:word" + at + "example.com/", true},
		{"this machine by name", "connection to localhost:54321 failed", true},
		{"this machine by number", `server at "127.0.0.1:5432"`, true},
		{"the container host", "host.docker.internal:32768", true},
		{"a numeric host of version 6", "[::1]:5432", true},
	} {
		if err := addressErr(row.text); (err != nil) != row.found {
			t.Errorf("%s: addressErr = %v, want found %v", row.name, err, row.found)
		}
	}
}

// goldenTexts is every text in the golden file raw, by its place, with a packed
// answer unpacked: an address in a packed answer is not visible in the file.
func goldenTexts(t *testing.T, raw []byte) map[string]string {
	t.Helper()
	var document any
	if err := json.Unmarshal(raw, &document); err != nil {
		t.Fatal(err)
	}
	texts := map[string]string{}
	var walk func(where string, value any)
	walk = func(where string, value any) {
		switch typed := value.(type) {
		case map[string]any:
			for key, inner := range typed {
				walk(where+"."+key, inner)
			}
		case []any:
			for index, inner := range typed {
				walk(fmt.Sprintf("%s[%d]", where, index), inner)
			}
		case string:
			if strings.HasPrefix(typed, "gzip+base64:") {
				typed = venueoracle.UnpackBody(t, typed)
			}
			texts[where] = typed
		}
	}
	walk("$", document)
	return texts
}

func TestGoldenTextsAreReadAtEveryDepthAndUnpacked(t *testing.T) {
	packed := venueoracle.PackBody([]byte("rows from localhost:54321"))
	raw, err := json.Marshal(map[string]any{
		"header":   map[string]any{"test": "T"},
		"requests": []any{map[string]any{"body": packed}, map[string]any{"body": "plain"}},
		"rows":     map[string]any{"rows:t": map[string]any{"rows": "a | b"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	texts := goldenTexts(t, raw)
	want := map[string]string{"$.header.test": "T", "$.requests[0].body": "rows from localhost:54321", "$.requests[1].body": "plain", "$.rows.rows:t.rows": "a | b"}
	if len(texts) != len(want) {
		t.Fatalf("texts = %v, want %v", texts, want)
	}
	for where, text := range want {
		if texts[where] != text {
			t.Errorf("%s = %q, want %q", where, texts[where], text)
		}
	}
	if addressErr(texts["$.requests[0].body"]) == nil {
		t.Error("the address in a packed answer is not found")
	}
}

// TestNoVenueGoldenHoldsAnAddress reads every venue golden of this package and
// fails when a text in it, a packed answer included, has the shape of an
// address. A venue golden that is not in venueGoldenFiles fails it too.
func TestNoVenueGoldenHoldsAnAddress(t *testing.T) {
	found, err := filepath.Glob(filepath.Join("testdata", "golden", "venue-*.golden.json"))
	if err != nil {
		t.Fatal(err)
	}
	names := make([]string, len(found))
	for index, file := range found {
		names[index] = filepath.Base(file)
	}
	if !slices.Equal(names, venueGoldenFiles) {
		t.Fatalf("the venue goldens under testdata/golden are %v, venueGoldenFiles names %v", names, venueGoldenFiles)
	}
	for _, name := range venueGoldenFiles {
		raw, err := os.ReadFile(filepath.Join("testdata", "golden", name))
		if err != nil {
			t.Fatal(err)
		}
		texts := goldenTexts(t, raw)
		if len(texts) < 8 {
			t.Fatalf("%s: only %d texts were read: the check would measure nothing", name, len(texts))
		}
		for where, text := range texts {
			if err := addressErr(text); err != nil {
				t.Errorf("%s: %s %v: a golden holds no address of the run that recorded it", name, where, err)
			}
		}
	}
}
