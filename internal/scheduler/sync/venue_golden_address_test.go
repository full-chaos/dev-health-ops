package sync

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
	"venue-credential-fingerprint.golden.json",
	"venue-jira-rename.golden.json",
	"venue-zero-unit-stamp.golden.json",
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
		var document any
		if err := json.Unmarshal(raw, &document); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		texts := 0
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
				texts++
				if strings.HasPrefix(typed, "gzip+base64:") {
					typed = venueoracle.UnpackBody(t, typed)
				}
				if err := addressErr(typed); err != nil {
					t.Errorf("%s: %s %v: a golden holds no address of the run that recorded it", name, where, err)
				}
			}
		}
		walk("$", document)
		if texts < 8 {
			t.Fatalf("%s: only %d texts were read: the check would measure nothing", name, texts)
		}
	}
}
