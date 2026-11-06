package scripts_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
)

// A go-api that has no LINK_TOKEN_SECRET answers 503 config_missing on every
// link route, so each venue this repo defines for go-api sets it.
func TestGoAPIVenuesSetTheLinkTokenSecret(t *testing.T) {
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatal(err)
	}
	for _, venue := range []struct{ file, start, end string }{
		{"compose.yml", "\n  go-api:\n", "\n  go-"},
		{"ci/run_live_backend_e2e.sh", "export AUTH_ACCESS_TOKEN_ISSUER=", "exec \"${BIN_DIR}/dho\" api"},
	} {
		raw, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(venue.file)))
		if err != nil {
			t.Fatal(err)
		}
		text := string(raw)
		from := strings.Index(text, venue.start)
		if from < 0 {
			t.Fatalf("%s: no %q", venue.file, venue.start)
		}
		rest := text[from+len(venue.start):]
		to := strings.Index(rest, venue.end)
		if to < 0 {
			t.Fatalf("%s: no %q after the go-api start", venue.file, venue.end)
		}
		if !strings.Contains(rest[:to], "LINK_TOKEN_SECRET") {
			t.Errorf("%s: the go-api venue does not set LINK_TOKEN_SECRET", venue.file)
		}
	}
}
