package pdp

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
)

// S-e (D6721): the header carrier's cells are read in one place: this package builds the switch, and only the
// declared-settings row names its variable. A second reader would be a switch the gate's caller class does not pick.
func TestOnlyThePDPGateReadsTheHeaderCarrierCells(t *testing.T) {
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatal(err)
	}
	out, err := exec.Command("git", "-C", root, "ls-files", "-z", "--", "*.go").Output()
	if err != nil {
		t.Fatalf("git ls-files: %v", err)
	}
	want := map[string]bool{
		"internal/authzenforce/authzenforce.go":        true,
		"internal/platform/config/queryapi_options.go": true,
		"internal/queryapi/pdp/pdp.go":                 true,
	}
	got := map[string]bool{}
	for _, rel := range strings.Split(string(out), "\x00") {
		if rel == "" || strings.HasSuffix(rel, "_test.go") {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(root, rel))
		if err != nil {
			continue
		}
		if strings.Contains(string(raw), "QueryAPIHeaders") {
			got[rel] = true
		}
	}
	for rel := range got {
		if !want[rel] {
			t.Errorf("%s names the header carrier's cells; only the pdp gate reads them", rel)
		}
	}
	for rel := range want {
		if !got[rel] {
			t.Errorf("listed reader %s names no QueryAPIHeaders (stale list)", rel)
		}
	}
}
