package authzenforce

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
)

// A generated Go file that is not tracked (a build temp dir, a cover profile source) is not production code: the
// census reads the tracked tree only.
func TestTheCensusReadsTrackedFilesOnly(t *testing.T) {
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatal(err)
	}
	dir, err := os.MkdirTemp(root, ".authzenforce-census-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	if err := os.WriteFile(filepath.Join(dir, "generated.go"), []byte("package x\n\nconst name = \"GO_API_AUTHZ_ENFORCE\"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if offenders := censusOffenders(t, root); len(offenders) != 0 {
		t.Fatalf("the census read an untracked file: %v", offenders)
	}
}
