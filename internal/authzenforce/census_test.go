package authzenforce

import (
	"errors"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
)

// censusOffenders lists the tracked production .go files outside this package that name an enforcement variable.
// It reads the tracked tree only: an untracked file (a build temp dir, a cover profile source) is not production code.
func censusOffenders(t *testing.T, root string) []string {
	t.Helper()
	listing := exec.Command("git", "-C", root, "ls-files", "-z", "--", "*.go")
	out, err := listing.Output()
	if err != nil {
		t.Fatalf("git ls-files: %v", err)
	}
	var offenders []string
	for _, rel := range strings.Split(string(out), "\x00") {
		if rel == "" || strings.HasSuffix(rel, "_test.go") || strings.HasPrefix(rel, "internal/authzenforce/") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(root, rel))
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				continue
			}
			t.Fatal(err)
		}
		for _, name := range []string{"GO_API_AUTHZ_ENFORCE", "QUERY_API_AUTHZ_ENFORCE", "QUERY_API_HEADERS_AUTHZ_ENFORCE"} {
			if strings.Contains(string(data), name) {
				offenders = append(offenders, rel)
				break
			}
		}
	}
	return offenders
}

// The variable names are read by this package alone: a second reader would be a
// switch the gauge and the closed namespace set do not see.
func TestNoOtherProductionFileNamesTheVariables(t *testing.T) {
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatal(err)
	}
	if offenders := censusOffenders(t, root); len(offenders) != 0 {
		t.Fatalf("production files name the enforcement variables outside internal/authzenforce: %v", offenders)
	}
}
