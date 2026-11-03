package authzenforce

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
)

// censusOffenders lists the production .go files outside this package that name an enforcement variable.
func censusOffenders(t *testing.T, root string) []string {
	t.Helper()
	var offenders []string
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			case ".git", "node_modules", "worktrees", "third_party":
				return fs.SkipDir
			}
			if path == filepath.Join(root, "internal", "authzenforce") {
				return fs.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for _, name := range []string{"GO_API_AUTHZ_ENFORCE", "QUERY_API_AUTHZ_ENFORCE"} {
			if strings.Contains(string(data), name) {
				offenders = append(offenders, path)
				break
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
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
