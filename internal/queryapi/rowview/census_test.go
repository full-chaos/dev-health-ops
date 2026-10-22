package rowview

import (
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
)

// The census of the readers of the three tables of the view column. A new file that names one of the tables
// fails here until it is listed in readerRoles with a role: a reader that sums across work scopes then has to
// say that it takes OrgPredicate, and a reader that does not has to say why (CHAOS-8863).
func TestEveryFileThatNamesAViewTableIsInTheReaderAllowlist(t *testing.T) {
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatal(err)
	}
	found := map[string]bool{}
	for _, dir := range []string{"internal", "cmd"} {
		walkErr := filepath.WalkDir(filepath.Join(root, dir), func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() && (entry.Name() == "testdata" || entry.Name() == ".git") {
				return filepath.SkipDir
			}
			if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			body, readErr := os.ReadFile(path)
			if readErr != nil {
				return readErr
			}
			for _, table := range Tables() {
				if strings.Contains(string(body), table) {
					relative, _ := filepath.Rel(root, path)
					found[filepath.ToSlash(relative)] = true
					break
				}
			}
			return nil
		})
		if walkErr != nil {
			t.Fatal(walkErr)
		}
	}
	if len(found) < 50 {
		t.Fatalf("the scan found %d files that name a view table: it read too little", len(found))
	}
	var unlisted, stale []string
	for file := range found {
		if _, ok := readerRoles[file]; !ok {
			unlisted = append(unlisted, file)
		}
	}
	for file, entry := range readerRoles {
		if !found[file] {
			stale = append(stale, file)
		}
		switch entry.role {
		case "reader-sum", "reader-nosum", "writer", "names":
		default:
			t.Errorf("%s: role %q is not one of reader-sum, reader-nosum, writer, names", file, entry.role)
		}
		if entry.why == "" {
			t.Errorf("%s: no reason", file)
		}
	}
	sort.Strings(unlisted)
	sort.Strings(stale)
	for _, file := range unlisted {
		t.Errorf("%s names a table of the view column and is not in readerRoles: list it with its role", file)
	}
	for _, file := range stale {
		t.Errorf("%s is in readerRoles and names no table of the view column any more: remove it", file)
	}
}
