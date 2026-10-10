package teamsidentity

import (
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// deleted_at is set by the admin delete only. Every other writer of a team
// row names its columns and does not name this one, so its version of the row
// holds NULL there: a provider sync or a push after a delete is the team
// again, as it was when the delete removed the row. A writer that copied the
// column (or named it) would keep a team deleted for ever, or delete one, with
// no admin action. This reads the text of every production INSERT INTO teams
// of the module, up to the end of its string literal; a statement that is
// built from pieces is not seen whole.
func TestOnlyTheAdminStoreWritesDeletedAt(t *testing.T) {
	insert := regexp.MustCompile("(?s)INSERT INTO teams\\b[^;`\"]*")
	root := filepath.Join("..", "..", "..")
	found, writers := map[string]int{}, 0
	for _, directory := range []string{"internal", "cmd"} {
		err := filepath.WalkDir(filepath.Join(root, directory), func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") || strings.Contains(path, "testsupport") {
				return nil
			}
			raw, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			for _, statement := range insert.FindAllString(string(raw), -1) {
				writers++
				if strings.Contains(statement, "deleted_at") {
					relative, _ := filepath.Rel(root, path)
					found[filepath.ToSlash(relative)]++
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if writers < 8 {
		t.Fatalf("the census read %d INSERT INTO teams statements: it measured nothing", writers)
	}
	if len(found) != 1 || found["internal/api/teamsidentity/store.go"] != 1 {
		t.Errorf("writers of teams.deleted_at = %v, want only the one insert of internal/api/teamsidentity/store.go", found)
	}
}
