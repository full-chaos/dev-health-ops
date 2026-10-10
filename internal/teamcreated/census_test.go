package teamcreated

import (
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
)

var teamsInsert = regexp.MustCompile(`(?i)INSERT\s+INTO\s+teams\b`)

// Every Go statement that inserts into teams writes created_at, and the writer
// either calls this package or copies the column in the same statement
// (INSERT..SELECT). A new writer that drops the column starts the next version
// of every team it touches with a NULL creation time.
func TestEveryGoTeamsInsertWritesCreatedAt(t *testing.T) {
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatal(err)
	}
	found := 0
	for _, top := range []string{"internal", "cmd"} {
		walkErr := filepath.WalkDir(filepath.Join(root, top), func(path string, entry fs.DirEntry, err error) error {
			if err != nil || entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return err
			}
			raw, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			if rel, _ := filepath.Rel(root, path); strings.HasPrefix(filepath.ToSlash(rel), "internal/testsupport/") {
				return nil // test seeds, not a writer of production rows
			}
			source := string(raw)
			for _, at := range teamsInsert.FindAllStringIndex(source, -1) {
				found++
				rel, _ := filepath.Rel(root, path)
				statement := source[at[0]:min(at[0]+700, len(source))]
				columns := statement[:strings.IndexByte(statement, ')')+1]
				if !strings.Contains(columns, Column) {
					t.Errorf("%s: INSERT INTO teams does not list %s:\n%s", rel, Column, columns)
					continue
				}
				isSelect := strings.Contains(statement, "SELECT")
				if isSelect && strings.Count(statement, Column) < 2 {
					t.Errorf("%s: INSERT..SELECT INTO teams lists %s but does not select it", rel, Column)
				}
				if !isSelect && !strings.Contains(source, "teamcreated.") {
					t.Errorf("%s: INSERT INTO teams lists %s but the file never calls teamcreated.Carry/For", rel, Column)
				}
			}
			return nil
		})
		if walkErr != nil {
			t.Fatal(walkErr)
		}
	}
	// A scan that finds nothing is not a pass: the writers known on 2026-10-09.
	if found < 8 {
		t.Fatalf("found %d INSERT INTO teams statements, want at least 8 (github, gitlab, jira, linear, atlassian, retire, push, admin store)", found)
	}
}
