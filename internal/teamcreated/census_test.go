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
				if isSelect {
					selected := statement[strings.Index(statement, "SELECT"):]
					if !strings.Contains(selected, Column) && !strings.Contains(selected, "_created") {
						t.Errorf("%s: INSERT..SELECT INTO teams lists %s but does not select it", rel, Column)
					}
					continue
				}
				// Every function that uses this statement must carry the time.
				for _, user := range statementUsers(source, at[0]) {
					if !strings.Contains(user, "teamcreated.Carry(") {
						t.Errorf("%s: a function that uses an INSERT INTO teams does not call teamcreated.Carry:\n%.200s", rel, user)
					}
				}
				if !strings.Contains(source, "teamcreated.For(") {
					t.Errorf("%s: INSERT INTO teams lists %s but the file never calls teamcreated.For", rel, Column)
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

var constName = regexp.MustCompile(`(\w+)\s*=\s*[\x60"]\s*INSERT`)
var mapKey = regexp.MustCompile(`("[\w.]+"):\s*"INSERT`)

// statementUsers are the functions that use the statement at offset: for a
// const (name = `INSERT ...`) every function that names it, for a map entry
// ("key": "INSERT ...") every caller of the function that holds the map, but
// not the holder itself; for a
// statement written inside a function, that function.
func statementUsers(source string, offset int) []string {
	line := source[strings.LastIndex(source[:offset], "\n")+1 : offset+20]
	ident := ""
	holder := enclosingFunc(source, offset)
	funcStart := strings.LastIndex(source[:offset], "\nfunc ")
	insideFunc := funcStart >= 0 && !strings.Contains(source[funcStart:offset], "\n}\n")
	if insideFunc && strings.HasPrefix(line, "\t") && constName.MatchString(line) {
		return []string{holder} // a const local to the function that uses it
	}
	if m := constName.FindStringSubmatch(line); m != nil {
		ident = m[1]
	} else if mapKey.MatchString(line) {
		// A statement in a lookup function: its callers use it.
		if name := regexp.MustCompile(`^\s*func (\w+)\(`).FindStringSubmatch(holder); name != nil {
			ident = name[1] + "("
		}
	}
	if ident == "" {
		return []string{holder}
	}
	var users []string
	for _, fn := range strings.Split(source, "\nfunc ")[1:] {
		fn = "func " + fn
		if fn != strings.TrimPrefix(holder, "\n") && strings.Contains(fn, ident) {
			users = append(users, fn)
		}
	}
	if len(users) == 0 {
		return []string{holder} // not used by any function: fails the Carry check
	}
	return users
}

// enclosingFunc is the source of the top-level function that holds offset: from
// the last "\nfunc " before it to the next one. A const holding the statement
// is read with the function that follows it.
func enclosingFunc(source string, offset int) string {
	start := strings.LastIndex(source[:offset], "\nfunc ")
	if start < 0 {
		start = 0
	}
	end := strings.Index(source[offset:], "\nfunc ")
	if end < 0 {
		return source[start:]
	}
	return source[start : offset+end]
}
