package teamprovider

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

// Every Go statement that inserts into teams writes provider_team_id,
// provider_name and gone_at, and the writer either calls Carry in the function
// that uses the statement or copies the columns in the same statement
// (INSERT..SELECT). A new writer that drops them starts the next version of
// every team it touches with an empty provider id and no gone time.
func TestEveryGoTeamsInsertCarriesTheProviderIdentity(t *testing.T) {
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatal(err)
	}
	columnNames := []string{ColumnProviderTeamID, ColumnProviderName, ColumnGoneAt}
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
			rel, _ := filepath.Rel(root, path)
			if strings.HasPrefix(filepath.ToSlash(rel), "internal/testsupport/") {
				return nil // test seeds, not a writer of production rows
			}
			source := string(raw)
			for _, at := range teamsInsert.FindAllStringIndex(source, -1) {
				found++
				statement := source[at[0]:min(at[0]+900, len(source))]
				columns := statement[:strings.IndexByte(statement, ')')+1]
				for _, column := range columnNames {
					if !strings.Contains(columns, column) {
						t.Errorf("%s: INSERT INTO teams does not list %s:\n%s", rel, column, columns)
					}
				}
				if strings.Contains(statement, "SELECT") {
					selected := statement[strings.Index(statement, "SELECT"):]
					for _, column := range columnNames {
						if !strings.Contains(selected, column) {
							t.Errorf("%s: INSERT..SELECT INTO teams lists %s but does not select it", rel, column)
						}
					}
					continue
				}
				for _, user := range statementUsers(source, at[0]) {
					if !strings.Contains(user, "teamprovider.Carry(") {
						t.Errorf("%s: a function that uses an INSERT INTO teams does not call teamprovider.Carry:\n%.200s", rel, user)
					}
				}
				if !strings.Contains(source, "teamprovider.Merge(") && !strings.Contains(source, "teamprovider.Keep(") &&
					!strings.Contains(source, "stored[") {
					t.Errorf("%s: INSERT INTO teams lists the provider identity but the file never applies the carried values", rel)
				}
			}
			return nil
		})
		if walkErr != nil {
			t.Fatal(walkErr)
		}
	}
	// A scan that finds nothing is not a pass: the writers known on 2026-10-15.
	if found < 8 {
		t.Fatalf("found %d INSERT INTO teams statements, want at least 8 (github, gitlab, jira, linear, atlassian, retire, push, admin store)", found)
	}
}

// The team id carry (providersync/team_id_carry.go) rewrites teams rows under a
// new id through a column list, not an INSERT INTO teams statement: its list
// holds the three columns too.
func TestTeamIDCarryRewritesTheProviderIdentity(t *testing.T) {
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(root, "internal", "providersync", "team_id_carry.go"))
	if err != nil {
		t.Fatal(err)
	}
	line := ""
	for _, candidate := range strings.Split(string(raw), "\n") {
		if strings.Contains(candidate, "teamIDCarryTeamsColumns") && strings.Contains(candidate, "= \"") {
			line = candidate
		}
	}
	if line == "" {
		t.Fatal("teamIDCarryTeamsColumns not found")
	}
	for _, column := range []string{ColumnProviderTeamID, ColumnProviderName, ColumnGoneAt} {
		if !strings.Contains(line, column) {
			t.Errorf("teamIDCarryTeamsColumns does not list %s", column)
		}
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
