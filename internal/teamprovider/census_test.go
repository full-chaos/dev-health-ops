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

var carryCall = regexp.MustCompile(`teamprovider\.Carry\(`)

// The writers that write a stored team again from its own stored version
// through Restate (CHAOS-10099).
var restateWriters = []string{
	"internal/providersync/team_id_carry.go",
}

// The writers of teams that write through Insert (CHAOS-9999), on 2026-10-17.
var insertWriters = []string{
	"internal/providersync/github_team_catalog_effects_clickhouse.go",
	"internal/providersync/gitlab_team_catalog_effects_clickhouse.go",
	"internal/providersync/jira_team_catalog_effects_clickhouse.go",
	"internal/providersync/linear_reference_catalog_effects_clickhouse.go",
	"internal/providersync/team_id_carry.go",
	"internal/atlassianteams/write.go",
	"internal/api/teamsidentity/store.go",
	"internal/streamhandlers/external_clickhouse.go",
}

// Every Go writer of teams takes provider_team_id, provider_name and gone_at
// from the stored versions INSIDE the statement that writes: it calls Insert
// or Restate (the teamprovider statements),
// or its own statement is an INSERT..SELECT that joins
// StoredIdentityOfOrganization (the rule Insert reads with) and selects the
// three columns. A
// statement held as text (not an INSERT..SELECT) is only a column list that an
// Insert caller reads. A read before a separate insert erases a value another
// writer stored between the two, so no other shape passes; Carry is read for
// a tally only.
func TestEveryGoTeamsWriteCarriesTheProviderIdentityInItsStatement(t *testing.T) {
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatal(err)
	}
	columnNames := []string{ColumnProviderTeamID, ColumnProviderName, ColumnGoneAt}
	callers, restaters := map[string]bool{}, map[string]bool{}
	for _, top := range []string{"internal", "cmd"} {
		walkErr := filepath.WalkDir(filepath.Join(root, top), func(path string, entry fs.DirEntry, err error) error {
			if err != nil || entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return err
			}
			rel, _ := filepath.Rel(root, path)
			rel = filepath.ToSlash(rel)
			if strings.HasPrefix(rel, "internal/testsupport/") || strings.HasPrefix(rel, "internal/teamprovider/") {
				return nil // test seeds, and the one shared statement builder
			}
			raw, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			source := string(raw)
			if strings.Contains(source, "teamprovider.Insert(") {
				callers[rel] = true
			}
			if strings.Contains(source, "teamprovider.Restate(") {
				restaters[rel] = true
			}
			for _, at := range carryCall.FindAllStringIndex(source, -1) {
				if !strings.Contains(enclosingFunc(source, at[0]), "tally.Add(stored,") {
					t.Errorf("%s: calls teamprovider.Carry in a function with no tally: a read before a separate insert", rel)
				}
			}
			for _, at := range teamsInsert.FindAllStringIndex(source, -1) {
				statement := source[at[0]:min(at[0]+900, len(source))]
				columns := statement[:strings.IndexByte(statement, ')')+1]
				for _, column := range columnNames {
					if !strings.Contains(columns, column) {
						t.Errorf("%s: INSERT INTO teams does not list %s:\n%s", rel, column, columns)
					}
				}
				if strings.Contains(statement, "SELECT") {
					declaration := declarationAt(source, at[0])
					selected := declaration[strings.Index(declaration, "SELECT"):]
					for _, column := range []string{"stored_team_id", "stored_name", "stored_gone"} {
						if !strings.Contains(selected, column) {
							t.Errorf("%s: INSERT..SELECT INTO teams does not select %s of the stored identity", rel, column)
						}
					}
					if !joinsStoredIdentity(source, declaration) {
						t.Errorf("%s: INSERT..SELECT INTO teams does not join teamprovider.StoredIdentityOfOrganization: a copy of the newest version erases an older stored identity", rel)
					}
					continue
				}
				for _, user := range statementUsers(source, at[0]) {
					if !strings.Contains(user, "teamprovider.Insert(") {
						t.Errorf("%s: a function that holds an INSERT INTO teams does not write it through teamprovider.Insert:\n%.200s", rel, user)
					}
				}
			}
			return nil
		})
		if walkErr != nil {
			t.Fatal(walkErr)
		}
	}
	for _, writer := range insertWriters {
		if !callers[writer] {
			t.Errorf("%s does not call teamprovider.Insert", writer)
		}
	}
	for _, writer := range restateWriters {
		if !restaters[writer] {
			t.Errorf("%s does not call teamprovider.Restate", writer)
		}
	}
	if len(callers) < len(insertWriters) {
		t.Fatalf("found %d callers of teamprovider.Insert, want at least %d", len(callers), len(insertWriters))
	}
}

// declarationAt is the source from offset to the first blank line: the whole
// declaration that holds a statement.
func declarationAt(source string, offset int) string {
	declaration := source[offset:]
	if end := strings.Index(declaration, "\n\n"); end >= 0 {
		declaration = declaration[:end]
	}
	return declaration
}

var storedIdentityConst = regexp.MustCompile(`(?m)^(?:const )?\s*(\w+)\s*=\s*[^\n]*teamprovider\.StoredIdentityOfOrganization`)

// joinsStoredIdentity says the declaration joins the shared stored-identity
// subquery, directly or through a const of the same file that names it.
func joinsStoredIdentity(source, declaration string) bool {
	if strings.Contains(declaration, "teamprovider.StoredIdentityOfOrganization") {
		return true
	}
	for _, m := range storedIdentityConst.FindAllStringSubmatch(source, -1) {
		if regexp.MustCompile(`\b` + m[1] + `\b`).MatchString(declaration) {
			return true
		}
	}
	return false
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
