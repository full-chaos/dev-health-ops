package fixturescli

import (
	"bytes"
	"compress/gzip"
	"embed"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// CHAOS-9161: a column dropped by a migration is still a column of the frozen worlds, which are
// recorded files that are never rewritten (digest-pinned). This census finds them WITHOUT a CI run:
// every column that a migration drops and that any embedded frozen world still carries must be in
// retiredColumns (the loader then leaves it out, loudly); every retiredColumns entry must be a column
// a migration really drops (a stale entry would hide a typo from the strict load); and a column of a
// frozen table that is NOT in retiredColumns is still loaded and fails against the schema if it is gone.
var droppedColumn = regexp.MustCompile("(?is)\\balter\\s+table\\s+`?([a-z0-9_]+)`?\\s+drop\\s+column\\s+(?:if\\s+exists\\s+)?`?([a-z0-9_]+)`?")

func droppedColumnsOfTheMigrations(t *testing.T) map[string]map[string]bool {
	t.Helper()
	files, err := filepath.Glob(filepath.Join("..", "chmigrate", "sql", "*.sql"))
	if err != nil || len(files) < 10 {
		t.Fatalf("the census read %d migration files (%v): it did not measure the tree", len(files), err)
	}
	dropped := map[string]map[string]bool{}
	for _, file := range files {
		raw, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		for _, match := range droppedColumn.FindAllStringSubmatch(string(raw), -1) {
			table, column := strings.ToLower(match[1]), strings.ToLower(match[2])
			if dropped[table] == nil {
				dropped[table] = map[string]bool{}
			}
			dropped[table][column] = true
		}
	}
	return dropped
}

// frozenTables returns every table (name -> its column names) that the embedded frozen files hold,
// found by shape, so a new file layout is still seen.
func frozenTables(t *testing.T, files ...embed.FS) map[string]map[string]bool {
	t.Helper()
	tables := map[string]map[string]bool{}
	var walk func(node any)
	walk = func(node any) {
		switch typed := node.(type) {
		case map[string]any:
			name, isNamed := typed["name"].(string)
			columns, hasColumns := typed["columns"].([]any)
			if isNamed && hasColumns {
				if tables[name] == nil {
					tables[name] = map[string]bool{}
				}
				for _, column := range columns {
					if object, ok := column.(map[string]any); ok {
						if columnName, ok := object["name"].(string); ok {
							tables[name][strings.ToLower(columnName)] = true
						}
					}
				}
			}
			for _, value := range typed {
				walk(value)
			}
		case []any:
			for _, value := range typed {
				walk(value)
			}
		}
	}
	read := 0
	for _, fsys := range files {
		for _, dir := range []string{"testdata/generate", "testdata/synthetic"} {
			entries, err := fsys.ReadDir(dir)
			if err != nil {
				continue
			}
			for _, entry := range entries {
				raw, err := fsys.ReadFile(dir + "/" + entry.Name())
				if err != nil {
					t.Fatal(err)
				}
				reader, err := gzip.NewReader(bytes.NewReader(raw))
				if err != nil {
					t.Fatal(err)
				}
				plain, err := io.ReadAll(reader)
				if err != nil {
					t.Fatal(err)
				}
				var document any
				if err := json.Unmarshal(plain, &document); err != nil {
					t.Fatal(err)
				}
				walk(document)
				read++
			}
		}
	}
	if read < 3 {
		t.Fatalf("the census read %d frozen files: it did not measure the worlds", read)
	}
	return tables
}

func TestEveryDroppedColumnOfAFrozenWorldIsRetiredByName(t *testing.T) {
	dropped := droppedColumnsOfTheMigrations(t)
	var problems []string
	for table, columns := range frozenTables(t, worldFiles, syntheticFiles) {
		for column := range columns {
			if dropped[table][column] && !retiredColumns[table][column] {
				problems = append(problems, table+"."+column+": a migration drops it and a frozen world still carries it: add it to retiredColumns")
			}
		}
	}
	for table, columns := range retiredColumns {
		for column := range columns {
			if !dropped[table][column] {
				problems = append(problems, table+"."+column+": retired but no migration drops it: remove the entry (the load must stay strict)")
			}
		}
	}
	sort.Strings(problems)
	if len(problems) > 0 {
		t.Fatalf("%s", strings.Join(problems, "\n"))
	}
	if !dropped["teams"]["members"] || !frozenTables(t, worldFiles, syntheticFiles)["teams"]["members"] {
		t.Fatalf("the census no longer sees teams.members in the migrations and the frozen worlds: it did not measure what it exists for")
	}
}

// The strictness half: only the named column is left out of a load; any other column of a frozen table
// is still named by the INSERT, so a column the schema lacks fails the load there.
func TestRetiredColumnsAreOnlyTheNamedOnes(t *testing.T) {
	table := FrozenTable{
		Name:    "teams",
		Columns: []FrozenColumn{{Name: "id", Type: "String"}, {Name: "members", Type: "Array(String)"}, {Name: "not_in_the_schema", Type: "String"}},
		Rows:    [][]any{{"t1", []any{"a"}, "x"}},
	}
	got := table.WithoutServerStamped().columnList()
	if got != "`id`, `not_in_the_schema`" {
		t.Fatalf("the load names %s, want only members left out", got)
	}
	if len(retiredColumns) != 1 || len(retiredColumns["teams"]) != 1 {
		t.Fatalf("retiredColumns = %v: the list is exactly teams.members", retiredColumns)
	}
}

// CHAOS-9302: a table dropped by a migration is still a table of the frozen worlds. Every table that a
// migration drops and that a frozen world carries must be in retiredTables (the loader then leaves it
// out, loudly); every retiredTables entry must be a table a migration really drops.
var droppedTable = regexp.MustCompile("(?is)\\bdrop\\s+table\\s+(?:if\\s+exists\\s+)?`?([a-z0-9_]+)`?\\s*;")

func TestEveryDroppedTableOfAFrozenWorldIsRetiredByName(t *testing.T) {
	files, err := filepath.Glob(filepath.Join("..", "chmigrate", "sql", "*.sql"))
	if err != nil || len(files) < 10 {
		t.Fatalf("the census read %d migration files (%v): it did not measure the tree", len(files), err)
	}
	dropped := map[string]bool{}
	for _, file := range files {
		raw, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		for _, match := range droppedTable.FindAllStringSubmatch(string(raw), -1) {
			dropped[strings.ToLower(match[1])] = true
		}
	}
	carried := frozenTables(t, worldFiles, syntheticFiles)
	var problems []string
	for table := range carried {
		if dropped[table] && !retiredTables[table] {
			problems = append(problems, table+": a migration drops it and a frozen world still carries it: add it to retiredTables")
		}
	}
	for table := range retiredTables {
		if !dropped[table] {
			problems = append(problems, table+": retired but no migration drops it: remove the entry (the load must stay strict)")
		}
		if carried[table] == nil {
			problems = append(problems, table+": retired but no frozen world carries it: the census did not measure what it exists for")
		}
	}
	sort.Strings(problems)
	if len(problems) > 0 {
		t.Fatalf("%s", strings.Join(problems, "\n"))
	}
}
