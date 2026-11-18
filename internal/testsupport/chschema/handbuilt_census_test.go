package chschema

import (
	"bytes"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/chmigrate"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
)

// TestNoTestHandBuildsAChainTable: no test file (and no internal/testsupport helper) creates, by literal DDL, a table
// that a chain migration changes. A hand-typed copy keeps the shape of the day it was written, so the next migration
// of that table leaves it behind. Build it with CreateTables (or Apply). DDL built from a table name held in a
// variable is not seen.
func TestNoTestHandBuildsAChainTable(t *testing.T) {
	chain, err := chmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	tables := ChainTables(chain)
	if len(tables) == 0 {
		return
	}
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatal(err)
	}
	found, err := handBuiltTables(root, tables)
	if err != nil {
		t.Fatal(err)
	}
	if len(found) > 0 {
		t.Fatalf("%d hand-built table(s) that the chain changes; build each with chschema.CreateTables:\n%s",
			len(found), strings.Join(found, "\n"))
	}
}

func TestHandBuiltTablesCatchesEveryForm(t *testing.T) {
	root := t.TempDir()
	for path, body := range map[string]string{
		"internal/a/a_test.go":                        "package a\n\nconst ddl = `CREATE TABLE default.x (a UInt8)`\n",
		"internal/b/b_test.go":                        "package b\n\nconst ddl = `Create table x (a UInt8)`\n",
		"internal/testsupport/chschema/c_test.go":     "package chschema\n\nconst ddl = `CREATE TABLE x (a UInt8)`\n",
		"internal/testsupport/seed/seed.go":           "package seed\n\nconst ddl = \"CREATE TABLE IF NOT EXISTS `db`.`x` (a UInt8)\"\n",
		"internal/d/d_test.go":                        "package d\n\nconst ddl = `CREATE TABLE x_v2 (a UInt8)`\n",
		"internal/e/e.go":                             "package e\n\nconst ddl = `CREATE TABLE x (a UInt8)`\n",
		"internal/chmigrate/rehearsal_test.go":        "package chmigrate\n\nconst ddl = `CREATE TABLE x (a UInt8)`\n",
		"internal/testsupport/chschema/notes_test.md": "CREATE TABLE x (a UInt8)\n",
	} {
		file := filepath.Join(root, filepath.FromSlash(path))
		if err := os.MkdirAll(filepath.Dir(file), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(file, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	found, err := handBuiltTables(root, []string{"x"})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"internal/a/a_test.go:3 x",
		"internal/b/b_test.go:3 x",
		"internal/testsupport/chschema/c_test.go:3 x",
		"internal/testsupport/seed/seed.go:3 x",
	}
	if !reflect.DeepEqual(found, want) {
		t.Fatalf("found %q, want %q", found, want)
	}
}

var handBuiltRE = regexp.MustCompile("(?i)CREATE\\s+(?:OR\\s+REPLACE\\s+)?TABLE\\s+(?:IF\\s+NOT\\s+EXISTS\\s+)?(?:`?[A-Za-z0-9_]+`?\\.)?`?([A-Za-z0-9_]+)")

// handBuiltTables returns file:line and table for each literal CREATE TABLE of one of tables in a *_test.go file or an
// internal/testsupport file under root, in walk order.
func handBuiltTables(root string, tables []string) ([]string, error) {
	wanted := make(map[string]bool, len(tables))
	for _, table := range tables {
		wanted[table] = true
	}
	// internal/chmigrate is out of scope: its rehearsals build the shape before a migration on purpose.
	skip := map[string]bool{
		filepath.Join(root, ".git"):                  true,
		filepath.Join(root, "internal", "chmigrate"): true,
		filepath.Join(root, "node_modules"):          true,
	}
	testsupport := filepath.Join(root, "internal", "testsupport") + string(filepath.Separator)
	var found []string
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if skip[path] {
				return filepath.SkipDir
			}
			return nil
		}
		if filepath.Ext(path) != ".go" || !strings.HasSuffix(path, "_test.go") && !strings.HasPrefix(path, testsupport) {
			return nil
		}
		body, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if !bytes.Contains(bytes.ToLower(body), []byte("create")) {
			return nil
		}
		for _, match := range handBuiltRE.FindAllSubmatchIndex(body, -1) {
			table := string(body[match[2]:match[3]])
			if !wanted[table] {
				continue
			}
			line := 1 + bytes.Count(body[:match[0]], []byte("\n"))
			relative, err := filepath.Rel(root, path)
			if err != nil {
				return err
			}
			found = append(found, filepath.ToSlash(relative)+":"+strconv.Itoa(line)+" "+table)
		}
		return nil
	})
	return found, err
}
