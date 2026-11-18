package chschema

import (
	"bytes"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/chmigrate"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
)

// TestNoGoFileHandBuildsAChainTable: no Go file creates, by literal DDL, a table that a chain migration changes. A
// hand-typed copy keeps the shape of the day it was written, so the next migration of that table leaves it behind.
// Build it with CreateTables (or Apply). internal/chmigrate is out of scope: its rehearsals build the shape before a
// migration on purpose. DDL built from a table name held in a variable is not seen.
func TestNoGoFileHandBuildsAChainTable(t *testing.T) {
	chain, err := chmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	tables := ChainTables(chain)
	if len(tables) == 0 {
		return
	}
	quoted := make([]string, len(tables))
	for index, table := range tables {
		quoted[index] = regexp.QuoteMeta(table)
	}
	handBuilt := regexp.MustCompile("(?i)CREATE\\s+(?:OR\\s+REPLACE\\s+)?TABLE\\s+(?:IF\\s+NOT\\s+EXISTS\\s+)?`?(" +
		strings.Join(quoted, "|") + ")`?(?:[^A-Za-z0-9_`]|$)")
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatal(err)
	}
	skip := map[string]bool{
		filepath.Join(root, ".git"):                                true,
		filepath.Join(root, "internal", "chmigrate"):               true,
		filepath.Join(root, "internal", "testsupport", "chschema"): true,
		filepath.Join(root, "node_modules"):                        true,
	}
	var found []string
	err = filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if skip[path] {
				return filepath.SkipDir
			}
			return nil
		}
		if filepath.Ext(path) != ".go" {
			return nil
		}
		body, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if !bytes.Contains(body, []byte("CREATE")) && !bytes.Contains(body, []byte("create")) {
			return nil
		}
		for _, match := range handBuilt.FindAllSubmatchIndex(body, -1) {
			line := 1 + strings.Count(string(body[:match[0]]), "\n")
			relative, _ := filepath.Rel(root, path)
			found = append(found, relative+":"+strconv.Itoa(line)+" "+string(body[match[2]:match[3]]))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(found) > 0 {
		t.Fatalf("%d hand-built table(s) that the chain changes; build each with chschema.CreateTables:\n%s",
			len(found), strings.Join(found, "\n"))
	}
}
