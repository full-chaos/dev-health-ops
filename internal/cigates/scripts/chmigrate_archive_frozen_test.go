package scripts_test

import (
	"crypto/sha256"
	"encoding/hex"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// CHAOS-11751: internal/chmigrate/archive holds what the ClickHouse baseline replaced when it moved to the chain
// head (the 098 baseline and the chain files 099..144). The head baseline is proven equal to it by execution
// (TestBaselineEqualsTheArchivedChain), so the archive is byte-frozen: every file is listed in its SHA256SUMS with
// its digest, and no file is added, changed or removed.
func TestChmigrateArchiveIsByteFrozen(t *testing.T) {
	dir := filepath.Join(repoRoot(t), "internal", "chmigrate", "archive")
	raw, err := os.ReadFile(filepath.Join(dir, "SHA256SUMS"))
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{}
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 2 || len(fields[0]) != 64 {
			t.Fatalf("bad SHA256SUMS line %q", line)
		}
		if _, dup := want[fields[1]]; dup {
			t.Fatalf("SHA256SUMS lists %s twice", fields[1])
		}
		want[fields[1]] = fields[0]
	}
	got := map[string]bool{}
	err = filepath.WalkDir(dir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		name, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}
		name = filepath.ToSlash(name)
		if name == "SHA256SUMS" || name == "README.md" {
			return nil
		}
		got[name] = true
		expected, ok := want[name]
		if !ok {
			t.Errorf("%s is not in SHA256SUMS: the archive is frozen; a new ClickHouse migration goes to internal/chmigrate/sql", name)
			return nil
		}
		content, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		sum := sha256.Sum256(content)
		if hex.EncodeToString(sum[:]) != expected {
			t.Errorf("%s changed: the archive is byte-frozen", name)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for name := range want {
		if !got[name] {
			t.Errorf("%s is listed in SHA256SUMS but missing from the archive", name)
		}
	}
	if len(want) < 45 || !got["baseline-098/head.json"] {
		t.Fatalf("SHA256SUMS lists %d files (baseline-098/head.json present: %v): the archive is the 098 baseline and the 44 chain files 099..144",
			len(want), got["baseline-098/head.json"])
	}
}
