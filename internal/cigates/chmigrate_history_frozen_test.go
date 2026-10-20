package cigates_test

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// CHAOS-10235 R3: the ClickHouse tree under src/dev_health_ops/migrations is deleted and may not come back, and the
// pre-099 SQL text kept in internal/chmigrate/history stays byte-frozen.

func TestNoPythonMigrationsTreeReturns(t *testing.T) {
	root := repoRoot(t)
	if _, err := os.Stat(filepath.Join(root, "src", "dev_health_ops", "migrations")); !os.IsNotExist(err) {
		t.Fatalf("src/dev_health_ops/migrations exists again (stat err: %v); new ClickHouse migrations go only to internal/chmigrate/sql", err)
	}
}

func TestChmigrateHistoryIsByteFrozen(t *testing.T) {
	dir := filepath.Join(repoRoot(t), "internal", "chmigrate", "history")
	raw, err := os.ReadFile(filepath.Join(dir, "SHA256SUMS"))
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{}
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 2 {
			t.Fatalf("bad SHA256SUMS line %q", line)
		}
		want[fields[1]] = fields[0]
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, entry := range entries {
		name := entry.Name()
		if name == "SHA256SUMS" || name == "README.md" {
			continue
		}
		got = append(got, name)
		expected, ok := want[name]
		if !ok {
			t.Errorf("%s is not in SHA256SUMS: history is frozen, a new migration goes to internal/chmigrate/sql", name)
			continue
		}
		content, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(content)
		if hex.EncodeToString(sum[:]) != expected {
			t.Errorf("%s changed: history is byte-frozen", name)
		}
	}
	sort.Strings(got)
	for name := range want {
		if i := sort.SearchStrings(got, name); i == len(got) || got[i] != name {
			t.Errorf("%s is listed in SHA256SUMS but missing from history", name)
		}
	}
}
