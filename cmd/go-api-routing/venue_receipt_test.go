package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadVenueReceiptDomain(t *testing.T) {
	dir := t.TempDir()
	write := func(name, body string) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	good := `{"schema_digest":"s","candidate_build":"b","stage":"deployed_executed","exit_cause":"completed","venue":{"name":"bigboy-compose","principal_role":"admin","principal_superuser":false},"outcomes":[]}`
	if r, err := loadVenueReceipt(write("good.json", good)); err != nil || r.Digest == "" {
		t.Fatalf("good: %v", err)
	}
	link := filepath.Join(dir, "link.json")
	if err := os.Symlink(write("t.json", good), link); err != nil {
		t.Fatal(err)
	}
	for name, path := range map[string]string{
		"missing":  filepath.Join(dir, "none.json"),
		"dir":      dir,
		"symlink":  link,
		"trailing": write("trailing.json", good+"{}"),
		"garbage":  write("garbage.json", "nope"),
		"empty":    write("empty.json", ""),
		"oversize": write("big.json", strings.Repeat(" ", maxVenueReceiptBytes+1)),
	} {
		if _, err := loadVenueReceipt(path); err == nil {
			t.Errorf("%s accepted", name)
		}
	}
}
