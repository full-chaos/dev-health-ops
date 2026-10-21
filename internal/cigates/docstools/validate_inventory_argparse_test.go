package docstools

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The argument parsing of validate-inventory-review against Python 3.14.7 argparse, recorded once over 5657 argument
// vectors (testdata/validate_inventory_argparse.golden.json, recipe inside); 736 rows keep the different 3.12.3 result.
func TestValidateInventoryArgparseMatchesPython(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("testdata", "validate_inventory_argparse.golden.json"))
	if err != nil {
		t.Fatal(err)
	}
	var golden struct {
		Python string `json:"python"`
		Usage  string `json:"usage"`
		Help   string `json:"help"`
		Rows   []struct {
			Argv   []string `json:"argv"`
			RC     int      `json:"rc"`
			Help   bool     `json:"help"`
			Error  string   `json:"error"`
			Values []string `json:"values"`
		} `json:"rows"`
	}
	if err := json.Unmarshal(raw, &golden); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(golden.Python, "3.14.7 ") || len(golden.Rows) < 5000 || golden.Usage != ivUsage || golden.Help != ivHelp {
		t.Fatalf("golden has %d rows; usage/help match: %v %v", len(golden.Rows), golden.Usage == ivUsage, golden.Help == ivHelp)
	}
	bad := 0
	for _, row := range golden.Rows {
		var stdout, stderr bytes.Buffer
		vals, rc, done := ivParseArgs(row.Argv, &stdout, &stderr)
		wantOut, wantErr := "", ""
		if row.Help {
			wantOut = golden.Help
		}
		if row.Error != "" {
			wantErr = golden.Usage + row.Error
		}
		var got []string
		if !done {
			got = vals
		}
		if rc != row.RC || stdout.String() != wantOut || stderr.String() != wantErr || !equalStrings(got, row.Values) {
			bad++
			if bad <= 20 {
				t.Errorf("argv %q: rc %d stdout %q stderr %q values %q; Python rc %d help %v error %q values %q",
					row.Argv, rc, stdout.String(), stderr.String(), got, row.RC, row.Help, row.Error, row.Values)
			}
		}
	}
	if bad > 0 {
		t.Errorf("%d of %d argument vectors differ from Python", bad, len(golden.Rows))
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// str(Path(p)), with_suffix(".tsv"), with_name(...) and Path(p) / name, as Python 3.14.7 printed them (3.12.3 differs
// on "x.": its suffix rule gave "x..tsv").
func TestValidateInventoryPathSemantics(t *testing.T) {
	const empty = "ValueError: PosixPath(%s) has an empty name"
	for _, c := range []struct{ in, str, suffix, name, join string }{
		{".build/documentation-inventory.json", ".build/documentation-inventory.json", ".build/documentation-inventory.tsv", ".build/documentation-inventory-summary.md", ".build/documentation-inventory.json/documentation-inventory.json"},
		{"a.b.json", "a.b.json", "a.b.tsv", "documentation-inventory-summary.md", "a.b.json/documentation-inventory.json"},
		{".hidden", ".hidden", ".hidden.tsv", "documentation-inventory-summary.md", ".hidden/documentation-inventory.json"},
		{"..", "..", "...tsv", "documentation-inventory-summary.md", "../documentation-inventory.json"},
		{"x/", "x", "x.tsv", "documentation-inventory-summary.md", "x/documentation-inventory.json"},
		{"/a/b.json", "/a/b.json", "/a/b.tsv", "/a/documentation-inventory-summary.md", "/a/b.json/documentation-inventory.json"},
		{"//a/b.json", "//a/b.json", "//a/b.tsv", "//a/documentation-inventory-summary.md", "//a/b.json/documentation-inventory.json"},
		{"///a/b", "/a/b", "/a/b.tsv", "/a/documentation-inventory-summary.md", "/a/b/documentation-inventory.json"},
		{"a", "a", "a.tsv", "documentation-inventory-summary.md", "a/documentation-inventory.json"},
		{".", ".", fmt.Sprintf(empty, "'.'"), fmt.Sprintf(empty, "'.'"), "documentation-inventory.json"},
		{"/", "/", fmt.Sprintf(empty, "'/'"), fmt.Sprintf(empty, "'/'"), "/documentation-inventory.json"},
		{"//", "//", fmt.Sprintf(empty, "'//'"), fmt.Sprintf(empty, "'//'"), "//documentation-inventory.json"},
		{"", ".", fmt.Sprintf(empty, "'.'"), fmt.Sprintf(empty, "'.'"), "documentation-inventory.json"},
		{"a//b/./c/", "a/b/c", "a/b/c.tsv", "a/b/documentation-inventory-summary.md", "a/b/c/documentation-inventory.json"},
		{"x.", "x.", "x.tsv", "documentation-inventory-summary.md", "x./documentation-inventory.json"},
		{".x.", ".x.", ".x.tsv", "documentation-inventory-summary.md", ".x./documentation-inventory.json"},
		{"a..", "a..", "a..tsv", "documentation-inventory-summary.md", "a../documentation-inventory.json"},
		{"...", "...", "....tsv", "documentation-inventory-summary.md", ".../documentation-inventory.json"},
		{"x..y", "x..y", "x..tsv", "documentation-inventory-summary.md", "x..y/documentation-inventory.json"},
	} {
		show := func(s string, err error) string {
			if err != nil {
				return err.Error()
			}
			return s
		}
		suffix := show(ivWithSuffix(c.in, ".tsv"))
		name := show(ivWithName(c.in, "documentation-inventory-summary.md"))
		if got := ivPath(c.in); got != c.str || suffix != c.suffix || name != c.name || ivJoin(c.in, "documentation-inventory.json") != c.join {
			t.Errorf("%q: str %q with_suffix %q with_name %q join %q; Python %q %q %q %q", c.in, got, suffix, name,
				ivJoin(c.in, "documentation-inventory.json"), c.str, c.suffix, c.name, c.join)
		}
	}
}
