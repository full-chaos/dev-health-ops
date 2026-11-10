package routescan

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestShiftedTreeInsertsAboveTheDocCommentAndCopiesOnlyNonTestGo(t *testing.T) {
	repo := t.TempDir()
	write := func(rel, body string) {
		if err := os.MkdirAll(filepath.Dir(filepath.Join(repo, rel)), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(repo, rel), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("p/a.go", "package p\n\n// Routes is doc.\nfunc Routes() {}\n")
	write("p/a_test.go", "package p\n")
	write("p/data.json", "{}\n")
	write("q/cited.json", "{}\n")
	root := ShiftedTree(t, repo, []string{"p"}, []string{"q/cited.json"}, map[string]string{"p/a.go": "func Routes("})
	raw, err := os.ReadFile(filepath.Join(root, "p/a.go"))
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(string(raw), "\n")
	if lines[2] != "// line-shift fixture 1" || lines[2+ShiftLines] != "// Routes is doc." || lines[3+ShiftLines] != "func Routes() {}" {
		t.Fatalf("shifted file:\n%s", raw)
	}
	for rel, want := range map[string]bool{"p/a_test.go": false, "p/data.json": false, "q/cited.json": true} {
		if _, err := os.Stat(filepath.Join(root, rel)); (err == nil) != want {
			t.Errorf("%s copied = %v, want %v", rel, err == nil, want)
		}
	}
}

func TestShiftAboveRefusesAMissingOrRepeatedMarker(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "a.go")
	if err := os.WriteFile(path, []byte("package p\nfunc A() {}\nfunc A() {}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := shiftAbove(path, "func B("); err == nil || !strings.Contains(err.Error(), "no line starts with") {
		t.Errorf("missing marker: %v", err)
	}
	if err := shiftAbove(path, "func A("); err == nil || !strings.Contains(err.Error(), "starts two lines") {
		t.Errorf("repeated marker: %v", err)
	}
}
