package cigates_test

import (
	"go/scanner"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// CHAOS-10174 PR C: Go half of tests/tooling/test_tmp_dirs_removed.py (CHAOS-7185). A Go _test.go file that calls
// os.MkdirTemp must also call os.RemoveAll in code: a comment or a string that names it does not count. Tests built a
// ~100 MB binary into a MkdirTemp directory per run and never removed it. The Python half (pytest mkdtemp scan) stays in
// tests/tooling until the pytest steps go (CHAOS-10180).

var goTempScanSkipDirs = map[string]bool{"web": true, "node_modules": true, ".git": true, ".venv": true, "vendor": true}

// codeCalls reports whether the Go source calls pkg.fn( in code tokens (comments and literals never match).
func codeCalls(source []byte, pkg, fn string) bool {
	fset := token.NewFileSet()
	file := fset.AddFile("x.go", fset.Base(), len(source))
	var s scanner.Scanner
	s.Init(file, source, func(token.Position, string) {}, scanner.ScanComments)
	var window [4]string
	for {
		_, tok, lit := s.Scan()
		if tok == token.EOF {
			return false
		}
		if tok == token.COMMENT {
			continue
		}
		text := tok.String()
		if tok == token.IDENT {
			text = lit
		}
		window[0], window[1], window[2], window[3] = window[1], window[2], window[3], text
		if window[0] == pkg && window[1] == "." && window[2] == fn && window[3] == "(" {
			return true
		}
	}
}

func goTempDirOffenders(root string) ([]string, int, error) {
	var out []string
	scanned := 0
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(root, path)
		if d.IsDir() {
			if rel != "." && !strings.Contains(rel, string(filepath.Separator)) && goTempScanSkipDirs[d.Name()] {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(d.Name(), "_test.go") {
			return nil
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		scanned++
		if codeCalls(raw, "os", "MkdirTemp") && !codeCalls(raw, "os", "RemoveAll") {
			out = append(out, filepath.ToSlash(rel))
		}
		return nil
	})
	sort.Strings(out)
	return out, scanned, err
}

func TestGoTestsRemoveTheTempDirsTheyMake(t *testing.T) {
	offenders, scanned, err := goTempDirOffenders(rootOf(t))
	if err != nil {
		t.Fatal(err)
	}
	if scanned < 100 {
		t.Fatalf("scanned only %d _test.go files: the check read almost nothing", scanned)
	}
	if len(offenders) > 0 {
		t.Fatalf("these test files call os.MkdirTemp without a call of os.RemoveAll (use t.TempDir or remove the directory): %v", offenders)
	}
}

func TestGoTempDirCheckFlagsALeakAndAcceptsACleanup(t *testing.T) {
	write := func(t *testing.T, dir, body string) {
		t.Helper()
		if err := os.MkdirAll(filepath.Join(dir, "pkg"), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "pkg", "leak_test.go"), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	const leak = "package pkg\nvar _, _ = os.MkdirTemp(\"\", \"x-\")\n"
	cases := []struct {
		name string
		body string
		want []string
	}{
		{"a leak is flagged", leak, []string{"pkg/leak_test.go"}},
		{"a real cleanup is accepted", "package pkg\nvar d, _ = os.MkdirTemp(\"\", \"x-\")\nfunc f() { os.RemoveAll(d) }\n", nil},
		{"a line comment is not a cleanup", leak + "// os.RemoveAll(d)\n", []string{"pkg/leak_test.go"}},
		{"a block comment is not a cleanup", leak + "/* os.RemoveAll(d) */\n", []string{"pkg/leak_test.go"}},
		{"a string is not a cleanup", leak + "var note = \"os.RemoveAll(\"\n", []string{"pkg/leak_test.go"}},
		{"a raw string is not a cleanup", leak + "var note = `os.RemoveAll(`\n", []string{"pkg/leak_test.go"}},
		{"a comment naming MkdirTemp is not a call", "package pkg\n// os.MkdirTemp(\n", nil},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			write(t, dir, tc.body)
			got, _, err := goTempDirOffenders(dir)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Join(got, "|") != strings.Join(tc.want, "|") {
				t.Fatalf("offenders %q, want %q", got, tc.want)
			}
		})
	}
}
