package pyunicodedata

import (
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// forbiddenNormalizerImports are the packages whose Unicode answers differ
// from CPython 3.14's unicodedata (Unicode 16.0.0) and its "idna" codec.
// golang.org/x/text/unicode/norm follows a newer Unicode edition, truncates
// a composition starter to 16 bits (golang/go#81028) and inserts U+034F
// into a long run of non-starters; golang.org/x/net/idna implements
// IDNA 2008/UTS 46, not Python's IDNA 2003 codec; precis normalizes through
// norm. Go code that ports a Python normalize or idna call uses this
// package (NFC, NFKC) and internal/pythonparity/pyidna instead.
var forbiddenNormalizerImports = map[string]string{
	"golang.org/x/text/unicode/norm":    "use internal/pythonparity/pyunicodedata",
	"golang.org/x/net/idna":             "use internal/pythonparity/pyidna",
	"golang.org/x/text/secure/precis":   "normalizes with golang.org/x/text/unicode/norm; use internal/pythonparity/pyunicodedata",
	"golang.org/x/text/secure/bidirule": "Unicode tables of golang.org/x/text, not CPython's; use internal/pythonparity/pyunicodedata",
}

// normalizerImportAllowList names non-test files (slash paths relative to
// the module root) that may import a forbidden package. It is empty: add a
// file here only with a comment that says why its answers need not match
// Python.
var normalizerImportAllowList = map[string]bool{}

// TestNoNonTestGoFileImportsAForeignNormalizer fails on a non-test Go file
// anywhere in the module that imports a package in
// forbiddenNormalizerImports. It parses every file whatever its build tags,
// so a file behind integration or tools tags is checked too. Test files may
// import them (a test can compare against them).
func TestNoNonTestGoFileImportsAForeignNormalizer(t *testing.T) {
	root := moduleRoot(t)
	offenders, scanned := scanForeignNormalizerImports(t, root)
	// A walk that finds no Go files would pass whatever the tree holds.
	if scanned < 1000 {
		t.Fatalf("scanned only %d non-test Go files under %s; the walk is not seeing the module", scanned, root)
	}
	if !contains(offenders.scannedPaths, "internal/pythonparity/pyunicodedata/nfc.go") {
		t.Fatalf("the walk did not reach internal/pythonparity/pyunicodedata/nfc.go; it is not seeing the module")
	}
	if len(offenders.found) > 0 {
		t.Fatalf("non-test Go files import a normalizer whose answers differ from CPython's:\n%s",
			strings.Join(offenders.found, "\n"))
	}
}

// TestTheNormalizerImportScanDetectsAnImport plants each forbidden import
// (plain, renamed, blank and inside a grouped block, behind a build tag)
// in a scratch module and requires the scan to report it, and requires a
// _test.go file with the same import to pass.
func TestTheNormalizerImportScanDetectsAnImport(t *testing.T) {
	for path := range forbiddenNormalizerImports {
		for name, source := range map[string]string{
			"plain.go":   "package p\n\nimport \"" + path + "\"\n",
			"renamed.go": "package p\n\nimport x \"" + path + "\"\n",
			"blank.go":   "package p\n\nimport _ \"" + path + "\"\n",
			"grouped.go": "//go:build integration\n\npackage p\n\nimport (\n\t\"fmt\"\n\t\"" + path + "\"\n)\n",
		} {
			root := t.TempDir()
			writeFile(t, filepath.Join(root, "go.mod"), "module example.test\n")
			writeFile(t, filepath.Join(root, "a", "b", name), source)
			writeFile(t, filepath.Join(root, "a", "b", "only_test.go"), "package p\n\nimport \""+path+"\"\n")
			offenders, _ := scanForeignNormalizerImports(t, root)
			want := "a/b/" + name + ": " + path
			if len(offenders.found) != 1 || !strings.HasPrefix(offenders.found[0], want) {
				t.Errorf("%s importing %s: scan reported %q, want one line starting %q", name, path, offenders.found, want)
			}
		}
	}
}

type normalizerScan struct {
	found        []string
	scannedPaths []string
}

func scanForeignNormalizerImports(t *testing.T, root string) (normalizerScan, int) {
	t.Helper()
	var scan normalizerScan
	fileSet := token.NewFileSet()
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if path == root {
				return nil
			}
			name := entry.Name()
			if strings.HasPrefix(name, ".") || name == "node_modules" || name == "vendor" || name == "testdata" {
				return filepath.SkipDir
			}
			// A nested module is its own build; its go.mod pins its own
			// dependencies.
			if _, statErr := os.Stat(filepath.Join(path, "go.mod")); statErr == nil {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		relative = filepath.ToSlash(relative)
		scan.scannedPaths = append(scan.scannedPaths, relative)
		parsed, err := parser.ParseFile(fileSet, path, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		for _, spec := range parsed.Imports {
			imported, err := strconv.Unquote(spec.Path.Value)
			if err != nil {
				return err
			}
			if reason, forbidden := forbiddenNormalizerImports[imported]; forbidden && !normalizerImportAllowList[relative] {
				scan.found = append(scan.found, relative+": "+imported+" ("+reason+")")
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walking %s: %v", root, err)
	}
	sort.Strings(scan.found)
	return scan, len(scan.scannedPaths)
}

func moduleRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		content, err := os.ReadFile(filepath.Join(dir, "go.mod"))
		if err == nil && strings.HasPrefix(string(content), "module github.com/full-chaos/dev-health-ops\n") {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("no go.mod for module github.com/full-chaos/dev-health-ops above the test directory")
		}
		dir = parent
	}
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
