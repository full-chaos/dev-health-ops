package pythonparity_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
)

const pythonparityPath = "github.com/full-chaos/dev-health-ops/internal/pythonparity"

// CHAOS-7937: the exact Python port (SanitizeErrorText, pinned by the frozen oracle) is referenced by NO production code outside
// this package; every caller that stores or returns error text uses SanitizeErrorTextHardened. The check works on the IMPORTS
// of every non-test Go file (go/parser): a file that imports pythonparity under a dot or blank name fails (it hides every call
// form); a file that imports it under any other name is listed (the list is printed) and fails when it selects SanitizeErrorText
// on that name in ANY form (call, parenthesised call, function value). An empty file set read, or no importer found, fails.
func productionImporters(t *testing.T, root string) (importers []string, violations []string, files int) {
	t.Helper()
	fset := token.NewFileSet()
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			switch entry.Name() {
			case ".git", "node_modules", "testdata":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		rel, _ := filepath.Rel(root, path)
		if strings.HasPrefix(rel, "internal/pythonparity/") {
			return nil
		}
		files++
		file, parseErr := parser.ParseFile(fset, path, nil, parser.ParseComments)
		if parseErr != nil {
			return parseErr
		}
		local := ""
		for _, spec := range file.Imports {
			value, _ := strconv.Unquote(spec.Path.Value)
			if value != pythonparityPath {
				continue
			}
			name := "pythonparity"
			if spec.Name != nil {
				name = spec.Name.Name
			}
			if name == "." || name == "_" {
				violations = append(violations, rel+" imports pythonparity as "+strconv.Quote(name))
				continue
			}
			local = name
			importers = append(importers, rel)
		}
		if local == "" {
			return nil
		}
		ast.Inspect(file, func(node ast.Node) bool {
			selector, ok := node.(*ast.SelectorExpr)
			if !ok || selector.Sel.Name != "SanitizeErrorText" {
				return true
			}
			if ident, ok := selector.X.(*ast.Ident); ok && ident.Name == local {
				violations = append(violations, rel+":"+strconv.Itoa(fset.Position(selector.Pos()).Line)+" selects the exact SanitizeErrorText")
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(importers)
	return importers, violations, files
}

func TestNoProductionCodeReferencesTheExactPythonSanitizer(t *testing.T) {
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	importers, violations, files := productionImporters(t, root)
	t.Logf("%d production files read; %d import pythonparity (allow-list, derived): %v", files, len(importers), importers)
	if files < 500 || len(importers) < 9 {
		t.Fatalf("read %d production files and found %d importers of pythonparity, want at least 500 and 9", files, len(importers))
	}
	if len(violations) != 0 {
		t.Fatalf("production code reaches the exact Python port (use SanitizeErrorTextHardened): %v", violations)
	}
}

// The check is exercised on a synthetic tree: each form is found, a clean tree and an empty tree are not accepted silently.
func TestTheImportCheckFindsEveryForm(t *testing.T) {
	write := func(t *testing.T, dir, name, body string) {
		t.Helper()
		if err := writeFile(filepath.Join(dir, name), body); err != nil {
			t.Fatal(err)
		}
	}
	head := "package x\n\nimport pp \"" + pythonparityPath + "\"\n\n"
	for name, body := range map[string]string{
		"alias call":     head + "var _ = pp.SanitizeErrorText(\"\", 1)\n",
		"parenthesised":  head + "var _ = (pp.SanitizeErrorText)(\"\", 1)\n",
		"function value": head + "var f = pp.SanitizeErrorText\n",
		"dot import":     "package x\n\nimport . \"" + pythonparityPath + "\"\n",
		"blank import":   "package x\n\nimport _ \"" + pythonparityPath + "\"\n",
		"plain import":   "package x\n\nimport \"" + pythonparityPath + "\"\n\nvar _ = pythonparity.SanitizeErrorText(\"\", 1)\n",
	} {
		dir := t.TempDir()
		write(t, dir, "f.go", body)
		_, violations, files := productionImporters(t, dir)
		if files != 1 || len(violations) == 0 {
			t.Fatalf("%s: files=%d violations=%v, want a violation", name, files, violations)
		}
	}
	dir := t.TempDir()
	write(t, dir, "f.go", head+"var _ = pp.SanitizeErrorTextHardened(\"\", 1)\n")
	if importers, violations, _ := productionImporters(t, dir); len(violations) != 0 || len(importers) != 1 {
		t.Fatalf("the hardened form was flagged or the importer missed: %v %v", importers, violations)
	}
	if _, _, files := productionImporters(t, t.TempDir()); files != 0 {
		t.Fatal("an empty tree reported files")
	}
}
