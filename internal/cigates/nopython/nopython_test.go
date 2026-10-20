package nopython_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

var pythonPrograms = map[string]bool{"python": true, "python3": true, "uv": true, "pytest": true}

const pyoracleImport = "github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"

func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("go.mod not found above the test directory")
		}
		dir = parent
	}
}

func skipDir(name string) bool {
	return name == ".git" || name == "node_modules" || name == "vendor" || name == ".venv"
}

func goFiles(t *testing.T, root string) []string {
	t.Helper()
	var files []string
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if path != root && skipDir(entry.Name()) {
				return filepath.SkipDir
			}
			return nil
		}
		if strings.HasSuffix(path, ".go") {
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return files
}

func pythonProgram(expr ast.Expr) bool {
	if ident, ok := expr.(*ast.Ident); ok {
		return strings.Contains(strings.ToLower(ident.Name), "python")
	}
	literal, ok := expr.(*ast.BasicLit)
	if !ok || literal.Kind != token.STRING {
		return false
	}
	value, err := strconv.Unquote(literal.Value)
	if err != nil {
		return false
	}
	return pythonPrograms[filepath.Base(value)]
}

func TestNoGoCodeRunsPython(t *testing.T) {
	root := repoRoot(t)
	fset := token.NewFileSet()
	for _, path := range goFiles(t, root) {
		file, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
		if err != nil {
			t.Fatal(err)
		}
		rel, _ := filepath.Rel(root, path)
		for _, spec := range file.Imports {
			if value, _ := strconv.Unquote(spec.Path.Value); value == pyoracleImport {
				t.Errorf("%s imports %s, the Python interpreter resolver", rel, pyoracleImport)
			}
		}
		ast.Inspect(file, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			selector, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			pkg, ok := selector.X.(*ast.Ident)
			if !ok || pkg.Name != "exec" {
				return true
			}
			var program ast.Expr
			switch selector.Sel.Name {
			case "Command", "LookPath":
				if len(call.Args) > 0 {
					program = call.Args[0]
				}
			case "CommandContext":
				if len(call.Args) > 1 {
					program = call.Args[1]
				}
			}
			if program != nil && pythonProgram(program) {
				t.Errorf("%s: exec.%s runs a Python program", fset.Position(call.Pos()).String()[len(root)+1:], selector.Sel.Name)
			}
			return true
		})
	}
}

func TestNoPythonFilesUnderInternal(t *testing.T) {
	root := repoRoot(t)
	count := 0
	err := filepath.WalkDir(filepath.Join(root, "internal"), func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() && strings.HasSuffix(path, ".py") {
			count++
			if count <= 20 {
				rel, _ := filepath.Rel(root, path)
				t.Errorf("%s is Python; freeze what a Go test needs as data, not as a .py file", rel)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if count > 20 {
		t.Errorf("%d .py files under internal/ in all", count)
	}
}
