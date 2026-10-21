package nopython_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"sort"
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

func goFiles(t *testing.T, root string) []string {
	t.Helper()
	var files []string
	for _, rel := range trackedFiles(t, root, "*.go") {
		files = append(files, filepath.Join(root, filepath.FromSlash(rel)))
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
	files := trackedFiles(t, root, "internal/*.py")
	for i, rel := range files {
		if i == 20 {
			break
		}
		t.Errorf("%s is Python; freeze what a Go test needs as data, not as a .py file", rel)
	}
	if len(files) > 20 {
		t.Errorf("%d .py files under internal/ in all", len(files))
	}
}

// scriptPython finds a Python command word: python, python3, a versioned python3.12, uv or pytest, bare or behind a
// path (/usr/bin/python3, .venv/bin/python, ./venv/bin/pytest) or `env`. Word boundaries are Unicode letters, numbers,
// "_", "." and "-", so names like pythonista, épython3 or foo-pytest are not commands.
var scriptPython = regexp.MustCompile(`(^|[^\p{L}\p{N}_.-])(?:[^\s'"\x60;|&<>()=]*/)?(python(?:3(?:\.[0-9]+)?)?|uv|pytest)([^\p{L}\p{N}_.-]|$)`)

// shellPythonPending is the closed list of tracked shell scripts that still name Python, each owned by an open
// change that removes it. An entry goes when its owner merges; a new entry needs a ruling.
var shellPythonPending = map[string]string{
	"ci/bigboy/bigboy-grants-check.sh":   "#4565 ports its helper to Go",
	"ci/bigboy/bump-swap.sh":             "#4565 ports its helper to Go",
	"ci/bigboy/check-dho-api-ch-user.sh": "#4565 ports its helper to Go",
	"ci/bigboy/cut-contents.sh":          "#4565 ports its helpers to Go",
	"ci/local_validate.sh":               "#4567 and the pytest/uv removal leaves",
}

// shellPythonData reads testdata/shell_python_data_lines.tsv: lines that name Python as data and run none.
func shellPythonData(t *testing.T) map[string]bool {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", "shell_python_data_lines.tsv"))
	if err != nil {
		t.Fatal(err)
	}
	lines := map[string]bool{}
	for _, line := range strings.Split(string(raw), "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if !strings.Contains(line, "\t") {
			t.Fatalf("testdata/shell_python_data_lines.tsv: %q is not path<TAB>line", line)
		}
		lines[line] = false
	}
	return lines
}

// shellPythonLines is every line of a shell script, outside comments, that names a Python command: number -> text.
func shellPythonLines(raw string) map[int]string {
	hits := map[int]string{}
	for number, line := range strings.Split(raw, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "#") || !scriptPython.MatchString(line) {
			continue
		}
		hits[number+1] = strings.TrimSpace(line)
	}
	return hits
}

// trackedShellPython is every tracked .sh line under root that names Python: "path:line" -> text.
func trackedShellPython(t *testing.T, root string) (map[string]string, int) {
	t.Helper()
	found := map[string]string{}
	files := trackedFiles(t, root, "*.sh")
	for _, rel := range files {
		raw, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(rel)))
		if err != nil {
			t.Fatal(err)
		}
		for number, line := range shellPythonLines(string(raw)) {
			found[rel+":"+strconv.Itoa(number)] = line
		}
	}
	return found, len(files)
}

func TestNoShellScriptRunsPython(t *testing.T) {
	root := repoRoot(t)
	data := shellPythonData(t)
	pendingHits := map[string]int{}
	found, scanned := trackedShellPython(t, root)
	if scanned == 0 {
		t.Fatal("found no tracked .sh file; the scan is broken")
	}
	keys := make([]string, 0, len(found))
	for key := range found {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		line := found[key]
		rel := key[:strings.LastIndex(key, ":")]
		if _, ok := data[rel+"\t"+line]; ok {
			data[rel+"\t"+line] = true
			continue
		}
		if owner, ok := shellPythonPending[rel]; ok {
			pendingHits[rel]++
			t.Logf("%s still names Python (pending: %s)", key, owner)
			continue
		}
		t.Errorf("%s names python/python3/uv/pytest: %s", key, line)
	}
	for entry, used := range data {
		if !used {
			t.Errorf("testdata/shell_python_data_lines.tsv: %q matches no line any more: remove it", entry)
		}
	}
	for rel := range shellPythonPending {
		if _, err := os.Stat(filepath.Join(root, rel)); err != nil {
			t.Errorf("%s is on the pending list but is gone: remove the entry", rel)
		} else if pendingHits[rel] == 0 {
			t.Errorf("%s is on the pending list but names no Python any more: remove the entry", rel)
		}
	}
}

func TestShellPythonScanSeesOnlyTrackedScripts(t *testing.T) {
	dir := plantedRepo(t, map[string]string{
		"ci/tracked.sh":          "python3 -c 'print(1)'\n",
		".uv-cache/untracked.sh": "python3 -c 'print(1)'\n",
		"scratch/untracked.sh":   "/usr/bin/python3 x.py\n",
	}, "ci/tracked.sh")
	found, scanned := trackedShellPython(t, dir)
	if scanned != 1 || len(found) != 1 || found["ci/tracked.sh:1"] == "" {
		t.Fatalf("scan of the planted repo = %v (%d scripts); want only ci/tracked.sh:1", found, scanned)
	}
}

func TestShellPythonPatternFindsInterpreterPaths(t *testing.T) {
	for line, want := range map[string]bool{
		"/usr/bin/python3 -c 'print(1)'": true,
		".venv/bin/python x.py":          true,
		"./venv/bin/pytest -q tests":     true,
		"env python3 x.py":               true,
		"/usr/bin/env python3 x.py":      true,
		"python3.12 -m venv .v":          true,
		"exec python \"$@\"":             true,
		"uv run x":                       true,
		"X=1 pytest -q":                  true,
		"$(command -v python3) -V":       true,
		"\u00a0python3 -V":               true,
		"pythonista --version":           false,
		"épython3 -V":                    false,
		"foo-pytest":                     false,
		"cp a.python b":                  false,
		"echo uvx":                       false,
		"python3x":                       false,
		"run_python_like_tool":           false,
	} {
		if got := scriptPython.MatchString(line); got != want {
			t.Errorf("scriptPython.MatchString(%q) = %v, want %v", line, got, want)
		}
	}
}
