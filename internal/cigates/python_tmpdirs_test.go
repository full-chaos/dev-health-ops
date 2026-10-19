package cigates_test

import (
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// CHAOS-10174 PR E: Go port of the Python half of tests/tooling/test_tmp_dirs_removed.py (CHAOS-7185). A Python test module
// that makes a temp directory outside pytest's tmp_path (a mkdtemp call, or a gettempdir() joined into a path) must also
// remove it (a real rmtree, addfinalizer or atexit.register call). The Python original walks the syntax tree; this scan
// blanks comments and string contents first, so a comment or a string that names a cleanup call counts for nothing, then
// matches calls on the code that is left. It guards Python test files, so it goes when the last of them goes (CHAOS-3165).

const pyTmpSelf = "tests/tooling/test_tmp_dirs_removed.py"

// pyCode returns the source with comments removed and the inside of every string literal blanked.
func pyCode(src string) string {
	var out strings.Builder
	n := len(src)
	for i := 0; i < n; {
		c := src[i]
		switch {
		case c == '#':
			for i < n && src[i] != '\n' {
				i++
			}
		case c == '"' || c == '\'':
			quote := string(c)
			if strings.HasPrefix(src[i:], strings.Repeat(quote, 3)) {
				quote = strings.Repeat(quote, 3)
			}
			out.WriteString(quote)
			i += len(quote)
			for i < n && !strings.HasPrefix(src[i:], quote) {
				if src[i] == '\\' {
					i++
				} else if len(quote) == 1 && src[i] == '\n' {
					break
				}
				i++
			}
			if i < n {
				out.WriteString(quote)
				i += len(quote)
			}
		default:
			out.WriteByte(c)
			i++
		}
	}
	return out.String()
}

var (
	pyFromTempfile = regexp.MustCompile(`(?m)^[ \t]*from\s+tempfile\s+import\s+(\([^)]*\)|[^\n]*)`)
	pyImportAs     = regexp.MustCompile(`(mkdtemp|gettempdir)(?:\s+as\s+(\w+))?`)
	pyImportLine   = regexp.MustCompile(`(?m)^[ \t]*(?:from\s+\S+\s+)?import\s+(?:\([^)]*\)|[^\n]*)`)
	pyCleanup      = regexp.MustCompile(`\b(rmtree|addfinalizer)\s*\(|\batexit\s*\.\s*register\s*\(`)
)

// pyTempAliases maps a local name bound by `from tempfile import x as y` to x.
func pyTempAliases(code string) map[string]string {
	out := map[string]string{}
	for _, m := range pyFromTempfile.FindAllStringSubmatch(code, -1) {
		for _, a := range pyImportAs.FindAllStringSubmatch(m[1], -1) {
			local := a[2]
			if local == "" {
				local = a[1]
			}
			out[local] = a[1]
		}
	}
	return out
}

// pyLogicalStatements splits code into statements, joining lines that sit inside brackets.
func pyLogicalStatements(code string) []string {
	var out []string
	var cur strings.Builder
	depth := 0
	for _, line := range strings.Split(code, "\n") {
		cur.WriteString(line)
		cur.WriteByte(' ')
		depth += strings.Count(line, "(") + strings.Count(line, "[") + strings.Count(line, "{")
		depth -= strings.Count(line, ")") + strings.Count(line, "]") + strings.Count(line, "}")
		if depth <= 0 && !strings.HasSuffix(strings.TrimSpace(line), "\\") {
			out = append(out, cur.String())
			cur.Reset()
			depth = 0
		}
	}
	if cur.Len() > 0 {
		out = append(out, cur.String())
	}
	return out
}

func pyMakesTempDir(source string) bool {
	code := pyCode(source)
	aliases := pyTempAliases(code)
	body := pyImportLine.ReplaceAllString(code, "")
	names := []string{`mkdtemp`, `gettempdir`}
	for local := range aliases {
		names = append(names, regexp.QuoteMeta(local))
	}
	call := regexp.MustCompile(`\b(` + strings.Join(names, "|") + `)\s*\(`)
	resolved := func(name string) string {
		if real, ok := aliases[name]; ok {
			return real
		}
		return name
	}
	for _, m := range call.FindAllStringSubmatch(body, -1) {
		if resolved(m[1]) == "mkdtemp" {
			return true
		}
	}
	for _, stmt := range pyLogicalStatements(body) {
		hasTempdir := false
		for _, m := range call.FindAllStringSubmatch(stmt, -1) {
			if resolved(m[1]) == "gettempdir" {
				hasTempdir = true
			}
		}
		if !hasTempdir {
			continue
		}
		flat := strings.ReplaceAll(stmt, "//", "")
		if strings.Contains(flat, "/") || regexp.MustCompile(`\bjoin\s*\(`).MatchString(stmt) {
			return true
		}
	}
	return false
}

func pyRemoves(source string) bool { return pyCleanup.MatchString(pyCode(source)) }

func pyTmpOffenders(t *testing.T, root string) []string {
	t.Helper()
	var out []string
	walkErr := filepath.WalkDir(filepath.Join(root, "tests"), func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(path, ".py") {
			return err
		}
		rel, _ := filepath.Rel(root, path)
		rel = filepath.ToSlash(rel)
		if rel == pyTmpSelf {
			return nil
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if pyMakesTempDir(string(raw)) && !pyRemoves(string(raw)) {
			out = append(out, rel)
		}
		return nil
	})
	if walkErr != nil {
		t.Fatal(walkErr)
	}
	sort.Strings(out)
	return out
}

func TestPythonTestsRemoveTheTempDirsTheyMake(t *testing.T) {
	root := repoRoot(t)
	if got := pyTmpOffenders(t, root); len(got) != 0 {
		t.Fatalf("Python tests make a temp directory and never remove it: %v", got)
	}
	// A scan that read no file would pass for any tree.
	n := 0
	_ = filepath.WalkDir(filepath.Join(root, "tests"), func(p string, d fs.DirEntry, _ error) error {
		if d != nil && !d.IsDir() && strings.HasSuffix(p, ".py") {
			n++
		}
		return nil
	})
	if n == 0 {
		t.Fatal("tests/ holds no Python file: the scan would pass vacuously")
	}
}

// Planted defects: each leak shape the Python guard flags, and each text or alias trick that must not fool it.
func TestPythonTmpDirScanPlantedDefects(t *testing.T) {
	leaks := []struct{ name, body string }{
		{"mkdtemp", "import tempfile\nd = tempfile.mkdtemp(prefix='x-')\n"},
		{"fixed gettempdir path", "import tempfile\nfrom pathlib import Path\nC = Path(tempfile.gettempdir()) / 'fixed'\n"},
		{"cleanup only in a comment", "import tempfile\nd = tempfile.mkdtemp()\n# shutil.rmtree(d) later\n"},
		{"cleanup only in a string", "import tempfile\nd = tempfile.mkdtemp()\nNOTE = 'rmtree atexit.register'\n"},
		{"cleanup only in a docstring", "import tempfile\nd = tempfile.mkdtemp()\n\"\"\"\nrmtree(d)\n\"\"\"\n"},
		{"aliased mkdtemp", "from tempfile import mkdtemp as make\nd = make()\n"},
		{"bare mkdtemp", "from tempfile import mkdtemp\nd = mkdtemp()\n"},
		{"aliased gettempdir", "from tempfile import gettempdir as g\nfrom pathlib import Path\nC = Path(g()) / 'fixed'\n"},
		{"os.path.join gettempdir", "import os, tempfile\nC = os.path.join(tempfile.gettempdir(), 'fixed')\n"},
		{"multi-line join", "import os, tempfile\nC = os.path.join(\n    tempfile.gettempdir(),\n    'fixed',\n)\n"},
		{"parenthesised import", "from tempfile import (\n    mkdtemp as make,\n)\nd = make()\n"},
	}
	for _, tc := range leaks {
		t.Run("leak/"+tc.name, func(t *testing.T) {
			if !pyMakesTempDir(tc.body) || pyRemoves(tc.body) {
				t.Fatalf("leak not flagged (makes=%v removes=%v)", pyMakesTempDir(tc.body), pyRemoves(tc.body))
			}
		})
	}
	clean := []struct{ name, body string }{
		{"rmtree", "import shutil, tempfile\nd = tempfile.mkdtemp()\nshutil.rmtree(d)\n"},
		{"atexit", "import tempfile\nd = tempfile.mkdtemp()\nimport atexit, shutil\natexit.register(shutil.rmtree, 'x')\n"},
		{"finalizer", "import tempfile\nd = tempfile.mkdtemp()\nrequest.addfinalizer(lambda: None)\n"},
		{"gettempdir read only", "import tempfile\nprint(tempfile.gettempdir())\n"},
		{"no temp at all", "def test_x():\n    assert 1 / 2\n"},
	}
	for _, tc := range clean {
		t.Run("clean/"+tc.name, func(t *testing.T) {
			if pyMakesTempDir(tc.body) && !pyRemoves(tc.body) {
				t.Fatal("clean module flagged")
			}
		})
	}
	t.Run("tree walk names the leaking file and skips the guard itself", func(t *testing.T) {
		root := t.TempDir()
		writeFile(t, filepath.Join(root, "tests", "test_leak.py"), "import tempfile\nd = tempfile.mkdtemp()\n")
		writeFile(t, filepath.Join(root, "tests", "test_ok.py"), "import shutil, tempfile\nshutil.rmtree(tempfile.mkdtemp())\n")
		writeFile(t, filepath.Join(root, pyTmpSelf), "import tempfile\nd = tempfile.mkdtemp()\n")
		got := pyTmpOffenders(t, root)
		if len(got) != 1 || got[0] != "tests/test_leak.py" {
			t.Fatalf("offenders = %v, want [tests/test_leak.py]", got)
		}
	})
}
