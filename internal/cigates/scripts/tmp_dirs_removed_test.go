package scripts_test

import (
	"go/scanner"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// CHAOS-10174 PR C: Go half of tests/tooling/test_tmp_dirs_removed.py (CHAOS-7185). A Go _test.go file that calls
// os.MkdirTemp must also call os.RemoveAll in code: a comment or a string that names it does not count. Tests built a
// ~100 MB binary into a MkdirTemp directory per run and never removed it. The Python half (the pytest mkdtemp scan of tests/**/*.py)
// is read below as text, with no Python run; it goes when the .py files go.

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

// ---- the Python half (tests/**/*.py), read as text: no Python runs --------------------------------------------------------

// pyCode returns the Python source with comments and the contents of string literals blanked, so a name in a comment or in
// a string never counts. Quotes of the three triple and two single kinds, prefixes and backslash escapes are handled; the
// expressions inside an f-string are treated as string text (a named stricter reading is not needed: nothing is hidden
// that a call on its own line would not show).
func pyCode(src string) string {
	var out strings.Builder
	rs := []rune(src)
	for i := 0; i < len(rs); {
		c := rs[i]
		switch {
		case c == '#':
			for i < len(rs) && rs[i] != '\n' {
				i++
			}
		case c == '\'' || c == '"':
			quote := c
			triple := i+2 < len(rs) && rs[i+1] == quote && rs[i+2] == quote
			if triple {
				i += 3
			} else {
				i++
			}
			out.WriteRune(quote)
			for i < len(rs) {
				if rs[i] == '\\' && i+1 < len(rs) {
					i += 2
					continue
				}
				if triple {
					if rs[i] == quote && i+2 < len(rs) && rs[i+1] == quote && rs[i+2] == quote {
						i += 3
						break
					}
				} else if rs[i] == quote || rs[i] == '\n' {
					i++
					break
				}
				if rs[i] == '\n' {
					out.WriteRune('\n')
				}
				i++
			}
			out.WriteRune(quote)
		default:
			out.WriteRune(c)
			i++
		}
	}
	return out.String()
}

var (
	pyFromTempfile = regexp.MustCompile(`(?m)^[ \t]*from[ \t]+tempfile[ \t]+import[ \t]+(.+)$`)
	pyImportAlias  = regexp.MustCompile(`(mkdtemp|gettempdir)(?:[ \t]+as[ \t]+(\w+))?`)
	pyRemoves      = regexp.MustCompile(`(?:^|[^\w])(?:rmtree|addfinalizer)[ \t]*\(|(?:^|[^\w])atexit[ \t]*\.[ \t]*register[ \t]*\(`)
)

// pyLogicalLines joins physical lines inside open brackets or after a backslash, so an expression is one unit.
func pyLogicalLines(code string) []string {
	var lines []string
	var cur strings.Builder
	depth := 0
	for _, l := range strings.Split(code, "\n") {
		cur.WriteString(l)
		for _, r := range l {
			switch r {
			case '(', '[', '{':
				depth++
			case ')', ']', '}':
				depth--
			}
		}
		if depth > 0 || strings.HasSuffix(strings.TrimRight(l, " \t"), `\`) {
			cur.WriteString(" ")
			continue
		}
		depth = 0
		lines = append(lines, cur.String())
		cur.Reset()
	}
	if cur.Len() > 0 {
		lines = append(lines, cur.String())
	}
	return lines
}

// pyMakesTempDir: a mkdtemp call, or a gettempdir() joined into a path (a read of the name alone makes nothing).
func pyMakesTempDir(src string) bool {
	code := pyCode(src)
	names := map[string]string{"mkdtemp": "mkdtemp", "gettempdir": "gettempdir"}
	for _, m := range pyFromTempfile.FindAllStringSubmatch(code, -1) {
		for _, a := range pyImportAlias.FindAllStringSubmatch(m[1], -1) {
			if a[2] != "" {
				names[a[2]] = a[1]
			}
		}
	}
	callsOf := func(line, want string) bool {
		for local, orig := range names {
			if orig != want {
				continue
			}
			if regexp.MustCompile(`(?:^|[^\w])` + regexp.QuoteMeta(local) + `[ \t]*\(`).MatchString(line) {
				return true
			}
		}
		return false
	}
	for _, line := range pyLogicalLines(code) {
		if callsOf(line, "mkdtemp") {
			return true
		}
		if callsOf(line, "gettempdir") && (strings.Contains(line, "/") || regexp.MustCompile(`(?:^|[^\w])join[ \t]*\(`).MatchString(line)) {
			return true
		}
	}
	return false
}

// pyRemoves: a real call of rmtree, addfinalizer or atexit.register; a comment or a string that names one is not.
func pyHasCleanup(src string) bool { return pyRemoves.MatchString(pyCode(src)) }

func pythonTempDirOffenders(root string) ([]string, int, error) {
	var out []string
	scanned := 0
	err := filepath.WalkDir(filepath.Join(root, "tests"), func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(d.Name(), ".py") {
			return nil
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		scanned++
		if pyMakesTempDir(string(raw)) && !pyHasCleanup(string(raw)) {
			rel, _ := filepath.Rel(root, path)
			out = append(out, filepath.ToSlash(rel))
		}
		return nil
	})
	sort.Strings(out)
	return out, scanned, err
}

func TestPythonTestsRemoveTheTempDirsTheyMake(t *testing.T) {
	offenders, scanned, err := pythonTempDirOffenders(rootOf(t))
	if err != nil {
		t.Fatal(err)
	}
	if scanned == 0 {
		t.Fatal("scanned no tests/**/*.py file: a scan that matched no file would pass for any tree")
	}
	if len(offenders) > 0 {
		t.Fatalf("these python tests make a temp directory and never remove it: %v", offenders)
	}
}

func TestPythonTempDirCheckFlagsALeakAndIsNotFooledByTextOrAnAlias(t *testing.T) {
	const leakA = "import tempfile\nd = tempfile.mkdtemp(prefix='x-')\n"
	const leakB = "import tempfile\nfrom pathlib import Path\nC = Path(tempfile.gettempdir()) / 'fixed'\n"
	for _, c := range []struct {
		name string
		body string
		want bool
	}{
		{"a mkdtemp leak", leakA, true},
		{"a joined gettempdir leak", leakB, true},
		{"a mkdtemp leak with an atexit cleanup", leakA + "import atexit, shutil\natexit.register(shutil.rmtree, 'x')\n", false},
		{"a gettempdir leak with an atexit cleanup", leakB + "import atexit, shutil\natexit.register(shutil.rmtree, 'x')\n", false},
		{"a comment is not a cleanup", "import tempfile\nd = tempfile.mkdtemp()\n# shutil.rmtree(d) later\n", true},
		{"a string is not a cleanup", "import tempfile\nd = tempfile.mkdtemp()\nNOTE = 'rmtree atexit.register'\n", true},
		{"a triple quoted string is not a cleanup", "import tempfile\nd = tempfile.mkdtemp()\n\"\"\"\nshutil.rmtree(d)\n\"\"\"\n", true},
		{"an aliased mkdtemp", "from tempfile import mkdtemp as make\nd = make()\n", true},
		{"a direct import of mkdtemp", "from tempfile import mkdtemp\nd = mkdtemp()\n", true},
		{"an aliased gettempdir joined with a slash", "from tempfile import gettempdir as g\nfrom pathlib import Path\nC = Path(g()) / 'fixed'\n", true},
		{"gettempdir joined with os.path.join", "import os, tempfile\nC = os.path.join(tempfile.gettempdir(), 'fixed')\n", true},
		{"a real rmtree cleanup", "import shutil, tempfile\nd = tempfile.mkdtemp()\nshutil.rmtree(d)\n", false},
		{"a read of the temp dir alone makes nothing", "import tempfile\nprint(tempfile.gettempdir())\n", false},
		{"a mkdtemp named only in a comment makes nothing", "# tempfile.mkdtemp()\nx = 1\n", false},
		{"a mkdtemp named only in a string makes nothing", "x = 'tempfile.mkdtemp()'\n", false},
	} {
		got := pyMakesTempDir(c.body) && !pyHasCleanup(c.body)
		if got != c.want {
			t.Errorf("%s: flagged=%v, want %v", c.name, got, c.want)
		}
	}
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "tests", "tooling"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "tests", "tooling", "test_leak.py"), []byte(leakA), 0o644); err != nil {
		t.Fatal(err)
	}
	got, _, err := pythonTempDirOffenders(dir)
	if err != nil || len(got) != 1 || got[0] != "tests/tooling/test_leak.py" {
		t.Fatalf("a leaking .py under tests/ is not reported: %v %v", got, err)
	}
}
