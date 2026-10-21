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
			fstring := isFStringStart(rs, i)
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
					if fstring && rs[i+1] == '{' {
						i++ // `\{` in an f-string: python keeps the backslash and the brace still opens an expression, raw or not
						continue
					}
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
				if fstring && rs[i] == '{' {
					if i+1 < len(rs) && rs[i+1] == '{' {
						i += 2
						continue
					}
					// the expression of an f-string is code: it is kept (a call in it counts), the literal text around it is not
					code, next := fstringField(rs, i+1)
					out.WriteString(code)
					i = next
					continue
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
	pyFromTempfile = regexp.MustCompile(`^[ \t]*from[ \t]+tempfile[ \t]+import[ \t]+(.+)$`)
	pyImportAlias  = regexp.MustCompile(`(mkdtemp|gettempdir)(?:[ \t]+as[ \t]+([\p{L}\p{N}_]+))?`)
	pyRemoves      = regexp.MustCompile(`(?:^|[^\p{L}\p{N}_])(?:rmtree|addfinalizer)[ \t]*\(|(?:^|[^\p{L}\p{N}_])atexit[ \t]*\.[ \t]*register[ \t]*\(`)
)

// pyLogicalLines joins physical lines inside open brackets or after a backslash, so an expression is one unit.
func pyLogicalLines(code string) []string {
	var lines []string
	var cur strings.Builder
	depth := 0
	for _, l := range strings.Split(code, "\n") {
		trimmed := strings.TrimRight(l, " \t")
		continued := strings.HasSuffix(trimmed, `\`)
		if continued {
			l = strings.TrimSuffix(trimmed, `\`)
		}
		cur.WriteString(l)
		for _, r := range l {
			switch r {
			case '(', '[', '{':
				depth++
			case ')', ']', '}':
				depth--
			}
		}
		if depth > 0 || continued {
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

// pyStatements: the logical lines, each cut at a top-level `;` (a `;` inside brackets belongs to the expression) and each
// compound header (`if x:`, `try:`, `except E:`, `else:`, `def f():`, ...) cut from the statement that follows its `:` on the same line.
func pyStatements(code string) []string {
	var out []string
	for _, line := range pyLogicalLines(code) {
		depth, start := 0, 0
		for i, r := range line {
			switch r {
			case '(', '[', '{':
				depth++
			case ')', ']', '}':
				depth--
			case ';':
				if depth == 0 {
					out = append(out, pySplitHeader(line[start:i])...)
					start = i + 1
				}
			}
		}
		out = append(out, pySplitHeader(line[start:])...)
	}
	return out
}

var pyCompoundKeyword = regexp.MustCompile(`^[ \t]*(?:async[ \t]+)?(?:if|elif|else|try|except|finally|for|while|with|def|class|match|case)(?:[^\p{L}\p{N}_]|$)`)

func pySplitHeader(stmt string) []string {
	if !pyCompoundKeyword.MatchString(stmt) {
		return []string{stmt}
	}
	depth := 0
	rs := []rune(stmt)
	for i, r := range rs {
		switch r {
		case '(', '[', '{':
			depth++
		case ')', ']', '}':
			depth--
		case ':':
			if depth == 0 && !(i+1 < len(rs) && rs[i+1] == '=') {
				rest := string(rs[i+1:])
				if strings.TrimSpace(rest) == "" {
					return []string{stmt}
				}
				return append([]string{string(rs[:i+1])}, pySplitHeader(rest)...)
			}
		}
	}
	return []string{stmt}
}

// pyMakesTempDir: a mkdtemp call, or a gettempdir() joined into a path (a read of the name alone makes nothing).
var pyJoinCall = regexp.MustCompile(`(?:^|[^\p{L}\p{N}_])join[ \t]*\(`)

func pyMakesTempDir(src string) bool {
	code := pyCode(src)
	names := map[string]string{"mkdtemp": "mkdtemp", "gettempdir": "gettempdir"}
	statements := pyStatements(code)
	for _, line := range statements {
		m := pyFromTempfile.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		imported := strings.NewReplacer("(", " ", ")", " ").Replace(m[1])
		for _, a := range pyImportAlias.FindAllStringSubmatch(imported, -1) {
			if a[2] != "" {
				names[a[2]] = a[1]
			}
		}
	}
	// one pattern per original name over all the local names bound to it, built once per file
	callPattern := func(want string) *regexp.Regexp {
		var locals []string
		for local, orig := range names {
			if orig == want {
				locals = append(locals, regexp.QuoteMeta(local))
			}
		}
		return regexp.MustCompile(`(?:^|[^\p{L}\p{N}_])(?:` + strings.Join(locals, "|") + `)[ \t]*\(`)
	}
	mkdtemp, gettempdir := callPattern("mkdtemp"), callPattern("gettempdir")
	for _, line := range statements {
		if !strings.Contains(line, "(") {
			continue
		}
		if mkdtemp.MatchString(line) {
			return true
		}
		if gettempdir.MatchString(line) && (strings.Contains(line, "/") || pyJoinCall.MatchString(line)) {
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
		{"an f-string expression that calls mkdtemp", "import tempfile\nx = f\"{tempfile.mkdtemp()}\"\n", true},
		{"a nested f-string expression that calls mkdtemp", "import tempfile\nx = rf'a{{b}}{tempfile.mkdtemp(prefix=\"x\")}'\n", true},
		{"f-string text naming mkdtemp outside its braces makes nothing", "x = f'tempfile.mkdtemp() {1}'\n", false},
		{"a parenthesised multi-line alias import", "from tempfile import (\n    mkdtemp as make,\n)\nx = make()\n", true},
		{"a backslash continued alias import", "from tempfile import \\\n    mkdtemp as make\nx = make()\n", true},
		{"an alias import after a semicolon", "import os; from tempfile import mkdtemp as make\nx = make()\n", true},
		{"a non-ASCII alias", "from tempfile import mkdtemp as café\nx = café()\n", true},
		{"a match statement with a case import", "match x:\n    case 1: from tempfile import mkdtemp as m\nm()\n", true},
		{"a case header cut from its body", "case 1: from tempfile import mkdtemp as m\nm()\n", true},
		{"a quote in an f-string format spec opens no string", "import tempfile\nx = f\"{1:'>10}\"\ny = '}'; d = tempfile.mkdtemp()\n", true},
		{"a format spec with a quote alone makes nothing", "x = f\"{1:'>10}\"\n", false},
		{"a conversion then a spec with a quote", "import tempfile\nx = f\"{y!r:'>{w}}\"\nd = tempfile.mkdtemp()\n", true},
		{"a nested field in a format spec is code", "import tempfile\nx = f\"{y:{tempfile.mkdtemp()}}\"\n", true},
		{"a not-equal sign is not a conversion", "import tempfile\nx = f\"{a != tempfile.mkdtemp()}\"\n", true},
		{"a dict colon inside braces does not start a spec", "import tempfile\nx = f\"{ {1: tempfile.mkdtemp()}[1] }\"\n", true},
		{"an alias import after an if header on the same line", "if True: from tempfile import mkdtemp as m\nx = m()\n", true},
		{"an alias import in a try body on the same line", "try: from tempfile import mkdtemp as m\nexcept ImportError: pass\nx = m()\n", true},
		{"an alias import and its call in a one-line def", "def f(): from tempfile import mkdtemp as m; return m()\n", true},
		{"an else header on one line", "if a: pass\nelse: from tempfile import mkdtemp as m\nx = m()\n", true},
		{"a raw f-string with a backslash before its brace", "import tempfile\nx = rf\"\\{tempfile.mkdtemp()}\"\n", true},
		{"an f-string expression with a closing brace inside a quoted string", "import tempfile\nx = f\"{'}' + tempfile.mkdtemp()}\"\n", true},
		{"a quoted string in an f-string expression names mkdtemp but calls nothing", "x = f\"{'tempfile.mkdtemp()'}\"\n", false},
		{"an annotation colon in brackets is no header", "x: int = 1\n", false},
		{"a call glued to a non-ASCII word is not the alias", "from tempfile import mkdtemp as make\nx = émake()\n", false},
		{"a semicolon inside brackets does not split a join", "import tempfile\nC = (1; 2) / tempfile.gettempdir()\n", true},
		{"a cleanup after a semicolon counts", "import shutil, tempfile\nd = tempfile.mkdtemp(); shutil.rmtree(d)\n", false},
		{"a multi-line gettempdir join", "import tempfile\nfrom pathlib import Path\nC = (\n    Path(tempfile.gettempdir())\n    / 'fixed'\n)\n", true},
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

// isFStringStart reports whether the quote at rs[i] opens an f-string: its prefix is made of letters from rbufRBUF and holds f or F.
func isFStringStart(rs []rune, i int) bool {
	hasF := false
	for j := i - 1; j >= 0 && i-j <= 3; j-- {
		switch rs[j] {
		case 'f', 'F':
			hasF = true
		case 'r', 'R', 'b', 'B', 'u', 'U':
		default:
			return hasF && !isPyIdentRune(rs[j])
		}
	}
	return hasF
}

func isPyIdentRune(r rune) bool {
	return r == '_' || (r >= '0' && r <= '9') || (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z') || r > 127
}

// fstringField reads one replacement field of an f-string from rs[start] (just after its `{`) and returns the code it holds and
// the index after its closing `}`. Only the expression part is code: a quote there opens a string that is skipped (braces in it
// do not count), a `:` or `!` outside brackets and nested braces starts the format spec, whose text is literal (a quote in it
// opens nothing), and a nested replacement field in the spec is code again.
func fstringField(rs []rune, start int) (string, int) {
	var code strings.Builder
	paren, brace, exprStart, j := 0, 1, start, start
	inSpec := false
	flush := func(end int) { code.WriteString(" " + pyCode(string(rs[exprStart:end])) + " ") }
	for j < len(rs) {
		c := rs[j]
		if inSpec {
			switch c {
			case '{':
				sub, next := fstringField(rs, j+1)
				code.WriteString(sub)
				j = next
				continue
			case '}':
				return code.String(), j + 1
			}
			j++
			continue
		}
		switch c {
		case '\'', '"':
			j++
			for j < len(rs) && rs[j] != c {
				if rs[j] == '\\' && j+1 < len(rs) {
					j++
				}
				j++
			}
		case '(', '[':
			paren++
		case ')', ']':
			paren--
		case '{':
			brace++
		case '}':
			brace--
			if brace == 0 {
				flush(j)
				return code.String(), j + 1
			}
		case ':':
			if paren == 0 && brace == 1 {
				flush(j)
				inSpec = true
			}
		case '!':
			if paren == 0 && brace == 1 && !(j+1 < len(rs) && rs[j+1] == '=') {
				flush(j)
				inSpec = true
			}
		}
		j++
	}
	if !inSpec {
		flush(len(rs))
	}
	return code.String(), j
}
