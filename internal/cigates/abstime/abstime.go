// Package abstime refuses a test that compares a measured duration with a constant (D6418): a loaded runner stretches
// wall time, so an absolute budget ejects a merge_group entry that did nothing wrong. A test counts operations, or
// compares two durations measured in the same process.
package abstime

import (
	"bufio"
	"bytes"
	"fmt"
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"io"
	"io/fs"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"unicode"
)

const (
	allowlistName = "internal/cigates/abstime/allowlist.tsv"
	markerPrefix  = "//abstime:allow"
	testSuffix    = "_test.go"
	packageScope  = "<package>"
)

// Row is one allowlist line: every absolute time assertion in File's top-level function Func is allowed, for Reason.
type Row struct {
	File   string
	Func   string
	Reason string
	Line   int
}

func (r Row) key() string { return r.File + "\x00" + r.Func }

// ParseAllowlist reads allowlist.tsv: `file<TAB>func<TAB>reason`, '#' comment lines and blank lines skipped. Any
// malformed row is an error that names its line; the error lists every bad row.
func ParseAllowlist(r io.Reader) ([]Row, error) {
	var rows []Row
	var bad []string
	seen := map[string]int{}
	scanner := bufio.NewScanner(r)
	for n := 1; scanner.Scan(); n++ {
		text := scanner.Text()
		if t := strings.TrimSpace(text); t == "" || strings.HasPrefix(t, "#") {
			continue
		}
		fail := func(format string, args ...any) {
			bad = append(bad, fmt.Sprintf("allowlist.tsv:%d: %s", n, fmt.Sprintf(format, args...)))
		}
		fields := strings.Split(text, "\t")
		if len(fields) != 3 {
			fail("want 3 tab-separated fields (file, func, reason), got %d", len(fields))
			continue
		}
		row := Row{File: strings.TrimSpace(fields[0]), Func: strings.TrimSpace(fields[1]), Reason: strings.TrimSpace(fields[2]), Line: n}
		switch {
		case row.Reason == "":
			fail("empty reason for %s %s", row.File, row.Func)
		case row.Func == "" || strings.ContainsAny(row.Func, " \t"):
			fail("bad func name %q", row.Func)
		case !cleanTestPath(row.File):
			fail("file %q must be a repository-relative, slash-separated path of a %s file", row.File, testSuffix)
		case seen[row.key()] != 0:
			fail("duplicate row for %s %s (first at line %d)", row.File, row.Func, seen[row.key()])
		default:
			seen[row.key()] = n
			rows = append(rows, row)
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	if len(bad) > 0 {
		return nil, fmt.Errorf("%s", strings.Join(bad, "\n"))
	}
	return rows, nil
}

func cleanTestPath(p string) bool {
	if p == "" || strings.HasPrefix(p, "/") || strings.Contains(p, "\\") || !strings.HasSuffix(p, testSuffix) {
		return false
	}
	for _, elem := range strings.Split(p, "/") {
		if elem == "" || elem == "." || elem == ".." {
			return false
		}
	}
	return true
}

type finding struct {
	file string
	line int
	fn   string
	expr string
}

// Check scans every *_test.go below root (hidden, testdata, vendor and node_modules directories skipped) and returns the
// absolute time assertions neither marked with //abstime:allow <reason> nor listed in rows, the markers without a
// reason, and the rows that no longer match an assertion. A file that does not parse is an error: a gate that skipped
// it would scan nothing.
func Check(root string, rows []Row) ([]string, error) {
	byKey := map[string]*Row{}
	for i := range rows {
		byKey[rows[i].key()] = &rows[i]
	}
	used := map[string]bool{}
	var violations []string
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		name := d.Name()
		if d.IsDir() {
			if path != root && (strings.HasPrefix(name, ".") || name == "testdata" || name == "vendor" || name == "node_modules") {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(name, testSuffix) {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, path, nil, parser.ParseComments|parser.SkipObjectResolution)
		if err != nil {
			return fmt.Errorf("abstime: cannot parse %s: %w", rel, err)
		}
		markers, markerViolations := collectMarkers(fset, file, rel)
		violations = append(violations, markerViolations...)
		for _, f := range findAssertions(fset, file, rel) {
			if markers[f.line] || markers[f.line-1] {
				continue
			}
			key := Row{File: f.file, Func: f.fn}.key()
			if byKey[key] != nil {
				used[key] = true
				continue
			}
			violations = append(violations, fmt.Sprintf("%s:%d: %s: absolute time assertion `%s`: use an operation count or a ratio against a baseline measured in the same process (D6418; a duration a child process reports as KEY=<number> is measured too); a hang ceiling takes //abstime:allow <reason> on or above the line", f.file, f.line, f.fn, f.expr))
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	for _, row := range rows {
		if !used[row.key()] {
			violations = append(violations, fmt.Sprintf("%s:%d: stale allowlist row %s %s: no absolute time assertion there any more; delete the row", allowlistName, row.Line, row.File, row.Func))
		}
	}
	sort.Strings(violations)
	return violations, nil
}

// collectMarkers returns the lines that carry a //abstime:allow marker with a reason, and a violation for each marker
// without one.
func collectMarkers(fset *token.FileSet, file *ast.File, rel string) (map[int]bool, []string) {
	lines := map[int]bool{}
	var violations []string
	for _, group := range file.Comments {
		for _, c := range group.List {
			rest, ok := strings.CutPrefix(c.Text, markerPrefix)
			if !ok || (rest != "" && rest[0] != ' ' && rest[0] != '\t') {
				continue
			}
			line := fset.Position(c.Slash).Line
			if strings.TrimSpace(rest) == "" {
				violations = append(violations, fmt.Sprintf("%s:%d: %s needs a reason: say why this is a hang ceiling and not a budget", rel, line, markerPrefix))
				continue
			}
			lines[line] = true
		}
	}
	return lines, violations
}

// measuredNames hold a duration measured by a helper the scan cannot see through.
var measuredNames = map[string]bool{"elapsed": true, "took": true, "waited": true, "spent": true}

var comparisons = map[token.Token]bool{token.GTR: true, token.GEQ: true, token.LSS: true, token.LEQ: true}

// findAssertions returns every comparison in file that has a measured duration on exactly one side. A duration is
// measured when it holds time.Since, time.Until, a Getrusage call or an Elapsed field (never a Sub call: that is mostly timestamp arithmetic on fake clocks and dates), or a variable
// assigned from one in the same top-level declaration. A comparison of two measured durations is a ratio, not an
// absolute budget. A comparison with the literal 0 is a sign check.
//
// A duration a child process measures and reports as KEY=<number> is measured too (CHAOS-11757). A key is emitted
// when a measured value follows `"KEY="` in a + chain or fills the `KEY=%d` verb of a Printf-family format, or when
// a script literal holds KEY=$((SECONDS ...)), KEY=$SECONDS, KEY=$EPOCHREALTIME or KEY=$(date +%s). It is carried
// when an Sscanf format reads `KEY=%d` into &v (v is measured), or when a call of a local function (not pkg.Func)
// has the key as a string literal argument (its results are measured).
// Stated limits, each pinned by a test:
//   - A ratio of two carried values is legal, like a ratio of two time.Since values.
//   - A key that no measured value fills (PORT=, CODE=) is not an emitter; its readers are not measured.
//   - strings.Index/Contains/Cut on a key text carry nothing.
//   - The emitter and the reader must be in the same file; a key emitted in another file is not seen.
//   - Only literal KEY= channels are seen: JSON, files, exit codes and computed keys are not.
func findAssertions(fset *token.FileSet, file *ast.File, rel string) []finding {
	var out []finding
	keys := carriedKeys(file)
	for _, decl := range file.Decls {
		name := packageScope
		if fd, ok := decl.(*ast.FuncDecl); ok {
			name = fd.Name.Name
		}
		tracked := trackDecl(decl, keys)
		isMeasured := func(e ast.Node) bool { return measured(e, tracked, keys) }
		ast.Inspect(decl, func(n ast.Node) bool {
			be, ok := n.(*ast.BinaryExpr)
			if !ok || !comparisons[be.Op] {
				return true
			}
			left, right := isMeasured(be.X), isMeasured(be.Y)
			if left == right || signCheck(be.X, left) || signCheck(be.Y, right) {
				return true
			}
			var buf bytes.Buffer
			if err := printer.Fprint(&buf, fset, be); err != nil {
				buf.Reset()
				buf.WriteString("?")
			}
			expr := strings.Join(strings.Fields(buf.String()), " ")
			if len(expr) > 90 {
				expr = expr[:90] + "..."
			}
			out = append(out, finding{file: rel, line: fset.Position(be.Pos()).Line, fn: name, expr: expr})
			return true
		})
	}
	return out
}

// trackDecl returns the names in decl that hold a measured duration, found by three passes over the assignments.
func trackDecl(decl ast.Decl, keys map[string]bool) map[string]bool {
	tracked := map[string]bool{}
	isMeasured := func(e ast.Node) bool { return measured(e, tracked, keys) }
	for range 3 {
		ast.Inspect(decl, func(n ast.Node) bool {
			switch s := n.(type) {
			case *ast.AssignStmt:
				track(tracked, s.Lhs, s.Rhs, isMeasured)
			case *ast.ValueSpec:
				lhs := make([]ast.Expr, len(s.Names))
				for i, id := range s.Names {
					lhs[i] = id
				}
				track(tracked, lhs, s.Values, isMeasured)
			case *ast.RangeStmt:
				if isMeasured(s.X) {
					for _, e := range []ast.Expr{s.Key, s.Value} {
						if id := baseIdent(e); id != "" {
							tracked[id] = true
						}
					}
				}
			case *ast.CallExpr:
				trackScanned(s, keys, tracked)
			}
			return true
		})
	}
	return tracked
}

var (
	keyAtEnd     = regexp.MustCompile(`([A-Z][A-Z0-9_]*)=$`)
	scriptKeyRes = []*regexp.Regexp{
		regexp.MustCompile(`\b([A-Z][A-Z0-9_]*)=\$\(\([^)]*\bSECONDS\b`),
		regexp.MustCompile(`\b([A-Z][A-Z0-9_]*)=\$\{?(?:SECONDS|EPOCHREALTIME)\b`),
		regexp.MustCompile(`\b([A-Z][A-Z0-9_]*)=\$\(date \+%s`),
	}
)

// formatArg is the index of the format string in the Printf and Scanf families, by function name.
var formatArg = map[string]int{"Printf": 0, "Sprintf": 0, "Errorf": 0, "Fprintf": 1, "Sscanf": 1, "Fscanf": 1}

func callName(c *ast.CallExpr) string {
	switch f := c.Fun.(type) {
	case *ast.SelectorExpr:
		return f.Sel.Name
	case *ast.Ident:
		return f.Name
	}
	return ""
}

func stringLit(e ast.Expr) (string, bool) {
	lit, ok := e.(*ast.BasicLit)
	if !ok || lit.Kind != token.STRING {
		return "", false
	}
	v, err := strconv.Unquote(lit.Value)
	return v, err == nil
}

// verbPrefixes returns, for each verb of format, the text before it.
func verbPrefixes(format string) []string {
	var out []string
	for i := 0; i < len(format); i++ {
		if format[i] != '%' {
			continue
		}
		if i+1 < len(format) && format[i+1] == '%' {
			i++
			continue
		}
		out = append(out, format[:i])
		j := i + 1
		for j < len(format) && !unicode.IsLetter(rune(format[j])) {
			j++
		}
		i = j
	}
	return out
}

// formatCall returns the format text of a Printf/Scanf-family call and the index of its first verb argument.
func formatCall(c *ast.CallExpr) (string, int, bool) {
	at, ok := formatArg[callName(c)]
	if !ok || at >= len(c.Args) {
		return "", 0, false
	}
	format, ok := stringLit(c.Args[at])
	return format, at + 1, ok
}

func trackScanned(c *ast.CallExpr, keys map[string]bool, tracked map[string]bool) {
	if len(keys) == 0 {
		return
	}
	if name := callName(c); name != "Sscanf" && name != "Fscanf" {
		return
	}
	format, first, ok := formatCall(c)
	if !ok {
		return
	}
	for i, prefix := range verbPrefixes(format) {
		m := keyAtEnd.FindStringSubmatch(prefix)
		if m == nil || !keys[m[1]] || first+i >= len(c.Args) {
			continue
		}
		if u, ok := c.Args[first+i].(*ast.UnaryExpr); ok && u.Op == token.AND {
			if id := baseIdent(u.X); id != "" {
				tracked[id] = true
			}
		}
	}
}

// carriedKeys returns the KEY names a measured value is reported under in file.
func carriedKeys(file *ast.File) map[string]bool {
	keys := map[string]bool{}
	for _, decl := range file.Decls {
		tracked := trackDecl(decl, nil)
		isMeasured := func(e ast.Node) bool { return measured(e, tracked, nil) }
		ast.Inspect(decl, func(n ast.Node) bool {
			switch x := n.(type) {
			case *ast.BinaryExpr:
				if x.Op != token.ADD {
					return true
				}
				ops := plusOperands(x)
				for i := 0; i+1 < len(ops); i++ {
					if text, ok := stringLit(ops[i]); ok && isMeasured(ops[i+1]) {
						if m := keyAtEnd.FindStringSubmatch(text); m != nil {
							keys[m[1]] = true
						}
					}
				}
			case *ast.CallExpr:
				if callName(x) == "Sscanf" || callName(x) == "Fscanf" {
					return true
				}
				format, first, ok := formatCall(x)
				if !ok {
					return true
				}
				for i, prefix := range verbPrefixes(format) {
					if m := keyAtEnd.FindStringSubmatch(prefix); m != nil && first+i < len(x.Args) && isMeasured(x.Args[first+i]) {
						keys[m[1]] = true
					}
				}
			case *ast.BasicLit:
				if text, ok := stringLit(x); ok {
					for _, re := range scriptKeyRes {
						for _, m := range re.FindAllStringSubmatch(text, -1) {
							keys[m[1]] = true
						}
					}
				}
			}
			return true
		})
	}
	return keys
}

func plusOperands(e ast.Expr) []ast.Expr {
	if be, ok := e.(*ast.BinaryExpr); ok && be.Op == token.ADD {
		return append(plusOperands(be.X), plusOperands(be.Y)...)
	}
	return []ast.Expr{e}
}

func signCheck(e ast.Expr, measured bool) bool {
	lit, ok := e.(*ast.BasicLit)
	return !measured && ok && lit.Kind == token.INT && lit.Value == "0"
}

func track(tracked map[string]bool, lhs, rhs []ast.Expr, isMeasured func(ast.Node) bool) {
	for i, l := range lhs {
		var r ast.Expr
		switch {
		case len(rhs) == len(lhs):
			r = rhs[i]
		case len(rhs) == 1:
			r = rhs[0]
		default:
			continue
		}
		if isMeasured(r) {
			if id := baseIdent(l); id != "" {
				tracked[id] = true
			}
		}
	}
}

func baseIdent(e ast.Expr) string {
	switch x := e.(type) {
	case *ast.Ident:
		if x.Name == "_" {
			return ""
		}
		return x.Name
	case *ast.IndexExpr:
		return baseIdent(x.X)
	}
	return ""
}

func measured(n ast.Node, tracked map[string]bool, keys map[string]bool) bool {
	if n == nil {
		return false
	}
	found := false
	ast.Inspect(n, func(c ast.Node) bool {
		if found {
			return false
		}
		switch x := c.(type) {
		case *ast.Ident:
			found = tracked[x.Name] || measuredNames[x.Name]
		case *ast.SelectorExpr:
			if x.Sel.Name == "Elapsed" {
				found = true
			}
		case *ast.CallExpr:
			switch fun := x.Fun.(type) {
			case *ast.SelectorExpr:
				switch fun.Sel.Name {
				case "Since", "Until", "Getrusage":
					found = true
				}
			case *ast.Ident:
				for _, arg := range x.Args {
					if text, ok := stringLit(arg); ok && keys[strings.TrimSuffix(text, "=")] {
						found = true
					}
				}
			}
		}
		return !found
	})
	return found
}
