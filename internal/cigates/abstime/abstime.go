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
// when a measured value follows `"KEY="` in a + chain, fills the `KEY=%d` or `KEY=%[n]d` verb of a Printf-family
// format, or follows a `"KEY="` argument of a Print, Println, Fprint, Fprintln, Sprint or Sprintln call; or when a script
// literal assigns a value that CONTAINS $((... SECONDS ...)), $SECONDS, $EPOCHREALTIME or $(date +%s), or a variable
// already measured (followed to a fixpoint, so two hops and date-inside-arithmetic count), to an upper-case KEY, or when
// a printf line's KEY=%s verb takes such a value. It is carried when an Sscanf
// format reads `KEY=%d` into &v (v is measured) from a source that is not a fixed literal, or when a call of a function
// declared in the same file with the key as a string literal argument and a non-literal source argument returns it
// (the function must scan or parse: Sscan*, strconv.Parse*/Atoi, time.ParseDuration, json Unmarshal/Decode, or it calls
// such a function of the file: one level of call depth; a helper that only logs the key reads nothing).
// A quotient of two measured values is a ratio and is not measured, whether compared directly or through a variable.
// A comparison of two measured values is still refused when a side carries a fixed duration floor: a time.<Unit> constant
// that is a direct operand of + or -, or an argument of max, min, math.Max or math.Min, at any depth (max(25*b,
// time.Second), 2*b+50*time.Millisecond). A product (2*b) is no floor.
// Stated limits, each pinned by a test:
//   - A ratio of two carried values is legal, like a ratio of two time.Since values
//     (TestRatioOfTwoCarriedValuesIsNotAnAbsoluteBudget).
//   - A floor held in a named constant or variable, or written as a bare integer, is not seen
//     (TestAFloorHeldInANamedValueIsOutOfScope).
//   - A key that no measured value fills (PORT=, CODE=) is not an emitter; its readers are not measured
//     (TestAKeyThatCarriesNoMeasurementIsNotTracked).
//   - A read of a fixed literal, or of an identifier that only holds literals, is not a child's report, so a diagnostic
//     print of the same key elsewhere does not taint it (TestFixedLiteralInputIsNotAChildReport,
//     TestAReassignedIdentifierIsNotALiteralSource).
//   - strings.Index, Contains, Cut and Count on a key text carry nothing (TestStringsIndexOfAKeyIsNotACarriedValue,
//     TestStringsContainsAndCutOfAKeyCarryNothing).
//   - The emitter and the reader must be in the same file; a key emitted in another file is not seen, and a reader
//     helper declared in another file is not seen (TestEmitterInAnotherFileIsOutOfScope,
//     TestAHelperDeclaredInAnotherFileIsNotAReader).
//   - Reader helpers are followed one call level deep (TestTwoLevelsOfHelperCallsAreOutOfScope).
//   - Script keys are read inside one literal: no flow between literals, no eval (TestScriptKeysAreReadInsideOneLiteral).
//   - Only literal upper-case KEY= channels are seen: JSON, files, exit codes, computed keys and lower-case keys are not
//     (TestDurationCarriedAsJSONIsOutOfScope, TestAFileChannelIsOutOfScope, TestAComputedKeyIsOutOfScope,
//     TestALowercaseKeyIsOutOfScope).
func findAssertions(fset *token.FileSet, file *ast.File, rel string) []finding {
	var out []finding
	c := carriedKeys(file)
	for _, decl := range file.Decls {
		name := packageScope
		if fd, ok := decl.(*ast.FuncDecl); ok {
			name = fd.Name.Name
		}
		cd := c.scoped(decl)
		tracked := trackDecl(decl, c)
		isMeasured := func(e ast.Node) bool { return measured(e, tracked, cd) }
		ast.Inspect(decl, func(n ast.Node) bool {
			be, ok := n.(*ast.BinaryExpr)
			if !ok || !comparisons[be.Op] {
				return true
			}
			left, right := isMeasured(be.X), isMeasured(be.Y)
			if left == right && !(left && (carriesFloor(be.X, isMeasured) || carriesFloor(be.Y, isMeasured))) {
				return true
			}
			if signCheck(be.X, left) || signCheck(be.Y, right) {
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
func trackDecl(decl ast.Decl, c *carried) map[string]bool {
	c = c.scoped(decl)
	tracked := map[string]bool{}
	isMeasured := func(e ast.Node) bool { return measured(e, tracked, c) }
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
				trackScanned(s, c, tracked)
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

var (
	shellAssignName   = regexp.MustCompile(`\b([A-Za-z_][A-Za-z0-9_]*)=`)
	shellKeyName      = regexp.MustCompile(`^[A-Z][A-Z0-9_]*$`)
	shellMeasuredWord = regexp.MustCompile(`\$\(\([^)]*\bSECONDS\b|\$\{?(?:SECONDS|EPOCHREALTIME)\b|\$\(date \+%s`)
)

// scriptKeys returns the KEY names a script literal reports a measured value under: KEY=$((SECONDS ...)) and the like;
// KEY=$var, where var was assigned such a value in the same literal; and a printf line whose KEY=%s verb takes such a
// word or variable. One literal, no eval, no flow between literals.
func scriptKeys(text string) []string {
	var out []string
	for _, re := range scriptKeyRes {
		for _, m := range re.FindAllStringSubmatch(text, -1) {
			out = append(out, m[1])
		}
	}
	// An assign is measured when its right-hand side CONTAINS a measured word or a variable already measured (bare in
	// arithmetic, $v elsewhere); followed to a fixpoint. An upper-case name assigned such a value is a key.
	var assigns [][]string
	for _, loc := range shellAssignName.FindAllStringSubmatchIndex(text, -1) {
		assigns = append(assigns, []string{"", text[loc[2]:loc[3]], assignValue(text, loc[1])})
	}
	vars := map[string]*regexp.Regexp{}
	bare := map[string]*regexp.Regexp{}
	refers := func(rhs string) bool {
		if shellMeasuredWord.MatchString(rhs) {
			return true
		}
		for v, re := range vars {
			if re.MatchString(rhs) || strings.Contains(rhs, "$((") && bare[v].MatchString(rhs) {
				return true
			}
		}
		return false
	}
	for changed := true; changed; {
		changed = false
		for _, m := range assigns {
			if _, done := vars[m[1]]; !done && refers(m[2]) {
				vars[m[1]] = regexp.MustCompile(`\$\{?` + regexp.QuoteMeta(m[1]) + `\b`)
				bare[m[1]] = regexp.MustCompile(`\b` + regexp.QuoteMeta(m[1]) + `\b`)
				changed = true
			}
		}
	}
	for _, m := range assigns {
		if shellKeyName.MatchString(m[1]) && refers(m[2]) {
			out = append(out, m[1])
		}
	}
	measuredWord := refers
	for _, line := range strings.Split(text, "\n") {
		i := strings.Index(line, "printf ")
		if i < 0 {
			continue
		}
		words := shellWords(line[i+len("printf "):])
		if len(words) == 0 {
			continue
		}
		args := words[1:]
		for _, v := range formatVerbs(strings.Trim(words[0], "'\"")) {
			if m := keyAtEnd.FindStringSubmatch(v.prefix); m != nil && v.arg < len(args) && measuredWord(args[v.arg]) {
				out = append(out, m[1])
			}
		}
	}
	return out
}

// assignValue returns the value of the assign whose right-hand side starts at i: it ends at a blank, a semicolon or a
// quote outside parentheses (so $(( ... )) and $( ... ) groups hold their blanks); leading quotes are skipped. Each NAME=
// is scanned on its own, so a later KEY= on the same line is not swallowed.
func assignValue(text string, i int) string {
	start := i
	for i < len(text) && (text[i] == '"' || text[i] == '\'') {
		i++
	}
	depth := 0
	for ; i < len(text); i++ {
		switch ch := text[i]; {
		case ch == '(':
			depth++
		case ch == ')':
			if depth == 0 {
				return text[start:i]
			}
			depth--
		case depth == 0 && (ch == ' ' || ch == '\t' || ch == '\n' || ch == ';' || ch == '"' || ch == '\''):
			return text[start:i]
		}
	}
	return text[start:]
}

// shellWords splits s on blanks outside quotes and outside a $( ... ) group.
func shellWords(s string) []string {
	var words []string
	var cur strings.Builder
	var quote byte
	depth := 0
	flush := func() {
		if cur.Len() > 0 {
			words = append(words, cur.String())
			cur.Reset()
		}
	}
	for i := 0; i < len(s); i++ {
		ch := s[i]
		switch {
		case quote != 0:
			cur.WriteByte(ch)
			if ch == quote && !(quote == '"' && i > 0 && s[i-1] == '\\') {
				quote = 0
			}
		case ch == '\'' || ch == '"':
			quote = ch
			cur.WriteByte(ch)
		case ch == '(':
			depth++
			cur.WriteByte(ch)
		case ch == ')' && depth > 0:
			depth--
			cur.WriteByte(ch)
		case (ch == ' ' || ch == '\t') && depth == 0:
			flush()
		default:
			cur.WriteByte(ch)
		}
	}
	flush()
	return words
}

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

// verb is one verb of a format: the text before it and the index of the argument it takes (an explicit %[n] index wins).
type verb struct {
	prefix string
	arg    int
}

func formatVerbs(format string) []verb {
	var out []verb
	next := 0
	for i := 0; i < len(format); i++ {
		if format[i] != '%' {
			continue
		}
		if i+1 < len(format) && format[i+1] == '%' {
			i++
			continue
		}
		j := i + 1
		for j < len(format) && !unicode.IsLetter(rune(format[j])) {
			if format[j] == '[' {
				if k := strings.IndexByte(format[j:], ']'); k > 0 {
					if n, err := strconv.Atoi(format[j+1 : j+k]); err == nil && n > 0 {
						next = n - 1
					}
					j += k
				}
			}
			j++
		}
		out = append(out, verb{prefix: format[:i], arg: next})
		next++
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

func trackScanned(call *ast.CallExpr, c *carried, tracked map[string]bool) {
	if c == nil || len(c.keys) == 0 {
		return
	}
	name := callName(call)
	if name != "Sscanf" && name != "Fscanf" {
		return
	}
	if name == "Sscanf" && len(call.Args) > 0 && c.isLiteral(call.Args[0]) {
		return
	}
	format, first, ok := formatCall(call)
	if !ok {
		return
	}
	for _, v := range formatVerbs(format) {
		m := keyAtEnd.FindStringSubmatch(v.prefix)
		if m == nil || !c.keys[m[1]] || first+v.arg >= len(call.Args) {
			continue
		}
		if u, ok := call.Args[first+v.arg].(*ast.UnaryExpr); ok && u.Op == token.AND {
			if id := baseIdent(u.X); id != "" {
				tracked[id] = true
			}
		}
	}
}

// carried is what a file's child-process reporting looks like: the emitted keys, and the functions declared in the file
// that scan or parse their input.
type carried struct {
	keys    map[string]bool
	readers map[string]bool
	lits    map[string]bool // identifiers of the declaration in scope that only ever hold a literal
}

// scoped returns c with the literal-only identifiers of decl; a read of a fixed literal is not a child's report.
func (c *carried) scoped(decl ast.Decl) *carried {
	if c == nil {
		return nil
	}
	cp := *c
	cp.lits = literalIdents(decl)
	return &cp
}

func (c *carried) isLiteral(e ast.Expr) bool {
	switch x := e.(type) {
	case *ast.BasicLit:
		return true
	case *ast.Ident:
		return c != nil && c.lits[x.Name]
	}
	return false
}

// literalIdents returns the identifiers that every assignment in decl gives a literal.
func literalIdents(decl ast.Decl) map[string]bool {
	lit, other := map[string]bool{}, map[string]bool{}
	note := func(lhs, rhs []ast.Expr) {
		for i, l := range lhs {
			id := baseIdent(l)
			if id == "" {
				continue
			}
			if len(rhs) == len(lhs) {
				if _, ok := rhs[i].(*ast.BasicLit); ok {
					lit[id] = true
					continue
				}
			}
			other[id] = true
		}
	}
	ast.Inspect(decl, func(n ast.Node) bool {
		switch s := n.(type) {
		case *ast.AssignStmt:
			note(s.Lhs, s.Rhs)
		case *ast.ValueSpec:
			lhs := make([]ast.Expr, len(s.Names))
			for i, id := range s.Names {
				lhs[i] = id
			}
			note(lhs, s.Values)
		case *ast.RangeStmt:
			for _, e := range []ast.Expr{s.Key, s.Value} {
				if id := baseIdent(e); id != "" {
					other[id] = true
				}
			}
		}
		return true
	})
	for id := range other {
		delete(lit, id)
	}
	return lit
}

var parseCalls = map[string]bool{"Sscan": true, "Sscanf": true, "Sscanln": true, "Fscan": true, "Fscanf": true, "Fscanln": true,
	"Atoi": true, "ParseInt": true, "ParseUint": true, "ParseFloat": true, "ParseDuration": true, "Unmarshal": true, "Decode": true}

// printFamily names the calls whose arguments are printed one after the other; the F forms take a writer first.
var printFamily = map[string]int{"Print": 0, "Println": 0, "Sprint": 0, "Sprintln": 0, "Fprint": 1, "Fprintln": 1}

// carriedKeys returns the KEY names a measured value is reported under in file, and the functions that read input.
func carriedKeys(file *ast.File) *carried {
	c := &carried{keys: map[string]bool{}, readers: map[string]bool{}}
	for _, decl := range file.Decls {
		if fd, ok := decl.(*ast.FuncDecl); ok && fd.Recv == nil && fd.Body != nil {
			ast.Inspect(fd.Body, func(n ast.Node) bool {
				if call, ok := n.(*ast.CallExpr); ok && parseCalls[callName(call)] {
					c.readers[fd.Name.Name] = true
				}
				return true
			})
		}
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
							c.keys[m[1]] = true
						}
					}
				}
			case *ast.CallExpr:
				name := callName(x)
				if skip, ok := printFamily[name]; ok {
					for i := skip; i+1 < len(x.Args); i++ {
						if text, ok := stringLit(x.Args[i]); ok && isMeasured(x.Args[i+1]) {
							if m := keyAtEnd.FindStringSubmatch(text); m != nil {
								c.keys[m[1]] = true
							}
						}
					}
					return true
				}
				if name == "Sscanf" || name == "Fscanf" {
					return true
				}
				format, first, ok := formatCall(x)
				if !ok {
					return true
				}
				for _, v := range formatVerbs(format) {
					if m := keyAtEnd.FindStringSubmatch(v.prefix); m != nil && first+v.arg < len(x.Args) && isMeasured(x.Args[first+v.arg]) {
						c.keys[m[1]] = true
					}
				}
			case *ast.BasicLit:
				if text, ok := stringLit(x); ok {
					for _, k := range scriptKeys(text) {
						c.keys[k] = true
					}
				}
			}
			return true
		})
	}
	// One level of call depth: a function that calls a parsing function of the file reads too.
	direct := map[string]bool{}
	for name := range c.readers {
		direct[name] = true
	}
	for _, decl := range file.Decls {
		if fd, ok := decl.(*ast.FuncDecl); ok && fd.Recv == nil && fd.Body != nil {
			ast.Inspect(fd.Body, func(n ast.Node) bool {
				if call, ok := n.(*ast.CallExpr); ok {
					if id, ok := call.Fun.(*ast.Ident); ok && direct[id.Name] {
						c.readers[fd.Name.Name] = true
					}
				}
				return true
			})
		}
	}
	return c
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

func measured(n ast.Node, tracked map[string]bool, c *carried) bool {
	if n == nil {
		return false
	}
	found := false
	ast.Inspect(n, func(node ast.Node) bool {
		if found {
			return false
		}
		switch x := node.(type) {
		case *ast.BinaryExpr:
			// A quotient of two measured values is a ratio.
			if x.Op == token.QUO && measured(x.X, tracked, c) && measured(x.Y, tracked, c) {
				return false
			}
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
				if c == nil || !c.readers[fun.Name] {
					break
				}
				keyed, source := false, false
				for _, arg := range x.Args {
					if text, ok := stringLit(arg); ok && c.keys[strings.TrimSuffix(text, "=")] {
						keyed = true
					} else if !c.isLiteral(arg) {
						source = true
					}
				}
				if keyed && source {
					found = true
				}
			}
		}
		return !found
	})
	return found
}

var durationUnits = map[string]bool{"Nanosecond": true, "Microsecond": true, "Millisecond": true, "Second": true, "Minute": true, "Hour": true}

func isFloorCall(c *ast.CallExpr) bool {
	switch f := c.Fun.(type) {
	case *ast.Ident:
		return f.Name == "max" || f.Name == "min"
	case *ast.SelectorExpr:
		return f.Sel.Name == "Max" || f.Sel.Name == "Min"
	}
	return false
}

func constDuration(e ast.Expr, isMeasured func(ast.Node) bool) bool {
	if isMeasured(e) {
		return false
	}
	found := false
	ast.Inspect(e, func(n ast.Node) bool {
		if sel, ok := n.(*ast.SelectorExpr); ok && durationUnits[sel.Sel.Name] {
			if id, ok := sel.X.(*ast.Ident); ok && id.Name == "time" {
				found = true
			}
		}
		return !found
	})
	return found
}

func carriesFloor(e ast.Expr, isMeasured func(ast.Node) bool) bool {
	found := false
	ast.Inspect(e, func(n ast.Node) bool {
		if found {
			return false
		}
		var ops []ast.Expr
		switch x := n.(type) {
		case *ast.BinaryExpr:
			if x.Op == token.ADD || x.Op == token.SUB {
				ops = []ast.Expr{x.X, x.Y}
			}
		case *ast.CallExpr:
			if isFloorCall(x) {
				ops = x.Args
			}
		}
		for _, o := range ops {
			if constDuration(o, isMeasured) {
				found = true
			}
		}
		return !found
	})
	return found
}
