package synchandoff

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
)

// mintCallers is every production file that writes a trigger through Mint,
// with its number of Mint calls. Mint is the one writer of
// sync_manual_triggers and holds the scope guard (checkSelectedSources,
// CHAOS-10064), so a new trigger writer is a design change: it is added here on
// purpose, mapping UnselectedSourcesError to its own refusal, or not at all.
var mintCallers = map[string]struct {
	calls   int
	refusal string
}{
	"internal/backfillrun/handoff.go":             {1, "dho backfill run: refused with exit 3"},
	"internal/api/integrationsadmin/handoff.go":   {1, "integration Sync now and Backfill: 400"},
	"internal/api/syncadmin/manual_trigger.go":    {1, "Sync admin Sync now and Backfill wizard: 400"},
	"internal/scheduler/sync/webhook_requests.go": {1, "webhook sync requests: the request is refused"},
	"internal/scheduler/sync/initial_backfill.go": {1, "the initial backfill: names no sources"},
}

// triggerWriter is the one function that may write sync_manual_triggers.
const triggerWriter = "internal/synchandoff/handoff.go:Mint"

const (
	modulePath   = "github.com/full-chaos/dev-health-ops/"
	handoffDir   = "internal/synchandoff"
	triggerTable = "sync_manual_triggers"
	callerLabelP = `^[a-z0-9_.:]+$`
)

var (
	// triggerTableWrite matches a normalized statement (sqlText) that writes
	// the table: INSERT INTO, UPDATE [ONLY], MERGE INTO, or COPY ... FROM.
	triggerTableWrite = regexp.MustCompile(`\b(insert into|update( only)?|merge into) (public\.)?sync_manual_triggers\b|\bcopy (public\.)?sync_manual_triggers\b( ?\([^)]*\))? from\b`)
	sqlComment        = regexp.MustCompile(`(?s)/\*.*?\*/|--[^\n]*`)
	sqlSpace          = regexp.MustCompile(`\s+`)
	sqlDot            = regexp.MustCompile(` ?\. ?`)
	callerLabel       = regexp.MustCompile(callerLabelP)
	pgxImport         = regexp.MustCompile(`^github\.com/jackc/pgx/v[0-9]+$`)
)

// sqlText is a statement as Postgres reads it, for matching: comments are
// space, quoted identifiers lose their quotes, whitespace is one space, and
// case is lower.
func sqlText(value string) string {
	value = sqlComment.ReplaceAllString(value, " ")
	value = strings.ReplaceAll(value, `"`, "")
	value = sqlSpace.ReplaceAllString(value, " ")
	value = sqlDot.ReplaceAllString(value, ".")
	return strings.ToLower(value)
}

// writesTable reports a statement that writes the table. Valid SQL spells the
// table name as one unbroken token, so a value without it is not normalized.
func writesTable(value string) bool {
	if !strings.Contains(strings.ToLower(value), triggerTable) {
		return false
	}
	return triggerTableWrite.MatchString(sqlText(value))
}

type censusFile struct {
	path, dir string
	syntax    *ast.File
	imports   map[string]string // local name -> module-relative dir, "fmt" -> "fmt"
}

// census is the parsed production tree: every file, and every package-level
// const and variable by directory, with each value written for it.
type census struct {
	fset   *token.FileSet
	files  []*censusFile
	consts map[string]map[string][]ast.Expr
	// resolving guards a const cycle.
	resolving map[string]bool
}

// triggerCensus reads the production Go files under internal/ and cmd/ of fsys
// and returns every way they write sync_manual_triggers outside Mint or reach
// Mint outside mintCallers, and the number of files it read.
//
// It reads Go syntax, not text. A string expression is folded to every value
// Go source gives it: literals, "+" chains, consts and package-level
// variables of any package, the function's own local names (every value
// assigned or appended to them, regardless of order), fmt.Sprintf, and
// strings.Join over a literal slice. The strings of one function are also read
// in source order as one text, joined with "" and with " ". Each value is read
// as Postgres reads SQL (comments, quoted identifiers, whitespace and case do
// not matter) for INSERT INTO, UPDATE [ONLY], MERGE INTO and COPY ... FROM of
// the table, and a pgx.Identifier naming the table (tx.CopyFrom) is a write
// too. A Mint call is found through any import name, and Mint used as a value
// fails.
//
// Limit: a static census cannot be complete. It does not see a value that
// crosses a function boundary (a parameter, a return value, a struct field,
// a map or slice element) or is built from run-time data (a file, the
// database, the environment). Mint's refusal of an unnamed Caller
// (ErrUnnamedCaller) holds at run time for every Mint call; a direct writer
// that the census misses is held only by the database grants, which is
// CHAOS-10161.
func triggerCensus(fsys fs.FS) (failures []string, files int) {
	c := &census{fset: token.NewFileSet(), consts: map[string]map[string][]ast.Expr{}, resolving: map[string]bool{}}
	for _, dir := range []string{"internal", "cmd"} {
		err := fs.WalkDir(fsys, dir, func(name string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
				return nil
			}
			source, err := fs.ReadFile(fsys, name)
			if err != nil {
				return err
			}
			syntax, err := parser.ParseFile(c.fset, name, source, parser.SkipObjectResolution)
			if err != nil {
				return err
			}
			files++
			c.add(name, syntax)
			return nil
		})
		if err != nil && !os.IsNotExist(err) {
			failures = append(failures, err.Error())
		}
	}

	var writers []string
	calls := map[string]int{}
	for _, file := range c.files {
		writers = append(writers, c.writers(file)...)
		count, problems := c.mintCalls(file)
		failures = append(failures, problems...)
		if count > 0 {
			calls[file.path] = count
		}
	}
	sort.Strings(writers)
	if want := []string{triggerWriter}; !reflect.DeepEqual(writers, want) {
		failures = append(failures, fmt.Sprintf("sync_manual_triggers is written in %v; only %s may write it, behind its scope guard", writers, triggerWriter))
	}
	for file, count := range calls {
		if want, ok := mintCallers[file]; !ok || want.calls != count {
			failures = append(failures, fmt.Sprintf("%s calls Mint %d times, the census names %d: a new trigger writer is added to mintCallers with its refusal, on purpose", file, count, want.calls))
		}
	}
	for file, want := range mintCallers {
		if calls[file] == 0 {
			failures = append(failures, fmt.Sprintf("%s is in mintCallers (%d calls) but does not call Mint: remove the stale entry", file, want.calls))
		}
	}
	sort.Strings(failures)
	return failures, files
}

func (c *census) add(name string, syntax *ast.File) {
	file := &censusFile{path: name, dir: path.Dir(name), syntax: syntax, imports: map[string]string{}}
	for _, spec := range syntax.Imports {
		importPath, _ := strconv.Unquote(spec.Path.Value)
		local := path.Base(importPath)
		if spec.Name != nil {
			local = spec.Name.Name
		}
		switch {
		case importPath == "fmt", importPath == "strings":
			file.imports[local] = importPath
		case pgxImport.MatchString(importPath):
			if spec.Name == nil {
				local = "pgx"
			}
			file.imports[local] = "pgx"
		case strings.HasPrefix(importPath, modulePath):
			file.imports[local] = strings.TrimPrefix(importPath, modulePath)
		}
	}
	consts := c.consts[file.dir]
	if consts == nil {
		consts = map[string][]ast.Expr{}
		c.consts[file.dir] = consts
	}
	for _, decl := range syntax.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || (gen.Tok != token.CONST && gen.Tok != token.VAR) {
			continue
		}
		for _, spec := range gen.Specs {
			value := spec.(*ast.ValueSpec)
			for i, name := range value.Names {
				if i < len(value.Values) {
					consts[name.Name] = append(consts[name.Name], value.Values[i])
				}
			}
		}
	}
	c.files = append(c.files, file)
}

// maxValues caps the candidate values of one expression.
const maxValues = 64

// scope is where a name is looked up: the file, and the string names local to
// the function being read, each with every value the function assigns to it.
type scope struct {
	file      *censusFile
	locals    map[string]*localName
	memo      map[string][]string
	resolving map[string]bool
}

// localName is every assignment to one name in a function, read without
// regard to order or control flow: assigned values (":=", "=", var, const)
// and appended ones ("+=", and "x = x + e").
type localName struct {
	assigned, appended []ast.Expr
}

func newScope(file *censusFile) scope {
	return scope{file: file, locals: map[string]*localName{}, memo: map[string][]string{}, resolving: map[string]bool{}}
}

func (s scope) name(name string) *localName {
	l := s.locals[name]
	if l == nil {
		l = &localName{}
		s.locals[name] = l
	}
	return l
}

// collectLocals records every assignment to a plain identifier in node.
func collectLocals(s scope, node ast.Node) {
	ast.Inspect(node, func(node ast.Node) bool {
		switch n := node.(type) {
		case *ast.AssignStmt:
			if len(n.Lhs) != len(n.Rhs) {
				return true
			}
			for i, lhs := range n.Lhs {
				name, ok := lhs.(*ast.Ident)
				if !ok || name.Name == "_" {
					continue
				}
				rhs := n.Rhs[i]
				switch n.Tok {
				case token.ADD_ASSIGN:
					s.name(name.Name).appended = append(s.name(name.Name).appended, rhs)
				case token.DEFINE, token.ASSIGN:
					if sum, ok := rhs.(*ast.BinaryExpr); ok && sum.Op == token.ADD {
						if self, ok := sum.X.(*ast.Ident); ok && self.Name == name.Name {
							s.name(name.Name).appended = append(s.name(name.Name).appended, sum.Y)
							continue
						}
					}
					s.name(name.Name).assigned = append(s.name(name.Name).assigned, rhs)
				}
			}
		case *ast.ValueSpec:
			for i, name := range n.Names {
				if i < len(n.Values) {
					s.name(name.Name).assigned = append(s.name(name.Name).assigned, n.Values[i])
				} else {
					s.name(name.Name)
				}
			}
		}
		return true
	})
}

// fold returns the first value of expr when the census can compute one.
func (c *census) fold(s scope, expr ast.Expr) (string, bool) {
	values := c.values(s, expr)
	if len(values) == 0 {
		return "", false
	}
	return values[0], true
}

// values returns the string values expr can take, as far as Go source shows
// them: literals, "+", consts and package-level variables (of this or an
// imported package), the function's local names, fmt.Sprintf, and
// strings.Join over a literal slice. Nil means unknown.
func (c *census) values(s scope, expr ast.Expr) []string {
	switch e := expr.(type) {
	case *ast.BasicLit:
		if e.Kind != token.STRING {
			return nil
		}
		value, err := strconv.Unquote(e.Value)
		if err != nil {
			return nil
		}
		return []string{value}
	case *ast.ParenExpr:
		return c.values(s, e.X)
	case *ast.BinaryExpr:
		if e.Op != token.ADD {
			return nil
		}
		return concat(c.values(s, e.X), c.values(s, e.Y))
	case *ast.Ident:
		if local, ok := s.locals[e.Name]; ok {
			return c.localValues(s, e.Name, local)
		}
		return c.constant(s.file, s.file.dir, e.Name)
	case *ast.SelectorExpr:
		if pkg, ok := e.X.(*ast.Ident); ok {
			if dir, ok := s.file.imports[pkg.Name]; ok && dir != "fmt" && dir != "strings" && dir != "pgx" {
				return c.constant(s.file, dir, e.Sel.Name)
			}
		}
	case *ast.CallExpr:
		switch c.stdlibCall(s, e) {
		case "fmt.Sprintf":
			return c.sprintf(s, e.Args)
		case "strings.Join":
			return c.join(s, e.Args)
		}
	}
	return nil
}

func (c *census) stdlibCall(s scope, call *ast.CallExpr) string {
	selector, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return ""
	}
	pkg, ok := selector.X.(*ast.Ident)
	if !ok {
		return ""
	}
	switch dir := s.file.imports[pkg.Name]; {
	case dir == "fmt" && selector.Sel.Name == "Sprintf" && len(call.Args) > 0:
		return "fmt.Sprintf"
	case dir == "strings" && selector.Sel.Name == "Join" && len(call.Args) == 2:
		return "strings.Join"
	}
	return ""
}

// sprintf formats each format value with the real fmt.Sprintf over every
// combination of the arguments' values (an argument the census cannot compute
// reads as ""), so explicit argument indexes, %q and %% format as at run time.
func (c *census) sprintf(s scope, args []ast.Expr) []string {
	combinations := [][]any{nil}
	for _, arg := range args[1:] {
		values := c.values(s, arg)
		if len(values) == 0 {
			values = []string{""}
		}
		var next [][]any
		for _, combination := range combinations {
			for _, value := range values {
				if len(next) == maxValues {
					break
				}
				next = append(next, append(append([]any(nil), combination...), value))
			}
		}
		combinations = next
	}
	var out []string
	for _, format := range c.values(s, args[0]) {
		for _, combination := range combinations {
			out = append(out, fmt.Sprintf(format, combination...))
		}
	}
	return capped(out)
}

func (c *census) join(s scope, args []ast.Expr) []string {
	elements, ok := args[0].(*ast.CompositeLit)
	if !ok {
		return nil
	}
	separators := c.values(s, args[1])
	if len(separators) == 0 {
		return nil
	}
	var out []string
	for _, separator := range separators {
		result := []string{""}
		for i, element := range elements.Elts {
			values := c.values(s, element)
			if len(values) == 0 {
				return nil
			}
			if i > 0 {
				result = concat(result, []string{separator})
			}
			result = concat(result, values)
		}
		out = append(out, result...)
	}
	return capped(out)
}

// localValues is every value of a local name: each assigned value alone,
// followed by each appended value, and followed by all of them in source order.
func (c *census) localValues(s scope, name string, local *localName) []string {
	if values, ok := s.memo[name]; ok {
		return values
	}
	if s.resolving[name] {
		return nil
	}
	s.resolving[name] = true
	defer delete(s.resolving, name)
	var bases []string
	for _, expr := range local.assigned {
		bases = append(bases, c.values(s, expr)...)
	}
	if len(local.assigned) == 0 {
		bases = []string{""}
	}
	values := append([]string(nil), bases...)
	all := bases
	for _, expr := range local.appended {
		appended := c.values(s, expr)
		values = append(values, concat(bases, appended)...)
		all = concat(all, appended)
	}
	if len(local.appended) > 1 {
		values = append(values, all...)
	}
	values = capped(values)
	s.memo[name] = values
	return values
}

func concat(left, right []string) []string {
	if len(left) == 0 || len(right) == 0 {
		return nil
	}
	out := make([]string, 0, len(left)*len(right))
	for _, l := range left {
		for _, r := range right {
			out = append(out, l+r)
			if len(out) == maxValues {
				return out
			}
		}
	}
	return out
}

func capped(values []string) []string {
	if len(values) > maxValues {
		return values[:maxValues]
	}
	return values
}

func (c *census) constant(from *censusFile, dir, name string) []string {
	exprs, ok := c.consts[dir][name]
	key := dir + "." + name
	if !ok || c.resolving[key] {
		return nil
	}
	c.resolving[key] = true
	defer delete(c.resolving, key)
	owner := from
	if dir != from.dir {
		owner = &censusFile{dir: dir, imports: map[string]string{}}
		for _, file := range c.files {
			if file.dir == dir {
				owner = file
				break
			}
		}
	}
	var values []string
	for _, expr := range exprs {
		values = append(values, c.values(newScope(owner), expr)...)
	}
	return capped(values)
}

// writers returns the units of file that write sync_manual_triggers: a
// function as "path:Name" (a method as "path:Recv.Name"), and package-level
// declarations as "path:<package scope>".
func (c *census) writers(file *censusFile) []string {
	var units []string
	for _, decl := range file.syntax.Decls {
		name := "<package scope>"
		if fn, ok := decl.(*ast.FuncDecl); ok {
			name = fn.Name.Name
			if fn.Recv != nil && len(fn.Recv.List) > 0 {
				name = receiverName(fn.Recv.List[0].Type) + "." + name
			}
		}
		if c.writes(file, decl) {
			units = append(units, file.path+":"+name)
		}
	}
	return units
}

func receiverName(expr ast.Expr) string {
	switch e := expr.(type) {
	case *ast.StarExpr:
		return receiverName(e.X)
	case *ast.IndexExpr:
		return receiverName(e.X)
	case *ast.IndexListExpr:
		return receiverName(e.X)
	case *ast.Ident:
		return e.Name
	}
	return "?"
}

// writes reports whether one declaration holds a write of the table: in any
// value of one string expression, or in its strings read in source order as
// one text, joined with "" and with " ".
func (c *census) writes(file *censusFile, decl ast.Decl) bool {
	s := newScope(file)
	if fn, ok := decl.(*ast.FuncDecl); ok {
		collectLocals(s, fn)
	}
	var pieces []string
	found := false
	ast.Inspect(decl, func(node ast.Node) bool {
		if found {
			return false
		}
		expr, ok := node.(ast.Expr)
		if !ok {
			return true
		}
		values := c.values(s, expr)
		for _, value := range values {
			if writesTable(value) {
				found = true
				return false
			}
		}
		if c.namesTableIdentifier(s, expr) {
			found = true
			return false
		}
		switch expr.(type) {
		case *ast.BasicLit, *ast.Ident:
			if len(values) > 0 {
				pieces = append(pieces, values[0])
			}
		case *ast.SelectorExpr:
			if len(values) > 0 {
				pieces = append(pieces, values[0])
				return false
			}
		}
		return true
	})
	return found || writesTable(strings.Join(pieces, "")) || writesTable(strings.Join(pieces, " "))
}

// namesTableIdentifier reports a pgx.Identifier literal naming the table: the
// form tx.CopyFrom writes rows with, without SQL text.
func (c *census) namesTableIdentifier(s scope, expr ast.Expr) bool {
	literal, ok := expr.(*ast.CompositeLit)
	if !ok {
		return false
	}
	selector, ok := literal.Type.(*ast.SelectorExpr)
	if !ok || selector.Sel.Name != "Identifier" {
		return false
	}
	if pkg, ok := selector.X.(*ast.Ident); !ok || s.file.imports[pkg.Name] != "pgx" {
		return false
	}
	for _, element := range literal.Elts {
		for _, value := range c.values(s, element) {
			if sqlText(value) == triggerTable {
				return true
			}
		}
	}
	return false
}

// mintCalls counts the Mint calls of file and returns a problem for Mint used
// as a value and for a call whose MintInput does not name its Caller.
func (c *census) mintCalls(file *censusFile) (int, []string) {
	var qualifiers []string
	bare := file.dir == handoffDir
	for local, dir := range file.imports {
		if dir != handoffDir || local == "_" {
			continue
		}
		if local == "." {
			bare = true
			continue
		}
		qualifiers = append(qualifiers, local)
	}
	if len(qualifiers) == 0 && !bare {
		return 0, nil
	}
	isMint := func(node ast.Node) bool {
		switch e := node.(type) {
		case *ast.SelectorExpr:
			pkg, ok := e.X.(*ast.Ident)
			if !ok || e.Sel.Name != "Mint" {
				return false
			}
			for _, qualifier := range qualifiers {
				if pkg.Name == qualifier {
					return true
				}
			}
		case *ast.Ident:
			return bare && e.Name == "Mint"
		}
		return false
	}
	var problems []string
	count := 0
	for _, decl := range file.syntax.Decls {
		fn, _ := decl.(*ast.FuncDecl)
		var stack []ast.Node
		ast.Inspect(decl, func(node ast.Node) bool {
			if node == nil {
				stack = stack[:len(stack)-1]
				return true
			}
			parent := ast.Node(nil)
			if len(stack) > 0 {
				parent = stack[len(stack)-1]
			}
			if !isMint(node) || isDeclaredName(node, fn, parent) {
				stack = append(stack, node)
				return true
			}
			position := c.fset.Position(node.Pos())
			call, ok := parent.(*ast.CallExpr)
			if !ok || call.Fun != node {
				problems = append(problems, fmt.Sprintf("%s:%d uses Mint as a value; the census pins Mint calls", file.path, position.Line))
				return false
			}
			count++
			if problem := c.namedCaller(file, fn, call); problem != "" {
				problems = append(problems, fmt.Sprintf("%s:%d %s", file.path, position.Line, problem))
			}
			return false
		})
	}
	return count, problems
}

// isDeclaredName reports a Mint identifier that is not a reference: the
// function's own name, or the selected name of another selector.
func isDeclaredName(node ast.Node, fn *ast.FuncDecl, parent ast.Node) bool {
	ident, ok := node.(*ast.Ident)
	if !ok {
		return false
	}
	if fn != nil && ident == fn.Name {
		return true
	}
	selector, ok := parent.(*ast.SelectorExpr)
	return ok && selector.Sel == ident
}

// namedCaller checks the call's MintInput: a composite literal with a Caller
// whose leading string is a synclog label, written at the call or assigned to
// the identifier the call passes, in the same function.
func (c *census) namedCaller(file *censusFile, fn *ast.FuncDecl, call *ast.CallExpr) string {
	if len(call.Args) != 5 {
		return "calls Mint without its five arguments"
	}
	s := newScope(file)
	literal := func(expr ast.Expr) string {
		composite, ok := expr.(*ast.CompositeLit)
		if !ok {
			return "passes a MintInput the census cannot read: build it as a literal naming Caller"
		}
		for _, element := range composite.Elts {
			pair, ok := element.(*ast.KeyValueExpr)
			if !ok {
				continue
			}
			if key, ok := pair.Key.(*ast.Ident); ok && key.Name == "Caller" {
				if lead := c.leadingString(s, pair.Value); callerLabel.MatchString(lead) {
					return ""
				}
				return "names a Caller that does not start with a synclog label"
			}
		}
		return "calls Mint without naming its Caller (the scope guard's log line names the path)"
	}
	ident, ok := call.Args[3].(*ast.Ident)
	if !ok {
		return literal(call.Args[3])
	}
	if fn == nil || fn.Body == nil {
		return "passes a MintInput the census cannot read: build it as a literal naming Caller"
	}
	var definitions []ast.Expr
	ast.Inspect(fn.Body, func(node ast.Node) bool {
		switch n := node.(type) {
		case *ast.AssignStmt:
			for i, lhs := range n.Lhs {
				if name, ok := lhs.(*ast.Ident); ok && name.Name == ident.Name && i < len(n.Rhs) && len(n.Lhs) == len(n.Rhs) {
					definitions = append(definitions, n.Rhs[i])
				}
			}
		case *ast.ValueSpec:
			for i, name := range n.Names {
				if name.Name == ident.Name && i < len(n.Values) {
					definitions = append(definitions, n.Values[i])
				}
			}
		}
		return true
	})
	if len(definitions) == 0 {
		return "passes a MintInput the census cannot read: build it as a literal naming Caller"
	}
	for _, definition := range definitions {
		if problem := literal(definition); problem != "" {
			return problem
		}
	}
	return ""
}

// leadingString is the folded value of expr, or of the leftmost operand of a
// "+" chain whose rest is computed at run time.
func (c *census) leadingString(s scope, expr ast.Expr) string {
	if value, ok := c.fold(s, expr); ok {
		return value
	}
	if binary, ok := expr.(*ast.BinaryExpr); ok && binary.Op == token.ADD {
		return c.leadingString(s, binary.X)
	}
	return ""
}

// TestEveryTriggerWriteGoesThroughTheScopeGuard fails when production code
// writes sync_manual_triggers anywhere but Mint, or reaches Mint from a call
// mintCallers does not pin: either would bypass or silently inherit the one
// scope guard of trigger writes.
func TestEveryTriggerWriteGoesThroughTheScopeGuard(t *testing.T) {
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatal(err)
	}
	failures, files := triggerCensus(os.DirFS(root))
	if files < 500 {
		t.Fatalf("read %d Go files; the census must see the production tree", files)
	}
	for _, failure := range failures {
		t.Error(failure)
	}
}

const censusModule = "github.com/full-chaos/dev-health-ops/internal/synchandoff"

// censusTree is a production tree the census accepts: Mint writes the table,
// and each mintCallers file calls Mint once, naming its Caller.
func censusTree() fstest.MapFS {
	tree := fstest.MapFS{
		"internal/synchandoff/handoff.go": {Data: []byte(`package synchandoff

func Mint(ctx context.Context, tx pgx.Tx, config *Config, input MintInput, now time.Time) (Trigger, error) {
	_, err := tx.Exec(ctx, ` + "`" + `
INSERT INTO public.sync_manual_triggers (occurrence_id, mode)
VALUES ($1, $2)` + "`" + `, 1, input.Mode)
	return Trigger{}, err
}
`)},
		"internal/api/syncadmin/manual_trigger.go": {Data: []byte(`package syncadmin

import "` + censusModule + `"

func handOff(spec spec) error {
	input := synchandoff.MintInput{Mode: spec.mode, Caller: "syncadmin." + spec.triggeredBy}
	_, err := synchandoff.Mint(ctx, tx, config, input, now)
	return err
}
`)},
	}
	for _, file := range []string{
		"internal/backfillrun/handoff.go",
		"internal/api/integrationsadmin/handoff.go",
		"internal/scheduler/sync/webhook_requests.go",
		"internal/scheduler/sync/initial_backfill.go",
	} {
		tree[file] = &fstest.MapFile{Data: []byte(`package caller

import "` + censusModule + `"

func trigger() error {
	_, err := synchandoff.Mint(ctx, tx, config, synchandoff.MintInput{Mode: "backfill", Caller: "census_fixture"}, now)
	return err
}
`)}
	}
	return tree
}

// TestTriggerCensusFailsEveryBypass plants, one at a time, each way to write
// sync_manual_triggers outside Mint or to reach Mint without the census seeing
// the call, in a tree the census otherwise accepts.
func TestTriggerCensusFailsEveryBypass(t *testing.T) {
	if failures, _ := triggerCensus(censusTree()); len(failures) > 0 {
		t.Fatalf("the census refuses the accepted tree: %v", failures)
	}
	const sneak = `package synchandoff

func sneak(ctx context.Context, tx pgx.Tx) error {
	_, err := tx.Exec(ctx, %s)
	return err
}
`
	for name, plant := range map[string]map[string]string{
		"a second writing function in handoff.go": {
			"internal/synchandoff/handoff.go": string(censusTree()["internal/synchandoff/handoff.go"].Data) +
				strings.TrimPrefix(strings.Replace(sneak, "%s", "`INSERT INTO public.sync_manual_triggers (occurrence_id) VALUES ($1)`, 1", 1), "package synchandoff\n"),
		},
		"a split literal in package synchandoff": {
			"internal/synchandoff/sneak.go": strings.Replace(sneak, "%s", `"INSERT INTO public.sync_manual" + "_triggers (occurrence_id) VALUES ($1)", 1`, 1),
		},
		"a package-level const table name": {
			"internal/scheduler/sync/sneak.go": `package sync

const triggerTable = "sync_manual_triggers"

func sneak(ctx context.Context, tx pgx.Tx) error {
	_, err := tx.Exec(ctx, "INSERT INTO public." + triggerTable + " (occurrence_id) VALUES ($1)", 1)
	return err
}
`,
		},
		"a statement split across variables": {
			"internal/scheduler/sync/sneak.go": `package sync

func sneak(ctx context.Context, tx pgx.Tx) error {
	verb := "UPDATE public.sync_manual"
	statement := verb + "_triggers SET mode = $1"
	_, err := tx.Exec(ctx, statement, "x")
	return err
}
`,
		},
		"a local table name concatenated after it is assigned": {
			"internal/scheduler/sync/sneak.go": `package sync

func sneak(ctx context.Context, tx pgx.Tx) error {
	table := "sync_manual_triggers"
	statement := "INSERT INTO public." + table + " (occurrence_id) VALUES ($1)"
	_, err := tx.Exec(ctx, statement, 1)
	return err
}
`,
		},
		"a statement built with +=": {
			"internal/scheduler/sync/sneak.go": `package sync

func sneak(ctx context.Context, tx pgx.Tx) error {
	statement := "INSERT INTO public."
	statement += "sync_manual_triggers"
	statement += " (occurrence_id) VALUES ($1)"
	_, err := tx.Exec(ctx, statement, 1)
	return err
}
`,
		},
		"a reassigned variable": {
			"internal/scheduler/sync/sneak.go": `package sync

func sneak(ctx context.Context, tx pgx.Tx) error {
	table := "scheduled_jobs"
	if ctx != nil {
		table = "sync_manual_triggers"
	}
	_, err := tx.Exec(ctx, "UPDATE public."+table+" SET mode = $1", "x")
	return err
}
`,
		},
		"a statement joined by strings.Join": {
			"internal/scheduler/sync/sneak.go": `package sync

import "strings"

func sneak(ctx context.Context, tx pgx.Tx) error {
	_, err := tx.Exec(ctx, strings.Join([]string{"INSERT INTO", "public.sync_manual_triggers", "(occurrence_id) VALUES ($1)"}, " "), 1)
	return err
}
`,
		},
		"a package-level variable table name": {
			"internal/scheduler/sync/sneak.go": `package sync

var triggerTable = "sync_manual_triggers"

func sneak(ctx context.Context, tx pgx.Tx) error {
	_, err := tx.Exec(ctx, "INSERT INTO public." + triggerTable + " (occurrence_id) VALUES ($1)", 1)
	return err
}
`,
		},
		"a block comment between the keywords": {
			"internal/scheduler/sync/sneak.go": `package sync

import (
	"fmt"

	"github.com/jackc/pgx/v5"
)

func sneak(ctx context.Context, tx pgx.Tx) error {
	_, err := tx.Exec(ctx, "INSERT /* x */ INTO public.sync_manual_triggers (occurrence_id) VALUES ($1)", 1)
	_ = fmt.Sprint
	return err
}
`,
		},
		"a line comment between the keywords": {
			"internal/scheduler/sync/sneak.go": `package sync

import (
	"fmt"

	"github.com/jackc/pgx/v5"
)

func sneak(ctx context.Context, tx pgx.Tx) error {
	_, err := tx.Exec(ctx, "INSERT -- x\nINTO public.sync_manual_triggers (occurrence_id) VALUES ($1)", 1)
	_ = fmt.Sprint
	return err
}
`,
		},
		"a positional fmt.Sprintf": {
			"internal/scheduler/sync/sneak.go": `package sync

import (
	"fmt"

	"github.com/jackc/pgx/v5"
)

func sneak(ctx context.Context, tx pgx.Tx) error {
	_, err := tx.Exec(ctx, fmt.Sprintf("INSERT INTO public.%[2]s (occurrence_id) VALUES (%[1]s)", "$1", "sync_manual_triggers"), 1)
	_ = fmt.Sprint
	return err
}
`,
		},
		"a quoted identifier": {
			"internal/scheduler/sync/sneak.go": `package sync

import (
	"fmt"

	"github.com/jackc/pgx/v5"
)

func sneak(ctx context.Context, tx pgx.Tx) error {
	_, err := tx.Exec(ctx, ` + "`" + `INSERT INTO "public" . "sync_manual_triggers" (occurrence_id) VALUES ($1)` + "`" + `, 1)
	_ = fmt.Sprint
	return err
}
`,
		},
		"a MERGE": {
			"internal/scheduler/sync/sneak.go": `package sync

import (
	"fmt"

	"github.com/jackc/pgx/v5"
)

func sneak(ctx context.Context, tx pgx.Tx) error {
	_, err := tx.Exec(ctx, "MERGE INTO public.sync_manual_triggers AS t USING (SELECT $1 AS id) s ON t.occurrence_id = s.id WHEN NOT MATCHED THEN INSERT (occurrence_id) VALUES (s.id)", 1)
	_ = fmt.Sprint
	return err
}
`,
		},
		"a COPY FROM": {
			"internal/scheduler/sync/sneak.go": `package sync

import (
	"fmt"

	"github.com/jackc/pgx/v5"
)

func sneak(ctx context.Context, tx pgx.Tx) error {
	_, err := tx.Exec(ctx, "COPY public.sync_manual_triggers (occurrence_id, mode) FROM STDIN", 1)
	_ = fmt.Sprint
	return err
}
`,
		},
		"an UPDATE ONLY": {
			"internal/scheduler/sync/sneak.go": `package sync

import (
	"fmt"

	"github.com/jackc/pgx/v5"
)

func sneak(ctx context.Context, tx pgx.Tx) error {
	_, err := tx.Exec(ctx, "UPDATE ONLY public.sync_manual_triggers SET mode = $1", "x")
	_ = fmt.Sprint
	return err
}
`,
		},
		"a pgx.Identifier for CopyFrom": {
			"internal/scheduler/sync/sneak.go": `package sync

import (
	"fmt"

	"github.com/jackc/pgx/v5"
)

func sneak(ctx context.Context, tx pgx.Tx) error {
	_, err := tx.CopyFrom(ctx, pgx.Identifier{"public", "sync_manual_triggers"}, []string{"occurrence_id"}, rows)
	_ = fmt.Sprint
	return err
}
`,
		},
		"a table name passed to fmt.Sprintf": {
			"internal/scheduler/sync/sneak.go": `package sync

import "fmt"

func sneak(ctx context.Context, tx pgx.Tx) error {
	_, err := tx.Exec(ctx, fmt.Sprintf("INSERT INTO public.%s (occurrence_id) VALUES ($1)", "sync_manual_triggers"), 1)
	return err
}
`,
		},
		"a second Mint call with no Caller in a listed file": {
			"internal/scheduler/sync/initial_backfill.go": `package sync

import "` + censusModule + `"

func trigger() error {
	_, err := synchandoff.Mint(ctx, tx, config, synchandoff.MintInput{Mode: "backfill", Caller: "census_fixture"}, now)
	return err
}

func again() error {
	_, err := synchandoff.Mint(ctx, tx, config, synchandoff.MintInput{Mode: "backfill"}, now)
	return err
}
`,
		},
		"a second named Mint call in a listed file": {
			"internal/backfillrun/handoff.go": `package backfillrun

import "` + censusModule + `"

func trigger() error {
	for _, caller := range []string{"a", "b"} {
		_, err := synchandoff.Mint(ctx, tx, config, synchandoff.MintInput{Mode: "backfill", Caller: "census_fixture"}, now)
		_, err = synchandoff.Mint(ctx, tx, config, synchandoff.MintInput{Mode: "backfill", Caller: "census_fixture_" + caller}, now)
		return err
	}
	return nil
}
`,
		},
		"a Mint call through an import alias": {
			"internal/other/aliased.go": `package other

import handoff "` + censusModule + `"

func trigger() error {
	_, err := handoff.Mint(ctx, tx, config, handoff.MintInput{Mode: "backfill", Caller: "aliased"}, now)
	return err
}
`,
		},
		"a Mint call through a dot import": {
			"internal/other/dotted.go": `package other

import . "` + censusModule + `"

func trigger() error {
	_, err := Mint(ctx, tx, config, MintInput{Mode: "backfill", Caller: "dotted"}, now)
	return err
}
`,
		},
		"Mint taken as a value": {
			"internal/other/value.go": `package other

import "` + censusModule + `"

var mint = synchandoff.Mint

func trigger() error {
	_, err := mint(ctx, tx, config, synchandoff.MintInput{Mode: "backfill", Caller: "value"}, now)
	return err
}
`,
		},
		"a MintInput built without Caller": {
			"internal/api/syncadmin/manual_trigger.go": `package syncadmin

import "` + censusModule + `"

func handOff(spec spec) error {
	input := synchandoff.MintInput{Mode: spec.mode}
	input.Mode = "backfill"
	_, err := synchandoff.Mint(ctx, tx, config, input, now)
	return err
}
`,
		},
	} {
		tree := censusTree()
		for path, source := range plant {
			tree[path] = &fstest.MapFile{Data: []byte(source)}
		}
		failures, _ := triggerCensus(tree)
		if len(failures) == 0 {
			t.Errorf("%s: the census accepts it", name)
		}
		t.Logf("%s: %v", name, failures)
	}
}
