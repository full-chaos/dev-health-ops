// Package membershipreads refuses a go-api or query-api read of the auth schema's memberships or organizations tables
// (CHAOS-10985, acp OPTION P, D6358).
//
// users, organizations and memberships stay in the public tables. auth-service writes the auth rows and the public rows in
// one transaction, so go-api and query-api read public.memberships and public.organizations only: a read of the auth
// copies would make them a second read authority and would need an auth-schema GRANT that nobody holds. The query-api
// login holds no privilege on those two auth tables; this gate is the code side of the same rule.
//
// A SITE is a string literal, or a `+` chain folded into one string, of production code under one of Scopes that names
// `<qualifier>.memberships` or `<qualifier>.organizations` where the qualifier is anything but `public`: auth, a quoted
// "auth", a Sprintf verb (%s), or no visible qualifier at all (a literal that starts with ".memberships" because a schema that
// is not a constant "public" is concatenated in front of it). Case, spaces and SQL comments (/* */, nested, and -- to the end
// of the line) around the schema, the dot and the table do not matter; the text is also scanned as written, so a quoted
// `--` cannot hide a read.
//
// Constants are evaluated the way the compiler evaluates them (go/constant over the AST): literals, integer and rune
// conversions, string(...), arithmetic, shifts, len of a constant string, constants of the same package (any file of the
// directory, grouped blocks, typed or untyped, built from other constants) or of the enclosing function. A constant of
// ANOTHER package is not loaded: a table name right behind one is refused by name as unresolved.
//
// A variable, a call or a field is not folded. In a `+` chain the constant operands around it fold into runs, and a run that
// begins with ".memberships" or ".organizations" (the table behind a non-constant qualifier) is a finding; so is a verb
// right before the dot. There is no allow list: a read of those tables has no legitimate place in these packages.
//
// NOT covered, stated: this gate is a source tripwire for the readable spellings. The enforcement is the query-api login's
// column grants (the posture test and the role-permission integration test). Code outside Scopes (the shared storage packages,
// internal/auth/**, which is auth-service and holds the writes); an unqualified `memberships` that a search_path resolves to
// the auth schema (the logins pin their search_path and the posture test pins their grants); a table name that no constant,
// literal or verb spells (a name assembled by a function call at run time); and _test.go files.
package membershipreads

import (
	"fmt"
	"go/ast"
	"go/constant"
	"go/parser"
	"go/token"
	"io/fs"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"unicode"
)

// Scope is one source root and the service whose code lives there.
type Scope struct {
	Root    string
	Service string
}

// Scopes are the production packages of go-api and query-api. Every root is a directory below the module root.
var Scopes = []Scope{
	{"internal/api", "go-api"},
	{"internal/apiservice", "go-api"},
	{"internal/queryapi", "query-api"},
	{"internal/queryapiservice", "query-api"},
}

var (
	// tableDot finds `.memberships` / `.organizations`: the dot, optional spaces and an optional quote, then the table name.
	tableDot = regexp.MustCompile(`(?i)\.\s*"?(memberships|organizations)\b`)
	// qualifierEnd is the token that ends a string: a quoted identifier or a bare word (letters, digits, underscore, a % verb).
	qualifierEnd = regexp.MustCompile(`("[^"]*"|[A-Za-z0-9_%$]+)\s*$`)
)

// unresolved stands in the folded text for a constant the gate cannot resolve (a constant of another package, or a name
// declared more than once in a scope): a table name right behind it is refused by name.
const unresolved = "\x00"

type parsedFile struct {
	rel  string
	dir  string
	fset *token.FileSet
	file *ast.File
}

// Check scans the non-test .go files below each scope root of root and returns one named finding per site, sorted. A
// file that does not parse is an error: a gate that skipped it would scan nothing.
func Check(root string, scopes []Scope) ([]string, error) {
	var findings []string
	for _, scope := range scopes {
		files, err := collect(root, scope)
		if err != nil {
			return nil, err
		}
		tables := packageConsts(files)
		for _, pf := range files {
			imports := importNames(pf.file)
			for _, decl := range pf.file.Decls {
				fl := &folder{pkg: tables[pf.dir], imports: imports}
				if fd, ok := decl.(*ast.FuncDecl); ok && fd.Body != nil {
					fl.local = declaredConsts(fd.Body)
				}
				report := func(n ast.Node, text string) {
					for _, ref := range refsAll(text) {
						findings = append(findings, fmt.Sprintf("%s:%d: membership-read: %s code names %s: go-api and query-api read public.memberships and public.organizations only; auth-service writes both copies in one transaction (D6358, CHAOS-10985)",
							pf.rel, pf.fset.Position(n.Pos()).Line, scope.Service, ref))
					}
				}
				var visit func(n ast.Node) bool
				visit = func(n ast.Node) bool {
					switch e := n.(type) {
					case *ast.BasicLit:
						if text, ok := fl.fold(e); ok {
							report(e, text)
						}
						return false
					case *ast.BinaryExpr:
						if e.Op != token.ADD {
							return true
						}
						if text, ok := fl.fold(e); ok {
							report(e, text)
							return false
						}
						scanChain(fl, e, report, visit)
						return false
					}
					return true
				}
				ast.Inspect(decl, visit)
			}
		}
	}
	sort.Strings(findings)
	return findings, nil
}

// scanChain folds the constant operands of a `+` chain into runs, scans each run, and descends into the operands that are
// not constant. A run that follows a non-constant operand and begins with a dot and a table name is a finding by itself.
func scanChain(fl *folder, e *ast.BinaryExpr, report func(ast.Node, string), visit func(ast.Node) bool) {
	var run strings.Builder
	var runStart ast.Node
	flush := func() {
		if runStart != nil {
			report(runStart, run.String())
		}
		run.Reset()
		runStart = nil
	}
	for _, op := range flattenAdd(e) {
		if text, ok := fl.fold(op); ok {
			if runStart == nil {
				runStart = op
			}
			run.WriteString(text)
			continue
		}
		flush()
		ast.Inspect(op, visit)
	}
	flush()
}

func flattenAdd(e ast.Expr) []ast.Expr {
	switch x := e.(type) {
	case *ast.ParenExpr:
		if inner, ok := x.X.(*ast.BinaryExpr); ok && inner.Op == token.ADD {
			return flattenAdd(inner)
		}
	case *ast.BinaryExpr:
		if x.Op == token.ADD {
			return append(flattenAdd(x.X), flattenAdd(x.Y)...)
		}
	}
	return []ast.Expr{e}
}

// Packages is the sorted list of module-relative package directories Check scans (one per directory that holds a
// non-test .go file), so a reviewer can cite every package the gate reads.
func Packages(root string, scopes []Scope) ([]string, error) {
	seen := map[string]bool{}
	for _, scope := range scopes {
		files, err := collect(root, scope)
		if err != nil {
			return nil, err
		}
		for _, pf := range files {
			seen[pf.dir] = true
		}
	}
	out := make([]string, 0, len(seen))
	for p := range seen {
		out = append(out, p)
	}
	sort.Strings(out)
	return out, nil
}

func collect(root string, scope Scope) ([]parsedFile, error) {
	base := filepath.Join(root, filepath.FromSlash(scope.Root))
	var files []parsedFile
	err := filepath.WalkDir(base, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			if p == base {
				return nil // a scope root that does not exist scans nothing; the real-tree test requires every root to hold code
			}
			return err
		}
		name := d.Name()
		if d.IsDir() {
			if p != base && (strings.HasPrefix(name, ".") || name == "testdata" || name == "vendor" || name == "node_modules") {
				return fs.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			return nil
		}
		rel, err := filepath.Rel(root, p)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, p, nil, parser.SkipObjectResolution)
		if err != nil {
			return fmt.Errorf("membershipreads: cannot parse %s: %w", rel, err)
		}
		files = append(files, parsedFile{rel: rel, dir: path.Dir(rel), fset: fset, file: file})
		return nil
	})
	return files, err
}

// constTable maps a constant name to the expressions that declare it; more than one means the name is ambiguous.
type constTable map[string][]ast.Expr

func (t constTable) add(spec *ast.ValueSpec, values []ast.Expr) {
	for i, name := range spec.Names {
		if name.Name == "_" || i >= len(values) {
			continue
		}
		t[name.Name] = append(t[name.Name], values[i])
	}
}

// declaredConsts are the constants of a function body (every const declaration inside it).
func declaredConsts(body ast.Node) constTable {
	table := constTable{}
	ast.Inspect(body, func(n ast.Node) bool {
		if gd, ok := n.(*ast.GenDecl); ok && gd.Tok == token.CONST {
			addConstSpecs(table, gd)
		}
		return true
	})
	return table
}

// addConstSpecs adds a const block: a spec with no values repeats the values of the one before it, as Go does.
func addConstSpecs(table constTable, gd *ast.GenDecl) {
	var values []ast.Expr
	for _, sp := range gd.Specs {
		vs := sp.(*ast.ValueSpec)
		if len(vs.Values) > 0 {
			values = vs.Values
		}
		table.add(vs, values)
	}
}

// packageConsts is the constant table of each directory: the top-level const blocks of every non-test file in it.
func packageConsts(files []parsedFile) map[string]constTable {
	tables := map[string]constTable{}
	for _, pf := range files {
		table := tables[pf.dir]
		if table == nil {
			table = constTable{}
			tables[pf.dir] = table
		}
		for _, decl := range pf.file.Decls {
			if gd, ok := decl.(*ast.GenDecl); ok && gd.Tok == token.CONST {
				addConstSpecs(table, gd)
			}
		}
	}
	return tables
}

// importNames are the names the file's imports bind (an alias, or the last element of the path).
func importNames(file *ast.File) map[string]bool {
	names := map[string]bool{}
	for _, imp := range file.Imports {
		if imp.Name != nil {
			if imp.Name.Name != "_" && imp.Name.Name != "." {
				names[imp.Name.Name] = true
			}
			continue
		}
		if p, err := strconv.Unquote(imp.Path.Value); err == nil {
			names[path.Base(p)] = true
		}
	}
	return names
}

// folder evaluates a constant expression the way the compiler does. Not constant (ok false): a variable, a call, a field, or
// anything else that is not a constant. A constant the gate cannot resolve folds to the unresolved marker.
type folder struct {
	pkg     constTable
	local   constTable
	imports map[string]bool
	depth   int
}

var unresolvedValue = constant.MakeString(unresolved)

var conversions = map[string]bool{
	"string": true, "rune": true, "byte": true, "int": true, "int8": true, "int16": true, "int32": true, "int64": true,
	"uint": true, "uint8": true, "uint16": true, "uint32": true, "uint64": true, "uintptr": true, "float32": true, "float64": true,
}

func (f *folder) fold(e ast.Expr) (string, bool) {
	v, ok := f.eval(e)
	if !ok || v.Kind() != constant.String {
		return "", false
	}
	return constant.StringVal(v), true
}

func (f *folder) eval(e ast.Expr) (v constant.Value, ok bool) {
	defer func() {
		if recover() != nil { // a constant operation the compiler would reject (mixed kinds): not a value the gate can use
			v, ok = unresolvedValue, true
		}
	}()
	if f.depth > 32 {
		return unresolvedValue, true
	}
	switch x := e.(type) {
	case *ast.BasicLit:
		lit := constant.MakeFromLiteral(x.Value, x.Kind, 0)
		return lit, lit.Kind() != constant.Unknown
	case *ast.ParenExpr:
		return f.eval(x.X)
	case *ast.UnaryExpr:
		switch x.Op {
		case token.ADD, token.SUB, token.XOR, token.NOT:
		default:
			return nil, false
		}
		a, ok := f.eval(x.X)
		if !ok {
			return nil, false
		}
		return constant.UnaryOp(x.Op, a, 0), true
	case *ast.BinaryExpr:
		return f.evalBinary(x)
	case *ast.CallExpr:
		return f.evalCall(x)
	case *ast.Ident:
		table := f.table(x.Name)
		if table == nil {
			switch x.Name {
			case "true":
				return constant.MakeBool(true), true
			case "false":
				return constant.MakeBool(false), true
			}
			return nil, false
		}
		exprs := table[x.Name]
		if len(exprs) != 1 {
			return unresolvedValue, true
		}
		child := &folder{pkg: f.pkg, imports: f.imports, depth: f.depth + 1}
		if _, isLocal := f.local[x.Name]; isLocal {
			child.local = f.local
		}
		return child.eval(exprs[0])
	case *ast.SelectorExpr:
		if id, ok := x.X.(*ast.Ident); ok && f.imports[id.Name] && f.table(id.Name) == nil {
			return unresolvedValue, true
		}
	}
	return nil, false
}

func (f *folder) evalBinary(x *ast.BinaryExpr) (constant.Value, bool) {
	a, ok := f.eval(x.X)
	if !ok {
		return nil, false
	}
	b, ok := f.eval(x.Y)
	if !ok {
		return nil, false
	}
	switch x.Op {
	case token.SHL, token.SHR:
		n, exact := constant.Uint64Val(constant.ToInt(b))
		if !exact || n > 1024 {
			return nil, false
		}
		return constant.Shift(constant.ToInt(a), x.Op, uint(n)), true
	case token.EQL, token.NEQ, token.LSS, token.LEQ, token.GTR, token.GEQ:
		return constant.MakeBool(constant.Compare(a, x.Op, b)), true
	case token.QUO:
		if a.Kind() == constant.Int && b.Kind() == constant.Int {
			return constant.BinaryOp(a, token.QUO_ASSIGN, b), true
		}
		return constant.BinaryOp(a, token.QUO, b), true
	case token.LAND, token.LOR, token.ADD, token.SUB, token.MUL, token.REM, token.AND, token.OR, token.XOR, token.AND_NOT:
		return constant.BinaryOp(a, x.Op, b), true
	}
	return nil, false
}

// evalCall evaluates a conversion to a basic type, or len of a constant string, when the name is the predeclared one.
func (f *folder) evalCall(x *ast.CallExpr) (constant.Value, bool) {
	id, isID := x.Fun.(*ast.Ident)
	if !isID || len(x.Args) != 1 || f.table(id.Name) != nil {
		return nil, false
	}
	arg, ok := f.eval(x.Args[0])
	if !ok {
		return nil, false
	}
	switch {
	case id.Name == "len":
		if arg.Kind() != constant.String {
			return nil, false
		}
		text := constant.StringVal(arg)
		if strings.Contains(text, unresolved) {
			return unresolvedValue, true
		}
		return constant.MakeInt64(int64(len(text))), true
	case id.Name == "string":
		switch arg.Kind() {
		case constant.String:
			return arg, true
		case constant.Int:
			n, exact := constant.Int64Val(arg)
			if !exact || n < 0 || n > unicode.MaxRune {
				return constant.MakeString("\uFFFD"), true
			}
			return constant.MakeString(string(rune(n))), true
		}
		return nil, false
	case conversions[id.Name]:
		return arg, true
	}
	return nil, false
}

// table is the scope that declares name: the function's constants first, then the package's; nil when neither does.
func (f *folder) table(name string) constTable {
	if _, ok := f.local[name]; ok {
		return f.local
	}
	if _, ok := f.pkg[name]; ok {
		return f.pkg
	}
	return nil
}

// refsAll lists the references in text as written and with its SQL comments replaced by a space (a comment inside the name
// must not hide it, and a quoted comment marker must not hide what follows it), without duplicates.
func refsAll(text string) []string {
	var out []string
	seen := map[string]bool{}
	for _, t := range []string{text, stripSQLComments(text)} {
		for _, r := range refs(t) {
			if !seen[r] {
				seen[r] = true
				out = append(out, r)
			}
		}
	}
	return out
}

// stripSQLComments replaces /* ... */ (nested, as PostgreSQL reads it) and -- to the end of the line with one space.
func stripSQLComments(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); {
		switch {
		case strings.HasPrefix(s[i:], "/*"):
			depth, j := 1, i+2
			for j < len(s) && depth > 0 {
				switch {
				case strings.HasPrefix(s[j:], "/*"):
					depth++
					j += 2
				case strings.HasPrefix(s[j:], "*/"):
					depth--
					j += 2
				default:
					j++
				}
			}
			b.WriteByte(' ')
			i = j
		case strings.HasPrefix(s[i:], "--"):
			j := i + 2
			for j < len(s) && s[j] != '\n' {
				j++
			}
			b.WriteByte(' ')
			i = j
		default:
			b.WriteByte(s[i])
			i++
		}
	}
	return b.String()
}

// refs lists the references in text to a table that is not in the public schema, as they are written.
func refs(text string) []string {
	var out []string
	for _, m := range tableDot.FindAllStringSubmatchIndex(text, -1) {
		table := strings.ToLower(text[m[2]:m[3]])
		qualifier := ""
		if strings.HasSuffix(text[:m[0]], unresolved) {
			out = append(out, fmt.Sprintf("%s after a constant of another package the gate does not load (refused as unresolved)", strings.ToLower(text[m[2]:m[3]])))
			continue
		}
		if q := qualifierEnd.FindString(text[:m[0]]); q != "" {
			qualifier = strings.TrimSpace(q)
		}
		if strings.Trim(strings.ToLower(qualifier), `"`) == "public" {
			continue
		}
		if qualifier == "" {
			out = append(out, `".`+table+`" with no public qualifier (a schema that is not a constant public, a variable, a call or a field, stands in front of it)`)
			continue
		}
		out = append(out, fmt.Sprintf("%s.%s", strings.Trim(qualifier, `"`), table))
	}
	return out
}
