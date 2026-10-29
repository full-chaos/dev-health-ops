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
// "auth", a Sprintf verb (%s), or no visible qualifier at all (a literal that starts with ".memberships" because the schema is
// concatenated in front). Case and spaces around the dot do not matter. A chain is folded when every operand is a literal, a
// string conversion, or a constant of the same package (declared in any file of the directory, in a grouped block, typed or
// untyped, built from other constants) or of the enclosing function; a constant of ANOTHER package is not loaded, and a table
// name right behind one is refused by name as unresolved. There is no allow list: a read of those tables has no legitimate
// place in these packages.
//
// NOT covered, stated: code outside Scopes (the shared storage packages, internal/auth/**, which is auth-service and holds
// the writes), an unqualified `memberships` that a search_path resolves to the auth schema (the query-api and go-api
// logins pin their search_path and the posture test pins their grants), SQL assembled from a variable or a call between
// the fragments (those operands are not folded; the fragments are scanned one by one), and _test.go files.
package membershipreads

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
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
// file that does not parse is an error: a gate that skipped it would scan nothing. A string expression is folded when
// every operand is a literal, a string conversion, or a constant of the same package (any file of the directory; typed or
// untyped; in a grouped block; built from other constants) or of the enclosing function.
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
				ast.Inspect(decl, func(n ast.Node) bool {
					text, ok := "", false
					switch e := n.(type) {
					case *ast.BasicLit:
						text, ok = fl.fold(e)
					case *ast.BinaryExpr:
						text, ok = fl.fold(e)
					default:
						return true
					}
					if !ok {
						return true
					}
					for _, ref := range refs(text) {
						findings = append(findings, fmt.Sprintf("%s:%d: membership-read: %s code names %s: go-api and query-api read public.memberships and public.organizations only; auth-service writes both copies in one transaction (D6358, CHAOS-10985)",
							pf.rel, pf.fset.Position(n.Pos()).Line, scope.Service, ref))
					}
					return false
				})
			}
		}
	}
	sort.Strings(findings)
	return findings, nil
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

// folder folds a string expression to its text. Not folded (ok false): a variable, a call, a field, or anything that is
// not a constant string. A constant the gate cannot resolve folds to the unresolved marker.
type folder struct {
	pkg     constTable
	local   constTable
	imports map[string]bool
	depth   int
}

func (f *folder) fold(e ast.Expr) (string, bool) {
	if f.depth > 32 {
		return unresolved, true
	}
	switch x := e.(type) {
	case *ast.BasicLit:
		if x.Kind != token.STRING {
			return "", false
		}
		s, err := strconv.Unquote(x.Value)
		return s, err == nil
	case *ast.ParenExpr:
		return f.fold(x.X)
	case *ast.BinaryExpr:
		if x.Op != token.ADD {
			return "", false
		}
		left, ok := f.fold(x.X)
		if !ok {
			return "", false
		}
		right, ok := f.fold(x.Y)
		if !ok {
			return "", false
		}
		return left + right, true
	case *ast.CallExpr:
		if id, ok := x.Fun.(*ast.Ident); ok && id.Name == "string" && len(x.Args) == 1 && f.table(id.Name) == nil {
			return f.fold(x.Args[0])
		}
	case *ast.Ident:
		table := f.table(x.Name)
		if table == nil {
			return "", false
		}
		exprs := table[x.Name]
		if len(exprs) != 1 {
			return unresolved, true
		}
		child := &folder{pkg: f.pkg, imports: f.imports, depth: f.depth + 1}
		if _, isLocal := f.local[x.Name]; isLocal {
			child.local = f.local
		}
		return child.fold(exprs[0])
	case *ast.SelectorExpr:
		if id, ok := x.X.(*ast.Ident); ok && f.imports[id.Name] && f.table(id.Name) == nil {
			return unresolved, true
		}
	}
	return "", false
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
			out = append(out, `".`+table+`" with no public qualifier (the schema is concatenated in front of it)`)
			continue
		}
		out = append(out, fmt.Sprintf("%s.%s", strings.Trim(qualifier, `"`), table))
	}
	return out
}
