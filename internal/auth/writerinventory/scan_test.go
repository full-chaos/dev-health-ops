// Package writerinventory (tests only) finds every Go SQL writer (INSERT, UPDATE, DELETE, TRUNCATE, ON CONFLICT DO UPDATE) of an auth table and compares it with a keyed allowlist (stale entries fail).
//
// The scan cannot see SQL built at runtime (Sprintf, table names from variables, constants imported from another package), SQL joined to a map field with a runtime suffix beyond the field text, or SQL outside non-test Go files of the root module.
package writerinventory

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// Writer is one SQL writer. Key() is the allowlist key.
type Writer struct {
	File  string // slash path relative to the scan root
	Func  string // "Name", "Recv.Name", or "pkgdecl:NAME" for an unreferenced package-level SQL constant
	Table string // "public.users", "auth.sessions"
	Verb  string // INSERT, UPSERT, UPDATE, DELETE, TRUNCATE
}

func (w Writer) Key() string {
	return w.File + "|" + w.Func + "|" + w.Table + "|" + w.Verb
}

var publicTables = []string{
	"users", "refresh_tokens", "email_verification_tokens", "password_reset_tokens",
	"login_attempts", "org_invites", "memberships", "sso_providers", "impersonation_sessions",
}

const unresolved = "\x00"

// tok is one SQL token: a lowercase word, a quoted identifier (q, case kept), a string literal ("'", q), or one punctuation rune.
type tok struct {
	s string
	q bool
}

func isWordRune(r byte) bool {
	return r == '_' || r == '$' || (r >= '0' && r <= '9') || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || r >= 0x80
}

// sqlTokens lexes SQL: comments (nested /* */ and --) are dropped, string literals collapse to one token, quoted identifiers keep their case.
func sqlTokens(sql string) []tok {
	var out []tok
	for i := 0; i < len(sql); {
		c := sql[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f':
			i++
		case c == '-' && i+1 < len(sql) && sql[i+1] == '-':
			for i < len(sql) && sql[i] != '\n' {
				i++
			}
		case c == '/' && i+1 < len(sql) && sql[i+1] == '*':
			depth := 0
			for i < len(sql) {
				if strings.HasPrefix(sql[i:], "/*") {
					depth++
					i += 2
				} else if strings.HasPrefix(sql[i:], "*/") {
					depth--
					i += 2
					if depth == 0 {
						break
					}
				} else {
					i++
				}
			}
		case c == '\'':
			escapes := i > 0 && (sql[i-1] == 'e' || sql[i-1] == 'E')
			i++
			for i < len(sql) {
				if escapes && sql[i] == '\\' {
					i += 2
					continue
				}
				if sql[i] == '\'' {
					if i+1 < len(sql) && sql[i+1] == '\'' {
						i += 2
						continue
					}
					i++
					break
				}
				i++
			}
			out = append(out, tok{s: "'", q: true})
		case c == '"':
			i++
			var b strings.Builder
			for i < len(sql) {
				if sql[i] == '"' {
					if i+1 < len(sql) && sql[i+1] == '"' {
						b.WriteByte('"')
						i += 2
						continue
					}
					i++
					break
				}
				b.WriteByte(sql[i])
				i++
			}
			out = append(out, tok{s: b.String(), q: true})
		case isWordRune(c):
			j := i
			for j < len(sql) && isWordRune(sql[j]) {
				j++
			}
			out = append(out, tok{s: strings.ToLower(sql[i:j])})
			i = j
		default:
			out = append(out, tok{s: string(c)})
			i++
		}
	}
	return out
}

func kw(ts []tok, i int, w string) bool {
	return i >= 0 && i < len(ts) && !ts[i].q && ts[i].s == w
}

func isIdent(t tok) bool {
	if t.q {
		return t.s != "'"
	}
	return t.s != "" && (t.s[0] == '_' || (t.s[0] >= 'a' && t.s[0] <= 'z'))
}

// qname reads ident or ident.ident at i and returns the schema ("" if none), name and the next index.
func qname(ts []tok, i int) (schema, name string, next int, ok bool) {
	if i >= len(ts) || !isIdent(ts[i]) {
		return "", "", i, false
	}
	if i+2 < len(ts) && ts[i+1].s == "." && !ts[i+1].q && isIdent(ts[i+2]) {
		return ts[i].s, ts[i+2].s, i + 3, true
	}
	return "", ts[i].s, i + 1, true
}

func qualified(schema, name string) string {
	if schema == "" {
		schema = "public"
	}
	return schema + "." + name
}

// sqlWriters returns the (table, verb) pairs one SQL string writes, watched tables only.
func sqlWriters(sql string, watched map[string]bool) [][2]string {
	ts := sqlTokens(sql)
	var out [][2]string
	add := func(schema, name, verb string) {
		if t := qualified(schema, name); watched[t] {
			out = append(out, [2]string{t, verb})
		}
	}
	skipOnly := func(i int) int {
		if kw(ts, i, "only") {
			return i + 1
		}
		return i
	}
	for i := range ts {
		if ts[i].q {
			continue
		}
		switch ts[i].s {
		case "insert":
			if !kw(ts, i+1, "into") {
				continue
			}
			schema, name, j, ok := qname(ts, skipOnly(i+2))
			if !ok {
				continue
			}
			verb := "INSERT"
			conflict := false
			for k := j; k < len(ts); k++ {
				if kw(ts, k, ";") || (kw(ts, k, "insert") && kw(ts, k+1, "into")) {
					break
				}
				if kw(ts, k, "on") && kw(ts, k+1, "conflict") {
					conflict = true
				}
				if conflict && kw(ts, k, "do") && kw(ts, k+1, "update") {
					verb = "UPSERT"
					break
				}
			}
			add(schema, name, verb)
		case "update":
			if kw(ts, i-1, "do") || kw(ts, i-1, "for") {
				continue
			}
			schema, name, j, ok := qname(ts, skipOnly(i+1))
			if !ok {
				continue
			}
			if kw(ts, j, "as") {
				j++
			}
			if !kw(ts, j, "set") && j < len(ts) && isIdent(ts[j]) {
				j++
			}
			if kw(ts, j, "set") {
				add(schema, name, "UPDATE")
			}
		case "delete":
			if !kw(ts, i+1, "from") {
				continue
			}
			if schema, name, _, ok := qname(ts, skipOnly(i+2)); ok {
				add(schema, name, "DELETE")
			}
		case "truncate":
			j := i + 1
			if kw(ts, j, "table") {
				j++
			}
			for {
				schema, name, next, ok := qname(ts, skipOnly(j))
				if !ok {
					break
				}
				add(schema, name, "TRUNCATE")
				if !kw(ts, next, ",") {
					break
				}
				j = next + 1
			}
		}
	}
	return out
}

// AuthSchemaTables reads the auth-schema table names from the embedded migration lineage.
func AuthSchemaTables(root string) ([]string, error) {
	dir := filepath.Join(root, "internal", "storage", "postgres", "authschema", "migrations")
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	for _, e := range entries {
		if e.IsDir() || !strings.EqualFold(filepath.Ext(e.Name()), ".sql") {
			continue
		}
		b, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			return nil, err
		}
		ts := sqlTokens(string(b))
		for i := range ts {
			if !kw(ts, i, "create") {
				continue
			}
			j := i + 1
			if kw(ts, j, "unlogged") || kw(ts, j, "temporary") || kw(ts, j, "temp") {
				j++
			}
			if !kw(ts, j, "table") {
				continue
			}
			j++
			if kw(ts, j, "if") && kw(ts, j+1, "not") && kw(ts, j+2, "exists") {
				j += 3
			}
			if schema, name, _, ok := qname(ts, j); ok && (schema == "" || schema == "auth") {
				seen["auth."+name] = true
			}
		}
	}
	out := make([]string, 0, len(seen))
	for t := range seen {
		out = append(out, t)
	}
	sort.Strings(out)
	return out, nil
}

// Watched returns the set of watched table names, schema-qualified.
func Watched(root string) (map[string]bool, error) {
	auth, err := AuthSchemaTables(root)
	if err != nil {
		return nil, err
	}
	w := map[string]bool{}
	for _, t := range auth {
		w[t] = true
	}
	for _, t := range publicTables {
		w["public."+t] = true
	}
	return w, nil
}

type decl struct {
	spec *ast.ValueSpec
	expr ast.Expr
}

type pkgDecls struct {
	vals map[string]decl
	used map[string]bool
}

type evaluator struct {
	pkg      *pkgDecls
	local    map[string]decl
	inFunc   bool
	visiting map[string]bool
}

// lookup resolves an identifier lexically: a use whose parser object is a different declaration (a parameter, a shadowing local) is not the package-level or local constant.
func (e *evaluator) lookup(id *ast.Ident) (d decl, local, ok bool) {
	if id.Obj != nil {
		spec, isSpec := id.Obj.Decl.(*ast.ValueSpec)
		if !isSpec {
			return decl{}, false, false
		}
		if l, found := e.local[id.Name]; found && l.spec == spec {
			return l, true, true
		}
		if p, found := e.pkg.vals[id.Name]; found && p.spec == spec {
			return p, false, true
		}
		return decl{}, false, false
	}
	if p, found := e.pkg.vals[id.Name]; found {
		return p, false, true
	}
	return decl{}, false, false
}

func (e *evaluator) eval(x ast.Expr) string {
	switch n := x.(type) {
	case *ast.BasicLit:
		if n.Kind == token.STRING {
			if s, err := strconv.Unquote(n.Value); err == nil {
				return s
			}
		}
	case *ast.ParenExpr:
		return e.eval(n.X)
	case *ast.BinaryExpr:
		if n.Op == token.ADD {
			return e.eval(n.X) + e.eval(n.Y)
		}
	case *ast.Ident:
		if d, isLocal, ok := e.lookup(n); ok && !e.visiting[n.Name] {
			if !isLocal && e.inFunc {
				e.pkg.used[n.Name] = true
			}
			e.visiting[n.Name] = true
			defer delete(e.visiting, n.Name)
			return e.eval(d.expr)
		}
	}
	return unresolved
}

// specPairs yields (name, spec, value) for each spec of a const or var block; a const spec with no values repeats the previous values.
func specPairs(gd *ast.GenDecl) (names []string, specs []*ast.ValueSpec, values []ast.Expr) {
	if gd.Tok != token.CONST && gd.Tok != token.VAR {
		return nil, nil, nil
	}
	var prev []ast.Expr
	for _, sp := range gd.Specs {
		vs := sp.(*ast.ValueSpec)
		vals := vs.Values
		if len(vals) == 0 && gd.Tok == token.CONST {
			vals = prev
		} else {
			prev = vals
		}
		if len(vs.Names) != len(vals) {
			continue
		}
		for i, nm := range vs.Names {
			names = append(names, nm.Name)
			specs = append(specs, vs)
			values = append(values, vals[i])
		}
	}
	return names, specs, values
}

func collectDecls(gd *ast.GenDecl, into map[string]decl) {
	names, specs, values := specPairs(gd)
	for i, n := range names {
		into[n] = decl{spec: specs[i], expr: values[i]}
	}
}

func isStringExpr(x ast.Expr) bool {
	switch n := x.(type) {
	case *ast.BasicLit, *ast.Ident:
		return true
	case *ast.ParenExpr:
		return isStringExpr(n.X)
	case *ast.BinaryExpr:
		return n.Op == token.ADD
	}
	return false
}

func funcName(fd *ast.FuncDecl) string {
	if fd.Recv == nil || len(fd.Recv.List) == 0 {
		return fd.Name.Name
	}
	t := fd.Recv.List[0].Type
	for {
		switch n := t.(type) {
		case *ast.StarExpr:
			t = n.X
			continue
		case *ast.IndexExpr:
			t = n.X
			continue
		case *ast.IndexListExpr:
			t = n.X
			continue
		case *ast.Ident:
			return n.Name + "." + fd.Name.Name
		}
		return "?." + fd.Name.Name
	}
}

// walkStrings calls fn for each maximal string expression under root; a sum is both evaluated whole and walked into, so a map field inside it is still attributed.
func walkStrings(root ast.Node, ev *evaluator, fn func(string)) {
	ast.Inspect(root, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.BasicLit:
			if x.Kind == token.STRING {
				fn(ev.eval(x))
			}
			return false
		case *ast.BinaryExpr:
			if x.Op == token.ADD {
				fn(ev.eval(x))
			}
		case *ast.SelectorExpr:
			// X[key].field on a package-level map or struct: only the keyed field values are this function's SQL
			base := x.X
			for {
				if ix, ok := base.(*ast.IndexExpr); ok {
					base = ix.X
				} else if pe, ok := base.(*ast.ParenExpr); ok {
					base = pe.X
				} else {
					break
				}
			}
			id, ok := base.(*ast.Ident)
			if !ok {
				return true
			}
			d, isLocal, ok := ev.lookup(id)
			if !ok || isStringExpr(d.expr) {
				return true
			}
			var found bool
			ast.Inspect(d.expr, func(m ast.Node) bool {
				if kv, ok := m.(*ast.KeyValueExpr); ok {
					if k, ok := kv.Key.(*ast.Ident); ok && k.Name == x.Sel.Name {
						found = true
						walkStrings(kv.Value, ev, fn)
						return false
					}
				}
				return true
			})
			if !found {
				return true
			}
			if ev.inFunc && !isLocal {
				ev.pkg.used[id.Name] = true
			}
			return false
		case *ast.Ident:
			d, isLocal, ok := ev.lookup(x)
			if !ok || ev.visiting[x.Name] {
				return true
			}
			if !isLocal && ev.inFunc {
				ev.pkg.used[x.Name] = true
			}
			ev.visiting[x.Name] = true
			walkStrings(d.expr, ev, fn)
			delete(ev.visiting, x.Name)
		}
		return true
	})
}

func skipDir(path, name string) bool {
	switch name {
	case ".git", "node_modules", "vendor", "testdata":
		return true
	}
	if strings.HasPrefix(name, ".") || strings.HasPrefix(name, "_") {
		return true
	}
	_, err := os.Stat(filepath.Join(path, "go.mod"))
	return err == nil
}

// Scan walks root and returns every writer of a watched table in non-test Go files of the root module (nested modules, vendor and testdata are skipped).
func Scan(root string, watched map[string]bool) ([]Writer, error) {
	byDir := map[string][]string{}
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if p != root && skipDir(p, d.Name()) {
				return filepath.SkipDir
			}
			return nil
		}
		if strings.HasSuffix(p, ".go") && !strings.HasSuffix(p, "_test.go") {
			byDir[filepath.Dir(p)] = append(byDir[filepath.Dir(p)], p)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	seen := map[string]Writer{}
	record := func(file, fn, sql string) {
		for _, tv := range sqlWriters(sql, watched) {
			w := Writer{File: file, Func: fn, Table: tv[0], Verb: tv[1]}
			seen[w.Key()] = w
		}
	}
	fset := token.NewFileSet()
	for _, files := range byDir {
		sort.Strings(files)
		pkg := &pkgDecls{vals: map[string]decl{}, used: map[string]bool{}}
		parsed := map[string]*ast.File{}
		for _, f := range files {
			af, err := parser.ParseFile(fset, f, nil, 0)
			if err != nil {
				return nil, err
			}
			parsed[f] = af
			for _, d := range af.Decls {
				if gd, ok := d.(*ast.GenDecl); ok {
					collectDecls(gd, pkg.vals)
				}
			}
		}
		for _, f := range files {
			rel, _ := filepath.Rel(root, f)
			rel = filepath.ToSlash(rel)
			for _, d := range parsed[f].Decls {
				fd, ok := d.(*ast.FuncDecl)
				if !ok || fd.Body == nil {
					continue
				}
				local := map[string]decl{}
				ast.Inspect(fd.Body, func(n ast.Node) bool {
					if ds, ok := n.(*ast.DeclStmt); ok {
						if gd, ok := ds.Decl.(*ast.GenDecl); ok {
							collectDecls(gd, local)
						}
					}
					return true
				})
				ev := &evaluator{pkg: pkg, local: local, inFunc: true, visiting: map[string]bool{}}
				name := funcName(fd)
				walkStrings(fd.Body, ev, func(s string) { record(rel, name, s) })
			}
		}
		for _, f := range files {
			rel, _ := filepath.Rel(root, f)
			rel = filepath.ToSlash(rel)
			for _, d := range parsed[f].Decls {
				gd, ok := d.(*ast.GenDecl)
				if !ok {
					continue
				}
				names, _, values := specPairs(gd)
				for i, name := range names {
					if pkg.used[name] {
						continue
					}
					ev := &evaluator{pkg: pkg, visiting: map[string]bool{name: true}}
					walkStrings(values[i], ev, func(s string) { record(rel, "pkgdecl:"+name, s) })
				}
			}
		}
	}
	out := make([]Writer, 0, len(seen))
	for _, w := range seen {
		out = append(out, w)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key() < out[j].Key() })
	return out, nil
}
