// Package writerinventory (tests only) finds every Go SQL writer (INSERT, UPDATE, DELETE, TRUNCATE, ON CONFLICT DO UPDATE) of an auth table and compares it with a keyed allowlist (stale entries fail).
//
// The scan cannot see SQL built at runtime (Sprintf, table names from variables, constants imported from another package) or SQL outside non-test Go files.
package writerinventory

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
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

var skipDirs = map[string]bool{
	".git": true, "node_modules": true, "third_party": true, "vendor": true,
	"testdata": true, "src": true, "tests": true,
}

const unresolved = "\x00"

var (
	reInsert   = regexp.MustCompile(`(?i)\binsert\s+into\s+(?:only\s+)?([a-z0-9_."]+)`)
	reUpdate   = regexp.MustCompile(`(?i)\bupdate\s+(?:only\s+)?([a-z0-9_."]+)\s+(?:(?:as\s+)?[a-z0-9_]+\s+)?set\b`)
	reDelete   = regexp.MustCompile(`(?i)\bdelete\s+from\s+(?:only\s+)?([a-z0-9_."]+)`)
	reTruncate = regexp.MustCompile(`(?i)\btruncate\s+(?:table\s+)?(?:only\s+)?([a-z0-9_."]+)`)
	reUpsert   = regexp.MustCompile(`(?i)\bon\s+conflict\b.*?\bdo\s+update\b`)
	reCreate   = regexp.MustCompile(`(?i)create\s+table\s+(?:if\s+not\s+exists\s+)?([a-z0-9_]+)`)
	reSpace    = regexp.MustCompile(`\s+`)
)

// AuthSchemaTables reads the auth-schema table names from the embedded migration lineage.
func AuthSchemaTables(root string) ([]string, error) {
	dir := filepath.Join(root, "internal", "storage", "postgres", "authschema", "migrations")
	files, err := filepath.Glob(filepath.Join(dir, "*.sql"))
	if err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			return nil, err
		}
		for _, m := range reCreate.FindAllStringSubmatch(string(b), -1) {
			seen["auth."+strings.ToLower(m[1])] = true
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

func normTable(raw string) string {
	t := strings.ToLower(strings.ReplaceAll(raw, `"`, ""))
	if !strings.Contains(t, ".") {
		return "public." + t
	}
	return t
}

// sqlWriters returns the (table, verb) pairs one SQL string writes, watched tables only.
func sqlWriters(sql string, watched map[string]bool) [][2]string {
	sql = reSpace.ReplaceAllString(sql, " ")
	var out [][2]string
	add := func(re *regexp.Regexp, verb string) {
		for _, m := range re.FindAllStringSubmatchIndex(sql, -1) {
			t := normTable(sql[m[2]:m[3]])
			if !watched[t] {
				continue
			}
			v := verb
			if verb == "INSERT" {
				// an ON CONFLICT ... DO UPDATE in the rest of this statement makes it an upsert
				rest := sql[m[1]:]
				if i := strings.Index(strings.ToLower(rest), "insert into"); i >= 0 {
					rest = rest[:i]
				}
				if reUpsert.MatchString(rest) {
					v = "UPSERT"
				}
			}
			out = append(out, [2]string{t, v})
		}
	}
	add(reInsert, "INSERT")
	add(reUpdate, "UPDATE")
	add(reDelete, "DELETE")
	add(reTruncate, "TRUNCATE")
	return out
}

type pkgDecls struct {
	vals map[string]ast.Expr
	used map[string]bool
}

type evaluator struct {
	pkg      *pkgDecls
	local    map[string]ast.Expr
	inFunc   bool
	visiting map[string]bool
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
		if v, ok := e.local[n.Name]; ok && !e.visiting[n.Name] {
			e.visiting[n.Name] = true
			defer delete(e.visiting, n.Name)
			return e.eval(v)
		}
		if v, ok := e.pkg.vals[n.Name]; ok && !e.visiting[n.Name] {
			if e.inFunc {
				e.pkg.used[n.Name] = true
			}
			e.visiting[n.Name] = true
			defer delete(e.visiting, n.Name)
			return e.eval(v)
		}
	}
	return unresolved
}

func collectDecls(gd *ast.GenDecl, into map[string]ast.Expr) {
	if gd.Tok != token.CONST && gd.Tok != token.VAR {
		return
	}
	for _, sp := range gd.Specs {
		vs := sp.(*ast.ValueSpec)
		if len(vs.Names) != len(vs.Values) {
			continue
		}
		for i, nm := range vs.Names {
			into[nm.Name] = vs.Values[i]
		}
	}
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

// isStringExpr reports whether x is a literal, a concatenation, or a name of one; other package-level values (maps, structs) are keyed on the declaration.
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

// walkStrings calls fn for each maximal string expression under root.
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
				return false
			}
		case *ast.Ident:
			if v, ok := ev.local[x.Name]; ok {
				if !ev.visiting[x.Name] {
					ev.visiting[x.Name] = true
					walkStrings(v, ev, fn)
					delete(ev.visiting, x.Name)
				}
			} else if v, ok := ev.pkg.vals[x.Name]; ok && isStringExpr(v) && !ev.visiting[x.Name] {
				if ev.inFunc {
					ev.pkg.used[x.Name] = true
				}
				ev.visiting[x.Name] = true
				walkStrings(v, ev, fn)
				delete(ev.visiting, x.Name)
			}
		}
		return true
	})
}

// Scan walks root and returns every writer of a watched table in non-test Go files.
func Scan(root string, watched map[string]bool) ([]Writer, error) {
	byDir := map[string][]string{}
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if p != root && (skipDirs[d.Name()] || strings.HasPrefix(d.Name(), ".")) {
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
		pkg := &pkgDecls{vals: map[string]ast.Expr{}, used: map[string]bool{}}
		parsed := map[string]*ast.File{}
		for _, f := range files {
			af, err := parser.ParseFile(fset, f, nil, parser.SkipObjectResolution)
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
				local := map[string]ast.Expr{}
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
				if !ok || (gd.Tok != token.CONST && gd.Tok != token.VAR) {
					continue
				}
				for _, sp := range gd.Specs {
					vs := sp.(*ast.ValueSpec)
					if len(vs.Names) != len(vs.Values) {
						continue
					}
					for i, nm := range vs.Names {
						if pkg.used[nm.Name] {
							continue
						}
						ev := &evaluator{pkg: pkg, visiting: map[string]bool{nm.Name: true}}
						walkStrings(vs.Values[i], ev, func(s string) { record(rel, "pkgdecl:"+nm.Name, s) })
					}
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
