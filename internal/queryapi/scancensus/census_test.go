// Package scancensus holds the scan-type census of the query API (CHAOS-5866):
// every `rows.Scan(&x, ...)` of a ClickHouse read in internal/queryapi is
// paired with the column types the real server infers for the statement, and
// each pair is scanned by the real driver. See census_integration_test.go.
package scancensus

import (
	"fmt"
	"go/ast"
	"go/constant"
	"go/token"
	"go/types"
	"os"
	"regexp"
	"strconv"
	"strings"

	"golang.org/x/tools/go/packages"
)

// Site is one Scan call of a RowScanner.
type Site struct {
	File, Func string
	Line       int
	// Dests holds the Go type of each Scan argument, package-qualified
	// ("*float64", "**float64", "*time.Time"). A variadic spread (`[]any`) is
	// one entry, "[]any".
	Dests []string
	// Statements holds every maximal constant SQL string of the enclosing
	// function that holds a SELECT.
	Statements []string
	// Source is the enclosing function and every SQL string constant of its file.
	Source string
}

// LoadSites returns every Scan call of a RowScanner in the non-test code of
// the packages matching pattern, resolved from moduleRoot. Each site's
// Statements are the SQL texts its rows can have been read with: the
// statement argument of the Query call whose result the scanner is, evaluated
// through constants, Sprintf, concatenation, local variables and same-module
// functions that return a statement.
func LoadSites(moduleRoot, pattern string) ([]Site, error) {
	cfg := &packages.Config{
		Mode: packages.NeedName | packages.NeedFiles | packages.NeedSyntax | packages.NeedTypes |
			packages.NeedTypesInfo | packages.NeedImports | packages.NeedDeps,
		Dir: moduleRoot,
	}
	pkgs, err := packages.Load(cfg, pattern, "./internal/storage/...")
	if err != nil {
		return nil, err
	}
	r := &resolver{funcs: map[string]*funcRef{}, calls: map[string][]callSite{}, globals: map[string]globalRef{}}
	packages.Visit(pkgs, nil, func(p *packages.Package) {
		for _, f := range p.Syntax {
			for _, d := range f.Decls {
				if gd, ok := d.(*ast.GenDecl); ok {
					for _, spec := range gd.Specs {
						if vs, ok := spec.(*ast.ValueSpec); ok && len(vs.Names) == len(vs.Values) {
							for i, name := range vs.Names {
								if o := p.TypesInfo.Defs[name]; o != nil {
									r.globals[globalKey(o)] = globalRef{p, vs.Values[i]}
								}
							}
						}
					}
				}
				if fd, ok := d.(*ast.FuncDecl); ok && fd.Body != nil {
					if obj := p.TypesInfo.Defs[fd.Name]; obj != nil {
						r.funcs[funcKey(obj)] = &funcRef{pkg: p, decl: fd}
					}
					ast.Inspect(fd.Body, func(n ast.Node) bool {
						if c, ok := n.(*ast.CallExpr); ok {
							var id *ast.Ident
							switch f := c.Fun.(type) {
							case *ast.Ident:
								id = f
							case *ast.SelectorExpr:
								id = f.Sel
							}
							if id != nil {
								if o := p.TypesInfo.ObjectOf(id); o != nil {
									r.calls[funcKey(o)] = append(r.calls[funcKey(o)], callSite{p, fd, c})
								}
							}
						}
						return true
					})
				}
			}
		}
	})
	var out []Site
	for _, p := range pkgs {
		if !strings.Contains(p.PkgPath, "/internal/queryapi") {
			continue
		}
		if len(p.Errors) > 0 {
			return nil, fmt.Errorf("scancensus: package %s: %v", p.PkgPath, p.Errors[0])
		}
		qualifier := func(pkg *types.Package) string { return pkg.Name() }
		for _, f := range p.Syntax {
			for _, d := range f.Decls {
				fd, ok := d.(*ast.FuncDecl)
				if !ok || fd.Body == nil {
					continue
				}
				ast.Inspect(fd.Body, func(n ast.Node) bool {
					call, ok := n.(*ast.CallExpr)
					if !ok {
						return true
					}
					sel, ok := call.Fun.(*ast.SelectorExpr)
					if !ok || sel.Sel.Name != "Scan" {
						return true
					}
					if !scansRows(p, sel.X) {
						return true
					}
					site := Site{Func: fd.Name.Name, Source: siteSource(p, f, fd)}
					pos := p.Fset.Position(call.Pos())
					site.File, site.Line = pos.Filename, pos.Line
					for _, a := range call.Args {
						site.Dests = append(site.Dests, types.TypeString(p.TypesInfo.TypeOf(a), qualifier))
					}
					site.Statements = r.statementsOf(p, fd, sel.X)
					out = append(out, site)
					return true
				})
			}
		}
	}
	return out, nil
}

type funcRef struct {
	pkg  *packages.Package
	decl *ast.FuncDecl
}

type callSite struct {
	pkg  *packages.Package
	decl *ast.FuncDecl
	call *ast.CallExpr
}

type resolver struct {
	// env binds the parameters of the function being evaluated to the values
	// its call passes.
	env   map[types.Object][]string
	funcs map[string]*funcRef
	calls map[string][]callSite
	// globals are the package-level variables and constants with an initializer.
	globals map[string]globalRef
}

type globalRef struct {
	pkg  *packages.Package
	expr ast.Expr
}

// statementsOf finds the Query call whose result is the scanner expression
// and evaluates its statement argument.
func (r *resolver) statementsOf(p *packages.Package, fd *ast.FuncDecl, scanner ast.Expr) []string {
	id, ok := scanner.(*ast.Ident)
	if !ok {
		return nil
	}
	obj := p.TypesInfo.ObjectOf(id)
	var out []string
	if index := paramIndex(p, fd, obj); index >= 0 {
		for _, cs := range r.calls[funcKey(p.TypesInfo.Defs[fd.Name])] {
			if cs.decl != fd && index < len(cs.call.Args) {
				out = append(out, r.statementsOf(cs.pkg, cs.decl, cs.call.Args[index])...)
			}
		}
		return out
	}
	if lit := r.litContext(p, fd, obj); lit != nil {
		return lit
	}
	ast.Inspect(fd.Body, func(n ast.Node) bool {
		as, ok := n.(*ast.AssignStmt)
		if !ok || len(as.Rhs) != 1 {
			return true
		}
		for _, l := range as.Lhs {
			li, ok := l.(*ast.Ident)
			if !ok || p.TypesInfo.ObjectOf(li) != obj {
				continue
			}
			c, ok := as.Rhs[0].(*ast.CallExpr)
			if !ok {
				continue
			}
			for _, a := range c.Args {
				for _, v := range r.eval(p, fd, a, 0) {
					if strings.Contains(strings.ToUpper(v), "SELECT") {
						out = append(out, v)
					}
				}
			}
		}
		return true
	})
	return out
}

const evalDepth = 10

// eval returns the possible string values of e; nil means unknown.
func (r *resolver) eval(p *packages.Package, fd *ast.FuncDecl, e ast.Expr, depth int) []string {
	if depth > evalDepth {
		return nil
	}
	if tv, ok := p.TypesInfo.Types[e]; ok && tv.Value != nil && tv.Value.Kind() == constant.String {
		return []string{constant.StringVal(tv.Value)}
	}
	switch x := e.(type) {
	case *ast.ParenExpr:
		return r.eval(p, fd, x.X, depth)
	case *ast.BinaryExpr:
		if x.Op != token.ADD {
			return nil
		}
		l, rr := r.eval(p, fd, x.X, depth), r.eval(p, fd, x.Y, depth)
		if l == nil && rr == nil {
			return nil
		}
		// An operand that cannot be evaluated is a spliced fragment: the
		// verb stands for it, and SelectListSplices refuses it in a select list.
		if l == nil {
			l = []string{"%s"}
		}
		if rr == nil {
			rr = []string{"%s"}
		}
		var out []string
		for _, a := range l {
			for _, b := range rr {
				out = append(out, a+b)
			}
		}
		return out
	case *ast.CallExpr:
		return r.evalCall(p, fd, x, depth)
	case *ast.Ident:
		obj := p.TypesInfo.ObjectOf(x)
		if obj == nil {
			return nil
		}
		if v, ok := r.env[obj]; ok {
			return v
		}
		if v, ok := obj.(*types.Var); ok && v.Pkg() != nil && v.Parent() == v.Pkg().Scope() {
			if g, ok := r.globals[globalKey(obj)]; ok {
				saved := r.env
				r.env = nil
				defer func() { r.env = saved }()
				return r.eval(g.pkg, nil, g.expr, depth+1)
			}
			return nil
		}
		if fd == nil {
			return nil
		}
		var out []string
		found := false
		ast.Inspect(fd.Body, func(n ast.Node) bool {
			as, ok := n.(*ast.AssignStmt)
			if !ok || as.Tok != token.DEFINE && as.Tok != token.ASSIGN {
				return true
			}
			for i, l := range as.Lhs {
				li, ok := l.(*ast.Ident)
				if !ok || p.TypesInfo.ObjectOf(li) != obj || len(as.Rhs) != len(as.Lhs) {
					continue
				}
				found = true
				v := r.eval(p, fd, as.Rhs[i], depth+1)
				if v == nil {
					out = nil
					found = false
					return false
				}
				out = append(out, v...)
			}
			return true
		})
		if found {
			return out
		}
		return r.evalParam(p, fd, obj, depth)
	}
	return nil
}

// evalParam resolves a parameter of fd to the union of what every caller in
// the module passes for it; one unresolvable caller leaves it unknown.
func (r *resolver) evalParam(p *packages.Package, fd *ast.FuncDecl, obj types.Object, depth int) []string {
	index := paramIndex(p, fd, obj)
	if index < 0 {
		return nil
	}
	fnObj := p.TypesInfo.Defs[fd.Name]
	sites := r.calls[funcKey(fnObj)]
	if len(sites) == 0 {
		return nil
	}
	var out []string
	for _, cs := range sites {
		if cs.decl == fd || index >= len(cs.call.Args) {
			return nil
		}
		v := r.eval(cs.pkg, cs.decl, cs.call.Args[index], depth+1)
		if v == nil {
			return nil
		}
		out = append(out, v...)
	}
	return out
}

func (r *resolver) evalCall(p *packages.Package, fd *ast.FuncDecl, c *ast.CallExpr, depth int) []string {
	var callee types.Object
	switch f := c.Fun.(type) {
	case *ast.SelectorExpr:
		callee = p.TypesInfo.ObjectOf(f.Sel)
	case *ast.Ident:
		callee = p.TypesInfo.ObjectOf(f)
	}
	if callee == nil {
		return nil
	}
	if fn, ok := callee.(*types.Func); ok && fn.Pkg() != nil {
		switch fn.FullName() {
		case "fmt.Sprintf":
			return r.sprintf(p, fd, c, depth)
		case "strings.TrimSpace", "strings.ToUpper", "strings.ToLower":
			return r.eval(p, fd, c.Args[0], depth+1)
		case "strings.TrimSuffix", "strings.TrimPrefix", "strings.ReplaceAll", "strings.Replace":
			return r.eval(p, fd, c.Args[0], depth+1)
		}
	}
	ref, ok := r.funcs[funcKey(callee)]
	if !ok || ref.decl.Type.Results == nil || len(ref.decl.Type.Results.List) != 1 {
		return nil
	}
	var out []string
	ok = true
	saved := r.env
	newEnv := map[types.Object][]string{}
	pi := 0
	for _, field := range ref.decl.Type.Params.List {
		for _, name := range field.Names {
			if pi < len(c.Args) {
				if v := r.eval(p, fd, c.Args[pi], depth+1); v != nil {
					newEnv[ref.pkg.TypesInfo.Defs[name]] = v
				}
			}
			pi++
		}
	}
	r.env = newEnv
	defer func() { r.env = saved }()
	ast.Inspect(ref.decl.Body, func(n ast.Node) bool {
		if _, isLit := n.(*ast.FuncLit); isLit {
			return false
		}
		ret, isRet := n.(*ast.ReturnStmt)
		if !isRet || len(ret.Results) != 1 {
			return true
		}
		v := r.eval(ref.pkg, ref.decl, ret.Results[0], depth+1)
		if v == nil {
			ok = false
			return false
		}
		out = append(out, v...)
		return true
	})
	if !ok {
		return nil
	}
	return out
}

// sprintf evaluates the format and fills each %s with the single value its
// argument evaluates to; an argument that is not a known single string leaves
// the verb for Neutralize.
func (r *resolver) sprintf(p *packages.Package, fd *ast.FuncDecl, c *ast.CallExpr, depth int) []string {
	formats := r.eval(p, fd, c.Args[0], depth+1)
	if formats == nil {
		return nil
	}
	args := make([]string, len(c.Args)-1)
	known := make([]bool, len(args))
	for i, a := range c.Args[1:] {
		if v := r.eval(p, fd, a, depth+1); len(v) == 1 {
			args[i], known[i] = v[0], true
		}
	}
	var out []string
	for _, f := range formats {
		next := 0
		out = append(out, formatVerb.ReplaceAllStringFunc(strings.ReplaceAll(f, "%%", "\x00"), func(m string) string {
			idx := next
			if sub := indexVerb.FindStringSubmatch(m); sub != nil {
				n, _ := strconv.Atoi(sub[1])
				idx = n - 1
			} else {
				next++
			}
			if idx >= 0 && idx < len(args) && known[idx] {
				return args[idx]
			}
			return m
		}))
	}
	for i := range out {
		out[i] = strings.ReplaceAll(out[i], "\x00", "%%")
	}
	return out
}

var indexVerb = regexp.MustCompile(`^%\[(\d+)\]`)

var (
	formatVerb = regexp.MustCompile(`%(?:\[\d+\])?[sdvq]`)
	bindParam  = regexp.MustCompile(`\{[A-Za-z_][A-Za-z0-9_]*:([^{}]*(?:\([^{}]*\))?[^{}]*)\}`)
)

var splice = regexp.MustCompile(`@@[A-Z_]+@@`)

// Neutralize turns a statement template into SQL variants a server can
// DESCRIBE: a Sprintf verb becomes a scope/filter/settings fragment (tried as
// nothing, then as `1`, then as an empty string, because the readers splice
// both optional clauses and operands) and a `{name:Type}` bind becomes the
// type's default value, which keeps every select-list type the server infers
// unchanged.
func Neutralize(statement string) []string {
	s := strings.ReplaceAll(statement, "%%", "\x00")
	s = bindParam.ReplaceAllStringFunc(s, func(m string) string {
		typ := bindParam.FindStringSubmatch(m)[1]
		return "defaultValueOfTypeName('" + strings.ReplaceAll(typ, "'", `\'`) + "')"
	})
	s = digitVerb.ReplaceAllString(s, "1")
	var out []string
	for _, v := range []string{
		fillContextually(s, ""),
		fillContextually(s, "WHERE 1"),
		formatVerb.ReplaceAllString(s, ""),
		formatVerb.ReplaceAllString(s, "1"),
		formatVerb.ReplaceAllString(s, "''"),
	} {
		v = splice.ReplaceAllString(v, "''")
		out = append(out, strings.ReplaceAll(v, "\x00", "%"))
	}
	return out
}

var digitVerb = regexp.MustCompile(`%(?:\[\d+\])?d`)

var operandBefore = regexp.MustCompile(`(?is)(?:\b(?:AND|OR|WHERE|ON|HAVING|NOT|IN|BETWEEN|THEN|ELSE|WHEN|BY)|[=<>(,+*/-])\s*$`)

// fillContextually puts `1` where a verb stands in an operand position (after
// AND, a comparison, an open parenthesis) and nothing where it stands as a
// whole clause (the scope fragments and the trailing SETTINGS).
func fillContextually(s, clause string) string {
	var b strings.Builder
	last := 0
	for _, loc := range formatVerb.FindAllStringIndex(s, -1) {
		b.WriteString(s[last:loc[0]])
		if operandBefore.MatchString(s[:loc[0]]) {
			b.WriteString("1")
		} else {
			b.WriteString(clause)
		}
		last = loc[1]
	}
	b.WriteString(s[last:])
	return b.String()
}

// SelectListSplices reports whether a spliced fragment (an unevaluated Sprintf
// verb) stands in a select-list item before the first top-level FROM, outside
// a condition: such a column has no type the census can name. A verb in a
// WHERE/AND/ON of a subquery filters rows and cannot change a type.
func SelectListSplices(statement string) bool {
	s := strings.ReplaceAll(statement, "%%", "")
	end := len(s)
	depth, inString := 0, false
	for i := 0; i < len(s) && end == len(s); i++ {
		c := s[i]
		switch {
		case inString:
			if c == '\\' {
				i++
			} else if c == '\'' {
				inString = false
			}
		case c == '\'':
			inString = true
		case c == '(':
			depth++
		case c == ')':
			depth--
		case depth == 0 && (c == 'F' || c == 'f') && i+4 <= len(s) && strings.EqualFold(s[i:i+4], "FROM") &&
			(i == 0 || !isIdent(s[i-1])) && (i+4 == len(s) || !isIdent(s[i+4])):
			end = i
		}
	}
	list := s[:end]
	for _, loc := range formatVerb.FindAllStringIndex(list, -1) {
		group := innermostGroup(list[:loc[0]])
		if group == "" || !conditionKeyword.MatchString(withoutGroups(group)) {
			return true
		}
	}
	return false
}

// innermostGroup is the text from the innermost parenthesis still open at the
// end of s, or "" at depth 0.
func innermostGroup(s string) string {
	var open []int
	inString := false
	for i := 0; i < len(s); i++ {
		switch c := s[i]; {
		case inString:
			if c == '\\' {
				i++
			} else if c == '\'' {
				inString = false
			}
		case c == '\'':
			inString = true
		case c == '(':
			open = append(open, i)
		case c == ')' && len(open) > 0:
			open = open[:len(open)-1]
		}
	}
	if len(open) == 0 {
		return ""
	}
	return s[open[len(open)-1]:]
}

// withoutGroups drops every balanced parenthesised group of s, so a WHERE
// inside a closed sibling subquery is not taken for the keyword of the group
// the verb stands in.
func withoutGroups(s string) string {
	var b strings.Builder
	depth := 0
	for i := 1; i < len(s); i++ {
		switch s[i] {
		case '(':
			depth++
		case ')':
			depth--
		default:
			if depth == 0 {
				b.WriteByte(s[i])
			}
		}
	}
	return b.String()
}

var conditionKeyword = regexp.MustCompile(`(?i)\b(WHERE|AND|OR|ON|HAVING|PREWHERE)\b`)

func isIdent(b byte) bool {
	return b == '_' || b >= '0' && b <= '9' || b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z'
}

// funcKey names a function by package path and full name: the same function
// is a different types.Object when seen from a dependent package's export data.
func funcKey(o types.Object) string {
	if fn, ok := o.(*types.Func); ok && fn.Pkg() != nil {
		return fn.Pkg().Path() + "|" + fn.FullName()
	}
	return fmt.Sprintf("%p", o)
}

// paramIndex is the position of obj among fd's parameters, or -1.
func paramIndex(p *packages.Package, fd *ast.FuncDecl, obj types.Object) int {
	i := 0
	for _, field := range fd.Type.Params.List {
		for _, name := range field.Names {
			if p.TypesInfo.Defs[name] == obj {
				return i
			}
			i++
		}
	}
	return -1
}

// litContext handles a scanner that is the parameter of a function literal
// handed to a runner (`run(ctx, name, sql, args, func(rows RowScanner) error)`)
// or kept beside its statement in a table of steps: the statements are the
// SELECT strings of the literal's sibling arguments or elements.
func (r *resolver) litContext(p *packages.Package, fd *ast.FuncDecl, obj types.Object) []string {
	var stack []ast.Node
	var out []string
	found := false
	ast.Inspect(fd.Body, func(n ast.Node) bool {
		if n == nil {
			stack = stack[:len(stack)-1]
			return true
		}
		if lit, ok := n.(*ast.FuncLit); ok && len(stack) > 0 && !found {
			for _, f := range lit.Type.Params.List {
				for _, name := range f.Names {
					if p.TypesInfo.Defs[name] != obj {
						continue
					}
					found = true
					var siblings []ast.Expr
					switch parent := stack[len(stack)-1].(type) {
					case *ast.CallExpr:
						siblings = parent.Args
					case *ast.CompositeLit:
						siblings = parent.Elts
					case *ast.KeyValueExpr:
						if len(stack) > 1 {
							if cl, ok := stack[len(stack)-2].(*ast.CompositeLit); ok {
								for _, e := range cl.Elts {
									if kv, ok := e.(*ast.KeyValueExpr); ok {
										siblings = append(siblings, kv.Value)
									}
								}
							}
						}
					}
					for _, sib := range siblings {
						for _, v := range r.eval(p, fd, sib, 0) {
							if strings.Contains(strings.ToUpper(v), "SELECT") {
								out = append(out, v)
							}
						}
					}
				}
			}
		}
		stack = append(stack, n)
		return true
	})
	return out
}

func globalKey(o types.Object) string { return o.Pkg().Path() + "|" + o.Name() }

// scansRows reports whether the receiver has the method set of a row scanner,
// Scan(...any) error, whatever its declared type is called: a reader that
// declares its own narrow interface is a reader too.
func scansRows(p *packages.Package, recv ast.Expr) bool {
	return hasScanMethod(p.TypesInfo.TypeOf(recv), p.Types)
}

func hasScanMethod(t types.Type, pkg *types.Package) bool {
	if t == nil {
		return false
	}
	obj, _, _ := types.LookupFieldOrMethod(t, true, pkg, "Scan")
	fn, ok := obj.(*types.Func)
	if !ok {
		return false
	}
	sig := fn.Type().(*types.Signature)
	if !sig.Variadic() || sig.Params().Len() != 1 || sig.Results().Len() != 1 {
		return false
	}
	elem, ok := sig.Params().At(0).Type().(*types.Slice)
	if !ok {
		return false
	}
	if iface, ok := elem.Elem().Underlying().(*types.Interface); !ok || iface.NumMethods() != 0 {
		return false
	}
	return sig.Results().At(0).Type().String() == "error"
}

// siteSource is the text of fd and every maximal SQL string constant of its
// file: the statement a pinned site reads is usually a constant beside it, and
// an edit elsewhere in the file leaves the pin alone.
func siteSource(p *packages.Package, f *ast.File, fd *ast.FuncDecl) string {
	var b strings.Builder
	start, end := p.Fset.Position(fd.Pos()), p.Fset.Position(fd.End())
	if data, err := os.ReadFile(start.Filename); err == nil && end.Offset <= len(data) {
		b.Write(data[start.Offset:end.Offset])
	}
	ast.Inspect(f, func(n ast.Node) bool {
		e, ok := n.(ast.Expr)
		if !ok {
			return true
		}
		tv, ok := p.TypesInfo.Types[e]
		if !ok || tv.Value == nil || tv.Value.Kind() != constant.String {
			return true
		}
		if v := constant.StringVal(tv.Value); strings.Contains(strings.ToUpper(v), "SELECT") {
			b.WriteString("\x00")
			b.WriteString(v)
		}
		return false
	})
	return b.String()
}

// pinIdentity is the site a pin line names: file, function and destinations.
func pinIdentity(line string) string {
	parts := strings.Split(line, "|")
	if len(parts) < 3 {
		return line
	}
	return strings.Join(parts[:3], "|")
}

// diffPins compares the pinned lines with the lines the census derived.
// changed are derived lines whose site is pinned under another digest or
// reason; added are derived lines of sites with no pin; stale are pinned lines
// whose site is no longer unverifiable (verified now, or gone).
func diffPins(listed, got []string) (changed, added, stale []string) {
	remaining := map[string]int{}
	for _, l := range listed {
		remaining[l]++
	}
	var unmatched []string
	for _, g := range got {
		if remaining[g] > 0 {
			remaining[g]--
			continue
		}
		unmatched = append(unmatched, g)
	}
	var leftover []string
	for _, l := range listed {
		if remaining[l] > 0 {
			remaining[l]--
			leftover = append(leftover, l)
		}
	}
	byIdentity := map[string]int{}
	for _, l := range leftover {
		byIdentity[pinIdentity(l)]++
	}
	pairedPins := map[string]int{}
	for _, g := range unmatched {
		id := pinIdentity(g)
		if byIdentity[id] > 0 {
			byIdentity[id]--
			pairedPins[id]++
			changed = append(changed, g)
		} else {
			added = append(added, g)
		}
	}
	for _, l := range leftover {
		id := pinIdentity(l)
		if pairedPins[id] > 0 {
			pairedPins[id]--
			continue
		}
		stale = append(stale, l)
	}
	return changed, added, stale
}

// removePins returns listed without one occurrence of each stale line.
func removePins(listed, stale []string) []string {
	drop := map[string]int{}
	for _, s := range stale {
		drop[s]++
	}
	var out []string
	for _, l := range listed {
		if drop[l] > 0 {
			drop[l]--
			continue
		}
		out = append(out, l)
	}
	return out
}
