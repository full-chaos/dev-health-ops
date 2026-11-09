package passwordpolicy

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"io/fs"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"golang.org/x/tools/go/packages"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/passwordwriters"
)

// typedScope is the code the census type-checks. A writer outside it, or an
// exported function on a path referenced by name outside it, fails the cell.
var typedScope = []string{"./internal/api/...", "./internal/apiservice/...", "./internal/auth/...", "./internal/testsupport/..."}

// A notPasswordSetting row ends a path that writes a password-hash column but
// sets no password a person chose. Kinds: "writer" (the function holding the SQL),
// "edge" (one call of a writer: caller -> callee), "uncalled" (a function with no
// non-test caller; the row fails once it has one). at is a file:line inside fn
// (for an edge: the call line inside caller). A row that matches nothing fails.
type notPasswordSetting struct {
	kind, fn, callee, at, why string
}

const mod = "github.com/full-chaos/dev-health-ops/"

var reviewed = []notPasswordSetting{
	{kind: "writer", fn: mod + "internal/api/session.handlers.createSocialUser", at: "internal/api/session/social.go:223",
		why: "the INSERT writes the literal NULL as password_hash"},
	{kind: "edge", fn: mod + "internal/auth/identity.Store.provisionFederated", callee: mod + "internal/auth/identity.insertPublicUser",
		at: "internal/auth/identity/identities.go:358", why: "federatedPublicUser (publicusers.go:122) sets no PasswordHash"},
	{kind: "writer", fn: mod + "internal/auth/identity.Store.insertBackfill", at: "internal/auth/identity/backfill.go:289",
		why: "copies the verifier usersbackfill read from public.users (usersbackfill/source.go:41); no new password"},
	{kind: "writer", fn: mod + "internal/auth/identity.Store.applyBackfillCredentials", at: "internal/auth/identity/backfill.go:329",
		why: "copies the verifier usersbackfill read from public.users (usersbackfill/source.go:41); no new password"},
	{kind: "writer", fn: mod + "internal/testsupport/sessionscenario.Seed", at: "internal/testsupport/sessionscenario/sessionscenario.go:183",
		why: "test fixture seed: takes *testing.T and a fixed hash"},
	{kind: "uncalled", fn: mod + "internal/auth/identity.Store.SetPasswordHash", at: "internal/auth/identity/local.go:188",
		why: "only tests call it; a non-test caller must be walked"},
}

// Every path that writes a user's password hash (the shared census,
// internal/testsupport/passwordwriters, public.users and auth.users) reaches
// passwordpolicy.ValidateForHash: walking up from each writing function through
// its callers (go/types references, method values and interface methods it
// implements included), every path ends at a function that calls ValidateForHash,
// or a no-receiver function that does, or at a reviewed row.
func TestEveryPasswordSettingPathCallsTheSharedPolicy(t *testing.T) {
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatal(err)
	}
	res := censusPasswordPaths(root, typedScope, reviewed)
	for _, p := range res.problems {
		t.Error(p)
	}
	for _, p := range res.paths {
		t.Log(p)
	}
}

// A load that fails, or type errors in a package, fail the census: it never
// walks a partial graph.
func TestPasswordPathCensusFailsOnABrokenLoad(t *testing.T) {
	root, err := filepath.Abs("testdata/brokenload")
	if err != nil {
		t.Fatal(err)
	}
	res := censusPasswordPaths(root, []string{"./..."}, nil)
	t.Log(res.problems)
	if len(res.paths) != 0 || !hasPrefix(res.problems, "load: brokenload: ") {
		t.Fatalf("a package with a type error gave paths %v, problems %v; want a load problem and no path", res.paths, res.problems)
	}
	res = censusPasswordPaths(root, []string{"./nosuchdir/..."}, nil)
	t.Log(res.problems)
	if !hasPrefix(res.problems, "load: ") {
		t.Fatalf("a pattern that loads nothing gave problems %v; want a load problem", res.problems)
	}
}

func hasPrefix(lines []string, prefix string) bool {
	for _, l := range lines {
		if strings.HasPrefix(l, prefix) {
			return true
		}
	}
	return false
}

type censusResult struct {
	problems []string
	paths    []string
}

type pathFunc struct {
	key    string
	name   string
	recv   bool
	file   string
	lo, hi int
	refs   map[string]bool // every function it references
	calls  map[string]bool // every function it calls
	ifaces []ifaceRef      // interface methods it references
}

type ifaceRef struct {
	name   string
	iface  *types.Interface
	caller string
}

const policyKey = mod + "internal/auth/passwordpolicy.ValidateForHash"

func funcKey(f *types.Func) string {
	f = f.Origin()
	sig, _ := f.Type().(*types.Signature)
	if sig == nil || sig.Recv() == nil {
		if f.Pkg() == nil {
			return f.Name()
		}
		return f.Pkg().Path() + "." + f.Name()
	}
	t := sig.Recv().Type()
	if p, ok := t.(*types.Pointer); ok {
		t = p.Elem()
	}
	if n, ok := t.(*types.Named); ok {
		n = n.Origin()
		if n.Obj().Pkg() != nil {
			return n.Obj().Pkg().Path() + "." + n.Obj().Name() + "." + f.Name()
		}
	}
	return types.TypeString(t, nil) + "." + f.Name()
}

func censusPasswordPaths(root string, scope []string, rows []notPasswordSetting) (res censusResult) {
	fail := func(format string, args ...any) { res.problems = append(res.problems, fmt.Sprintf(format, args...)) }
	sites, scanned, err := passwordwriters.Sites(root)
	if err != nil {
		fail("census: %v", err)
	}
	loaded, err := packages.Load(&packages.Config{
		Mode: packages.NeedName | packages.NeedFiles | packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo,
		Dir:  root,
	}, scope...)
	if err != nil {
		fail("load: %v", err)
		return res
	}
	if len(loaded) == 0 {
		fail("load: %v matched no package", scope)
		return res
	}
	for _, pkg := range loaded {
		for _, e := range pkg.Errors {
			fail("load: %s: %v", pkg.PkgPath, e)
		}
		if pkg.TypesInfo == nil || pkg.Types == nil {
			fail("load: %s: no type information", pkg.PkgPath)
		}
	}
	if len(res.problems) > 0 {
		return res
	}
	if scanned < 100 || len(sites) == 0 {
		fail("census: scanned %d Go files, found %d password-hash writes; the census walked too little of the module", scanned, len(sites))
	}

	funcs := map[string]*pathFunc{}
	byFile := map[string][]*pathFunc{}
	inScope := map[string]bool{}
	for _, pkg := range loaded {
		for _, file := range pkg.Syntax {
			rel, err := filepath.Rel(root, pkg.Fset.Position(file.Pos()).Filename)
			if err != nil {
				fail("load: %v", err)
				continue
			}
			rel = filepath.ToSlash(rel)
			inScope[rel] = true
			for _, decl := range file.Decls {
				pf := &pathFunc{refs: map[string]bool{}, calls: map[string]bool{}, file: rel}
				var body ast.Node = decl
				if fd, ok := decl.(*ast.FuncDecl); ok {
					obj, _ := pkg.TypesInfo.Defs[fd.Name].(*types.Func)
					if obj == nil || fd.Body == nil {
						continue
					}
					pf.key, pf.name, pf.recv = funcKey(obj), obj.Name(), fd.Recv != nil
					body = fd.Body
				} else {
					// A reference from a package-level declaration is a caller with no caller of its own.
					pf.key, pf.name = pkg.PkgPath+".<package-level "+rel+">", "<package-level>"
				}
				pf.lo, pf.hi = pkg.Fset.Position(decl.Pos()).Line, pkg.Fset.Position(decl.End()).Line
				ast.Inspect(body, func(n ast.Node) bool {
					switch x := n.(type) {
					case *ast.Ident:
						f, ok := pkg.TypesInfo.Uses[x].(*types.Func)
						if !ok {
							return true
						}
						pf.refs[funcKey(f)] = true
						if sig, _ := f.Type().(*types.Signature); sig != nil && sig.Recv() != nil {
							if it, ok := sig.Recv().Type().Underlying().(*types.Interface); ok {
								pf.ifaces = append(pf.ifaces, ifaceRef{name: f.Name(), iface: it, caller: pf.key})
							}
						}
					case *ast.CallExpr:
						id, _ := x.Fun.(*ast.Ident)
						if sel, ok := x.Fun.(*ast.SelectorExpr); ok {
							id = sel.Sel
						}
						if idx, ok := x.Fun.(*ast.IndexExpr); ok {
							id, _ = idx.X.(*ast.Ident)
						}
						if id != nil {
							if f, ok := pkg.TypesInfo.Uses[id].(*types.Func); ok {
								pf.calls[funcKey(f)] = true
							}
						}
					}
					return true
				})
				if prev, ok := funcs[pf.key]; ok && prev.name == "<package-level>" {
					for k := range pf.refs {
						prev.refs[k] = true
					}
					prev.ifaces = append(prev.ifaces, pf.ifaces...)
				} else {
					funcs[pf.key] = pf
				}
				byFile[rel] = append(byFile[rel], pf)
			}
		}
	}
	if _, ok := funcs[policyKey]; !ok {
		fail("load: %s is not in the typed scope %v", policyKey, scope)
		return res
	}

	methods := map[string][]*types.Func{}
	for _, pkg := range loaded {
		sc := pkg.Types.Scope()
		for _, name := range sc.Names() {
			tn, ok := sc.Lookup(name).(*types.TypeName)
			if !ok || tn.IsAlias() {
				continue
			}
			if _, ok := tn.Type().Underlying().(*types.Interface); ok {
				continue
			}
			ms := types.NewMethodSet(types.NewPointer(tn.Type()))
			for i := 0; i < ms.Len(); i++ {
				f := ms.At(i).Obj().(*types.Func)
				methods[f.Name()] = append(methods[f.Name()], f)
			}
		}
	}
	callers := map[string][]string{}
	for _, pf := range funcs {
		for r := range pf.refs {
			callers[r] = append(callers[r], pf.key)
		}
	}
	for _, pf := range funcs {
		for _, ir := range pf.ifaces {
			for _, m := range methods[ir.name] {
				recv := m.Type().(*types.Signature).Recv().Type()
				if implementsByName(recv, ir.iface) {
					callers[funcKey(m)] = append(callers[funcKey(m)], ir.caller)
				}
			}
		}
	}
	for k := range callers {
		sort.Strings(callers[k])
		callers[k] = dedupe(callers[k])
	}

	direct := func(key string) bool { pf := funcs[key]; return pf != nil && pf.calls[policyKey] }
	guard := func(key string) string {
		pf := funcs[key]
		if pf == nil {
			return ""
		}
		if pf.calls[policyKey] {
			return "ValidateForHash"
		}
		var via []string
		for c := range pf.calls {
			if h := funcs[c]; h != nil && !h.recv && h.name != "<package-level>" && direct(c) {
				via = append(via, h.name)
			}
		}
		sort.Strings(via)
		return strings.Join(via, ",")
	}

	used := make([]bool, len(rows))
	rowAt := func(i int, fn string) {
		r := rows[i]
		file, line, ok := splitAt(r.at)
		pf := funcs[fn]
		if !ok || pf == nil || pf.file != file || line < pf.lo || line > pf.hi {
			fail("reviewed row %s %s: at %s is not inside %s", r.kind, r.fn, r.at, fn)
		}
	}
	writerRow := func(key string) bool {
		for i, r := range rows {
			if r.kind == "writer" && r.fn == key {
				used[i] = true
				rowAt(i, key)
				return true
			}
		}
		return false
	}
	edgeRow := func(caller, callee string) bool {
		for i, r := range rows {
			if r.kind == "edge" && r.fn == caller && r.callee == callee {
				used[i] = true
				rowAt(i, caller)
				return true
			}
		}
		return false
	}
	uncalledRow := func(key string) (int, bool) {
		for i, r := range rows {
			if r.kind == "uncalled" && r.fn == key {
				return i, true
			}
		}
		return 0, false
	}

	var outside map[string][]string
	outsideRefs := func(name string) []string {
		if outside == nil {
			outside = namesOutside(root, inScope, &res)
		}
		return outside[name]
	}

	writers := map[string]bool{}
	for _, s := range sites {
		if !inScope[s.File] {
			fail("%s:%d writes %s.users.password_hash outside the typed scope %v: widen typedScope", s.File, s.Line, s.Table, scope)
			continue
		}
		var at *pathFunc
		for _, pf := range byFile[s.File] {
			if s.Line >= pf.lo && s.Line <= pf.hi {
				at = pf
			}
		}
		if at == nil || at.name == "<package-level>" {
			fail("%s:%d writes %s.users.password_hash outside a function", s.File, s.Line, s.Table)
			continue
		}
		writers[at.key] = true
	}
	if len(writers) == 0 {
		fail("census: no function writes a password hash; the census found nothing to walk")
	}
	keys := make([]string, 0, len(writers))
	for k := range writers {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	guarded := 0
	for _, w := range keys {
		if writerRow(w) {
			res.paths = append(res.paths, w+" [reviewed: not password-setting]")
			continue
		}
		seen := map[string]bool{}
		var walk func(key string, chain []string)
		walk = func(key string, chain []string) {
			chain = append(chain, key)
			if seen[key] {
				return
			}
			seen[key] = true
			if g := guard(key); g != "" {
				guarded++
				res.paths = append(res.paths, strings.Join(chain, " <- ")+" ["+g+"]")
				return
			}
			pf := funcs[key]
			if pf != nil && pf.name != "<package-level>" && token.IsExported(pf.name) {
				for _, ref := range outsideRefs(pf.name) {
					fail("%s is on a password path and %s names %s outside the typed scope: widen typedScope", key, ref, pf.name)
				}
			}
			var up []string
			for _, c := range callers[key] {
				if c != key && !edgeRow(c, key) {
					up = append(up, c)
				}
			}
			if i, ok := uncalledRow(key); ok {
				if len(up) == 0 {
					used[i] = true
					rowAt(i, key)
					res.paths = append(res.paths, strings.Join(chain, " <- ")+" [reviewed: uncalled]")
					return
				}
				used[i] = true
				fail("reviewed row uncalled %s: it has callers %v now; remove the row", key, up)
			}
			if len(up) == 0 {
				fail("password path reaches no ValidateForHash call: %s", strings.Join(chain, " <- "))
				return
			}
			for _, c := range up {
				walk(c, chain)
			}
		}
		walk(w, nil)
	}
	if len(writers) > 0 && guarded == 0 {
		fail("census: no password path reached ValidateForHash; the walk is broken")
	}
	for i, r := range rows {
		if !used[i] {
			fail("reviewed row %s %s (%s) matches no path: remove it", r.kind, r.fn, r.at)
		}
	}
	return res
}

// implementsByName reports whether recv (or *recv) has every method of iface with
// the same name and signature text, so it does not depend on object identity
// between packages loaded from source and from export data.
func implementsByName(recv types.Type, iface *types.Interface) bool {
	if p, ok := recv.(*types.Pointer); ok {
		recv = p.Elem()
	}
	ms := types.NewMethodSet(types.NewPointer(recv))
	q := func(p *types.Package) string { return p.Path() }
	for i := 0; i < iface.NumMethods(); i++ {
		m := iface.Method(i)
		sel := ms.Lookup(m.Pkg(), m.Name())
		if sel == nil {
			return false
		}
		if shape(sel.Obj().Type().(*types.Signature), q) != shape(m.Type().(*types.Signature), q) {
			return false
		}
	}
	return iface.NumMethods() > 0
}

// shape is a signature's parameter and result types, without names.
func shape(sig *types.Signature, q types.Qualifier) string {
	var b strings.Builder
	for _, tuple := range []*types.Tuple{sig.Params(), sig.Results()} {
		b.WriteString("(")
		for i := 0; i < tuple.Len(); i++ {
			b.WriteString(types.TypeString(tuple.At(i).Type(), q) + ",")
		}
		b.WriteString(")")
	}
	if sig.Variadic() {
		b.WriteString("...")
	}
	return b.String()
}

// namesOutside maps each identifier name to the file:line sites that name it in the
// module's non-test Go files outside the typed scope.
func namesOutside(root string, inScope map[string]bool, res *censusResult) map[string][]string {
	out := map[string][]string{}
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if name := d.Name(); path != root && (strings.HasPrefix(name, ".") || name == "node_modules" || name == "vendor" || name == "testdata") {
				return filepath.SkipDir
			}
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if !strings.HasSuffix(rel, ".go") || strings.HasSuffix(rel, "_test.go") || inScope[rel] {
			return nil
		}
		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
		if err != nil {
			return err
		}
		ast.Inspect(file, func(n ast.Node) bool {
			if id, ok := n.(*ast.Ident); ok {
				out[id.Name] = append(out[id.Name], fmt.Sprintf("%s:%d", rel, fset.Position(id.Pos()).Line))
			}
			return true
		})
		return nil
	})
	if err != nil {
		res.problems = append(res.problems, fmt.Sprintf("census: %v", err))
	}
	return out
}

func splitAt(at string) (string, int, bool) {
	i := strings.LastIndex(at, ":")
	if i < 0 {
		return "", 0, false
	}
	var line int
	if _, err := fmt.Sscanf(at[i+1:], "%d", &line); err != nil {
		return "", 0, false
	}
	return at[:i], line, true
}

func dedupe(s []string) []string {
	out := s[:0]
	for i, v := range s {
		if i == 0 || v != s[i-1] {
			out = append(out, v)
		}
	}
	return out
}
