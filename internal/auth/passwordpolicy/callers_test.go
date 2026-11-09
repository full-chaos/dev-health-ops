package passwordpolicy

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"golang.org/x/tools/go/packages"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/passwordwriters"
)

// typedScope is the code the census type-checks. A writer outside it, or an
// exported function on a path that a selector outside it names, fails the cell.
var typedScope = []string{"./internal/api/...", "./internal/apiservice/...", "./internal/auth/...", "./internal/testsupport/..."}

// A notPasswordSetting row ends a path that writes a password-hash column but
// sets no password a person chose. Kinds: "writer" (the function holding the SQL),
// "edge" (one call of a writer: caller -> callee), "uncalled" (a function with no
// non-test caller; the row fails once it has one). at is a file:line inside fn
// (for an edge: the call line inside caller). A row that matches nothing fails.
//
// Kind "hop" ends at fn, an auth-service handler that go-api reaches over internal
// HTTP: the walk cannot follow the request, so it goes on from every function
// outside internal/auth that names the route constant (a hopRoutes entry). That
// set must equal callers (sorted function keys; empty while no go-api caller
// exists), and each of them must reach a guard.
type notPasswordSetting struct {
	kind, fn, callee, at, why string
	route                     string
	callers                   []string
}

// A hopRoute is an internal route that carries a password hash from go-api to
// auth-service. Outside internal/auth its path is named only through constant
// (pkgpath.Name); its literal anywhere else fails TestHopRouteLiteralsOnlyThroughTheirConstant.
type hopRoute struct {
	path, constant string
}

var hopRoutes = []hopRoute{
	{path: "/internal/v1/users/create", constant: mod + "internal/api/authprovision.PathUsersCreate"},
	{path: "/internal/v1/users/password", constant: mod + "internal/api/authprovision.PathUsersPassword"},
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
	{kind: "hop", fn: mod + "internal/auth/authapi.provisioningHandlers.usersCreate", at: "internal/auth/authapi/provisioning_users.go:275",
		route: "/internal/v1/users/create", why: "internal HTTP: go-api posts the hash after its own policy check"},
	{kind: "hop", fn: mod + "internal/auth/authapi.provisioningHandlers.usersPassword", at: "internal/auth/authapi/provisioning_users.go:410",
		route: "/internal/v1/users/password", why: "internal HTTP: go-api posts the hash after its own policy check"},
}

// Every path that writes a user's password hash (the shared census matcher,
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
	res := censusPasswordPaths(root, typedScope, reviewed, hopRoutes, 100)
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
	// Written at run time: a checked-in module that does not type-check would fail every vet of the repository's modules.
	root := t.TempDir()
	for name, src := range map[string]string{
		"go.mod":    "module brokenload\n\ngo 1.22\n",
		"broken.go": "package brokenload\n\nvar Broken int = \"not an int\"\n",
	} {
		if err := os.WriteFile(filepath.Join(root, name), []byte(src), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	res := censusPasswordPaths(root, []string{"./..."}, nil, nil, 1)
	t.Log(res.problems)
	if len(res.paths) != 0 || !hasPrefix(res.problems, "load: brokenload: ") {
		t.Fatalf("a package with a type error gave paths %v, problems %v; want a load problem and no path", res.paths, res.problems)
	}
	res = censusPasswordPaths(root, []string{"./nosuchdir/..."}, nil, nil, 1)
	t.Log(res.problems)
	if !hasPrefix(res.problems, "load: ") {
		t.Fatalf("a pattern that loads nothing gave problems %v; want a load problem", res.problems)
	}
}

// Outside internal/auth a hop route path is written only as its constant's value:
// a literal copy would reach auth-service with no caller the hop rows can see.
func TestHopRouteLiteralsOnlyThroughTheirConstant(t *testing.T) {
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatal(err)
	}
	problems, scanned := hopLiterals(root, hopRoutes)
	if scanned < 100 {
		t.Fatalf("scanned %d Go files outside internal/auth; the scan walked too little of the module", scanned)
	}
	for _, p := range problems {
		t.Error(p)
	}
}

// The hop-row mechanism on a fixture module (testdata/hopfixture): a go-api
// handler checks the policy and posts through the route constant; auth-service's
// handler for that route writes the hash. Each case adds one file to a copy.
func TestPasswordPathCensusHopRows(t *testing.T) {
	createRow := notPasswordSetting{kind: "hop", fn: mod + "internal/auth/authapi.handlers.usersCreate", at: "internal/auth/authapi/routes.go:15",
		route: "/internal/v1/users/create", callers: []string{mod + "internal/api/authprovision.PostCreate"}, why: "internal HTTP"}
	passwordRow := notPasswordSetting{kind: "hop", fn: mod + "internal/auth/authapi.handlers.usersPassword", at: "internal/auth/authapi/routes.go:17",
		route: "/internal/v1/users/password", callers: []string{mod + "internal/api/authprovision.PostPassword"}, why: "internal HTTP"}
	both := []notPasswordSetting{createRow, passwordRow}
	noCreateCaller, ghostCaller, dormantPassword := createRow, createRow, passwordRow
	noCreateCaller.callers = nil
	dormantPassword.callers = nil
	ghostCaller.callers = []string{mod + "internal/api/authprovision.PostCreate", mod + "internal/api/authprovision.postGhost"}
	cases := []struct {
		name   string
		files  map[string]string // written over the copy
		remove []string
		rows   []notPasswordSetting
		want   string // "" = no problem
	}{
		{name: "both hops pinned", rows: both},
		{name: "no go-api caller yet, pinned empty", rows: []notPasswordSetting{noCreateCaller, dormantPassword},
			remove: []string{"internal/apiservice/admin/users.go"},
			files: map[string]string{
				"internal/api/authprovision/authprovision.go": "package authprovision\n\nconst (\n\tPathUsersCreate   = \"/internal/v1/users/create\"\n" +
					"\tPathUsersPassword = \"/internal/v1/users/password\"\n)\n",
				// a guarded writer keeps one known-good path, so the walk is not vacuous
				"internal/auth/identity/local.go": "package identity\n\nimport \"github.com/full-chaos/dev-health-ops/internal/auth/passwordpolicy\"\n\n" +
					"func changeLocal(x Exec, pw string) {\n\tif len(passwordpolicy.ValidateForHash(pw)) > 0 {\n\t\treturn\n\t}\n" +
					"\tx(`UPDATE public.users SET password_hash = $2, token_version = token_version + 1 WHERE id = $1`, pw)\n}\n"}},
		{name: "no hop rows", rows: nil, want: "password path reaches no ValidateForHash call: " + mod + "internal/auth/identity.InsertUser"},
		{name: "pinned set misses a caller", rows: []notPasswordSetting{noCreateCaller, passwordRow},
			want: "reviewed row hop " + mod + "internal/auth/authapi.handlers.usersCreate: functions outside internal/auth that name"},
		{name: "pinned set names an absent caller", rows: []notPasswordSetting{ghostCaller, passwordRow},
			want: "reviewed row hop " + mod + "internal/auth/authapi.handlers.usersCreate: functions outside internal/auth that name"},
		{name: "unguarded caller of the route client", rows: both, files: map[string]string{"internal/apiservice/admin/force.go": "package admin\n\n" +
			"import \"github.com/full-chaos/dev-health-ops/internal/api/authprovision\"\n\nfunc forceSet(pw string) { authprovision.PostPassword(pw) }\n"},
			want: "password path reaches no ValidateForHash call: " + mod + "internal/auth/identity.SetPassword"},
		{name: "new caller names the constant", rows: both, files: map[string]string{"internal/api/authprovision/other.go": "package authprovision\n\n" +
			"func postOther(hash string) { post(PathUsersPassword, hash) }\n"},
			want: "reviewed row hop " + mod + "internal/auth/authapi.handlers.usersPassword: functions outside internal/auth that name"},
		{name: "the only pinned caller is absent", rows: []notPasswordSetting{noCreateCaller, passwordRow},
			remove: []string{"internal/apiservice/admin/users.go"},
			files: map[string]string{
				"internal/api/authprovision/authprovision.go": "package authprovision\n\nconst (\n\tPathUsersCreate   = \"/internal/v1/users/create\"\n" +
					"\tPathUsersPassword = \"/internal/v1/users/password\"\n)\n",
				"internal/auth/identity/local.go": "package identity\n\nimport \"github.com/full-chaos/dev-health-ops/internal/auth/passwordpolicy\"\n\n" +
					"func changeLocal(x Exec, pw string) {\n\tif len(passwordpolicy.ValidateForHash(pw)) > 0 {\n\t\treturn\n\t}\n" +
					"\tx(`UPDATE public.users SET password_hash = $2, token_version = token_version + 1 WHERE id = $1`, pw)\n}\n"},
			want: "reviewed row hop " + mod + "internal/auth/authapi.handlers.usersPassword: functions outside internal/auth that name " +
				mod + "internal/api/authprovision.PathUsersPassword = [], want [" + mod + "internal/api/authprovision.PostPassword]"},
		{name: "the pinned caller has only an unguarded path", rows: both,
			files: map[string]string{
				"internal/apiservice/admin/users.go": "package admin\n\nimport (\n\t\"github.com/full-chaos/dev-health-ops/internal/api/authprovision\"\n" +
					"\t\"github.com/full-chaos/dev-health-ops/internal/auth/passwordpolicy\"\n)\n\n" +
					"func createUser(password string) {\n\tif len(passwordpolicy.ValidateForHash(password)) > 0 {\n\t\treturn\n\t}\n\tauthprovision.PostCreate(password)\n}\n\n" +
					"func forceSet(password string) { authprovision.PostPassword(password) }\n"},
			want: "password path reaches no ValidateForHash call: " + mod + "internal/auth/identity.SetPassword <- " + mod +
				"internal/auth/authapi.handlers.usersPassword <- <HTTP /internal/v1/users/password> <- " + mod +
				"internal/api/authprovision.PostPassword <- " + mod + "internal/apiservice/admin.forceSet"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			root := copyFixture(t, "testdata/hopfixture")
			for _, f := range c.remove {
				if err := os.Remove(filepath.Join(root, filepath.FromSlash(f))); err != nil {
					t.Fatal(err)
				}
			}
			for f, src := range c.files {
				if err := os.WriteFile(filepath.Join(root, filepath.FromSlash(f)), []byte(src), 0o644); err != nil {
					t.Fatal(err)
				}
			}
			res := censusPasswordPaths(root, []string{"./..."}, c.rows, fixtureRoutes, 1)
			t.Log(res.problems)
			switch {
			case c.want == "" && len(res.problems) > 0:
				t.Fatalf("problems %v, want none", res.problems)
			case c.want == "" && len(res.paths) == 0:
				t.Fatal("no path walked")
			case c.want != "" && !hasPrefix(res.problems, c.want):
				t.Fatalf("problems %v, want one that starts %q", res.problems, c.want)
			}
		})
	}
	t.Run("route literal outside internal/auth", func(t *testing.T) {
		root := copyFixture(t, "testdata/hopfixture")
		if problems, _ := hopLiterals(root, fixtureRoutes); len(problems) != 0 {
			t.Fatalf("the fixture as is: %v, want none", problems)
		}
		// a constant of the same name in another package is not the route's own declaration
		src := "package admin\n\nconst PathUsersPassword = \"/internal/v1/users/password\"\n"
		if err := os.WriteFile(filepath.Join(root, "internal", "apiservice", "admin", "raw.go"), []byte(src), 0o644); err != nil {
			t.Fatal(err)
		}
		problems, _ := hopLiterals(root, fixtureRoutes)
		t.Log(problems)
		if !hasPrefix(problems, "internal/apiservice/admin/raw.go:3 names the route /internal/v1/users/password as a literal") {
			t.Fatalf("problems %v, want the raw.go literal", problems)
		}
	})
}

var fixtureRoutes = []hopRoute{
	{path: "/internal/v1/users/create", constant: mod + "internal/api/authprovision.PathUsersCreate"},
	{path: "/internal/v1/users/password", constant: mod + "internal/api/authprovision.PathUsersPassword"},
}

// copyFixture copies the fixture module dir into a new temporary directory. The
// fixture's go.mod is checked in as go.mod.fixture so the repository holds no
// nested module; the copy gets it back as go.mod.
func copyFixture(t *testing.T, dir string) string {
	t.Helper()
	dst := t.TempDir()
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}
		if d.IsDir() {
			return os.MkdirAll(filepath.Join(dst, rel), 0o755)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if filepath.Base(rel) == "go.mod.fixture" {
			rel = filepath.Join(filepath.Dir(rel), "go.mod")
		}
		return os.WriteFile(filepath.Join(dst, rel), data, 0o644)
	})
	if err != nil {
		t.Fatal(err)
	}
	return dst
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
	consts map[string]bool // every constant it names
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

func censusPasswordPaths(root string, scope []string, rows []notPasswordSetting, routes []hopRoute, minFiles int) (res censusResult) {
	fail := func(format string, args ...any) { res.problems = append(res.problems, fmt.Sprintf(format, args...)) }
	sites, scanned, err := writerSites(root)
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
	if scanned < minFiles || len(sites) == 0 {
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
				pf := &pathFunc{refs: map[string]bool{}, consts: map[string]bool{}, calls: map[string]bool{}, file: rel}
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
						if c, ok := pkg.TypesInfo.Uses[x].(*types.Const); ok && c.Pkg() != nil {
							pf.consts[c.Pkg().Path()+"."+c.Name()] = true
						}
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
					for k := range pf.consts {
						prev.consts[k] = true
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
	routeConst := map[string]string{}
	for _, r := range routes {
		routeConst[r.path] = r.constant
	}
	hopRow := func(key string) (int, bool) {
		for i, r := range rows {
			if r.kind == "hop" && r.fn == key {
				return i, true
			}
		}
		return 0, false
	}
	// hopCallers are the functions outside internal/auth that name constant.
	hopCallers := func(constant string) []string {
		var out []string
		for k, pf := range funcs {
			if pf.consts[constant] && !strings.HasPrefix(k, mod+"internal/auth/") {
				out = append(out, k)
			}
		}
		sort.Strings(out)
		return out
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
	checkedOutside := map[string]bool{}
	outsideRefs := func(pf *pathFunc) []string {
		if outside == nil {
			outside = refsOutside(root, inScope, &res)
		}
		if pf.recv {
			return outside["."+pf.name]
		}
		return outside[pf.key]
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
			if i, ok := hopRow(key); ok {
				r := rows[i]
				used[i] = true
				rowAt(i, key)
				constant, known := routeConst[r.route]
				if !known {
					fail("reviewed row hop %s: route %q is not a hopRoutes entry", key, r.route)
					return
				}
				want := append([]string(nil), r.callers...)
				sort.Strings(want)
				found := hopCallers(constant)
				if strings.Join(found, ",") != strings.Join(want, ",") {
					fail("reviewed row hop %s: functions outside internal/auth that name %s = %v, want %v", key, constant, found, want)
				}
				if len(found) == 0 {
					res.paths = append(res.paths, strings.Join(chain, " <- ")+" [hop "+r.route+": no caller]")
				}
				for _, c := range found {
					walk(c, append(chain, "<HTTP "+r.route+">"))
				}
				return
			}
			pf := funcs[key]
			if pf != nil && pf.name != "<package-level>" && token.IsExported(pf.name) && !checkedOutside[key] {
				checkedOutside[key] = true
				if refs := outsideRefs(pf); len(refs) > 0 {
					fail("%s is on a password path and %d site(s) outside the typed scope name it (%s): widen typedScope", key, len(refs), strings.Join(refs[:min(3, len(refs))], ", "))
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

// hopLiterals reports every string literal in root's non-test Go files outside
// internal/auth that holds a hop route path, other than the value of that route's
// own constant declaration.
func hopLiterals(root string, routes []hopRoute) (problems []string, scanned int) {
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
		if !strings.HasSuffix(rel, ".go") || strings.HasSuffix(rel, "_test.go") || strings.HasPrefix(rel, "internal/auth/") {
			return nil
		}
		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
		if err != nil {
			return err
		}
		scanned++
		own := map[ast.Node]bool{}
		for _, decl := range file.Decls {
			gd, ok := decl.(*ast.GenDecl)
			if !ok || gd.Tok != token.CONST {
				continue
			}
			for _, spec := range gd.Specs {
				vs := spec.(*ast.ValueSpec)
				for i, name := range vs.Names {
					for _, r := range routes {
						if i < len(vs.Values) && r.constant == mod+filepath.ToSlash(filepath.Dir(rel))+"."+name.Name {
							own[vs.Values[i]] = true
						}
					}
				}
			}
		}
		ast.Inspect(file, func(n ast.Node) bool {
			lit, ok := n.(*ast.BasicLit)
			if !ok || lit.Kind != token.STRING || own[lit] {
				return true
			}
			text, err := strconv.Unquote(lit.Value)
			if err != nil {
				return true
			}
			for _, r := range routes {
				if strings.Contains(text, r.path) {
					problems = append(problems, fmt.Sprintf("%s:%d names the route %s as a literal: use %s", rel, fset.Position(lit.Pos()).Line, r.path, r.constant))
				}
			}
			return true
		})
		return nil
	})
	if err != nil {
		problems = append(problems, fmt.Sprintf("census: %v", err))
	}
	return problems, scanned
}

// writerSites runs the shared password-writer census over root's non-test Go
// files (dot dirs, node_modules, vendor and testdata skipped).
func writerSites(root string) (sites []passwordwriters.Site, scanned int, err error) {
	err = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if name := d.Name(); path != root && (strings.HasPrefix(name, ".") || name == "node_modules" || name == "vendor" || name == "testdata") {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		scanned++
		sites = append(sites, passwordwriters.FileSites(fset, file, filepath.ToSlash(rel))...)
		return nil
	})
	return sites, scanned, err
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

// refsOutside maps the selectors of the module's non-test Go files outside the
// typed scope to their file:line sites: "<import path>.<name>" for a package
// selector, ".<name>" for every selector (a method can be reached from any value).
func refsOutside(root string, inScope map[string]bool, res *censusResult) map[string][]string {
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
		imports := map[string]string{}
		for _, spec := range file.Imports {
			ipath, err := strconv.Unquote(spec.Path.Value)
			if err != nil {
				return err
			}
			name := ipath[strings.LastIndex(ipath, "/")+1:]
			if spec.Name != nil {
				name = spec.Name.Name
			}
			imports[name] = ipath
		}
		ast.Inspect(file, func(n ast.Node) bool {
			sel, ok := n.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			at := fmt.Sprintf("%s:%d", rel, fset.Position(sel.Sel.Pos()).Line)
			out["."+sel.Sel.Name] = append(out["."+sel.Sel.Name], at)
			if x, ok := sel.X.(*ast.Ident); ok && imports[x.Name] != "" {
				key := imports[x.Name] + "." + sel.Sel.Name
				out[key] = append(out[key], at)
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
