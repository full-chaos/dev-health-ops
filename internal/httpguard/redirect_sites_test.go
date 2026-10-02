package httpguard

import (
	"bufio"
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"golang.org/x/tools/go/packages"
)

var updateSites = flag.Bool("update-sites", false, "rewrite redirect_sites.tsv from production code (new rows are UNCLASSIFIED and fail the test until classified)")

// The row set of redirect_sites.tsv is DERIVED from production code, by TYPE (go/packages + go/types), never written by
// hand and never matched by spelling: an import alias, a dot import or a type alias cannot hide a site. A SITE is
//
//	client-literal        a composite literal (elided ones too) whose type is net/http.Client
//	client-new            new(net/http.Client)
//	client-decl           a var, field (embedded too), parameter or result DECLARED with the non-pointer type net/http.Client
//	client-type           a defined type or an alias whose underlying type is net/http.Client's
//	default-client        any use of net/http.DefaultClient
//	http.Get|Post|PostForm|Head   any reference to those package-level functions (a call or a method value)
//	external-client-call  a call to a function OUTSIDE this module whose result holds a net/http.Client (oauth2.NewClient)
//	checkredirect-assign  an assignment to the CheckRedirect field of a net/http.Client
//
// Every site needs exactly one row per (file, symbol, kind) with the same count; an unlisted site, a stale row, a count
// mismatch, an unclassified row, a row without a cite or a probe that does not exist FAILS. Regenerate the row set with
//
//	go test ./internal/httpguard -run TestEveryHTTPClientSiteIsClassified -args -update-sites
//
// then classify the new rows. Every walker clause has a planted fixture package under testdata/redirectsites that the
// clause must find (TestTheSiteWalkerFindsEveryKind): a clause removed is RED.
//
// A row names its PROBE, "Test" or "dir:Test" (a test of the package of the row, or of the dir named). The probe must exist,
// and its class must agree with it: guarded and never-follows name a test that exercises a redirect (its body mentions
// redirect), drops-credential names a test that asserts on Authorization, no-credential names none ("-") and its cite says
// what travels. A class changed without a probe changed fails here; whether the probe is RED without the guard is shown
// by the planted guard-off runs recorded in the PR, not by this test.
//
// NOT covered, stated and not claimed: a client made inside a dependency (oauth2.Config.Client, an SDK's own client); a
// production file excluded by a build tag of the default build; the semantic content of a cite.
//
// Classes:
//
//	guarded          the callee that sends the credential wraps its client with httpguard (or the provider origin guard).
//	never-follows    the client is built with its own no-redirect policy, or refuses redirects.
//	drops-credential the client follows, and the credential header is dropped off-origin (DropCredentialsOnHostChange).
//	no-credential    the client follows and nothing credential-bearing travels in header, query or body (cite what).
type site struct {
	file, symbol, kind string
}

var siteClasses = map[string]bool{"guarded": true, "never-follows": true, "drops-credential": true, "no-credential": true}

type row struct {
	count              int
	class, probe, cite string
}

const (
	modulePath    = "github.com/full-chaos/dev-health-ops"
	fixturesBase  = modulePath + "/internal/httpguard/testdata/redirectsites"
	rowFile       = "redirect_sites.tsv"
	moduleRootRel = "../.."
)

func TestEveryHTTPClientSiteIsClassified(t *testing.T) {
	root, err := filepath.Abs(moduleRootRel)
	if err != nil {
		t.Fatal(err)
	}
	found, problems := scanPackages(t, root, []string{"./..."}, func(path string) bool {
		return strings.HasPrefix(path, modulePath+"/internal/testsupport")
	})
	rows := readRows(t, rowFile)
	if *updateSites {
		writeRows(t, rowFile, found, rows)
		rows = readRows(t, rowFile)
	}
	problems = append(problems, compareSites(found, rows, root)...)
	sort.Strings(problems)
	for _, problem := range problems {
		t.Error(problem)
	}
	t.Logf("%d sites, %d rows, %d problems", len(found), len(rows), len(problems))
}

// compareSites is the verdict: every defect of the row set against the found sites, as sorted text.
func compareSites(found map[site]int, rows map[site]row, root string) []string {
	var problems []string
	for key, count := range found {
		got, ok := rows[key]
		switch {
		case !ok:
			problems = append(problems, fmt.Sprintf("UNLISTED site: %s %s %s (x%d)", key.file, key.symbol, key.kind, count))
			continue
		case got.count != count:
			problems = append(problems, fmt.Sprintf("COUNT differs: %s %s %s: table %d, code %d", key.file, key.symbol, key.kind, got.count, count))
		case !siteClasses[got.class]:
			problems = append(problems, fmt.Sprintf("UNCLASSIFIED: %s %s %s (class %q)", key.file, key.symbol, key.kind, got.class))
			continue
		case strings.TrimSpace(got.cite) == "":
			problems = append(problems, fmt.Sprintf("NO CITE: %s %s %s", key.file, key.symbol, key.kind))
		}
		if problem := checkProbe(root, key, got); problem != "" {
			problems = append(problems, problem)
		}
	}
	for key := range rows {
		if _, ok := found[key]; !ok {
			problems = append(problems, fmt.Sprintf("STALE row: %s %s %s", key.file, key.symbol, key.kind))
		}
	}
	sort.Strings(problems)
	return problems
}

// checkProbe: the probe named by the row exists, and agrees with the class.
func checkProbe(root string, key site, got row) string {
	label := fmt.Sprintf("%s %s %s", key.file, key.symbol, key.kind)
	if got.class == "no-credential" {
		if got.probe != "-" {
			return "PROBE on a no-credential row (must be -): " + label
		}
		return ""
	}
	dir, name := filepath.Dir(key.file), got.probe
	if i := strings.Index(got.probe, ":"); i >= 0 {
		dir, name = got.probe[:i], got.probe[i+1:]
	}
	body, ok := testBody(filepath.Join(root, dir), name)
	if !ok {
		return fmt.Sprintf("PROBE missing: %s names %s in %s", label, name, dir)
	}
	switch got.class {
	case "drops-credential":
		if !strings.Contains(body, "Authorization") {
			return fmt.Sprintf("PROBE does not assert on Authorization: %s (%s)", label, name)
		}
	default:
		if !strings.Contains(strings.ToLower(body), "redirect") {
			return fmt.Sprintf("PROBE does not exercise a redirect: %s (%s)", label, name)
		}
	}
	return ""
}

// testBody is the source text of the Test function name in the _test.go files of dir.
func testBody(dir, name string) (string, bool) {
	files, _ := filepath.Glob(filepath.Join(dir, "*_test.go"))
	for _, path := range files {
		source, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, path, source, 0)
		if err != nil {
			continue
		}
		for _, declaration := range file.Decls {
			if fn, ok := declaration.(*ast.FuncDecl); ok && fn.Recv == nil && fn.Name.Name == name && strings.HasPrefix(name, "Test") && fn.Body != nil {
				return string(source[fset.Position(fn.Body.Pos()).Offset:fset.Position(fn.Body.End()).Offset]), true
			}
		}
	}
	return "", false
}

// scanPackages loads the packages matching patterns (non-test files only) under root and returns their sites by
// (file, symbol, kind) with the count, and the problems of loading.
func scanPackages(t *testing.T, root string, patterns []string, skip func(pkgPath string) bool) (map[site]int, []string) {
	t.Helper()
	cfg := &packages.Config{
		Dir:  root,
		Mode: packages.NeedName | packages.NeedFiles | packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo | packages.NeedImports | packages.NeedModule,
	}
	loaded, err := packages.Load(cfg, patterns...)
	if err != nil {
		t.Fatalf("packages.Load: %v", err)
	}
	found := map[site]int{}
	var problems []string
	for _, pkg := range loaded {
		if skip(pkg.PkgPath) {
			continue
		}
		for _, loadErr := range pkg.Errors {
			problems = append(problems, fmt.Sprintf("LOAD error in %s: %v", pkg.PkgPath, loadErr))
		}
		if pkg.TypesInfo == nil {
			continue
		}
		modPath := modulePath
		if pkg.Module != nil {
			modPath = pkg.Module.Path
		}
		w := walker{info: pkg.TypesInfo, module: modPath, clientUnderlying: netHTTPClientUnderlying(pkg.Types)}
		for _, file := range pkg.Syntax {
			name := pkg.Fset.Position(file.Pos()).Filename
			if strings.HasSuffix(name, "_test.go") {
				continue
			}
			rel, relErr := filepath.Rel(root, name)
			if relErr != nil {
				t.Fatal(relErr)
			}
			rel = filepath.ToSlash(rel)
			for _, declaration := range file.Decls {
				symbol := declSymbol(declaration)
				ast.Inspect(declaration, func(node ast.Node) bool {
					for _, kind := range w.kindsOf(node) {
						found[site{rel, symbol, kind}]++
					}
					return true
				})
			}
		}
	}
	return found, problems
}

func declSymbol(declaration ast.Decl) string {
	switch d := declaration.(type) {
	case *ast.FuncDecl:
		if d.Recv != nil && len(d.Recv.List) == 1 {
			return receiverName(d.Recv.List[0].Type) + "." + d.Name.Name
		}
		return d.Name.Name
	case *ast.GenDecl:
		if len(d.Specs) > 0 {
			switch spec := d.Specs[0].(type) {
			case *ast.ValueSpec:
				if len(spec.Names) > 0 {
					return "var " + spec.Names[0].Name
				}
			case *ast.TypeSpec:
				return "type " + spec.Name.Name
			}
		}
	}
	return "package-level"
}

func receiverName(expression ast.Expr) string {
	switch e := expression.(type) {
	case *ast.StarExpr:
		return receiverName(e.X)
	case *ast.Ident:
		return e.Name
	case *ast.IndexExpr:
		return receiverName(e.X)
	}
	return "?"
}

type walker struct {
	info             *types.Info
	module           string
	clientUnderlying types.Type
}

// isClient: the type is exactly net/http.Client (not a pointer to it).
func isClient(t types.Type) bool {
	named, ok := types.Unalias(t).(*types.Named)
	if !ok {
		return false
	}
	obj := named.Obj()
	return obj.Pkg() != nil && obj.Pkg().Path() == "net/http" && obj.Name() == "Client"
}

// holdsClient: the type is, points to, or has a field (to a few levels) of net/http.Client.
func holdsClient(t types.Type, depth int, seen map[types.Type]bool) bool {
	if t == nil || depth > 3 || seen[t] {
		return false
	}
	seen[t] = true
	if isClient(t) {
		return true
	}
	switch x := types.Unalias(t).(type) {
	case *types.Pointer:
		return holdsClient(x.Elem(), depth+1, seen)
	case *types.Slice:
		return holdsClient(x.Elem(), depth+1, seen)
	case *types.Array:
		return holdsClient(x.Elem(), depth+1, seen)
	case *types.Map:
		return holdsClient(x.Elem(), depth+1, seen)
	case *types.Named:
		if x.Obj().Pkg() != nil && x.Obj().Pkg().Path() == "net/http" {
			return false
		}
		return holdsClient(x.Underlying(), depth+1, seen)
	case *types.Struct:
		for i := 0; i < x.NumFields(); i++ {
			if holdsClient(x.Field(i).Type(), depth+1, seen) {
				return true
			}
		}
	case *types.Tuple:
		for i := 0; i < x.Len(); i++ {
			if holdsClient(x.At(i).Type(), depth+1, seen) {
				return true
			}
		}
	}
	return false
}

func inNetHTTP(obj types.Object) bool {
	return obj != nil && obj.Pkg() != nil && obj.Pkg().Path() == "net/http"
}

func (w walker) kindsOf(node ast.Node) []string {
	switch x := node.(type) {
	case *ast.CompositeLit:
		if isClient(w.info.TypeOf(x)) {
			return []string{"client-literal"}
		}
	case *ast.CallExpr:
		if ident, ok := x.Fun.(*ast.Ident); ok {
			if builtin, isBuiltin := w.info.Uses[ident].(*types.Builtin); isBuiltin && builtin.Name() == "new" && len(x.Args) == 1 && isClient(w.info.TypeOf(x.Args[0])) {
				return []string{"client-new"}
			}
		}
		var callee types.Object
		switch fun := x.Fun.(type) {
		case *ast.Ident:
			callee = w.info.Uses[fun]
		case *ast.SelectorExpr:
			callee = w.info.Uses[fun.Sel]
		}
		if fn, ok := callee.(*types.Func); ok && fn.Pkg() != nil && !strings.HasPrefix(fn.Pkg().Path(), w.module) && fn.Pkg().Path() != "net/http" {
			if sig, ok := fn.Type().(*types.Signature); ok && holdsClient(sig.Results(), 0, map[types.Type]bool{}) {
				return []string{"external-client-call"}
			}
		}
	case *ast.Ident:
		if obj := w.info.Uses[x]; obj != nil {
			switch o := obj.(type) {
			case *types.Var:
				if inNetHTTP(o) && o.Name() == "DefaultClient" {
					return []string{"default-client"}
				}
			case *types.Func:
				if sig, ok := o.Type().(*types.Signature); ok && inNetHTTP(o) && sig.Recv() == nil {
					switch o.Name() {
					case "Get", "Post", "PostForm", "Head":
						return []string{"http." + o.Name()}
					}
				}
			}
		}
		if obj := w.info.Defs[x]; obj != nil {
			switch o := obj.(type) {
			case *types.Var:
				if isClient(o.Type()) {
					return []string{"client-decl"}
				}
			case *types.TypeName:
				if o.IsAlias() && isClient(o.Type()) {
					return []string{"client-type"}
				}
				if named, ok := o.Type().(*types.Named); ok && !isClient(named) && w.clientUnderlying != nil && types.Identical(named.Underlying(), w.clientUnderlying) {
					return []string{"client-type"}
				}
			}
		}
	case *ast.AssignStmt:
		var kinds []string
		for _, left := range x.Lhs {
			if selector, ok := left.(*ast.SelectorExpr); ok {
				if field, ok := w.info.Uses[selector.Sel].(*types.Var); ok && field.IsField() && field.Name() == "CheckRedirect" && inNetHTTP(field) {
					kinds = append(kinds, "checkredirect-assign")
				}
			}
		}
		return kinds
	}
	return nil
}

func readRows(t *testing.T, name string) map[site]row {
	t.Helper()
	rows := map[site]row{}
	handle, err := os.Open(name)
	if err != nil {
		if os.IsNotExist(err) {
			return rows
		}
		t.Fatal(err)
	}
	defer handle.Close()
	scanner := bufio.NewScanner(handle)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "#") || strings.TrimSpace(line) == "" {
			continue
		}
		fields := strings.SplitN(line, "\t", 7)
		if len(fields) != 7 {
			t.Fatalf("%s: want 7 tab-separated fields (file, symbol, kind, count, class, probe, cite): %q", name, line)
		}
		count, convErr := strconv.Atoi(fields[3])
		if convErr != nil {
			t.Fatalf("%s: count of %q: %v", name, line, convErr)
		}
		key := site{fields[0], fields[1], fields[2]}
		if _, dup := rows[key]; dup {
			t.Fatalf("%s: duplicate row %v", name, key)
		}
		rows[key] = row{count, fields[4], fields[5], fields[6]}
	}
	return rows
}

const sitesHeader = `# Every http.Client construction, declaration and type, http.DefaultClient use, http.Get/Post/PostForm/Head reference,
# external call returning a client and CheckRedirect assignment of production code (CHAOS-7910), found by TYPE. The ROW SET
# is generated by TestEveryHTTPClientSiteIsClassified -update-sites; only class, probe and cite are edited by hand.
# file<TAB>symbol<TAB>kind<TAB>count<TAB>class (guarded|never-follows|drops-credential|no-credential)<TAB>probe (Test | dir:Test | -)<TAB>cite
`

func writeRows(t *testing.T, name string, found map[site]int, old map[site]row) {
	t.Helper()
	keys := make([]site, 0, len(found))
	for key := range found {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		a, b := keys[i], keys[j]
		if a.file != b.file {
			return a.file < b.file
		}
		if a.symbol != b.symbol {
			return a.symbol < b.symbol
		}
		return a.kind < b.kind
	})
	var out strings.Builder
	out.WriteString(sitesHeader)
	for _, key := range keys {
		class, probe, cite := "UNCLASSIFIED", "-", ""
		if previous, ok := old[key]; ok {
			class, probe, cite = previous.class, previous.probe, previous.cite
		}
		fmt.Fprintf(&out, "%s\t%s\t%s\t%d\t%s\t%s\t%s\n", key.file, key.symbol, key.kind, found[key], class, probe, cite)
	}
	if err := os.WriteFile(name, []byte(out.String()), 0o644); err != nil {
		t.Fatal(err)
	}
}

// netHTTPClientUnderlying is the struct type of net/http.Client as the package under analysis sees it.
func netHTTPClientUnderlying(pkg *types.Package) types.Type {
	if pkg == nil {
		return nil
	}
	for _, imported := range pkg.Imports() {
		if imported.Path() == "net/http" {
			if obj := imported.Scope().Lookup("Client"); obj != nil {
				return obj.Type().Underlying()
			}
		}
	}
	return nil
}

// The walker is itself tested: one planted fixture package per clause (testdata/redirectsites), each of which must be
// found with its exact kinds and counts, and one that must yield nothing. A clause removed from the walker is RED.
func TestTheSiteWalkerFindsEveryKind(t *testing.T) {
	root, err := filepath.Abs(moduleRootRel)
	if err != nil {
		t.Fatal(err)
	}
	var patterns []string
	for _, name := range []string{"literal", "newclient", "decl", "typedecl", "defaultclient", "calls", "external", "assign", "negative"} {
		patterns = append(patterns, fixturesBase+"/"+name)
	}
	found, problems := scanPackages(t, root, patterns, func(string) bool { return false })
	if len(problems) != 0 {
		t.Fatalf("fixtures do not load: %v", problems)
	}
	prefix := "internal/httpguard/testdata/redirectsites/"
	want := map[site]int{
		{prefix + "literal/literal.go", "Aliased", "client-literal"}:         1,
		{prefix + "literal/literal.go", "Dot", "client-literal"}:             1,
		{prefix + "literal/literal.go", "Elided", "client-literal"}:          2,
		{prefix + "newclient/newclient.go", "Make", "client-new"}:            1,
		{prefix + "decl/decl.go", "var Zero", "client-decl"}:                 1,
		{prefix + "decl/decl.go", "type Holder", "client-decl"}:              2,
		{prefix + "decl/decl.go", "Param", "client-decl"}:                    1,
		{prefix + "decl/decl.go", "Result", "client-literal"}:                1,
		{prefix + "typedecl/typedecl.go", "type Alias", "client-type"}:       1,
		{prefix + "typedecl/typedecl.go", "type Named", "client-type"}:       1,
		{prefix + "defaultclient/defaultclient.go", "Use", "default-client"}: 1,
		{prefix + "calls/calls.go", "Call", "http.Get"}:                      2,
		{prefix + "calls/calls.go", "Call", "http.Post"}:                     1,
		{prefix + "calls/calls.go", "Call", "http.PostForm"}:                 1,
		{prefix + "calls/calls.go", "Call", "http.Head"}:                     1,
		{prefix + "external/external.go", "Make", "external-client-call"}:    1,
		{prefix + "assign/assign.go", "Direct", "checkredirect-assign"}:      1,
		{prefix + "assign/assign.go", "Promoted", "checkredirect-assign"}:    1,
	}
	want[site{prefix + "decl/decl.go", "Result", "client-decl"}] = 0
	delete(want, site{prefix + "decl/decl.go", "Result", "client-decl"})
	for key, count := range want {
		if found[key] != count {
			t.Errorf("walker: %v: found %d, want %d", key, found[key], count)
		}
	}
	for key, count := range found {
		if _, ok := want[key]; !ok {
			t.Errorf("walker: unexpected %v x%d", key, count)
		}
	}
}

// The verdict is itself tested: a row table with one of EVERY defect, and the exact problem list asserted.
func TestTheSiteVerdictNamesEveryDefect(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "pkg"), 0o755); err != nil {
		t.Fatal(err)
	}
	probes := `package pkg

import "testing"

func TestRedirectProbe(t *testing.T)   { redirect() }
func TestNoRedirectHere(t *testing.T)  { other() }
func TestDropsAuthorization(t *testing.T) { _ = "Authorization" }
func TestNoAuthHere(t *testing.T) { redirect() }
`
	if err := os.WriteFile(filepath.Join(root, "pkg", "p_test.go"), []byte(probes), 0o644); err != nil {
		t.Fatal(err)
	}
	k := func(symbol, kind string) site { return site{"pkg/p.go", symbol, kind} }
	found := map[site]int{
		k("ok", "client-literal"): 1, k("unlisted", "client-literal"): 1, k("count", "client-literal"): 1,
		k("unclassified", "client-literal"): 1, k("bogus", "client-literal"): 1, k("nocite", "client-literal"): 1,
		k("noprobe", "client-literal"): 1, k("weak", "client-literal"): 1, k("drops", "client-literal"): 1,
		k("dropsweak", "client-literal"): 1, k("nocred", "client-literal"): 1, k("nocredprobe", "client-literal"): 1,
		k("crossdir", "client-literal"): 1,
	}
	rows := map[site]row{
		k("ok", "client-literal"):           {1, "guarded", "TestRedirectProbe", "c"},
		k("count", "client-literal"):        {4, "guarded", "TestRedirectProbe", "c"},
		k("unclassified", "client-literal"): {1, "UNCLASSIFIED", "TestRedirectProbe", "c"},
		k("bogus", "client-literal"):        {1, "whatever", "TestRedirectProbe", "c"},
		k("nocite", "client-literal"):       {1, "guarded", "TestRedirectProbe", "  "},
		k("noprobe", "client-literal"):      {1, "never-follows", "TestMissing", "c"},
		k("weak", "client-literal"):         {1, "guarded", "TestNoRedirectHere", "c"},
		k("drops", "client-literal"):        {1, "drops-credential", "TestDropsAuthorization", "c"},
		k("dropsweak", "client-literal"):    {1, "drops-credential", "TestNoAuthHere", "c"},
		k("nocred", "client-literal"):       {1, "no-credential", "-", "c"},
		k("nocredprobe", "client-literal"):  {1, "no-credential", "TestRedirectProbe", "c"},
		k("crossdir", "client-literal"):     {1, "guarded", "pkg:TestRedirectProbe", "c"},
		k("gone", "client-literal"):         {1, "guarded", "TestRedirectProbe", "c"},
	}
	got := strings.Join(compareSites(found, rows, root), "\n")
	want := strings.Join([]string{
		"COUNT differs: pkg/p.go count client-literal: table 4, code 1",
		"NO CITE: pkg/p.go nocite client-literal",
		"PROBE does not assert on Authorization: pkg/p.go dropsweak client-literal (TestNoAuthHere)",
		"PROBE does not exercise a redirect: pkg/p.go weak client-literal (TestNoRedirectHere)",
		"PROBE missing: pkg/p.go noprobe client-literal names TestMissing in pkg",
		"PROBE on a no-credential row (must be -): pkg/p.go nocredprobe client-literal",
		"STALE row: pkg/p.go gone client-literal",
		`UNCLASSIFIED: pkg/p.go bogus client-literal (class "whatever")`,
		`UNCLASSIFIED: pkg/p.go unclassified client-literal (class "UNCLASSIFIED")`,
		"UNLISTED site: pkg/p.go unlisted client-literal (x1)",
	}, "\n")
	if got != want {
		t.Errorf("verdict problems differ:\n got:\n%s\nwant:\n%s", got, want)
	}
}
