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
//	client-conversion     a conversion between net/http.Client (or a pointer to it) and a defined type of its shape
//	client-instantiate    a generic function or type instantiated with net/http.Client
//	nil-client-field-literal  a literal (or new) of a replaced-module type that leaves an exported *http.Client field unset
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
// WALK SCOPE: the main module and every module go.mod replaces with a local path (compiled into production, read as source),
// for GOOS=linux with GOARCH amd64 and arm64; the files outside those builds are PRINTED with their count (a stated limit).
// Nested modules (tests/compatibility/river/nminus1) hold tests only; no production package may import internal/testsupport
// (checked in the walk).
//
// SCOPE PIN: pinnedOutOfScope names every production file outside those builds; the test fails when the set changes.
//
// A row names its PROBE, "Test" or "dir:Test" (a test of the package of the row, or of the dir named). The probe must exist,
// and its class must agree with it: guarded names a redirectprobe test (the other origin sees no request), never-follows names a test of the client's own
// policy (CheckRedirect / ErrUseLastResponse), drops-credential names a test that asserts on Authorization, no-credential names none ("-") and its cite says
// what travels. A class changed without a probe changed fails here; whether the probe is RED without the guard is shown
// by the planted guard-off runs recorded in the PR, not by this test.
//
// NOT covered, stated and not claimed: a nil VALUE of a replaced-module client field reached through a variable
// (`var none *http.Client` then `HTTPClient: none`) or a later assignment (`c.HTTPClient = nil`): the walker sees a literal that
// omits the field or gives nil / a conversion of nil, and takes any other expression as supplied (CHAOS-7921 patches the vendored
// default so that nil follows no redirect by construction). The class is per FUNCTION: a function that calls httpguard for one
// client and builds a second, following one derives "guarded" for both. ValidatePagerDutyCredential's choice of the
// follow-and-drop client is pinned on the helper (pagerDutyValidationClient), not on that caller.
//
// Also not covered: a client made inside a dependency (oauth2.Config.Client, an SDK's own client); a
// production file excluded by a build tag of the default build; the semantic content of a cite.
//
// Classes:
//
//	guarded          the callee that sends the credential wraps its client with httpguard (or the provider origin guard).
//	never-follows    the client is built with its own no-redirect policy, or refuses redirects.
//	drops-credential the client follows, and the credential header is dropped off-origin (DropCredentialsOnHostChange).
//	no-credential    the client follows and nothing credential-bearing travels in header, query or body (cite what).
//	follows-unless-supplied  a replaced module's own default: it follows redirects when its HTTPClient is nil. Safe in production only
//	                 because every construction of its types in production sets HTTPClient: a literal that leaves it unset is a
//	                 nil-client-field-literal site that needs a row, so a new one FAILS here.
type site struct {
	file, symbol, kind string
}

// derivedClasses are decided by the walker from the code of the site's function, never typed: the row must carry the
// derived class. handClasses are typed by hand, allowed only where nothing is derived, and each has its own check.
var (
	derivedClasses = map[string]bool{"guarded": true, "never-follows": true, "drops-credential": true, "custom-policy": true}
	handClasses    = map[string]bool{"guarded-in-callee": true, "no-credential": true, "follows-unless-supplied": true}
)

// httpguard functions that make a client (or a doer) follow no redirect: a function that calls one is guarded.
var guardFunctions = map[string]bool{"NoRedirects": true, "NoRedirectsDoer": true, "NewClient": true}

// pinnedOutOfScope: the production .go files that no linux/amd64 or linux/arm64 build contains, with the reason each holds no
// client site: internal/auth/keystore/open_other.go is the non-Linux, non-Darwin keystore (no network); tools.go is
// `//go:build tools` (a list of tool imports).
var pinnedOutOfScope = []string{"internal/auth/keystore/open_other.go", "tools.go"}

// indexed is a function declaration with the type information of its own package.
type indexed struct {
	decl *ast.FuncDecl
	info *types.Info
}

// scopeProblems compares the production files outside the walked builds with the pinned list: a file that joins or leaves
// the list fails.
func scopeProblems(outside, pinned []string) []string {
	outside = append([]string(nil), outside...)
	sort.Strings(outside)
	if strings.Join(outside, " ") != strings.Join(pinned, " ") {
		return []string{fmt.Sprintf("SCOPE LIMIT changed: production files outside the linux/amd64 and linux/arm64 builds are %v, pinned %v (add the file with its reason, or move it into the build)", outside, pinned)}
	}
	return nil
}

type fnKey struct{ file, symbol string }

// fnFacts is what the walker reads from one function: whether it calls an httpguard function, and the redirect policies
// it sets (CheckRedirect keyed in an http.Client literal or assigned): refuse, drop, or custom.
type fnFacts struct {
	callsGuard bool
	policies   map[string]bool
}

// derive is the class of every site in the function, or "" when the code derives none. "MIXED" = two policies in one
// function (split it).
func (f *fnFacts) derive() string {
	switch {
	case f == nil:
		return ""
	case f.callsGuard:
		return "guarded"
	case len(f.policies) > 1:
		return "MIXED"
	case f.policies["refuse"]:
		return "never-follows"
	case f.policies["drop"]:
		return "drops-credential"
	case f.policies["custom"]:
		return "custom-policy"
	}
	return ""
}

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
	replaced := replacedModules(t, root)
	patterns := []string{"./..."}
	for _, module := range replaced {
		patterns = append(patterns, module+"/...")
	}
	facts := map[fnKey]*fnFacts{}
	found, problems, outside := scanPackages(t, root, patterns, replaced, func(string) bool { return false }, "", facts)
	rows := readRows(t, rowFile)
	if *updateSites {
		writeRows(t, rowFile, found, rows)
		rows = readRows(t, rowFile)
	}
	problems = append(problems, compareSites(found, rows, root, facts)...)
	// The scope limit is PINNED by name: a production file outside the linux/amd64 and linux/arm64 builds is not walked,
	// so it must be listed here with its reason, and a file that joins or leaves the list fails.
	problems = append(problems, scopeProblems(outside, pinnedOutOfScope)...)
	sort.Strings(problems)
	for _, problem := range problems {
		t.Error(problem)
	}
	t.Logf("%d sites, %d rows, %d problems", len(found), len(rows), len(problems))
}

// compareSites is the verdict: every defect of the row set against the found sites, as sorted text.
func compareSites(found map[site]int, rows map[site]row, root string, facts map[fnKey]*fnFacts) []string {
	var problems []string
	for key, count := range found {
		got, ok := rows[key]
		switch {
		case !ok:
			problems = append(problems, fmt.Sprintf("UNLISTED site: %s %s %s (x%d)", key.file, key.symbol, key.kind, count))
			continue
		case got.count != count:
			problems = append(problems, fmt.Sprintf("COUNT differs: %s %s %s: table %d, code %d", key.file, key.symbol, key.kind, got.count, count))
		case !derivedClasses[got.class] && !handClasses[got.class]:
			problems = append(problems, fmt.Sprintf("UNCLASSIFIED: %s %s %s (class %q)", key.file, key.symbol, key.kind, got.class))
			continue
		case strings.TrimSpace(got.cite) == "":
			problems = append(problems, fmt.Sprintf("NO CITE: %s %s %s", key.file, key.symbol, key.kind))
		}
		if problem := checkClass(key, got, facts); problem != "" {
			problems = append(problems, problem)
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

// checkClass: a derived class is the class the code derives (not the one typed); a hand class is allowed only where the
// code derives none, and "guarded-in-callee" names its callees (callee=file#Symbol,...), each of which must call httpguard
// or set a redirect policy.
func checkClass(key site, got row, facts map[fnKey]*fnFacts) string {
	label := fmt.Sprintf("%s %s %s", key.file, key.symbol, key.kind)
	derived := facts[fnKey{key.file, key.symbol}].derive()
	switch {
	case derived == "MIXED":
		return "MIXED policies in one function (split it): " + label
	case derived != "" && got.class != derived:
		return fmt.Sprintf("CLASS differs: %s: row says %s, the code derives %s", label, got.class, derived)
	case derived == "" && derivedClasses[got.class]:
		return fmt.Sprintf("CLASS not derivable: %s: row says %s, the code of the function shows no guard or policy", label, got.class)
	}
	if got.class == "guarded-in-callee" {
		const marker = "callee="
		i := strings.Index(got.cite, marker)
		if i < 0 {
			return "CALLEE not named (callee=file#Symbol): " + label
		}
		list := got.cite[i+len(marker):]
		if j := strings.IndexAny(list, " ;"); j >= 0 {
			list = list[:j]
		}
		for _, callee := range strings.Split(list, ",") {
			file, symbol, ok := strings.Cut(callee, "#")
			fact := facts[fnKey{file, symbol}]
			if !ok || fact == nil || (!fact.callsGuard && len(fact.policies) == 0) {
				return fmt.Sprintf("CALLEE %q does not call httpguard or set a redirect policy: %s", callee, label)
			}
		}
	}
	return ""
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
	case "guarded", "guarded-in-callee", "custom-policy":
		if !strings.Contains(body, "redirectprobe.") {
			return fmt.Sprintf("PROBE of a guarded row does not use the redirect probe (the second origin must see no request): %s (%s)", label, name)
		}
	case "never-follows":
		if !strings.Contains(body, "CheckRedirect") && !strings.Contains(body, "ErrUseLastResponse") {
			return fmt.Sprintf("PROBE of a never-follows row does not assert the client's own policy: %s (%s)", label, name)
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

// scanPackages loads the packages matching patterns (non-test files only) under root, once per production target
// (linux/amd64 and linux/arm64), and returns their sites by (file, symbol, kind) with the largest count seen, and the
// problems of loading. replaced names the module paths whose types are "replaced modules" (read as source): a literal of
// one of their types that leaves an exported *http.Client field unset is a site.
func scanPackages(t *testing.T, root string, patterns, replaced []string, skip func(pkgPath string) bool, scopeDir string, facts map[fnKey]*fnFacts) (map[site]int, []string, []string) {
	t.Helper()
	found := map[site]int{}
	var problems []string
	loadedFiles := map[string]bool{}
	for _, arch := range []string{"amd64", "arm64"} {
		cfg := &packages.Config{
			Dir:  root,
			Env:  append(os.Environ(), "GOOS=linux", "GOARCH="+arch),
			Mode: packages.NeedName | packages.NeedFiles | packages.NeedCompiledGoFiles | packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo | packages.NeedImports | packages.NeedModule,
		}
		loaded, err := packages.Load(cfg, patterns...)
		if err != nil {
			t.Fatalf("packages.Load: %v", err)
		}
		counts := map[site]int{}
		index := map[string]indexed{}
		for _, pkg := range loaded {
			for _, file := range pkg.Syntax {
				for _, declaration := range file.Decls {
					if fn, ok := declaration.(*ast.FuncDecl); ok && fn.Recv == nil && fn.Body != nil {
						index[pkg.PkgPath+"."+fn.Name.Name] = indexed{fn, pkg.TypesInfo}
					}
				}
			}
		}
		for _, pkg := range loaded {
			if skip(pkg.PkgPath) {
				continue
			}
			for _, loadErr := range pkg.Errors {
				problems = append(problems, fmt.Sprintf("LOAD error in %s (%s): %v", pkg.PkgPath, arch, loadErr))
			}
			importer := pkg.PkgPath == modulePath+"/internal/testsupport" || strings.HasPrefix(pkg.PkgPath, modulePath+"/internal/testsupport/")
			for imported := range pkg.Imports {
				if !importer && (imported == modulePath+"/internal/testsupport" || strings.HasPrefix(imported, modulePath+"/internal/testsupport/")) {
					problems = append(problems, fmt.Sprintf("PRODUCTION package %s imports %s (a test-support package: its clients are outside the walk)", pkg.PkgPath, imported))
				}
			}
			for _, goFile := range pkg.CompiledGoFiles {
				loadedFiles[goFile] = true
			}
			if pkg.TypesInfo == nil {
				continue
			}
			modPath := modulePath
			if pkg.Module != nil {
				modPath = pkg.Module.Path
			}
			w := walker{info: pkg.TypesInfo, module: modPath, replaced: replaced, clientUnderlying: netHTTPClientUnderlying(pkg.Types), index: index}
			for _, file := range pkg.Syntax {
				name := pkg.Fset.Position(file.Pos()).Filename
				rel, relErr := filepath.Rel(root, name)
				if relErr != nil {
					t.Fatal(relErr)
				}
				rel = filepath.ToSlash(rel)
				for _, declaration := range file.Decls {
					symbol := declSymbol(declaration)
					fact := &fnFacts{policies: map[string]bool{}}
					ast.Inspect(declaration, func(node ast.Node) bool {
						for _, kind := range w.kindsOf(node) {
							counts[site{rel, symbol, kind}]++
						}
						w.noteFacts(node, fact)
						return true
					})
					if facts != nil {
						key := fnKey{rel, symbol}
						if previous := facts[key]; previous != nil {
							fact.callsGuard = fact.callsGuard || previous.callsGuard
							for policy := range previous.policies {
								fact.policies[policy] = true
							}
						}
						facts[key] = fact
					}
				}
			}
		}
		for key, count := range counts {
			if count > found[key] {
				found[key] = count
			}
		}
	}
	return found, uniqueStrings(problems), reportOutOfScope(t, root, scopeDir, loadedFiles)
}

// reportOutOfScope PRINTS the limit of the walk: the production .go files of the main module and of the replaced
// module that no loaded package of linux/amd64 or linux/arm64 contains (another GOOS, a build tag): a stated limit,
// not a silent one.
func reportOutOfScope(t *testing.T, root, scopeDir string, loaded map[string]bool) []string {
	t.Helper()
	var outside []string
	walkRoot := root
	if scopeDir != "" {
		walkRoot = filepath.Join(root, scopeDir)
	}
	_ = filepath.WalkDir(walkRoot, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if entry.IsDir() {
			switch entry.Name() {
			case ".git", "node_modules":
				return filepath.SkipDir
			case "testdata":
				if scopeDir == "" {
					return filepath.SkipDir
				}
			}
			if rel, _ := filepath.Rel(root, path); filepath.ToSlash(rel) == "tests" {
				return filepath.SkipDir
			}
			return nil
		}
		if strings.HasSuffix(path, ".go") && !strings.HasSuffix(path, "_test.go") && !loaded[path] {
			rel, _ := filepath.Rel(root, path)
			outside = append(outside, filepath.ToSlash(rel))
		}
		return nil
	})
	sort.Strings(outside)
	t.Logf("SCOPE LIMIT: %d production .go files are outside the linux/amd64 and linux/arm64 builds (another GOOS or a build tag) and are NOT walked: %s", len(outside), strings.Join(outside, " "))
	return outside
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
	replaced         []string
	clientUnderlying types.Type
	index            map[string]indexed
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
	if t == nil || seen[t] {
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

// holdsClientByValue: the type is net/http.Client, or an array, slice, map, channel or struct (fields, to a few levels)
// that holds one BY VALUE (a pointer does not: it is made elsewhere).
func holdsClientByValue(t types.Type, depth int, seen map[types.Type]bool) bool {
	if t == nil || seen[t] {
		return false
	}
	seen[t] = true
	if isClient(t) {
		return true
	}
	switch x := types.Unalias(t).(type) {
	case *types.Array:
		return holdsClientByValue(x.Elem(), depth+1, seen)
	case *types.Slice:
		return holdsClientByValue(x.Elem(), depth+1, seen)
	case *types.Chan:
		return holdsClientByValue(x.Elem(), depth+1, seen)
	case *types.Map:
		return holdsClientByValue(x.Elem(), depth+1, seen)
	case *types.Named:
		if x.Obj().Pkg() != nil && x.Obj().Pkg().Path() == "net/http" {
			return false
		}
		return holdsClientByValue(x.Underlying(), depth+1, seen)
	case *types.Struct:
		for i := 0; i < x.NumFields(); i++ {
			if holdsClientByValue(x.Field(i).Type(), depth+1, seen) {
				return true
			}
		}
	}
	return false
}

func inNetHTTP(obj types.Object) bool {
	return obj != nil && obj.Pkg() != nil && obj.Pkg().Path() == "net/http"
}

// replacedType: the (named) type is declared in one of the replaced modules.
func (w walker) replacedType(t types.Type) *types.Named {
	named, ok := types.Unalias(t).(*types.Named)
	if !ok || named.Obj().Pkg() == nil {
		return nil
	}
	path := named.Obj().Pkg().Path()
	for _, prefix := range w.replaced {
		if path == prefix || strings.HasPrefix(path, prefix+"/") {
			return named
		}
	}
	return nil
}

// carrierFields: the exported *http.Client fields of a replaced-module struct type (a "carrier": its own default client
// follows redirects when the field is nil), or nil when the type is not one.
func (w walker) carrierFields(t types.Type) []int {
	named := w.replacedType(t)
	if named == nil {
		return nil
	}
	structType, ok := named.Underlying().(*types.Struct)
	if !ok {
		return nil
	}
	var fields []int
	for i := 0; i < structType.NumFields(); i++ {
		field := structType.Field(i)
		if pointer, ok := field.Type().(*types.Pointer); ok && field.Exported() && isClient(pointer.Elem()) {
			fields = append(fields, i)
		}
	}
	return fields
}

// holdsCarrierByValue: the type is a carrier, or an array, slice, map, channel or struct (fields, embedded ones too) that
// holds one BY VALUE at any depth (a pointer does not: it is made elsewhere).
func (w walker) holdsCarrierByValue(t types.Type, depth int, seen map[types.Type]bool) bool {
	if t == nil || seen[t] {
		return false
	}
	seen[t] = true
	if len(w.carrierFields(t)) > 0 {
		return true
	}
	switch x := types.Unalias(t).(type) {
	case *types.Array:
		return w.holdsCarrierByValue(x.Elem(), depth+1, seen)
	case *types.Slice:
		return w.holdsCarrierByValue(x.Elem(), depth+1, seen)
	case *types.Chan:
		return w.holdsCarrierByValue(x.Elem(), depth+1, seen)
	case *types.Map:
		return w.holdsCarrierByValue(x.Elem(), depth+1, seen)
	case *types.Named:
		if x.Obj().Pkg() != nil && x.Obj().Pkg().Path() == "net/http" {
			return false
		}
		return w.holdsCarrierByValue(x.Underlying(), depth+1, seen)
	case *types.Struct:
		for i := 0; i < x.NumFields(); i++ {
			if w.holdsCarrierByValue(x.Field(i).Type(), depth+1, seen) {
				return true
			}
		}
	}
	return false
}

// literalLeavesCarrierUnset: the literal builds a carrier (or a struct holding one by value) without a non-nil client:
// the field omitted, or set to nil. A positional literal sets every field; a nested carrier literal is checked on its own.
func (w walker) literalLeavesCarrierUnset(t types.Type, lit *ast.CompositeLit) bool {
	if fields := w.carrierFields(t); len(fields) > 0 {
		structType := types.Unalias(t).(*types.Named).Underlying().(*types.Struct)
		for _, index := range fields {
			value, set := w.literalFieldValue(structType, lit, index)
			if !set || w.isNilValue(value) {
				return true
			}
		}
		return false
	}
	if !w.holdsCarrierByValue(t, 0, map[types.Type]bool{}) {
		return false
	}
	switch body := t.Underlying().(type) {
	case *types.Struct: // named or anonymous
		for i := 0; i < body.NumFields(); i++ {
			if w.holdsCarrierByValue(body.Field(i).Type(), 0, map[types.Type]bool{}) {
				if _, set := w.literalFieldValue(body, lit, i); !set {
					return true
				}
			}
		}
	case *types.Array: // the elements a literal does not give are zero values
		return int64(len(lit.Elts)) < body.Len()
	}
	return false
}

// isNilValue: the expression is nil, or a conversion of nil ((*http.Client)(nil)). A nil VALUE reached through a variable or a
// later assignment is NOT seen (stated in the package comment; CHAOS-7921 makes the vendored default safe by construction).
func (w walker) isNilValue(expression ast.Expr) bool {
	if w.info.Types[expression].IsNil() {
		return true
	}
	if call, ok := expression.(*ast.CallExpr); ok && len(call.Args) == 1 && w.info.Types[call.Fun].IsType() {
		return w.isNilValue(call.Args[0])
	}
	if paren, ok := expression.(*ast.ParenExpr); ok {
		return w.isNilValue(paren.X)
	}
	return false
}

// literalFieldValue: the expression a literal gives field index of a struct, and whether it gives one.
func (w walker) literalFieldValue(structType *types.Struct, lit *ast.CompositeLit, index int) (ast.Expr, bool) {
	if len(lit.Elts) > 0 {
		if _, keyed := lit.Elts[0].(*ast.KeyValueExpr); !keyed {
			if index < len(lit.Elts) {
				return lit.Elts[index], true
			}
			return nil, false
		}
	}
	for _, element := range lit.Elts {
		if pair, ok := element.(*ast.KeyValueExpr); ok {
			if key, ok := pair.Key.(*ast.Ident); ok && key.Name == structType.Field(index).Name() {
				return pair.Value, true
			}
		}
	}
	return nil, false
}

func derefPointer(t types.Type) types.Type {
	if pointer, ok := types.Unalias(t).(*types.Pointer); ok {
		return pointer.Elem()
	}
	return t
}

// isClientShaped: the type is, or is a defined type of the same underlying struct as, net/http.Client.
func (w walker) isClientShaped(t types.Type) bool {
	if isClient(t) {
		return true
	}
	named, ok := types.Unalias(t).(*types.Named)
	return ok && w.clientUnderlying != nil && types.Identical(named.Underlying(), w.clientUnderlying)
}

// noteFacts records what a node tells about the redirect handling of its function: a call of an httpguard function, or a
// CheckRedirect policy (keyed in an http.Client literal, or assigned).
func (w walker) noteFacts(node ast.Node, fact *fnFacts) {
	switch x := node.(type) {
	case *ast.Ident:
		if fn, ok := w.info.Uses[x].(*types.Func); ok && fn.Pkg() != nil && fn.Pkg().Path() == modulePath+"/internal/httpguard" && guardFunctions[fn.Name()] {
			fact.callsGuard = true
		}
	case *ast.CompositeLit:
		if !isClient(w.info.TypeOf(x)) {
			return
		}
		for _, element := range x.Elts {
			if pair, ok := element.(*ast.KeyValueExpr); ok {
				if key, ok := pair.Key.(*ast.Ident); ok && key.Name == "CheckRedirect" {
					if policy := w.policyOf(pair.Value); policy != "" {
						fact.policies[policy] = true
					}
				}
			}
		}
	case *ast.AssignStmt:
		for i, left := range x.Lhs {
			if selector, ok := left.(*ast.SelectorExpr); ok {
				if field, ok := w.info.Uses[selector.Sel].(*types.Var); ok && field.IsField() && field.Name() == "CheckRedirect" && inNetHTTP(field) && i < len(x.Rhs) {
					if policy := w.policyOf(x.Rhs[i]); policy != "" {
						fact.policies[policy] = true
					}
				}
			}
		}
	}
}

// policyOf classifies a CheckRedirect value: "refuse" (every return is a non-nil error), "drop" (the providerfoundation
// policy that follows and drops the credential off-origin), "custom" (anything else), "" (nil: the default policy).
func (w walker) policyOf(value ast.Expr) string {
	switch v := value.(type) {
	case *ast.FuncLit:
		return bodyPolicy(v.Body, w.info)
	case *ast.Ident, *ast.SelectorExpr:
		var obj types.Object
		if ident, ok := v.(*ast.Ident); ok {
			if ident.Name == "nil" {
				return ""
			}
			obj = w.info.Uses[ident]
		} else {
			obj = w.info.Uses[v.(*ast.SelectorExpr).Sel]
		}
		fn, ok := obj.(*types.Func)
		if !ok || fn.Pkg() == nil {
			return "custom"
		}
		qualified := fn.Pkg().Path() + "." + fn.Name()
		if qualified == modulePath+"/internal/providerfoundation.DropCredentialsOnHostChange" {
			return "drop"
		}
		if entry, ok := w.index[qualified]; ok {
			return bodyPolicy(entry.decl.Body, entry.info)
		}
	}
	return "custom"
}

// bodyPolicy: "refuse" when the function returns at least once and EVERY return gives a value that is statically a
// non-nil error: a package-level error variable (http.ErrUseLastResponse, an errors.New var) or a call of errors.New or
// fmt.Errorf. A delegating call, a local variable, nil, or anything else makes the policy "custom": it may follow.
func bodyPolicy(body *ast.BlockStmt, info *types.Info) string {
	returns, refusing := 0, true
	ast.Inspect(body, func(node ast.Node) bool {
		switch n := node.(type) {
		case *ast.FuncLit:
			return false
		case *ast.ReturnStmt:
			returns++
			if len(n.Results) != 1 || !staticallyNonNilError(n.Results[0], info) {
				refusing = false
			}
		}
		return true
	})
	if returns > 0 && refusing {
		return "refuse"
	}
	return "custom"
}

func staticallyNonNilError(expression ast.Expr, info *types.Info) bool {
	switch e := expression.(type) {
	case *ast.Ident:
		variable, ok := info.Uses[e].(*types.Var)
		return ok && variable.Pkg() != nil && variable.Parent() == variable.Pkg().Scope() // a package-level variable
	case *ast.SelectorExpr:
		variable, ok := info.Uses[e.Sel].(*types.Var)
		return ok && variable.Pkg() != nil && variable.Parent() == variable.Pkg().Scope()
	case *ast.CallExpr:
		if selector, ok := e.Fun.(*ast.SelectorExpr); ok {
			if fn, ok := info.Uses[selector.Sel].(*types.Func); ok && fn.Pkg() != nil {
				return (fn.Pkg().Path() == "errors" && fn.Name() == "New") || (fn.Pkg().Path() == "fmt" && fn.Name() == "Errorf")
			}
		}
	}
	return false
}

func isBuiltinCall(info *types.Info, call *ast.CallExpr) bool {
	ident, ok := call.Fun.(*ast.Ident)
	if !ok {
		return false
	}
	_, builtin := info.Uses[ident].(*types.Builtin)
	return builtin
}

func (w walker) kindsOf(node ast.Node) []string {
	switch x := node.(type) {
	case *ast.CompositeLit:
		var kinds []string
		if holdsClientByValue(w.info.TypeOf(x), 0, map[types.Type]bool{}) {
			kinds = append(kinds, "client-literal")
		}
		if w.literalLeavesCarrierUnset(w.info.TypeOf(x), x) {
			kinds = append(kinds, "nil-client-field-literal")
		}
		return kinds
	case *ast.CallExpr:
		var kinds []string
		if ident, ok := x.Fun.(*ast.Ident); ok {
			if builtin, isBuiltin := w.info.Uses[ident].(*types.Builtin); isBuiltin && builtin.Name() == "new" && len(x.Args) == 1 {
				arg := w.info.TypeOf(x.Args[0])
				if holdsClientByValue(arg, 0, map[types.Type]bool{}) {
					kinds = append(kinds, "client-new")
				}
				if w.holdsCarrierByValue(arg, 0, map[types.Type]bool{}) {
					kinds = append(kinds, "nil-client-field-literal")
				}
			}
		}
		if ident, ok := x.Fun.(*ast.Ident); ok {
			if builtin, isBuiltin := w.info.Uses[ident].(*types.Builtin); isBuiltin && builtin.Name() == "make" && len(x.Args) >= 1 {
				if holdsClientByValue(w.info.TypeOf(x.Args[0]), 0, map[types.Type]bool{}) {
					kinds = append(kinds, "client-new")
				}
				if w.holdsCarrierByValue(w.info.TypeOf(x.Args[0]), 0, map[types.Type]bool{}) {
					kinds = append(kinds, "nil-client-field-literal")
				}
			}
		}
		// a call whose callee does not resolve to a declared function (a func-typed variable, a field, a call result) and
		// whose result holds a client: the callee cannot be read, so the call is a site
		if !w.info.Types[x.Fun].IsType() && !isBuiltinCall(w.info, x) {
			var callee types.Object
			fun := x.Fun
			switch generic := fun.(type) { // f[T](...) is a call of f
			case *ast.IndexExpr:
				fun = generic.X
			case *ast.IndexListExpr:
				fun = generic.X
			}
			switch name := fun.(type) {
			case *ast.Ident:
				callee = w.info.Uses[name]
			case *ast.SelectorExpr:
				callee = w.info.Uses[name.Sel]
			}
			if _, resolved := callee.(*types.Func); !resolved && holdsClient(w.info.TypeOf(x), 0, map[types.Type]bool{}) {
				kinds = append(kinds, "external-client-call")
			}
		}
		// a conversion between net/http.Client (or a pointer to it) and another type of its shape
		if len(x.Args) == 1 && w.info.Types[x.Fun].IsType() {
			to, from := derefPointer(w.info.TypeOf(x)), derefPointer(w.info.TypeOf(x.Args[0]))
			if to != nil && from != nil && w.isClientShaped(to) && w.isClientShaped(from) && !types.Identical(to, from) {
				kinds = append(kinds, "client-conversion")
			}
		}
		return kinds
	case *ast.Ident:
		var kinds []string
		if instance, ok := w.info.Instances[x]; ok {
			for i := 0; i < instance.TypeArgs.Len(); i++ {
				if holdsClientByValue(derefPointer(instance.TypeArgs.At(i)), 0, map[types.Type]bool{}) || w.holdsCarrierByValue(derefPointer(instance.TypeArgs.At(i)), 0, map[types.Type]bool{}) {
					kinds = append(kinds, "client-instantiate")
					break
				}
			}
		}
		if obj := w.info.Uses[x]; obj != nil {
			switch o := obj.(type) {
			case *types.Var:
				if inNetHTTP(o) && o.Name() == "DefaultClient" {
					kinds = append(kinds, "default-client")
				}
			case *types.Func:
				sig, ok := o.Type().(*types.Signature)
				if !ok {
					break
				}
				if inNetHTTP(o) && sig.Recv() == nil {
					switch o.Name() {
					case "Get", "Post", "PostForm", "Head":
						kinds = append(kinds, "http."+o.Name())
					}
				}
				if o.Pkg() != nil && !strings.HasPrefix(o.Pkg().Path(), w.module) && o.Pkg().Path() != "net/http" && holdsClient(sig.Results(), 0, map[types.Type]bool{}) {
					kinds = append(kinds, "external-client-call")
				}
			}
		}
		if obj := w.info.Defs[x]; obj != nil {
			switch o := obj.(type) {
			case *types.Var:
				if holdsClientByValue(o.Type(), 0, map[types.Type]bool{}) {
					kinds = append(kinds, "client-decl")
				}
				if w.holdsCarrierByValue(o.Type(), 0, map[types.Type]bool{}) {
					kinds = append(kinds, "client-carrier-decl")
				}
			case *types.TypeName:
				if o.IsAlias() && isClient(o.Type()) {
					kinds = append(kinds, "client-type")
				} else if named, ok := o.Type().(*types.Named); ok && !isClient(named) && w.clientUnderlying != nil && types.Identical(named.Underlying(), w.clientUnderlying) {
					kinds = append(kinds, "client-type")
				}
			}
		}
		return kinds
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
	for _, name := range []string{"literal", "newclient", "decl", "typedecl", "defaultclient", "calls", "external", "assign", "negative", "conversion", "instantiate", "outside", "nilfield", "method", "tagged", "importsupport", "policies", "outside/sub", "fakedefault"} {
		patterns = append(patterns, fixturesBase+"/"+name)
	}
	facts := map[fnKey]*fnFacts{}
	found, problems, outside := scanPackages(t, root, patterns, []string{fixturesBase + "/outside"}, func(string) bool { return false }, "internal/httpguard/testdata/redirectsites", facts)
	if len(problems) != 1 || !strings.Contains(problems[0], "importsupport") || !strings.Contains(problems[0], "test-support package") {
		t.Fatalf("want exactly the test-support import of the importsupport fixture as a problem, got %v", problems)
	}
	prefix := "internal/httpguard/testdata/redirectsites/"
	want := map[site]int{
		{prefix + "literal/literal.go", "Aliased", "client-literal"}:                   1,
		{prefix + "literal/literal.go", "Dot", "client-literal"}:                       1,
		{prefix + "literal/literal.go", "Elided", "client-literal"}:                    4,
		{prefix + "newclient/newclient.go", "Make", "client-new"}:                      1,
		{prefix + "decl/decl.go", "var Zero", "client-decl"}:                           1,
		{prefix + "decl/decl.go", "type Holder", "client-decl"}:                        2,
		{prefix + "decl/decl.go", "Param", "client-decl"}:                              1,
		{prefix + "decl/decl.go", "Result", "client-literal"}:                          1,
		{prefix + "typedecl/typedecl.go", "type Alias", "client-type"}:                 1,
		{prefix + "typedecl/typedecl.go", "type Named", "client-type"}:                 1,
		{prefix + "defaultclient/defaultclient.go", "Use", "default-client"}:           1,
		{prefix + "calls/calls.go", "Call", "http.Get"}:                                2,
		{prefix + "calls/calls.go", "Call", "http.Post"}:                               1,
		{prefix + "calls/calls.go", "Call", "http.PostForm"}:                           1,
		{prefix + "calls/calls.go", "Call", "http.Head"}:                               1,
		{prefix + "external/external.go", "Make", "external-client-call"}:              1,
		{prefix + "assign/assign.go", "Direct", "checkredirect-assign"}:                1,
		{prefix + "assign/assign.go", "Promoted", "checkredirect-assign"}:              1,
		{prefix + "conversion/conversion.go", "type shaped", "client-type"}:            1,
		{prefix + "conversion/conversion.go", "ToClient", "client-conversion"}:         1,
		{prefix + "conversion/conversion.go", "FromClient", "client-conversion"}:       1,
		{prefix + "instantiate/instantiate.go", "Make", "client-instantiate"}:          1,
		{prefix + "nilfield/nilfield.go", "Unset", "nil-client-field-literal"}:         1,
		{prefix + "nilfield/nilfield.go", "New", "nil-client-field-literal"}:           1,
		{prefix + "nilfield/nilfield.go", "ExplicitNil", "nil-client-field-literal"}:   1,
		{prefix + "nilfield/nilfield.go", "SubPackage", "nil-client-field-literal"}:    1,
		{prefix + "nilfield/nilfield.go", "var Zero", "client-carrier-decl"}:           1,
		{prefix + "nilfield/nilfield.go", "type Wrapper", "client-carrier-decl"}:       1,
		{prefix + "nilfield/nilfield.go", "type Named", "client-carrier-decl"}:         1,
		{prefix + "nilfield/nilfield.go", "Embedded", "nil-client-field-literal"}:      1,
		{prefix + "nilfield/nilfield.go", "NamedField", "nil-client-field-literal"}:    1,
		{prefix + "nilfield/nilfield.go", "Makes", "nil-client-field-literal"}:         1,
		{prefix + "nilfield/nilfield.go", "var Array", "client-carrier-decl"}:          1,
		{prefix + "method/method.go", "Call", "external-client-call"}:                  1,
		{prefix + "method/method.go", "Value", "external-client-call"}:                 1,
		{prefix + "method/method.go", "Variable", "external-client-call"}:              2,
		{prefix + "tagged/client_arm64.go", "Arm", "client-literal"}:                   1,
		{prefix + "tagged/client_amd64.go", "Amd", "client-literal"}:                   1,
		{prefix + "policies/policies.go", "Delegating", "client-literal"}:              1,
		{prefix + "policies/policies.go", "LocalVariable", "client-literal"}:           1,
		{prefix + "policies/policies.go", "Conditional", "client-literal"}:             1,
		{prefix + "policies/policies.go", "ForeignCall", "client-literal"}:             1,
		{prefix + "policies/policies.go", "FieldReturn", "client-literal"}:             1,
		{prefix + "policies/policies.go", "RefuseErrorf", "client-literal"}:            1,
		{prefix + "policies/policies.go", "RefusePackageVar", "client-literal"}:        1,
		{prefix + "nilfield/nilfield.go", "TypedNil", "nil-client-field-literal"}:      1,
		{prefix + "nilfield/nilfield.go", "var Chan", "client-carrier-decl"}:           1,
		{prefix + "nilfield/nilfield.go", "var Map", "client-carrier-decl"}:            1,
		{prefix + "fakedefault/fakedefault.go", "var DefaultClient", "client-literal"}: 1,
		{prefix + "decl/decl.go", "var Deep", "client-decl"}:                           1,
		{prefix + "decl/decl.go", "type Level1", "client-decl"}:                        4,
		{prefix + "nilfield/nilfield.go", "ArrayLiteral", "nil-client-field-literal"}:  1,
		{prefix + "nilfield/nilfield.go", "ArrayLiteral", "default-client"}:            1,
		{prefix + "nilfield/nilfield.go", "Anonymous", "nil-client-field-literal"}:     1,
		{prefix + "nilfield/nilfield.go", "Anonymous", "client-carrier-decl"}:          1,
		{prefix + "nilfield/nilfield.go", "Generic", "client-instantiate"}:             1,
		{prefix + "nilfield/nilfield.go", "var DeepCarrier", "client-carrier-decl"}:    1,
		{prefix + "policies/policies.go", "RefuseLiteral", "client-literal"}:           1,
		{prefix + "policies/policies.go", "RefuseFunction", "client-literal"}:          1,
		{prefix + "policies/policies.go", "RefuseAssign", "checkredirect-assign"}:      1,
		{prefix + "policies/policies.go", "Drop", "client-literal"}:                    1,
		{prefix + "policies/policies.go", "Custom", "client-literal"}:                  1,
		{prefix + "policies/policies.go", "Mixed", "checkredirect-assign"}:             2,
		{prefix + "policies/policies.go", "Bare", "client-literal"}:                    1,
		{prefix + "decl/decl.go", "var Arr", "client-decl"}:                            1,
		{prefix + "decl/decl.go", "var InHolder", "client-decl"}:                       1,
		{prefix + "decl/decl.go", "var Mp", "client-decl"}:                             1,
		{prefix + "decl/decl.go", "var Ch", "client-decl"}:                             1,
		{prefix + "decl/decl.go", "Makes", "client-new"}:                               2,
		{prefix + "instantiate/instantiate.go", "Array", "client-instantiate"}:         1,
		{prefix + "instantiate/instantiate.go", "Pointer", "client-instantiate"}:       1,
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
	// the class derived from the code of a function
	derived := map[string]string{"Guarded": "guarded", "GuardedNew": "guarded", "RefuseLiteral": "never-follows", "RefuseFunction": "never-follows",
		"RefuseAssign": "never-follows", "Delegating": "custom-policy", "LocalVariable": "custom-policy", "Conditional": "custom-policy",
		"RefuseErrorf": "never-follows", "ForeignCall": "custom-policy", "FieldReturn": "custom-policy", "RefusePackageVar": "never-follows", "Drop": "drops-credential", "Custom": "custom-policy", "Mixed": "MIXED", "Bare": ""}
	for symbol, class := range derived {
		if got := facts[fnKey{prefix + "policies/policies.go", symbol}].derive(); got != class {
			t.Errorf("derived class of %s: %q, want %q", symbol, got, class)
		}
	}
	// the stated limit: another GOOS and a build tag are reported, not walked
	joined := strings.Join(outside, " ")
	if !strings.Contains(joined, "tagged/client_windows.go") || !strings.Contains(joined, "tagged/client_tag.go") || strings.Contains(joined, "tagged/tagged.go") {
		t.Errorf("scope limit: want the windows and the tagged file reported out of scope, got %v", outside)
	}
}

// verdictFacts is the code the verdict fixture pretends to have read: what each function of pkg/p.go guards or sets.
func verdictFacts() map[fnKey]*fnFacts {
	guard := &fnFacts{callsGuard: true, policies: map[string]bool{}}
	policy := func(names ...string) *fnFacts {
		fact := &fnFacts{policies: map[string]bool{}}
		for _, name := range names {
			fact.policies[name] = true
		}
		return fact
	}
	facts := map[fnKey]*fnFacts{}
	for _, symbol := range []string{"ok", "count", "unclassified", "bogus", "nocite", "noprobe", "weakguard", "crossdir", "classdiff", "handoverderived", "helper"} {
		facts[fnKey{"pkg/p.go", symbol}] = guard
	}
	facts[fnKey{"pkg/p.go", "policy"}] = policy("refuse")
	facts[fnKey{"pkg/p.go", "weakpolicy"}] = policy("refuse")
	facts[fnKey{"pkg/p.go", "drops"}] = policy("drop")
	facts[fnKey{"pkg/p.go", "dropsweak"}] = policy("drop")
	facts[fnKey{"pkg/p.go", "custom"}] = policy("custom")
	facts[fnKey{"pkg/p.go", "mixed"}] = policy("refuse", "drop")
	facts[fnKey{"pkg/p.go", "bare"}] = policy()
	return facts
}

// The verdict is itself tested: a row table with one of EVERY defect, and the exact problem list asserted.
func TestTheSiteVerdictNamesEveryDefect(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "pkg"), 0o755); err != nil {
		t.Fatal(err)
	}
	probes := `package pkg

import "testing"

func TestRedirectProbe(t *testing.T)      { probe := redirectprobe.New(t); probe.Assert(t) }
func TestPolicy(t *testing.T)             { _ = http.ErrUseLastResponse }
func TestNothingHere(t *testing.T)        { other() }
func TestDropsAuthorization(t *testing.T) { _ = "Authorization" }
func TestFollowsRedirect(t *testing.T)    { redirect() }
`
	if err := os.WriteFile(filepath.Join(root, "pkg", "p_test.go"), []byte(probes), 0o644); err != nil {
		t.Fatal(err)
	}
	k := func(symbol string) site { return site{"pkg/p.go", symbol, "client-literal"} }
	symbols := []string{"ok", "unlisted", "count", "unclassified", "bogus", "nocite", "noprobe", "weakguard", "classdiff", "handoverderived", "weakpolicy", "policy",
		"drops", "dropsweak", "custom", "mixed", "bare", "notderivable", "callee", "calleebad", "calleebare", "nocallee", "nocred", "nocredprobe", "crossdir", "unless"}
	found := map[site]int{}
	for _, symbol := range symbols {
		found[k(symbol)] = 1
	}
	rows := map[site]row{
		k("ok"):              {1, "guarded", "TestRedirectProbe", "c"},
		k("count"):           {4, "guarded", "TestRedirectProbe", "c"},
		k("unclassified"):    {1, "UNCLASSIFIED", "TestRedirectProbe", "c"},
		k("bogus"):           {1, "whatever", "TestRedirectProbe", "c"},
		k("nocite"):          {1, "guarded", "TestRedirectProbe", "  "},
		k("noprobe"):         {1, "guarded", "TestMissing", "c"},
		k("weakguard"):       {1, "guarded", "TestNothingHere", "c"},
		k("classdiff"):       {1, "never-follows", "TestPolicy", "c"}, // guarded in the code, never-follows in the row
		k("handoverderived"): {1, "no-credential", "-", "c"},          // a hand class where the code derives one
		k("weakpolicy"):      {1, "never-follows", "TestNothingHere", "c"},
		k("policy"):          {1, "never-follows", "TestPolicy", "c"},
		k("drops"):           {1, "drops-credential", "TestDropsAuthorization", "c"},
		k("dropsweak"):       {1, "drops-credential", "TestNothingHere", "c"},
		k("custom"):          {1, "custom-policy", "TestRedirectProbe", "c"},
		k("mixed"):           {1, "never-follows", "TestPolicy", "c"},
		k("bare"):            {1, "never-follows", "TestPolicy", "c"}, // nothing derived, a derived class typed
		k("notderivable"):    {1, "guarded", "TestRedirectProbe", "c"},
		k("callee"):          {1, "guarded-in-callee", "TestRedirectProbe", "callee=pkg/p.go#helper"},
		k("calleebad"):       {1, "guarded-in-callee", "TestRedirectProbe", "callee=pkg/p.go#nothing"},
		k("nocallee"):        {1, "guarded-in-callee", "TestRedirectProbe", "somewhere else"},
		k("nocred"):          {1, "no-credential", "-", "c"},
		k("nocredprobe"):     {1, "no-credential", "TestRedirectProbe", "c"},
		k("crossdir"):        {1, "guarded", "pkg:TestRedirectProbe", "c"},
		k("unless"):          {1, "follows-unless-supplied", "TestNothingHere", "c"},
		k("calleebare"):      {1, "guarded-in-callee", "TestRedirectProbe", "callee=pkg/p.go#bare"},
		k("gone"):            {1, "guarded", "TestRedirectProbe", "c"},
	}
	got := strings.Join(compareSites(found, rows, root, verdictFacts()), "\n")
	want := strings.Join([]string{
		"CALLEE \"pkg/p.go#bare\" does not call httpguard or set a redirect policy: pkg/p.go calleebare client-literal",
		"CALLEE \"pkg/p.go#nothing\" does not call httpguard or set a redirect policy: pkg/p.go calleebad client-literal",
		"CALLEE not named (callee=file#Symbol): pkg/p.go nocallee client-literal",
		"CLASS differs: pkg/p.go classdiff client-literal: row says never-follows, the code derives guarded",
		"CLASS differs: pkg/p.go handoverderived client-literal: row says no-credential, the code derives guarded",
		"CLASS not derivable: pkg/p.go bare client-literal: row says never-follows, the code of the function shows no guard or policy",
		"CLASS not derivable: pkg/p.go notderivable client-literal: row says guarded, the code of the function shows no guard or policy",
		"COUNT differs: pkg/p.go count client-literal: table 4, code 1",
		"MIXED policies in one function (split it): pkg/p.go mixed client-literal",
		"NO CITE: pkg/p.go nocite client-literal",
		"PROBE does not assert on Authorization: pkg/p.go dropsweak client-literal (TestNothingHere)",
		"PROBE does not exercise a redirect: pkg/p.go unless client-literal (TestNothingHere)",
		"PROBE missing: pkg/p.go noprobe client-literal names TestMissing in pkg",
		"PROBE of a guarded row does not use the redirect probe (the second origin must see no request): pkg/p.go weakguard client-literal (TestNothingHere)",
		"PROBE of a never-follows row does not assert the client's own policy: pkg/p.go weakpolicy client-literal (TestNothingHere)",
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

// replacedModules is every module path go.mod replaces with a local directory: compiled into production, read as source.
func replacedModules(t *testing.T, root string) []string {
	t.Helper()
	source, err := os.ReadFile(filepath.Join(root, "go.mod"))
	if err != nil {
		t.Fatal(err)
	}
	var modules []string
	inBlock := false
	for _, line := range strings.Split(string(source), "\n") {
		line = strings.TrimSpace(line)
		switch {
		case line == "replace (":
			inBlock = true
			continue
		case inBlock && line == ")":
			inBlock = false
			continue
		case strings.HasPrefix(line, "replace "):
			line = strings.TrimPrefix(line, "replace ")
		case !inBlock:
			continue
		}
		if parts := strings.SplitN(line, "=>", 2); len(parts) == 2 {
			target := strings.TrimSpace(parts[1])
			if strings.HasPrefix(target, "./") || strings.HasPrefix(target, "../") {
				modules = append(modules, strings.Fields(parts[0])[0])
			}
		}
	}
	sort.Strings(modules)
	return modules
}

func uniqueStrings(values []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, value := range values {
		if !seen[value] {
			seen[value] = true
			out = append(out, value)
		}
	}
	return out
}

// A package that does not load is a problem (LOAD error), never a silent gap in the walk.
func TestTheSiteWalkerReportsAPackageThatDoesNotLoad(t *testing.T) {
	root, err := filepath.Abs(moduleRootRel)
	if err != nil {
		t.Fatal(err)
	}
	_, problems, _ := scanPackages(t, root, []string{fixturesBase + "/broken"}, nil, func(string) bool { return false }, "internal/httpguard/testdata/redirectsites/broken", nil)
	if len(problems) == 0 || !strings.Contains(problems[0], "LOAD error") {
		t.Fatalf("want a LOAD error problem, got %v", problems)
	}
}

// The pinned scope list is compared by a function of its own: a file that joins or leaves the list fails.
func TestTheScopePinFailsWhenTheListChanges(t *testing.T) {
	pinned := []string{"a.go", "b.go"}
	if got := scopeProblems([]string{"b.go", "a.go"}, pinned); len(got) != 0 {
		t.Fatalf("the same set in another order must pass, got %v", got)
	}
	if got := scopeProblems([]string{"a.go", "b.go", "c_windows.go"}, pinned); len(got) != 1 {
		t.Fatalf("a file that joins the list must fail, got %v", got)
	}
	if got := scopeProblems([]string{"a.go"}, pinned); len(got) != 1 {
		t.Fatalf("a file that leaves the list must fail, got %v", got)
	}
}
