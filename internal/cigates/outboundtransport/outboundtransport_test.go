// Package outboundtransport is the CI guard that every outbound HTTP client of production code dials with a transport
// of its own and never through a proxy taken from the process environment (HTTP_PROXY, HTTPS_PROXY, NO_PROXY).
//
// A client with no Transport uses http.DefaultTransport, whose Proxy is http.ProxyFromEnvironment: an ambient proxy
// variable then receives every request the client sends, credential headers included (a plain-http target is sent to
// the proxy in full). Which client carries a credential cannot be decided statically, so the rule covers every client.
//
// The walk is by TYPE (go/packages + go/types) over the main module and every module go.mod replaces with a local path,
// for GOOS=linux with GOARCH amd64 and arm64; an import alias or a dot import cannot hide a site. A SITE is
//
//	client-no-transport  a composite literal of net/http.Client whose Transport is absent or nil, new(net/http.Client),
//	                     or a var declared with the non-pointer type net/http.Client and no value
//	transport-nil        an assignment of nil to the Transport field of a net/http.Client
//	transport-unproven   a Transport (in a literal or by assignment) whose value is not PROVEN non-nil: a nil-valued
//	                     variable or parameter leaves the client on http.DefaultTransport at run time. Proven are: a
//	                     call to an approved constructor (approvedTransportConstructors), Clone of a proven
//	                     *http.Transport, a composite literal whose RoundTripper fields are all proven, a conversion of a
//	                     func literal, and a local variable whose every assignment is proven (an assignment excused by
//	                     an `if v == nil { v = <proven> }` fallback in the same function)
//	client-by-value      a struct field (embedded or named), a defined type or alias, or an array, slice, map or channel
//	                     element whose type is the non-pointer net/http.Client: its zero value has no Transport and its
//	                     (promoted) Do uses http.DefaultTransport, and no literal of net/http.Client is ever written
//	default-transport    any use of net/http.DefaultTransport
//	default-client       any use of net/http.DefaultClient or the package functions Get, Head, Post, PostForm
//	proxy                any use of net/http.ProxyFromEnvironment, net/http.ProxyURL or httpproxy.FromEnvironment, and a
//	                     Proxy field of a net/http.Transport set (in a literal or by assignment) to anything but nil
//
// Every site needs a row in allow.tsv (file, symbol, kind, count, reason): the transport helpers themselves and the
// documented customer-machine clients that honour a proxy on purpose. An unlisted site, a count that differs and a
// row that matches nothing FAIL. Production code takes its transport from httpguard.Transport (or
// externalurl.GuardedTransport for a URL externalurl.Validate approved).
//
// NOT covered, stated: a client built inside a dependency outside the walked modules (an SDK's own default client); a
// production file outside the linux builds (pinned by name, the test fails when that set changes); internal/testsupport
// (no production package may import it: the redirect-site walk in internal/httpguard checks that).
package outboundtransport

import (
	"bufio"
	"fmt"
	"go/ast"
	"go/types"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"golang.org/x/tools/go/packages"
)

const (
	modulePath   = "github.com/full-chaos/dev-health-ops"
	fixturesBase = modulePath + "/internal/cigates/outboundtransport/testdata/planted"
	allowFile    = "allow.tsv"
)

// pinnedOutOfScope: the production .go files no linux/amd64 or linux/arm64 build contains; none holds an HTTP client
// (the non-Linux keystore and envelope key file, and the `//go:build tools` import list).
var pinnedOutOfScope = []string{"internal/auth/keystore/open_other.go", "internal/envelopemint/custody_other.go", "tools.go"}

type site struct{ file, symbol, kind string }

type allowRow struct {
	count  int
	reason string
}

func moduleRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("go.mod not found above the test directory")
		}
		dir = parent
	}
}

func TestEveryOutboundClientHasATransportWithoutTheEnvironmentProxy(t *testing.T) {
	root := moduleRoot(t)
	replaced := replacedModules(t, root)
	patterns := []string{"./..."}
	for _, module := range replaced {
		patterns = append(patterns, module+"/...")
	}
	skip := func(pkgPath string) bool {
		return pkgPath == modulePath+"/internal/testsupport" || strings.HasPrefix(pkgPath, modulePath+"/internal/testsupport/")
	}
	found, problems, loaded := scan(t, root, patterns, skip)
	if len(found) == 0 {
		t.Fatal("the walk found no site at all: the transport helpers alone are sites, so the walk did not run")
	}
	problems = append(problems, compare(found, readAllow(t, allowFile))...)
	problems = append(problems, scopeProblems(outsideBuild(t, root, loaded), pinnedOutOfScope)...)
	for _, problem := range problems {
		t.Error(problem)
	}
	if len(problems) > 0 {
		t.Log("fix: give the client Transport: httpguard.Transport() (or externalurl.GuardedTransport() for a URL externalurl.Validate approved); a documented, deliberate proxy gets an allow.tsv row with its reason")
	}
}

// scan loads the packages matching patterns (non-test files) for linux/amd64 and linux/arm64 and returns every site
// with the largest count seen in one build, the load problems, and the files loaded.
func scan(t *testing.T, root string, patterns []string, skip func(string) bool) (map[site]int, []string, map[string]bool) {
	t.Helper()
	found := map[site]int{}
	var problems []string
	loaded := map[string]bool{}
	for _, arch := range []string{"amd64", "arm64"} {
		cfg := &packages.Config{
			Dir:  root,
			Env:  append(os.Environ(), "GOOS=linux", "GOARCH="+arch),
			Mode: packages.NeedName | packages.NeedFiles | packages.NeedCompiledGoFiles | packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo,
		}
		pkgs, err := packages.Load(cfg, patterns...)
		if err != nil {
			t.Fatalf("packages.Load: %v", err)
		}
		counts := map[site]int{}
		for _, pkg := range pkgs {
			if skip(pkg.PkgPath) {
				continue
			}
			for _, loadErr := range pkg.Errors {
				problems = append(problems, fmt.Sprintf("LOAD error in %s (%s): %v", pkg.PkgPath, arch, loadErr))
			}
			for _, name := range pkg.CompiledGoFiles {
				loaded[name] = true
			}
			if pkg.TypesInfo == nil {
				continue
			}
			for _, file := range pkg.Syntax {
				rel, relErr := filepath.Rel(root, pkg.Fset.Position(file.Pos()).Filename)
				if relErr != nil {
					t.Fatal(relErr)
				}
				rel = filepath.ToSlash(rel)
				for _, declaration := range file.Decls {
					symbol := declSymbol(declaration)
					ast.Inspect(declaration, func(node ast.Node) bool {
						for _, kind := range kindsOf(pkg.TypesInfo, declaration, node) {
							counts[site{rel, symbol, kind}]++
						}
						return true
					})
				}
			}
		}
		for key, count := range counts {
			if count > found[key] {
				found[key] = count
			}
		}
	}
	sort.Strings(problems)
	return found, uniqueStrings(problems), loaded
}

// kindsOf is the site kinds one node of declaration is.
func kindsOf(info *types.Info, declaration ast.Decl, node ast.Node) []string {
	switch n := node.(type) {
	case *ast.CompositeLit:
		tv, ok := info.Types[n]
		if !ok {
			return nil
		}
		switch {
		case isNamed(tv.Type, "Client"):
			value, ok := fieldValue(n, "Transport", 0)
			if !ok || isNil(info, value) {
				return []string{"client-no-transport"}
			}
			if !provenTransport(info, declaration, value, 0) {
				return []string{"transport-unproven"}
			}
		case isNamed(tv.Type, "Transport"):
			if value, ok := fieldValue(n, "Proxy", -1); ok && !isNil(info, value) {
				return []string{"proxy"}
			}
		}
	case *ast.CallExpr:
		if ident, ok := n.Fun.(*ast.Ident); ok && len(n.Args) == 1 {
			if builtin, ok := info.Uses[ident].(*types.Builtin); ok && builtin.Name() == "new" && isNamed(info.Types[n.Args[0]].Type, "Client") {
				return []string{"client-no-transport"}
			}
		}
	case *ast.StructType:
		var kinds []string
		for _, field := range n.Fields.List {
			if isNamed(info.Types[field.Type].Type, "Client") {
				kinds = append(kinds, "client-by-value")
			}
		}
		return kinds
	case *ast.TypeSpec:
		if isNamed(info.Types[n.Type].Type, "Client") {
			return []string{"client-by-value"}
		}
	case *ast.ArrayType:
		if isNamed(info.Types[n.Elt].Type, "Client") {
			return []string{"client-by-value"}
		}
	case *ast.MapType:
		if isNamed(info.Types[n.Key].Type, "Client") || isNamed(info.Types[n.Value].Type, "Client") {
			return []string{"client-by-value"}
		}
	case *ast.ChanType:
		if isNamed(info.Types[n.Value].Type, "Client") {
			return []string{"client-by-value"}
		}
	case *ast.ValueSpec:
		if n.Type != nil && len(n.Values) == 0 && isNamed(info.Types[n.Type].Type, "Client") {
			return []string{"client-no-transport"}
		}
	case *ast.AssignStmt:
		var kinds []string
		for i, lhs := range n.Lhs {
			selector, ok := lhs.(*ast.SelectorExpr)
			if !ok || i >= len(n.Rhs) || len(n.Lhs) != len(n.Rhs) {
				continue
			}
			owner := derefPointer(info.Types[selector.X].Type)
			switch {
			case selector.Sel.Name == "Transport" && isNamed(owner, "Client") && isNil(info, n.Rhs[i]):
				kinds = append(kinds, "transport-nil")
			case selector.Sel.Name == "Transport" && isNamed(owner, "Client") && !provenTransport(info, declaration, n.Rhs[i], 0):
				kinds = append(kinds, "transport-unproven")
			case selector.Sel.Name == "Proxy" && isNamed(owner, "Transport") && !isNil(info, n.Rhs[i]):
				kinds = append(kinds, "proxy")
			}
		}
		return kinds
	case *ast.Ident:
		obj := info.Uses[n]
		if obj == nil || obj.Pkg() == nil || obj.Pkg().Scope().Lookup(obj.Name()) != obj {
			return nil
		}
		switch obj.Pkg().Path() {
		case "net/http":
			switch obj.Name() {
			case "DefaultTransport":
				return []string{"default-transport"}
			case "DefaultClient", "Get", "Head", "Post", "PostForm":
				return []string{"default-client"}
			case "ProxyFromEnvironment", "ProxyURL":
				return []string{"proxy"}
			}
		case "golang.org/x/net/http/httpproxy":
			if obj.Name() == "FromEnvironment" {
				return []string{"proxy"}
			}
		}
	}
	return nil
}

// approvedTransportConstructors are the functions whose result is a non-nil RoundTripper that never uses an
// environment proxy, whatever their arguments: the transport helpers.
var approvedTransportConstructors = map[string]bool{
	modulePath + "/internal/httpguard.Transport":              true,
	modulePath + "/internal/httpguard.SharedTransport":        true,
	modulePath + "/internal/api/externalurl.GuardedTransport": true,
}

// approvedTransportWrappers are the functions whose result is a non-nil RoundTripper over the transport they are given
// (CompletePagesOnly puts httpguard.SharedTransport under a nil base itself): the result is proven only when every
// RoundTripper argument is the nil literal or itself proven, so a wrapper never launders a transport it was handed.
var approvedTransportWrappers = map[string]bool{
	modulePath + "/internal/atlassianteams.CompletePagesOnly": true,
}

const maxProofDepth = 8

// provenTransport reports whether expression, the value of a Transport, cannot be nil at run time. Anything it cannot
// prove (a parameter, a field read, a package variable, a call to another function) is unproven: the site then needs
// an allow row naming why it is safe, or a helper.
func provenTransport(info *types.Info, declaration ast.Decl, expression ast.Expr, depth int) bool {
	if depth > maxProofDepth || expression == nil {
		return false
	}
	switch e := ast.Unparen(expression).(type) {
	case *ast.UnaryExpr:
		if lit, ok := ast.Unparen(e.X).(*ast.CompositeLit); ok && e.Op.String() == "&" {
			return provenLiteral(info, declaration, lit, depth)
		}
	case *ast.CompositeLit:
		return provenLiteral(info, declaration, e, depth)
	case *ast.CallExpr:
		if tv, ok := info.Types[e.Fun]; ok && tv.IsType() {
			if len(e.Args) != 1 {
				return false
			}
			if _, ok := ast.Unparen(e.Args[0]).(*ast.FuncLit); ok {
				return true
			}
			return provenTransport(info, declaration, e.Args[0], depth+1)
		}
		fn := calledFunc(info, e)
		if fn == nil || fn.Pkg() == nil {
			return false
		}
		signature, _ := fn.Type().(*types.Signature)
		if signature != nil && signature.Recv() == nil {
			name := fn.Pkg().Path() + "." + fn.Name()
			if approvedTransportWrappers[name] {
				for _, argument := range e.Args {
					if tv, ok := info.Types[argument]; ok && (isNamed(tv.Type, "RoundTripper") || isNamed(derefPointer(tv.Type), "Transport")) && !isNil(info, argument) && !provenTransport(info, declaration, argument, depth+1) {
						return false
					}
				}
				return true
			}
			return approvedTransportConstructors[name]
		}
		if signature != nil && fn.Name() == "Clone" && isNamed(derefPointer(signature.Recv().Type()), "Transport") {
			if selector, ok := ast.Unparen(e.Fun).(*ast.SelectorExpr); ok {
				return provenTransport(info, declaration, selector.X, depth+1)
			}
		}
	case *ast.Ident:
		return provenVariable(info, declaration, e, depth)
	}
	return false
}

// provenLiteral: a net/http.Transport literal is a non-nil transport (its Proxy is checked as its own site); any other
// literal is proven when every field it holds of type net/http.RoundTripper or *net/http.Transport is proven, an
// omitted one included (a wrapper over a nil base).
func provenLiteral(info *types.Info, declaration ast.Decl, lit *ast.CompositeLit, depth int) bool {
	tv, ok := info.Types[lit]
	if !ok {
		return false
	}
	if isNamed(tv.Type, "Transport") {
		return true
	}
	structType, ok := derefPointer(tv.Type).Underlying().(*types.Struct)
	if !ok {
		return true
	}
	for i := 0; i < structType.NumFields(); i++ {
		field := structType.Field(i)
		if !isNamed(field.Type(), "RoundTripper") && !isNamed(derefPointer(field.Type()), "Transport") {
			continue
		}
		value, present := fieldValue(lit, field.Name(), i)
		if !present || !provenTransport(info, declaration, value, depth+1) {
			return false
		}
	}
	return true
}

// provenVariable: a local variable or parameter of declaration whose every assignment is proven. A parameter, an
// assignment that is not proven, a declaration without a value and an assignment from a multi-value expression are
// all excused only when the function also holds `if v == nil { v = <proven> }`. Package variables and receivers are
// unproven.
func provenVariable(info *types.Info, declaration ast.Decl, ident *ast.Ident, depth int) bool {
	variable, ok := info.Uses[ident].(*types.Var)
	if !ok {
		return false
	}
	fn, ok := declaration.(*ast.FuncDecl)
	if !ok || fn.Body == nil {
		return false
	}
	parameter := false
	if fn.Type.Params != nil {
		for _, field := range fn.Type.Params.List {
			for _, name := range field.Names {
				if info.Defs[name] == variable {
					parameter = true
				}
			}
		}
	}
	refersTo := func(expression ast.Expr) bool {
		id, ok := ast.Unparen(expression).(*ast.Ident)
		return ok && (info.Defs[id] == variable || info.Uses[id] == variable)
	}
	var assigned []ast.Expr
	declared, unprovable, fallback := parameter, parameter, false
	ast.Inspect(fn.Body, func(node ast.Node) bool {
		switch n := node.(type) {
		case *ast.ValueSpec:
			for i, name := range n.Names {
				if info.Defs[name] != variable {
					continue
				}
				declared = true
				if len(n.Values) != len(n.Names) {
					unprovable = true
				} else {
					assigned = append(assigned, n.Values[i])
				}
			}
		case *ast.AssignStmt:
			for i, lhs := range n.Lhs {
				if !refersTo(lhs) {
					continue
				}
				declared = true
				if len(n.Lhs) != len(n.Rhs) {
					unprovable = true
				} else {
					assigned = append(assigned, n.Rhs[i])
				}
			}
		case *ast.RangeStmt:
			if refersTo(n.Key) || (n.Value != nil && refersTo(n.Value)) {
				unprovable = true
			}
		case *ast.IfStmt:
			if nilCheckOf(info, n.Cond, refersTo) && assignsProven(info, declaration, n.Body, refersTo, depth) {
				fallback = true
			}
		}
		return true
	})
	if !declared || (unprovable && !fallback) {
		return false
	}
	for _, value := range assigned {
		if refersTo(value) {
			continue
		}
		if !fallback && !provenTransport(info, declaration, value, depth+1) {
			return false
		}
	}
	return true
}

// nilCheckOf: condition is `v == nil` (either order) for the variable refersTo names.
func nilCheckOf(info *types.Info, condition ast.Expr, refersTo func(ast.Expr) bool) bool {
	binary, ok := ast.Unparen(condition).(*ast.BinaryExpr)
	if !ok || binary.Op.String() != "==" {
		return false
	}
	return (refersTo(binary.X) && isNil(info, binary.Y)) || (refersTo(binary.Y) && isNil(info, binary.X))
}

// assignsProven: body assigns the variable a proven value at its top level.
func assignsProven(info *types.Info, declaration ast.Decl, body *ast.BlockStmt, refersTo func(ast.Expr) bool, depth int) bool {
	for _, statement := range body.List {
		assign, ok := statement.(*ast.AssignStmt)
		if !ok || len(assign.Lhs) != len(assign.Rhs) {
			continue
		}
		for i, lhs := range assign.Lhs {
			if refersTo(lhs) && provenTransport(info, declaration, assign.Rhs[i], depth+1) {
				return true
			}
		}
	}
	return false
}

func calledFunc(info *types.Info, call *ast.CallExpr) *types.Func {
	switch fun := ast.Unparen(call.Fun).(type) {
	case *ast.Ident:
		fn, _ := info.Uses[fun].(*types.Func)
		return fn
	case *ast.SelectorExpr:
		fn, _ := info.Uses[fun.Sel].(*types.Func)
		return fn
	}
	return nil
}

// isNamed: the type is exactly net/http.<name> (not a pointer to it).
func isNamed(t types.Type, name string) bool {
	if t == nil {
		return false
	}
	named, ok := types.Unalias(t).(*types.Named)
	if !ok {
		return false
	}
	obj := named.Obj()
	return obj.Pkg() != nil && obj.Pkg().Path() == "net/http" && obj.Name() == name
}

func derefPointer(t types.Type) types.Type {
	if t == nil {
		return nil
	}
	if pointer, ok := types.Unalias(t).(*types.Pointer); ok {
		return pointer.Elem()
	}
	return t
}

// fieldValue is the value the literal gives the field: by key, or (an unkeyed literal) by position when position >= 0.
func fieldValue(lit *ast.CompositeLit, field string, position int) (ast.Expr, bool) {
	for i, element := range lit.Elts {
		keyed, ok := element.(*ast.KeyValueExpr)
		if !ok {
			if i == position {
				return element, true
			}
			continue
		}
		if key, ok := keyed.Key.(*ast.Ident); ok && key.Name == field {
			return keyed.Value, true
		}
	}
	return nil, false
}

func isNil(info *types.Info, expression ast.Expr) bool {
	tv, ok := info.Types[expression]
	return ok && tv.IsNil()
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

// compare is the verdict of the found sites against the allow rows, as sorted text.
func compare(found map[site]int, rows map[site]allowRow) []string {
	var problems []string
	for key, count := range found {
		row, ok := rows[key]
		switch {
		case !ok:
			problems = append(problems, fmt.Sprintf("UNLISTED site: %s %s %s (x%d)", key.file, key.symbol, key.kind, count))
		case row.count != count:
			problems = append(problems, fmt.Sprintf("COUNT differs: %s %s %s: allow.tsv %d, code %d", key.file, key.symbol, key.kind, row.count, count))
		case strings.TrimSpace(row.reason) == "":
			problems = append(problems, fmt.Sprintf("NO REASON: %s %s %s", key.file, key.symbol, key.kind))
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

func readAllow(t *testing.T, name string) map[site]allowRow {
	t.Helper()
	handle, err := os.Open(name)
	if err != nil {
		t.Fatal(err)
	}
	defer handle.Close()
	rows := map[site]allowRow{}
	scanner := bufio.NewScanner(handle)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for line := 1; scanner.Scan(); line++ {
		text := scanner.Text()
		if text == "" || strings.HasPrefix(text, "#") {
			continue
		}
		fields := strings.Split(text, "\t")
		if len(fields) != 5 {
			t.Fatalf("%s:%d: want 5 tab-separated fields (file, symbol, kind, count, reason), got %d", name, line, len(fields))
		}
		count, err := strconv.Atoi(fields[3])
		if err != nil || count < 1 {
			t.Fatalf("%s:%d: count %q is not a positive integer", name, line, fields[3])
		}
		key := site{fields[0], fields[1], fields[2]}
		if _, dup := rows[key]; dup {
			t.Fatalf("%s:%d: duplicate row %v", name, line, key)
		}
		rows[key] = allowRow{count: count, reason: fields[4]}
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	return rows
}

// outsideBuild is every production .go file of the module that no loaded build contains (another GOOS, a build tag).
func outsideBuild(t *testing.T, root string, loaded map[string]bool) []string {
	t.Helper()
	var outside []string
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			switch entry.Name() {
			case ".git", "node_modules", "testdata":
				return filepath.SkipDir
			}
			rel, _ := filepath.Rel(root, path)
			rel = filepath.ToSlash(rel)
			if rel == "tests" || rel == "internal/testsupport" {
				return filepath.SkipDir
			}
			if path != root {
				if _, statErr := os.Stat(filepath.Join(path, "go.mod")); statErr == nil && !replacedDir(root, rel) {
					return filepath.SkipDir
				}
			}
			return nil
		}
		if strings.HasSuffix(path, ".go") && !strings.HasSuffix(path, "_test.go") && !loaded[path] {
			rel, _ := filepath.Rel(root, path)
			outside = append(outside, filepath.ToSlash(rel))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(outside)
	return outside
}

// replacedDirs are the module directories go.mod replaces with a local path (walked as production source).
var replacedDirs []string

func replacedDir(_ string, rel string) bool {
	for _, dir := range replacedDirs {
		if rel == dir {
			return true
		}
	}
	return false
}

func scopeProblems(outside, pinned []string) []string {
	if strings.Join(outside, " ") != strings.Join(pinned, " ") {
		return []string{fmt.Sprintf("SCOPE LIMIT changed: production files outside the linux/amd64 and linux/arm64 builds are %v, pinned %v (add the file with its reason, or move it into the build)", outside, pinned)}
	}
	return nil
}

// replacedModules is every module path go.mod replaces with a local directory; it records those directories too.
func replacedModules(t *testing.T, root string) []string {
	t.Helper()
	source, err := os.ReadFile(filepath.Join(root, "go.mod"))
	if err != nil {
		t.Fatal(err)
	}
	var modules []string
	replacedDirs = nil
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
			if strings.HasPrefix(target, "./") {
				modules = append(modules, strings.Fields(parts[0])[0])
				replacedDirs = append(replacedDirs, filepath.ToSlash(filepath.Clean(strings.Fields(target)[0])))
			}
		}
	}
	sort.Strings(modules)
	return modules
}

func uniqueStrings(values []string) []string {
	var out []string
	for i, value := range values {
		if i == 0 || value != values[i-1] {
			out = append(out, value)
		}
	}
	return out
}
