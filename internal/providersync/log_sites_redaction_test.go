package providersync

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"

	"golang.org/x/tools/go/packages"
)

// logSitePackages are the packages whose log lines can see a unit's result,
// a snapshot or provider rows.
var logSitePackages = []string{
	"./internal/providersync",
	"./internal/jobs/providerunit",
	"./internal/workerservice",
}

var slogScalarAttrConstructors = map[string]bool{
	"String": true, "Int": true, "Int64": true, "Uint64": true, "Float64": true,
	"Bool": true, "Time": true, "Duration": true,
}

// logArgumentIsSafe reports whether one argument of a log call can only
// carry a named scalar field.
func logArgumentIsSafe(info *types.Info, expression ast.Expr) (bool, string) {
	if call, ok := ast.Unparen(expression).(*ast.CallExpr); ok {
		if callee := typeutilCallee(info, call); callee != nil {
			// logging.ProviderIDAttr / ProviderIDsAttr build a string or a
			// []string attribute from an id that passed the id shape.
			if callee.Pkg() != nil && callee.Pkg().Path() == "github.com/full-chaos/dev-health-ops/internal/platform/logging" &&
				(callee.Name() == "ProviderIDAttr" || callee.Name() == "ProviderIDsAttr") {
				return true, ""
			}
			if callee.Pkg() != nil && callee.Pkg().Path() == "log/slog" {
				switch {
				case slogScalarAttrConstructors[callee.Name()]:
					return true, ""
				case callee.Name() == "Any" && len(call.Args) == 2:
					return logArgumentIsSafe(info, call.Args[1])
				case callee.Name() == "Group":
					for _, argument := range call.Args[1:] {
						if safe, why := logArgumentIsSafe(info, argument); !safe {
							return false, why
						}
					}
					return true, ""
				}
			}
		}
	}
	typ := info.TypeOf(expression)
	if typ == nil {
		return false, "untyped expression"
	}
	if types.Implements(typ, errorInterface) {
		return true, ""
	}
	if named, ok := typ.(*types.Named); ok && named.Obj().Pkg() != nil &&
		named.Obj().Pkg().Path() == "context" && named.Obj().Name() == "Context" {
		return true, ""
	}
	switch underlying := typ.Underlying().(type) {
	case *types.Basic:
		return true, ""
	case *types.Slice:
		if basic, ok := underlying.Elem().Underlying().(*types.Basic); ok && basic.Info()&types.IsString != 0 {
			return true, ""
		}
	}
	if named, ok := typ.(*types.Named); ok && named.Obj().Pkg() != nil && named.Obj().Pkg().Path() == "time" {
		return true, ""
	}
	return false, typ.String()
}

var errorInterface = types.Universe.Lookup("error").Type().Underlying().(*types.Interface)

func typeutilCallee(info *types.Info, call *ast.CallExpr) *types.Func {
	var ident *ast.Ident
	switch fun := ast.Unparen(call.Fun).(type) {
	case *ast.Ident:
		ident = fun
	case *ast.SelectorExpr:
		ident = fun.Sel
	default:
		return nil
	}
	function, _ := info.Uses[ident].(*types.Func)
	return function
}

// spreadMarker wraps an expression spread into a slice by append(x, y...).
type spreadMarker struct{ ast.Expr }

// spreadSliceElements maps each local slice in body to every expression the
// function puts into it -- composite-literal elements and append arguments --
// so a log call that spreads the slice is checked element by element.
func spreadSliceElements(info *types.Info, body *ast.BlockStmt) map[types.Object][]ast.Expr {
	elements := map[types.Object][]ast.Expr{}
	add := func(target ast.Expr, value ast.Expr) {
		ident, ok := ast.Unparen(target).(*ast.Ident)
		if !ok || info.ObjectOf(ident) == nil {
			return
		}
		object := info.ObjectOf(ident)
		switch built := ast.Unparen(value).(type) {
		case *ast.CompositeLit:
			elements[object] = append(elements[object], built.Elts...)
		case *ast.CallExpr:
			if fun, ok := built.Fun.(*ast.Ident); ok && fun.Name == "append" && len(built.Args) > 1 {
				added := built.Args[1:]
				if built.Ellipsis.IsValid() {
					// The last argument is itself spread: keep it marked so
					// the resolver follows what it carries.
					elements[object] = append(elements[object], added[:len(added)-1]...)
					elements[object] = append(elements[object], &spreadMarker{Expr: added[len(added)-1]})
					return
				}
				elements[object] = append(elements[object], added...)
			}
		}
	}
	ast.Inspect(body, func(node ast.Node) bool {
		switch statement := node.(type) {
		case *ast.AssignStmt:
			for index, target := range statement.Lhs {
				if index < len(statement.Rhs) {
					add(target, statement.Rhs[index])
				}
			}
		case *ast.ValueSpec:
			for index, target := range statement.Names {
				if index < len(statement.Values) {
					add(target, statement.Values[index])
				}
			}
		}
		return true
	})
	return elements
}

// spreadElements resolves the expressions a spread log argument carries: a
// local slice (by what the function put into it), an append (its base and
// its added elements), or a call to a function of the loaded packages that
// returns a slice it built (by what that function put into the slices it
// returns). Anything else cannot be resolved and is reported.
func spreadElements(
	info *types.Info, spreads map[types.Object][]ast.Expr, functions map[types.Object]*ast.FuncDecl,
	expression ast.Expr, depth int,
) ([]ast.Expr, bool) {
	if depth > 4 {
		return nil, false
	}
	switch value := ast.Unparen(expression).(type) {
	case *ast.Ident:
		object := info.ObjectOf(value)
		if object == nil {
			return nil, false
		}
		var resolved []ast.Expr
		for _, element := range spreads[object] {
			if marker, ok := element.(*spreadMarker); ok {
				nested, ok := spreadElements(info, spreads, functions, marker.Expr, depth+1)
				if !ok {
					return nil, false
				}
				resolved = append(resolved, nested...)
				continue
			}
			resolved = append(resolved, element)
		}
		return resolved, true
	case *ast.CallExpr:
		if fun, ok := value.Fun.(*ast.Ident); ok && fun.Name == "append" && len(value.Args) > 0 {
			base, ok := spreadElements(info, spreads, functions, value.Args[0], depth+1)
			if !ok {
				return nil, false
			}
			added := value.Args[1:]
			if value.Ellipsis.IsValid() && len(added) > 0 {
				last, ok := spreadElements(info, spreads, functions, added[len(added)-1], depth+1)
				if !ok {
					return nil, false
				}
				added = append(append([]ast.Expr(nil), added[:len(added)-1]...), last...)
			}
			return append(base, added...), true
		}
		callee := typeutilCallee(info, value)
		function := functions[callee]
		if callee == nil || function == nil {
			return nil, false
		}
		inner := spreadSliceElements(info, function.Body)
		var resolved []ast.Expr
		ok := true
		ast.Inspect(function.Body, func(node ast.Node) bool {
			returned, isReturn := node.(*ast.ReturnStmt)
			if !isReturn || !ok {
				return ok
			}
			for _, result := range returned.Results {
				elements, found := spreadElements(info, inner, functions, result, depth+1)
				if !found {
					ok = false
					return false
				}
				resolved = append(resolved, elements...)
			}
			return true
		})
		return resolved, ok
	}
	return nil, false
}

func isSlogLogCall(callee *types.Func) bool {
	if callee == nil || callee.Pkg() == nil || callee.Pkg().Path() != "log/slog" {
		return false
	}
	switch callee.Name() {
	case "Debug", "Info", "Warn", "Error", "DebugContext", "InfoContext", "WarnContext", "ErrorContext", "Log", "LogAttrs":
		return true
	}
	return false
}

// TestEveryLogSiteLogsOnlyScalars enumerates every slog call
// in the log-site packages and fails on any argument that is not a named
// scalar field (a map, a struct, a nested value).
func TestEveryLogSiteLogsOnlyScalars(t *testing.T) {
	loaded, err := packages.Load(&packages.Config{
		Mode: packages.NeedName | packages.NeedFiles | packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo,
		Dir:  "../..",
	}, logSitePackages...)
	if err != nil {
		t.Fatal(err)
	}
	if packages.PrintErrors(loaded) > 0 {
		t.Fatal("loading the log-site packages failed")
	}
	functions := map[types.Object]*ast.FuncDecl{}
	for _, pkg := range loaded {
		for _, file := range pkg.Syntax {
			for _, declaration := range file.Decls {
				if function, ok := declaration.(*ast.FuncDecl); ok && function.Body != nil {
					functions[pkg.TypesInfo.ObjectOf(function.Name)] = function
				}
			}
		}
	}
	sites, unsafe := 0, []string{}
	for _, pkg := range loaded {
		for _, file := range pkg.Syntax {
			name := pkg.Fset.Position(file.Pos()).Filename
			if index := strings.Index(name, "/internal/"); index >= 0 {
				name = name[index+1:]
			} else if index := strings.Index(name, "/cmd/"); index >= 0 {
				name = name[index+1:]
			}
			ast.Inspect(file, func(node ast.Node) bool {
				function, ok := node.(*ast.FuncDecl)
				if !ok || function.Body == nil {
					return true
				}
				spreads := spreadSliceElements(pkg.TypesInfo, function.Body)
				ast.Inspect(function.Body, func(inner ast.Node) bool {
					call, ok := inner.(*ast.CallExpr)
					if !ok || !isSlogLogCall(typeutilCallee(pkg.TypesInfo, call)) {
						return true
					}
					sites++
					for index, argument := range call.Args {
						arguments := []ast.Expr{argument}
						if call.Ellipsis.IsValid() && index == len(call.Args)-1 {
							resolved, ok := spreadElements(pkg.TypesInfo, spreads, functions, argument, 0)
							if !ok {
								unsafe = append(unsafe, fmt.Sprintf("%s:%d spread of a value built elsewhere", name, pkg.Fset.Position(argument.Pos()).Line))
								continue
							}
							arguments = resolved
						}
						for _, element := range arguments {
							if safe, why := logArgumentIsSafe(pkg.TypesInfo, element); !safe {
								unsafe = append(unsafe, fmt.Sprintf("%s:%d %s", name, pkg.Fset.Position(element.Pos()).Line, why))
							}
						}
					}
					return true
				})
				return false
			})
		}
	}
	sort.Strings(unsafe)
	t.Logf("log sites enumerated: %d", sites)
	if len(unsafe) > 0 {
		t.Fatalf("log arguments that are not a named scalar:\n%s", strings.Join(unsafe, "\n"))
	}
}

// TestLogLinesOfPreparedRecoveryCarryOnlyOwnValues pins the value arguments
// of the prepared-recovery log lines to expressions this package owns: the
// claim's identity, its own recovery and reason words, counts, and the
// classifier's vocabulary word for a protected key. route_completed must be
// built from routeCompletedLog.attrs alone.
func TestLogLinesOfPreparedRecoveryCarryOnlyOwnValues(t *testing.T) {
	loaded, err := packages.Load(&packages.Config{
		Mode: packages.NeedName | packages.NeedFiles | packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo,
		Dir:  "../..",
	}, "./internal/providersync")
	if err != nil || packages.PrintErrors(loaded) > 0 {
		t.Fatalf("load: %v", err)
	}
	ownValues := map[string]bool{
		"session.Claim.Provider": true, "session.Claim.Dataset": true, "session.Claim.ID": true,
		"session.Claim.GenerationKey()": true, "key": true,
	}
	events := map[string]bool{
		"provider_sync.prepared_snapshot_sensitive_key_fallback": false,
		"provider_sync.route_completed":                          false,
	}
	for _, pkg := range loaded {
		for _, file := range pkg.Syntax {
			ast.Inspect(file, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok || !isSlogLogCall(typeutilCallee(pkg.TypesInfo, call)) {
					return true
				}
				for index, argument := range call.Args {
					literal, ok := argument.(*ast.BasicLit)
					if !ok {
						continue
					}
					event, err := strconv.Unquote(literal.Value)
					if _, pinned := events[event]; err != nil || !pinned {
						continue
					}
					events[event] = true
					rest := call.Args[index+1:]
					if event == "provider_sync.route_completed" {
						if len(rest) != 1 || !strings.HasSuffix(types.ExprString(rest[0]), ".attrs()") || !call.Ellipsis.IsValid() {
							t.Errorf("route_completed carries %v, want routeCompletedLog.attrs() only", rest)
						}
						continue
					}
					for position := 1; position < len(rest); position += 2 {
						if value := types.ExprString(rest[position]); !ownValues[value] {
							t.Errorf("%s logs %s, not an own value", event, value)
						}
					}
				}
				return true
			})
		}
	}
	for event, found := range events {
		if !found {
			t.Errorf("log line %s not found", event)
		}
	}
}

// TestProtectedKeyNameIsAlwaysVocabulary holds the classifier to returning a
// name built only from its own vocabulary, whatever key a provider sends, so
// the fallback line's key field never carries provider text.
func TestProtectedKeyNameIsAlwaysVocabulary(t *testing.T) {
	vocabulary := map[string]bool{}
	for word := range protectedKeyWords {
		vocabulary[word] = true
	}
	for pair := range protectedKeyWordPairs {
		vocabulary[pair[0]+"_"+pair[1]] = true
	}
	for _, fragment := range protectedKeyFragments {
		vocabulary[fragment] = true
	}
	prefixes := []string{"", "x", "user", "Provider", "my-", "a.b.", "ZZZ", "tenant_42_"}
	suffixes := []string{"", "s", "Id", "_value", "-Header", ".json", "XYZ", "2"}
	stems := []string{"token", "Token", "API_KEY", "clientSecret", "rawPayload", "SessionCookie", "privateKeyPem",
		"authorization", "source_metadata", "integrationConfig", "bearerToken", "passwd", "cipherText", "certs"}
	checked := 0
	for _, prefix := range prefixes {
		for _, stem := range stems {
			for _, suffix := range suffixes {
				key := prefix + stem + suffix
				name, protected := isPreparedRouteSensitiveKeyName(key)
				if protected && !vocabulary[name] {
					t.Fatalf("key %q classified as %q, which is not a vocabulary name", key, name)
				}
				checked++
			}
		}
	}
	for pair := range protectedKeyWordPairs {
		want := pair[0] + "_" + pair[1]
		for _, first := range protectedWordSpellings(pair[0]) {
			for _, second := range protectedWordSpellings(pair[1]) {
				for _, key := range []string{
					first + "_" + second, first + "-" + second, first + "." + second,
					first + strings.ToUpper(second[:1]) + second[1:],
					strings.ToUpper(first[:1]) + first[1:] + strings.ToUpper(second[:1]) + second[1:],
					strings.ToUpper(first) + "_" + strings.ToUpper(second),
				} {
					name, protected := isPreparedRouteSensitiveKeyName(key)
					if !protected || name != want {
						t.Fatalf("key %q classified %q protected=%v want %q", key, name, protected, want)
					}
					checked++
				}
			}
		}
	}
	for word := range protectedKeyWords {
		for _, spelling := range protectedWordSpellings(word) {
			if name, protected := isPreparedRouteSensitiveKeyName(spelling); !protected || !vocabulary[name] {
				t.Fatalf("key %q classified %q protected=%v", spelling, name, protected)
			}
			checked++
		}
	}
	t.Logf("keys checked: %d", checked)
}

// protectedWordSpellings returns a word with its plural spellings: "-s",
// "-es", and "-ies" in place of a final "y".
func protectedWordSpellings(word string) []string {
	spellings := []string{word, word + "s", word + "es"}
	if stem, found := strings.CutSuffix(word, "y"); found {
		spellings = append(spellings, stem+"ies")
	}
	return spellings
}

// TestSensitiveKeyErrorIsBuiltOnlyFromTheClassifier pins that the fallback's
// key field can only hold what preparedRouteSensitiveKey returned.
func TestSensitiveKeyErrorIsBuiltOnlyFromTheClassifier(t *testing.T) {
	loaded, err := packages.Load(&packages.Config{
		Mode: packages.NeedName | packages.NeedFiles | packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo,
		Dir:  "../..",
	}, "./internal/providersync")
	if err != nil || packages.PrintErrors(loaded) > 0 {
		t.Fatalf("load: %v", err)
	}
	guarded := map[*ast.CompositeLit]bool{}
	constructed := []*ast.CompositeLit{}
	for _, pkg := range loaded {
		for _, file := range pkg.Syntax {
			ast.Inspect(file, func(node ast.Node) bool {
				switch typed := node.(type) {
				case *ast.CompositeLit:
					if types.ExprString(typed.Type) == "preparedSnapshotSensitiveKeyError" {
						constructed = append(constructed, typed)
					}
				case *ast.IfStmt:
					assign, ok := typed.Init.(*ast.AssignStmt)
					if !ok || len(assign.Rhs) != 1 || len(assign.Lhs) != 2 {
						return true
					}
					call, ok := assign.Rhs[0].(*ast.CallExpr)
					if !ok || types.ExprString(call.Fun) != "preparedRouteSensitiveKey" {
						return true
					}
					keyName := types.ExprString(assign.Lhs[0])
					ast.Inspect(typed.Body, func(inner ast.Node) bool {
						literal, ok := inner.(*ast.CompositeLit)
						if !ok || types.ExprString(literal.Type) != "preparedSnapshotSensitiveKeyError" || len(literal.Elts) != 1 {
							return true
						}
						if field, ok := literal.Elts[0].(*ast.KeyValueExpr); ok && types.ExprString(field.Value) == keyName {
							guarded[literal] = true
						}
						return true
					})
				}
				return true
			})
		}
	}
	if len(constructed) == 0 {
		t.Fatal("no construction of preparedSnapshotSensitiveKeyError found")
	}
	for _, literal := range constructed {
		if !guarded[literal] {
			t.Errorf("preparedSnapshotSensitiveKeyError built from something other than preparedRouteSensitiveKey's answer: %s", types.ExprString(literal))
		}
	}
}

// TestNoPackageFieldOrKeyIsProtectedUnlessListed runs the classifier over
// every struct json tag and every string map-literal key in the package's
// non-test sources. Only the listed matches may be protected; a new field or
// key the classifier protects fails here, so a route cannot start writing a
// protected column without the snapshot fallback being argued for it.
func TestNoPackageFieldOrKeyIsProtectedUnlessListed(t *testing.T) {
	allowed := map[string]string{
		"credential_modes": "capability matrix: names the credential modes a provider supports, carries no secret",
		"nextPageToken":    "Jira API page cursor read from the provider's response, never a row column",
	}
	vocabularyVariables := map[string]bool{"protectedKeyWords": true, "protectedKeyWordPairs": true}
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	fileSet := token.NewFileSet()
	matches := map[string]string{}
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		parsed, err := parser.ParseFile(fileSet, file, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		skip := map[ast.Node]bool{}
		ast.Inspect(parsed, func(node ast.Node) bool {
			if spec, ok := node.(*ast.ValueSpec); ok {
				for index, name := range spec.Names {
					if vocabularyVariables[name.Name] && index < len(spec.Values) {
						skip[spec.Values[index]] = true
					}
				}
			}
			if skip[node] {
				return false
			}
			switch typed := node.(type) {
			case *ast.Field:
				if typed.Tag == nil {
					return true
				}
				tag, err := strconv.Unquote(typed.Tag.Value)
				if err != nil {
					return true
				}
				if name := strings.Split(reflect.StructTag(tag).Get("json"), ",")[0]; name != "" && name != "-" {
					if _, protected := isPreparedRouteSensitiveKeyName(name); protected {
						matches[name] = file
					}
				}
			case *ast.KeyValueExpr:
				literal, ok := typed.Key.(*ast.BasicLit)
				if !ok || literal.Kind != token.STRING {
					return true
				}
				if key, err := strconv.Unquote(literal.Value); err == nil {
					if _, protected := isPreparedRouteSensitiveKeyName(key); protected {
						matches[key] = file
					}
				}
			}
			return true
		})
	}
	for name, file := range matches {
		if _, ok := allowed[name]; !ok {
			t.Errorf("%s: %q is protected by the classifier and not listed", file, name)
		}
	}
	for name := range allowed {
		if _, ok := matches[name]; !ok {
			t.Errorf("listed %q no longer appears; drop it from the list", name)
		}
	}
}
