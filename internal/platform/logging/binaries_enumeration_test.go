package logging_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"io"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// binaryLogging names how each binary under cmd/ reaches the redacting
// handler. The test regenerates the binary list from `go list` and fails
// when a binary is missing here, so a new binary is classified before it
// ships.
//
//	shell       internal/platform/shell.Main builds logging.NewJSON and
//	            installs it as the process default for the run
//	authruntime internal/auth/authruntime.Main does the same
//	installs    main() calls logging.InstallDefault itself
//	silent      nothing the binary links logs through slog.Default, the
//	            package-level slog functions or the standard log package;
//	            the sweep below proves it
//	stdlib_cli  an operator proof tool whose only log calls are its own
//	            fatal usage errors (log.Fatalf of its own error text)
//	cli         main calls internal/cli.Main, and cli.Main installs the
//	            redacting handler as the process default before dispatch
var binaryLogging = map[string]string{
	"ask-dev-jobs-probe":                     "installs",
	"auth-migrate":                           "installs",
	"auth-service":                           "authruntime",
	"cigate":                                 "installs",
	"dev-health-migration-matrix":            "installs",
	"dev-health-provider-fixture":            "installs",
	"dev-health-provider-normalized-fixture": "installs",
	"dev-health-worker-migrate":              "silent",
	"dho":                                    "cli",
	"gqlgen-guard":                           "silent",
	"faultproxy":                             "installs",
	"loadtest":                               "installs",
	"registrydump":                           "silent",
	"shadow-venue":                           "installs",
	"strip-agent-attribution":                "silent",
	"venue-extstub":                          "installs",
}

type listedPackage struct {
	ImportPath string
	Name       string
	Dir        string
	GoFiles    []string
	CgoFiles   []string
	Deps       []string
	Module     *struct{ Path string }
}

const modulePath = "github.com/full-chaos/dev-health-ops"

// defaultLoggingCalls are calls that write through the process default
// logger rather than a logger handed in.
var defaultLoggingCalls = map[string]map[string]bool{
	"log/slog": {
		"Default": true, "Debug": true, "Info": true, "Warn": true, "Error": true, "Log": true, "LogAttrs": true,
		"DebugContext": true, "InfoContext": true, "WarnContext": true, "ErrorContext": true,
	},
	"log": {
		"Print": true, "Printf": true, "Println": true, "Fatal": true, "Fatalf": true, "Fatalln": true,
		"Panic": true, "Panicf": true, "Panicln": true, "Default": true, "Output": true,
	},
}

// loadCmdPackages lists every package the binaries under cmd/ link (go list
// -deps) and parses the non-test files of the module's own packages.
func loadCmdPackages(t *testing.T) (map[string]listedPackage, map[string][]*ast.File, *token.FileSet) {
	t.Helper()
	root := moduleRoot(t)
	command := exec.Command("go", "list", "-deps", "-json=ImportPath,Name,Dir,GoFiles,CgoFiles,Deps,Module", "./cmd/...")
	command.Dir = root
	output, err := command.Output()
	if err != nil {
		t.Fatalf("go list: %v", err)
	}
	packages := map[string]listedPackage{}
	decoder := json.NewDecoder(bytes.NewReader(output))
	for {
		var listed listedPackage
		if err := decoder.Decode(&listed); err == io.EOF {
			break
		} else if err != nil {
			t.Fatalf("decode go list: %v", err)
		}
		packages[listed.ImportPath] = listed
	}
	if len(packages) == 0 {
		t.Fatal("go list returned no packages")
	}
	fileSet := token.NewFileSet()
	parsed := map[string][]*ast.File{}
	for path, listed := range packages {
		if listed.Module == nil || listed.Module.Path != modulePath {
			continue
		}
		for _, name := range linkedSourceFiles(listed) {
			file, err := parser.ParseFile(fileSet, filepath.Join(listed.Dir, name), nil, 0)
			if err != nil {
				t.Fatalf("parse %s: %v", name, err)
			}
			parsed[path] = append(parsed[path], file)
		}
	}
	return packages, parsed, fileSet
}

// linkedSourceFiles names the files of listed that are parsed for the scans.
func linkedSourceFiles(listed listedPackage) []string {
	return append(append([]string{}, listed.GoFiles...), listed.CgoFiles...)
}

// TestEveryBinaryLogsThroughTheRedactingHandler proves by enumeration that
// every binary under cmd/ logs through logging.NewJSON's handler, and that
// no production package builds its own slog handler to a real writer.
func TestEveryBinaryLogsThroughTheRedactingHandler(t *testing.T) {
	t.Parallel()
	packages, parsed, fileSet := loadCmdPackages(t)

	var binaries []string
	for path, listed := range packages {
		if listed.Name == "main" && strings.HasPrefix(path, modulePath+"/cmd/") && strings.Count(strings.TrimPrefix(path, modulePath+"/cmd/"), "/") == 0 {
			binaries = append(binaries, strings.TrimPrefix(path, modulePath+"/cmd/"))
		}
	}
	sort.Strings(binaries)
	if len(binaries) != len(binaryLogging) {
		t.Errorf("go list found %d binaries %v; binaryLogging classifies %d", len(binaries), binaries, len(binaryLogging))
	}
	for _, binary := range binaries {
		mechanism, classified := binaryLogging[binary]
		if !classified {
			t.Errorf("binary %s is not classified in binaryLogging", binary)
			continue
		}
		mainPath := modulePath + "/cmd/" + binary
		switch mechanism {
		case "shell":
			requireCall(t, binary, parsed[mainPath], modulePath+"/internal/platform/shell", "Main")
		case "authruntime":
			requireCall(t, binary, parsed[mainPath], modulePath+"/internal/auth/authruntime", "Main")
		case "cli":
			requireCall(t, binary, parsed[mainPath], modulePath+"/internal/cli", "Main")
			requireFuncCall(t, binary, parsed[modulePath+"/internal/cli"], "Main", modulePath+"/internal/platform/logging", "InstallDefault")
		case "installs":
			requireCall(t, binary, parsed[mainPath], modulePath+"/internal/platform/logging", "InstallDefault")
		case "silent", "stdlib_cli":
			for _, dependency := range append([]string{mainPath}, packages[mainPath].Deps...) {
				if dependency == modulePath+"/internal/platform/logging" {
					// InstallDefault reads slog.Default to restore it.
					continue
				}
				for _, call := range defaultLogging(parsed[dependency]) {
					if mechanism == "stdlib_cli" && dependency == mainPath && strings.HasPrefix(call, "log.Fatal") {
						continue
					}
					t.Errorf("binary %s is classified %s but %s calls %s", binary, mechanism, dependency, call)
				}
			}
		default:
			t.Errorf("binary %s has unknown mechanism %q", binary, mechanism)
		}
	}

	if len(parsed) == 0 {
		t.Fatal("no module package was parsed")
	}
	for path, files := range parsed {
		if path == modulePath+"/internal/platform/logging" {
			continue
		}
		for _, file := range files {
			for _, handler := range ownHandlers(file) {
				t.Errorf("%s builds its own slog handler: %s", fileSet.Position(handler.Pos()), render(handler))
			}
		}
	}
}

func moduleRoot(t *testing.T) string {
	t.Helper()
	output, err := exec.Command("go", "env", "GOMOD").Output()
	if err != nil {
		t.Fatalf("go env GOMOD: %v", err)
	}
	return filepath.Dir(strings.TrimSpace(string(output)))
}

// importNames maps each local import name in file to its import path.
func importNames(file *ast.File) map[string]string {
	names := map[string]string{}
	for _, spec := range file.Imports {
		path, _ := strconv.Unquote(spec.Path.Value)
		name := filepath.Base(path)
		if spec.Name != nil {
			name = spec.Name.Name
		}
		names[name] = path
	}
	return names
}

func calls(file *ast.File, visit func(path, function string, call *ast.CallExpr)) {
	names := importNames(file)
	ast.Inspect(file, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		selector, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		ident, ok := selector.X.(*ast.Ident)
		if !ok || ident.Obj != nil {
			return true
		}
		if path, imported := names[ident.Name]; imported {
			visit(path, selector.Sel.Name, call)
		}
		return true
	})
}

// requireCall checks that func main calls path.function as one of its own
// top-level statements (an expression, a defer, or the argument of one),
// so the call runs on every start and cannot sit behind a branch.
func requireCall(t *testing.T, binary string, files []*ast.File, path, function string) {
	t.Helper()
	requireFuncCall(t, binary, files, "main", path, function)
}

// requireFuncCall is requireCall for any top-level function named caller.
func requireFuncCall(t *testing.T, binary string, files []*ast.File, caller, path, function string) {
	t.Helper()
	if !funcCalls(files, caller, path, function) {
		t.Errorf("binary %s: func %s does not call %s.%s as a top-level statement", binary, caller, path, function)
	}
}

// funcCalls reports whether the top-level function named caller calls
// path.function as one of its own top-level statements.
func funcCalls(files []*ast.File, caller, path, function string) bool {
	found := false
	for _, file := range files {
		names := importNames(file)
		for _, declaration := range file.Decls {
			main, ok := declaration.(*ast.FuncDecl)
			if !ok || main.Name.Name != caller || main.Recv != nil || main.Body == nil {
				continue
			}
			for _, statement := range main.Body.List {
				var expression ast.Expr
				switch typed := statement.(type) {
				case *ast.ExprStmt:
					expression = typed.X
				case *ast.DeferStmt:
					expression = typed.Call
				}
				if expression != nil && topLevelCall(expression, names, path, function) {
					found = true
				}
			}
		}
	}
	return found
}

// topLevelCall reports whether expression is a call of path.function, or a
// call whose function or first argument is one (os.Exit(shell.Execute(...)),
// logging.InstallDefault(...)()).
func topLevelCall(expression ast.Expr, names map[string]string, path, function string) bool {
	call, ok := expression.(*ast.CallExpr)
	if !ok {
		return false
	}
	if selector, ok := call.Fun.(*ast.SelectorExpr); ok {
		if ident, ok := selector.X.(*ast.Ident); ok && ident.Obj == nil && names[ident.Name] == path && selector.Sel.Name == function {
			return true
		}
	}
	if inner, ok := call.Fun.(*ast.CallExpr); ok && topLevelCall(inner, names, path, function) {
		return true
	}
	return len(call.Args) > 0 && topLevelCall(call.Args[0], names, path, function)
}

func defaultLogging(files []*ast.File) []string {
	var found []string
	for _, file := range files {
		calls(file, func(path, function string, _ *ast.CallExpr) {
			if defaultLoggingCalls[path][function] {
				found = append(found, filepath.Base(path)+"."+function)
			}
		})
	}
	return found
}

// ownHandlers finds slog handlers built outside the logging package. A
// handler whose writer discards everything is allowed: it prints nothing.
func ownHandlers(file *ast.File) []*ast.CallExpr {
	var found []*ast.CallExpr
	calls(file, func(path, function string, call *ast.CallExpr) {
		if path != "log/slog" || (function != "NewJSONHandler" && function != "NewTextHandler") {
			return
		}
		if len(call.Args) > 0 {
			switch writer := render(call.Args[0]); writer {
			case "io.Discard", "discard{}":
				return
			}
		}
		found = append(found, call)
	})
	return found
}

func render(node ast.Node) string {
	var buffer bytes.Buffer
	if err := formatNode(&buffer, node); err != nil {
		return "?"
	}
	return buffer.String()
}

func formatNode(buffer *bytes.Buffer, node ast.Node) error {
	return printer.Fprint(buffer, token.NewFileSet(), node)
}

// deployedServices names the packages that run a long-lived service: each
// calls shell.Execute, which installs the JSON handler for the run. The
// services are subcommands of dho (api, query-api, reconciler, scheduler,
// stream-runner, worker); auth-service is its own binary (authruntime).
var deployedServices = []string{
	"internal/apiservice",
	"internal/queryapiservice",
	"internal/reconcilerservice",
	"internal/schedulerservice",
	"internal/streamrunnerservice",
	"internal/workerservice",
}

// TestEveryServiceInstallsTheJSONHandler proves by static scan (go list -deps
// of ./cmd/..., then the AST of every non-test Go and Cgo file the binaries
// link) that each deployed service reaches
// logging.InstallDefault(logging.NewJSON(...)), and that no linked production
// package installs another default handler or writes the standard log package
// raw.
//
// It resolves imports by path and refuses what it cannot follow without a
// type checker: a dot import of log, log/slog, logging or shell; one of the
// census functions used other than as the function of a call; a logger or
// writer variable that is not declared once by := and left alone. The one
// installed logger that is not logging.NewJSON is the value-redaction
// wrapper around slog.Default().Handler(), which keeps the installed handler.
//
// Limits: it reads source, it does not run the binaries; a handler type
// written by hand and passed to slog.New is not seen (only the slog
// constructors and setters are); a function that is handed the logger and
// writes through the pointer is not seen; the default the value-redaction
// wrapper keeps is not proven to be the JSON handler; reflect, unsafe and
// linkname are not seen; code a binary does not link is not scanned.
func TestEveryServiceInstallsTheJSONHandler(t *testing.T) {
	t.Parallel()
	packages, parsed, fileSet := loadCmdPackages(t)
	if len(packages[modulePath+"/cmd/auth-service"].GoFiles) == 0 {
		t.Error("cmd/auth-service is not listed")
	}
	for _, finding := range serviceHandlerCensus(fileSet, parsed) {
		t.Error(finding)
	}
}

// censusFunctions are the functions the census follows. A reference to one
// that is not the function of a call (a function value) is refused.
func censusFunctions(loggingPath, shellPath string) map[string]map[string]bool {
	return map[string]map[string]bool{
		"log/slog":  {"SetDefault": true, "NewTextHandler": true, "NewJSONHandler": true},
		"log":       {"SetOutput": true},
		loggingPath: {"InstallDefault": true, "NewJSON": true},
		shellPath:   {"Execute": true, "Main": true},
	}
}

// serviceHandlerCensus runs the census checks over parsed (import path to
// the parsed non-test files of that package) and returns one finding per
// failed check.
func serviceHandlerCensus(fileSet *token.FileSet, parsed map[string][]*ast.File) []string {
	var findings []string
	report := func(format string, arguments ...any) {
		findings = append(findings, fmt.Sprintf(format, arguments...))
	}
	loggingPath := modulePath + "/internal/platform/logging"
	shellPath := modulePath + "/internal/platform/shell"
	followed := censusFunctions(loggingPath, shellPath)

	for _, files := range parsed {
		for _, file := range files {
			names := importNames(file)
			for _, spec := range file.Imports {
				path, _ := strconv.Unquote(spec.Path.Value)
				if spec.Name != nil && spec.Name.Name == "." && followed[path] != nil {
					report("%s: dot import of %s hides the calls the census follows", fileSet.Position(spec.Pos()), path)
				}
			}
			called := map[*ast.SelectorExpr]bool{}
			ast.Inspect(file, func(node ast.Node) bool {
				if call, ok := node.(*ast.CallExpr); ok {
					if selector, ok := call.Fun.(*ast.SelectorExpr); ok {
						called[selector] = true
					}
				}
				return true
			})
			ast.Inspect(file, func(node ast.Node) bool {
				selector, ok := node.(*ast.SelectorExpr)
				if !ok || called[selector] {
					return true
				}
				if path, function, ok := importedSelector(selector, names); ok && followed[path][function] {
					report("%s: %s is used as a value, not called", fileSet.Position(selector.Pos()), render(selector))
				}
				return true
			})
		}
	}

	callers := map[string]bool{}
	for path, files := range parsed {
		if path == shellPath {
			continue
		}
		for _, file := range files {
			calls(file, func(callPath, function string, _ *ast.CallExpr) {
				if callPath == shellPath && (function == "Execute" || function == "Main") {
					callers[strings.TrimPrefix(path, modulePath+"/")] = true
				}
			})
		}
	}
	for _, service := range deployedServices {
		if !callers[service] {
			report("service %s does not call shell.Execute (or is not linked by a cmd binary)", service)
		}
		delete(callers, service)
	}
	for extra := range callers {
		report("%s calls shell.Execute but is not named in deployedServices", extra)
	}

	for _, entry := range []string{shellPath, modulePath + "/internal/auth/authruntime"} {
		if !installsJSON(parsed[entry], "Execute", loggingPath) {
			report("%s: func Execute does not install a logger built by logging.NewJSON through a top-level defer logging.InstallDefault", entry)
		}
	}
	if !funcCalls(parsed[modulePath+"/cmd/auth-service"], "main", modulePath+"/internal/auth/authruntime", "Main") {
		report("binary auth-service: func main does not call %s.Main as a top-level statement", modulePath+"/internal/auth/authruntime")
	}
	if !funcCalls(parsed[modulePath+"/cmd/dho"], "main", modulePath+"/internal/cli", "Main") {
		report("binary dho: func main does not call %s.Main as a top-level statement", modulePath+"/internal/cli")
	}

	for path, files := range parsed {
		if path == loggingPath {
			continue
		}
		discardTypes := discardWriterTypes(files)
		for _, file := range files {
			names := importNames(file)
			for _, declaration := range file.Decls {
				function, _ := declaration.(*ast.FuncDecl)
				ast.Inspect(declaration, func(node ast.Node) bool {
					call, ok := node.(*ast.CallExpr)
					if !ok {
						return true
					}
					selector, ok := call.Fun.(*ast.SelectorExpr)
					if !ok {
						return true
					}
					callPath, name, ok := importedSelector(selector, names)
					if !ok {
						return true
					}
					where := fileSet.Position(call.Pos())
					switch {
					case callPath == "log/slog" && (name == "NewTextHandler" || name == "NewJSONHandler"):
						if len(call.Args) == 0 || !discardsOutput(call.Args[0], function, names, discardTypes) {
							report("%s: %s builds a slog handler on a real writer", where, render(call))
						}
					case callPath == "log" && name == "SetOutput":
						if len(call.Args) == 0 || !discardsOutput(call.Args[0], function, names, discardTypes) {
							report("%s: %s points the standard log package at a raw writer", where, render(call))
						}
					case callPath == "log/slog" && name == "SetDefault":
						report("%s: %s installs a default handler outside logging.InstallDefault", where, render(call))
					case callPath == loggingPath && name == "InstallDefault":
						if len(call.Args) != 1 || !jsonLogger(call.Args[0], function, names, loggingPath) {
							report("%s: %s does not install logging.NewJSON", where, render(call))
						}
					}
					return true
				})
			}
		}
	}
	return findings
}

// importedSelector resolves selector to the import path and name it refers
// to when its left side is an import name (not a local that shadows one).
func importedSelector(selector *ast.SelectorExpr, names map[string]string) (string, string, bool) {
	ident, ok := selector.X.(*ast.Ident)
	if !ok || ident.Obj != nil || ident.Name == "." || ident.Name == "_" {
		return "", "", false
	}
	path, imported := names[ident.Name]
	return path, selector.Sel.Name, imported
}

// isImportedCall reports whether expression is a call of path.name.
func isImportedCall(expression ast.Expr, names map[string]string, path, name string) bool {
	call, ok := expression.(*ast.CallExpr)
	if !ok {
		return false
	}
	selector, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	callPath, function, ok := importedSelector(selector, names)
	return ok && callPath == path && function == name
}

// wrapsInstalledDefault reports whether expression is exactly
// slog.New(logging.WithValueRedaction(slog.Default().Handler(), <any>)): it
// adds redaction to the handler already installed and changes nothing else.
func wrapsInstalledDefault(expression ast.Expr, names map[string]string, loggingPath string) bool {
	if !isImportedCall(expression, names, "log/slog", "New") {
		return false
	}
	logger := expression.(*ast.CallExpr)
	if len(logger.Args) != 1 || logger.Ellipsis.IsValid() || !isImportedCall(logger.Args[0], names, loggingPath, "WithValueRedaction") {
		return false
	}
	wrapper := logger.Args[0].(*ast.CallExpr)
	if len(wrapper.Args) != 2 || wrapper.Ellipsis.IsValid() {
		return false
	}
	handler, ok := wrapper.Args[0].(*ast.CallExpr)
	if !ok || len(handler.Args) != 0 {
		return false
	}
	method, ok := handler.Fun.(*ast.SelectorExpr)
	if !ok || method.Sel.Name != "Handler" || !isImportedCall(method.X, names, "log/slog", "Default") {
		return false
	}
	return len(method.X.(*ast.CallExpr).Args) == 0
}

// isIODiscard reports whether expression is io.Discard.
func isIODiscard(expression ast.Expr, names map[string]string) bool {
	selector, ok := expression.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	path, name, ok := importedSelector(selector, names)
	return ok && path == "io" && name == "Discard"
}

// jsonLogger reports whether argument, passed to logging.InstallDefault
// inside function (nil at package level), is a logging.NewJSON(...) call,
// the value-redaction wrapper around the installed default, or a variable
// that function declares once by := logging.NewJSON(...) and never assigns
// or takes the address of again.
func jsonLogger(argument ast.Expr, function *ast.FuncDecl, names map[string]string, loggingPath string) bool {
	isNewJSON := func(expression ast.Expr) bool {
		return isImportedCall(expression, names, loggingPath, "NewJSON")
	}
	if isNewJSON(argument) || wrapsInstalledDefault(argument, names, loggingPath) {
		return true
	}
	ident, ok := argument.(*ast.Ident)
	return ok && function != nil && boundOnce(function, ident.Name, isNewJSON)
}

// discardsOutput reports whether writer, inside function (nil at package
// level), drops every write: io.Discard, a discard{} literal of the
// package's own drop-everything type, or a variable function declares once
// by := io.Discard and never assigns or takes the address of again.
func discardsOutput(writer ast.Expr, function *ast.FuncDecl, names map[string]string, discardTypes map[*ast.TypeSpec]bool) bool {
	if isIODiscard(writer, names) {
		return true
	}
	switch typed := writer.(type) {
	case *ast.CompositeLit:
		ident, ok := typed.Type.(*ast.Ident)
		if !ok || ident.Name != "discard" || len(typed.Elts) != 0 || len(discardTypes) == 0 {
			return false
		}
		if ident.Obj == nil {
			return true
		}
		spec, ok := ident.Obj.Decl.(*ast.TypeSpec)
		return ok && discardTypes[spec]
	case *ast.Ident:
		return function != nil && boundOnce(function, typed.Name, func(expression ast.Expr) bool {
			return isIODiscard(expression, names)
		})
	}
	return false
}

// discardWriterTypes finds the package-level type discard struct{} whose
// Write method is exactly `return len(p), nil`.
func discardWriterTypes(files []*ast.File) map[*ast.TypeSpec]bool {
	var spec *ast.TypeSpec
	dropsWrites := false
	for _, file := range files {
		for _, declaration := range file.Decls {
			switch typed := declaration.(type) {
			case *ast.GenDecl:
				for _, candidate := range typed.Specs {
					if typeSpec, ok := candidate.(*ast.TypeSpec); ok && typeSpec.Name.Name == "discard" && typeSpec.Assign == 0 {
						if structType, ok := typeSpec.Type.(*ast.StructType); ok && len(structType.Fields.List) == 0 {
							spec = typeSpec
						}
					}
				}
			case *ast.FuncDecl:
				if typed.Name.Name == "Write" && typed.Recv != nil && len(typed.Recv.List) == 1 && typed.Body != nil {
					if receiver, ok := typed.Recv.List[0].Type.(*ast.Ident); ok && receiver.Name == "discard" {
						dropsWrites = writeDropsEverything(typed)
					}
				}
			}
		}
	}
	if spec == nil || !dropsWrites {
		return nil
	}
	return map[*ast.TypeSpec]bool{spec: true}
}

// writeDropsEverything reports whether method is
// Write(p []byte) (int, error) { return len(p), nil }.
func writeDropsEverything(method *ast.FuncDecl) bool {
	params := method.Type.Params.List
	if len(params) != 1 || len(params[0].Names) != 1 || len(method.Body.List) != 1 {
		return false
	}
	parameter := params[0].Names[0].Name
	ret, ok := method.Body.List[0].(*ast.ReturnStmt)
	if !ok || len(ret.Results) != 2 {
		return false
	}
	length, ok := ret.Results[0].(*ast.CallExpr)
	if !ok || len(length.Args) != 1 {
		return false
	}
	lenIdent, ok := length.Fun.(*ast.Ident)
	argument, argOK := length.Args[0].(*ast.Ident)
	nilIdent, nilOK := ret.Results[1].(*ast.Ident)
	return ok && lenIdent.Name == "len" && lenIdent.Obj == nil &&
		argOK && argument.Name == parameter &&
		nilOK && nilIdent.Name == "nil" && nilIdent.Obj == nil
}

// boundOnce reports whether name, anywhere in function (its receiver,
// parameters, results and body, closures included), is declared exactly
// once, by name := <expression accepted by value>, and is never assigned
// again in any form, declared again, or has its address taken.
func boundOnce(function *ast.FuncDecl, name string, value func(ast.Expr) bool) bool {
	definitions := 0
	other := false
	fields := func(list *ast.FieldList) {
		if list == nil {
			return
		}
		for _, field := range list.List {
			for _, ident := range field.Names {
				if ident.Name == name {
					other = true
				}
			}
		}
	}
	fields(function.Recv)
	ast.Inspect(function, func(node ast.Node) bool {
		switch typed := node.(type) {
		case *ast.FuncType:
			fields(typed.TypeParams)
			fields(typed.Params)
			fields(typed.Results)
		case *ast.AssignStmt:
			for index, left := range typed.Lhs {
				if rootIdent(left) != name {
					continue
				}
				if _, bare := left.(*ast.Ident); bare && typed.Tok == token.DEFINE && len(typed.Lhs) == len(typed.Rhs) && value(typed.Rhs[index]) {
					definitions++
				} else {
					other = true
				}
			}
		case *ast.ValueSpec:
			for _, ident := range typed.Names {
				if ident.Name == name {
					other = true
				}
			}
		case *ast.RangeStmt:
			if rootIdent(typed.Key) == name || rootIdent(typed.Value) == name {
				other = true
			}
		case *ast.IncDecStmt:
			if rootIdent(typed.X) == name {
				other = true
			}
		case *ast.UnaryExpr:
			if typed.Op == token.AND && rootIdent(typed.X) == name {
				other = true
			}
		}
		return true
	})
	return definitions == 1 && !other
}

// rootIdent names the variable an assignable expression writes into: x for
// x, *x, x.f, x[i], (x).
func rootIdent(expression ast.Expr) string {
	for {
		switch typed := expression.(type) {
		case *ast.Ident:
			return typed.Name
		case *ast.ParenExpr:
			expression = typed.X
		case *ast.StarExpr:
			expression = typed.X
		case *ast.SelectorExpr:
			expression = typed.X
		case *ast.IndexExpr:
			expression = typed.X
		case *ast.IndexListExpr:
			expression = typed.X
		case *ast.SliceExpr:
			expression = typed.X
		default:
			return ""
		}
	}
}

// installsJSON reports whether the top-level function named function runs
// defer logging.InstallDefault(logger)() as one of its own top-level
// statements, where logger is declared once by := logging.NewJSON(...) and
// never assigned again, so every run of the entry point logs through the
// JSON handler.
func installsJSON(files []*ast.File, function, loggingPath string) bool {
	found := false
	for _, file := range files {
		names := importNames(file)
		for _, declaration := range file.Decls {
			decl, ok := declaration.(*ast.FuncDecl)
			if !ok || decl.Name.Name != function || decl.Recv != nil || decl.Body == nil {
				continue
			}
			for _, statement := range decl.Body.List {
				deferred, ok := statement.(*ast.DeferStmt)
				if !ok {
					continue
				}
				inner, ok := deferred.Call.Fun.(*ast.CallExpr)
				if !ok || len(inner.Args) != 1 || !isImportedCall(inner, names, loggingPath, "InstallDefault") {
					continue
				}
				if _, isIdent := inner.Args[0].(*ast.Ident); isIdent && jsonLogger(inner.Args[0], decl, names, loggingPath) {
					found = true
				}
			}
		}
	}
	return found
}

// censusBaseline is the smallest source tree serviceHandlerCensus accepts:
// keys are file paths relative to the module root.
func censusBaseline() map[string]string {
	const entry = `import (
	"os"

	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
)

func Execute() int {
	logger := logging.NewJSON(os.Stdout, 0)
	defer logging.InstallDefault(logger)()
	logger.Info("run")
	return 0
}

func Main() { os.Exit(Execute()) }
`
	files := map[string]string{
		"internal/platform/shell/shell.go":     "package shell\n\n" + entry,
		"internal/auth/authruntime/runtime.go": "package authruntime\n\n" + entry,
		"cmd/auth-service/main.go":             "package main\n\nimport \"github.com/full-chaos/dev-health-ops/internal/auth/authruntime\"\n\nfunc main() { authruntime.Main() }\n",
		"cmd/dho/main.go":                      "package main\n\nimport \"github.com/full-chaos/dev-health-ops/internal/cli\"\n\nfunc main() { cli.Main() }\n",
	}
	for _, service := range deployedServices {
		files[service+"/service.go"] = "package " + filepath.Base(service) + "\n\nimport \"github.com/full-chaos/dev-health-ops/internal/platform/shell\"\n\nfunc Run() int { return shell.Execute() }\n"
	}
	return files
}

// censusFixtures feed serviceHandlerCensus a synthetic tree: the baseline
// with files added or replaced. Each names the review cell (C*) or the rule
// clause (R*) it pins; accept says whether the census must accept the tree;
// want, when set, is a text one finding of a refused tree must hold.
var censusFixtures = []struct {
	name   string
	accept bool
	want   string
	files  map[string]string
}{
	{name: "baseline", accept: true},
	{name: "C1a linked package installs slog.Default through a variable", files: map[string]string{
		"internal/apiservice/zz_fixture.go": `package apiservice

import (
	"log/slog"

	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
)

func install() func() {
	logger := slog.Default()
	return logging.InstallDefault(logger)
}
`}},
	{name: "C1b shell entry point reassigns its JSON logger before the install", files: map[string]string{
		"internal/platform/shell/shell.go": `package shell

import (
	"log/slog"
	"os"

	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
)

func Execute() int {
	logger := logging.NewJSON(os.Stdout, 0)
	logger = slog.Default()
	defer logging.InstallDefault(logger)()
	return 0
}

func Main() { os.Exit(Execute()) }
`}},
	{name: "C1b authruntime entry point reassigns its JSON logger before the install", files: map[string]string{
		"internal/auth/authruntime/runtime.go": `package authruntime

import (
	"log/slog"
	"os"

	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
)

func Execute() int {
	logger := logging.NewJSON(os.Stdout, 0)
	logger = slog.Default()
	defer logging.InstallDefault(logger)()
	return 0
}

func Main() { os.Exit(Execute()) }
`}},
	{name: "C2 dot-imported slog installs a text handler", files: map[string]string{
		"internal/apiservice/zz_fixture.go": `package apiservice

import (
	. "log/slog"
	"os"
)

func init() { SetDefault(New(NewTextHandler(os.Stderr, nil))) }
`}},
	{name: "C2 dot-imported log writes raw to stderr", files: map[string]string{
		"internal/apiservice/zz_fixture.go": `package apiservice

import (
	. "log"
	"os"
)

func init() { SetOutput(os.Stderr) }
`}},
	{name: "C2 dot-imported logging installs slog.Default", files: map[string]string{
		"internal/apiservice/zz_fixture.go": `package apiservice

import (
	"log/slog"

	. "github.com/full-chaos/dev-health-ops/internal/platform/logging"
)

func init() { InstallDefault(slog.Default()) }
`}},
	{name: "C2 aliased slog installs a text handler (guard: refused before the fix too)", files: map[string]string{
		"internal/apiservice/zz_fixture.go": `package apiservice

import (
	"os"
	s "log/slog"
)

func init() { s.SetDefault(s.New(s.NewTextHandler(os.Stderr, nil))) }
`}},
	{name: "C3 cmd/dho calls shell.Execute through a dot import", files: map[string]string{
		"cmd/dho/main.go": `package main

import (
	"github.com/full-chaos/dev-health-ops/internal/cli"
	. "github.com/full-chaos/dev-health-ops/internal/platform/shell"
)

func main() { cli.Main() }

func serve() int { return Execute() }
`}},
	{name: "C3 cmd/dho calls shell.Main through a function variable", files: map[string]string{
		"cmd/dho/main.go": `package main

import (
	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/platform/shell"
)

func main() { cli.Main() }

func serve() {
	run := shell.Main
	run()
}
`}},
	{name: "C5 R4 a local alias of io.Discard is a discarding writer", accept: true, files: map[string]string{
		"internal/apiservice/zz_fixture.go": `package apiservice

import (
	"io"
	"log"
	"log/slog"
)

func quiet() {
	output := io.Discard
	log.SetOutput(output)
	_ = slog.NewTextHandler(output, nil)
	_ = slog.NewJSONHandler(output, nil)
}
`}},
	{name: "R1 dot import of logging, even around an otherwise allowed install", files: map[string]string{
		"internal/apiservice/zz_fixture.go": `package apiservice

import (
	"os"

	. "github.com/full-chaos/dev-health-ops/internal/platform/logging"
)

func init() { InstallDefault(NewJSON(os.Stderr, 0)) }
`}},
	{name: "R2 slog.SetDefault as a function value", files: map[string]string{
		"internal/apiservice/zz_fixture.go": `package apiservice

import (
	"log/slog"
	"os"

	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
)

func init() {
	set := slog.SetDefault
	set(logging.NewJSON(os.Stderr, 0))
}
`}},
	{name: "R2 slog.NewTextHandler as a function value", files: map[string]string{
		"internal/apiservice/zz_fixture.go": `package apiservice

import (
	"log/slog"
	"os"
)

var build = slog.NewTextHandler

func logger() *slog.Logger { return slog.New(build(os.Stderr, nil)) }
`}},
	{name: "R3a logger declared by var, not :=", files: map[string]string{
		"internal/apiservice/zz_fixture.go": `package apiservice

import (
	"os"

	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
)

func install() func() {
	var logger = logging.NewJSON(os.Stderr, 0)
	return logging.InstallDefault(logger)
}
`}},
	{name: "R3a logger is a package variable", files: map[string]string{
		"internal/apiservice/zz_fixture.go": `package apiservice

import (
	"os"

	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
)

var logger = logging.NewJSON(os.Stderr, 0)

func install() func() { return logging.InstallDefault(logger) }
`}},
	{name: "R3b logger reassigned in a nested block", files: map[string]string{
		"internal/apiservice/zz_fixture.go": `package apiservice

import (
	"log/slog"
	"os"

	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
)

func install(quiet bool) func() {
	logger := logging.NewJSON(os.Stderr, 0)
	if quiet {
		logger = slog.Default()
	}
	return logging.InstallDefault(logger)
}
`}},
	{name: "R3b logger reassigned in a closure", files: map[string]string{
		"internal/apiservice/zz_fixture.go": `package apiservice

import (
	"log/slog"
	"os"

	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
)

func install() func() {
	logger := logging.NewJSON(os.Stderr, 0)
	func() { logger = slog.Default() }()
	return logging.InstallDefault(logger)
}
`}},
	{name: "R3b logger reassigned in a multi-assign", files: map[string]string{
		"internal/apiservice/zz_fixture.go": `package apiservice

import (
	"log/slog"
	"os"

	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
)

func install() func() {
	logger := logging.NewJSON(os.Stderr, 0)
	var err error
	logger, err = slog.Default(), nil
	_ = err
	return logging.InstallDefault(logger)
}
`}},
	{name: "R3b entry point logger written through a pointer", files: map[string]string{
		"internal/platform/shell/shell.go": `package shell

import (
	"log/slog"
	"os"

	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
)

func Execute() int {
	logger := logging.NewJSON(os.Stdout, 0)
	*logger = *slog.Default()
	defer logging.InstallDefault(logger)()
	return 0
}

func Main() { os.Exit(Execute()) }
`}},
	{name: "R3c logger address taken", files: map[string]string{
		"internal/apiservice/zz_fixture.go": `package apiservice

import (
	"log/slog"
	"os"

	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
)

func install() func() {
	logger := logging.NewJSON(os.Stderr, 0)
	swap(&logger)
	return logging.InstallDefault(logger)
}

func swap(target **slog.Logger) { *target = slog.Default() }
`}},
	{name: "R4 a reassigned io.Discard alias is a real writer (guard: refused before the fix too)", files: map[string]string{
		"internal/apiservice/zz_fixture.go": `package apiservice

import (
	"io"
	"log"
	"os"
)

func loud() {
	output := io.Discard
	output = os.Stderr
	log.SetOutput(output)
}
`}},
	{name: "R4 a discard{} literal of a type that writes is a real writer", files: map[string]string{
		"internal/apiservice/zz_fixture.go": `package apiservice

import (
	"log/slog"
	"os"
)

type discard struct{}

func (discard) Write(p []byte) (int, error) { return os.Stderr.Write(p) }

func logger() *slog.Logger { return slog.New(slog.NewJSONHandler(discard{}, nil)) }
`}},
	{name: "R4 a discard{} literal of a type that drops every write (guard)", accept: true, files: map[string]string{
		"internal/apiservice/zz_fixture.go": `package apiservice

import "log/slog"

type discard struct{}

func (discard) Write(p []byte) (int, error) { return len(p), nil }

func logger() *slog.Logger { return slog.New(slog.NewJSONHandler(discard{}, nil)) }
`}},
	{name: "W value-redaction wrapper around slog.Default().Handler() (guard)", accept: true, files: map[string]string{
		"internal/synccli/zz_fixture.go": `package synccli

import (
	"log/slog"

	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
)

func redactProcessLogger(redact func(string) string) (restore func()) {
	return logging.InstallDefault(slog.New(logging.WithValueRedaction(slog.Default().Handler(), redact)))
}
`}},
	{name: "W value-redaction wrapper around a text handler (guard: its stderr writer is refused before the fix too)", files: map[string]string{
		"internal/synccli/zz_fixture.go": `package synccli

import (
	"log/slog"
	"os"

	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
)

func redactProcessLogger(redact func(string) string) (restore func()) {
	return logging.InstallDefault(slog.New(logging.WithValueRedaction(slog.NewTextHandler(os.Stderr, nil), redact)))
}
`}},
	{name: "W value-redaction wrapper around a handler variable", files: map[string]string{
		"internal/synccli/zz_fixture.go": `package synccli

import (
	"log/slog"

	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
)

func redactProcessLogger(redact func(string) string) (restore func()) {
	handler := slog.Default().Handler()
	return logging.InstallDefault(slog.New(logging.WithValueRedaction(handler, redact)))
}
`}},
	{name: "W value-redaction wrapper around slog.Default() without .Handler()", files: map[string]string{
		"internal/synccli/zz_fixture.go": `package synccli

import (
	"log/slog"

	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
)

func redactProcessLogger(redact func(string) string) (restore func()) {
	return logging.InstallDefault(slog.New(logging.WithValueRedaction(slog.Default(), redact)))
}
`}},
	{name: "P1 entry point defers InstallDefault on an inline logging.NewJSON", accept: true, files: map[string]string{
		"internal/platform/shell/shell.go": `package shell

import (
	"os"

	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
)

func Execute() int {
	defer logging.InstallDefault(logging.NewJSON(os.Stdout, 0))()
	return 0
}

func Main() { os.Exit(Execute()) }
`}},
	{name: "P1 entry point keeps the restore function in a variable and defers it", accept: true, files: map[string]string{
		"internal/platform/shell/shell.go": `package shell

import (
	"os"

	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
)

func Execute() int {
	logger := logging.NewJSON(os.Stdout, 0)
	restore := logging.InstallDefault(logger)
	defer restore()
	logger.Info("run")
	return 0
}

func Main() { os.Exit(Execute()) }
`}},
	{name: "P1 guard: restore variable reassigned", want: "func Execute does not install", files: map[string]string{
		"internal/platform/shell/shell.go": `package shell

import (
	"os"

	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
)

func Execute() int {
	restore := logging.InstallDefault(logging.NewJSON(os.Stdout, 0))
	restore = func() {}
	defer restore()
	return 0
}

func Main() { os.Exit(Execute()) }
`}},
	{name: "P1 guard: restore variable address taken", want: "func Execute does not install", files: map[string]string{
		"internal/platform/shell/shell.go": `package shell

import (
	"os"

	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
)

func Execute() int {
	restore := logging.InstallDefault(logging.NewJSON(os.Stdout, 0))
	keep(&restore)
	defer restore()
	return 0
}

func keep(target *func()) { *target = func() {} }

func Main() { os.Exit(Execute()) }
`}},
	{name: "P1 guard: restore variable never deferred at top level", want: "func Execute does not install", files: map[string]string{
		"internal/platform/shell/shell.go": `package shell

import (
	"os"

	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
)

func Execute() int {
	restore := logging.InstallDefault(logging.NewJSON(os.Stdout, 0))
	restore()
	return 0
}

func Main() { os.Exit(Execute()) }
`}},
	{name: "P1 guard: restore deferred only inside a nested block", want: "func Execute does not install", files: map[string]string{
		"internal/platform/shell/shell.go": `package shell

import (
	"os"

	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
)

func Execute() int {
	restore := logging.InstallDefault(logging.NewJSON(os.Stdout, 0))
	if len(os.Args) > 0 {
		defer restore()
	}
	return 0
}

func Main() { os.Exit(Execute()) }
`}},
	{name: "P1 guard: restore deferred only inside a closure", want: "func Execute does not install", files: map[string]string{
		"internal/platform/shell/shell.go": `package shell

import (
	"os"

	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
)

func Execute() int {
	restore := logging.InstallDefault(logging.NewJSON(os.Stdout, 0))
	func() { defer restore() }()
	return 0
}

func Main() { os.Exit(Execute()) }
`}},
	{name: "P1 guard: inline install of slog.Default", want: "func Execute does not install", files: map[string]string{
		"internal/platform/shell/shell.go": `package shell

import (
	"log/slog"
	"os"

	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
)

func Execute() int {
	defer logging.InstallDefault(slog.Default())()
	return 0
}

func Main() { os.Exit(Execute()) }
`}},
	{name: "P1 guard: restore variable over a reassigned logger", want: "func Execute does not install", files: map[string]string{
		"internal/platform/shell/shell.go": `package shell

import (
	"log/slog"
	"os"

	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
)

func Execute() int {
	logger := logging.NewJSON(os.Stdout, 0)
	logger = slog.Default()
	restore := logging.InstallDefault(logger)
	defer restore()
	return 0
}

func Main() { os.Exit(Execute()) }
`}},
}

func TestServiceHandlerCensusFixtures(t *testing.T) {
	t.Parallel()
	for _, fixture := range censusFixtures {
		t.Run(fixture.name, func(t *testing.T) {
			t.Parallel()
			tree := censusBaseline()
			for name, source := range fixture.files {
				tree[name] = source
			}
			fileSet := token.NewFileSet()
			parsed := map[string][]*ast.File{}
			for name, source := range tree {
				file, err := parser.ParseFile(fileSet, name, source, 0)
				if err != nil {
					t.Fatalf("parse %s: %v", name, err)
				}
				path := modulePath + "/" + filepath.Dir(name)
				parsed[path] = append(parsed[path], file)
			}
			findings := serviceHandlerCensus(fileSet, parsed)
			switch {
			case fixture.accept && len(findings) > 0:
				t.Errorf("census refused a tree it must accept: %q", findings)
			case !fixture.accept && len(findings) == 0:
				t.Error("census accepted a tree it must refuse")
			case fixture.want != "" && !strings.Contains(strings.Join(findings, "\n"), fixture.want):
				t.Errorf("census refused the tree without %q: %q", fixture.want, findings)
			}
		})
	}
}

// TestLinkedSourceFilesIncludeCgoFiles pins that the scans parse the Cgo
// files go list reports apart from GoFiles: a binary links them too.
func TestLinkedSourceFilesIncludeCgoFiles(t *testing.T) {
	t.Parallel()
	got := linkedSourceFiles(listedPackage{GoFiles: []string{"plain.go"}, CgoFiles: []string{"with_cgo.go"}})
	want := map[string]bool{"plain.go": true, "with_cgo.go": true}
	if len(got) != len(want) {
		t.Fatalf("linkedSourceFiles = %q, want %d files", got, len(want))
	}
	for _, name := range got {
		if !want[name] {
			t.Errorf("linkedSourceFiles returned unexpected %q", name)
		}
	}
}
