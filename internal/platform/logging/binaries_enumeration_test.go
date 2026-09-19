package logging_test

import (
	"bytes"
	"encoding/json"
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
var binaryLogging = map[string]string{
	"ask-dev-jobs-probe":                     "installs",
	"auth-migrate":                           "silent",
	"auth-service":                           "authruntime",
	"dev-health-migration-matrix":            "silent",
	"dev-health-provider-fixture":            "silent",
	"dev-health-provider-normalized-fixture": "silent",
	"dev-health-reconciler":                  "shell",
	"dev-health-scheduler":                   "shell",
	"dev-health-stream-runner":               "shell",
	"dev-health-worker":                      "shell",
	"dev-health-worker-migrate":              "silent",
	"dev-health-workerctl":                   "installs",
	"go-api-prove":                           "silent",
	"go-api-rest-prove":                      "stdlib_cli",
	"go-api-routing":                         "silent",
	"gqlgen-guard":                           "silent",
	"mint-edge-token":                        "silent",
	"mint-envelope":                          "silent",
	"query-api":                              "installs",
	"worker-contractcheck":                   "silent",
}

type listedPackage struct {
	ImportPath string
	Name       string
	Dir        string
	GoFiles    []string
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

// TestEveryBinaryLogsThroughTheRedactingHandler proves by enumeration that
// every binary under cmd/ logs through logging.NewJSON's handler, and that
// no production package builds its own slog handler to a real writer.
func TestEveryBinaryLogsThroughTheRedactingHandler(t *testing.T) {
	t.Parallel()
	root := moduleRoot(t)
	command := exec.Command("go", "list", "-deps", "-json=ImportPath,Name,Dir,GoFiles,Deps,Module", "./cmd/...")
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
		for _, name := range listed.GoFiles {
			file, err := parser.ParseFile(fileSet, filepath.Join(listed.Dir, name), nil, 0)
			if err != nil {
				t.Fatalf("parse %s: %v", name, err)
			}
			parsed[path] = append(parsed[path], file)
		}
	}

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
	found := false
	for _, file := range files {
		names := importNames(file)
		for _, declaration := range file.Decls {
			main, ok := declaration.(*ast.FuncDecl)
			if !ok || main.Name.Name != "main" || main.Recv != nil || main.Body == nil {
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
	if !found {
		t.Errorf("binary %s: func main does not call %s.%s as a top-level statement", binary, path, function)
	}
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
