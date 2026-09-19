package logging_test

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// providerOriginSink is one place where provider-origin content (an HTTP
// response body, a response header value, a response status line, a request
// URL with its query, or a transport error that embeds the URL) is formatted
// into an error or a string.
type providerOriginSink struct {
	file, function, call, source string
	line                         int
}

func (s providerOriginSink) String() string {
	return fmt.Sprintf("%s %s %s <- %s", s.file, s.function, s.call, s.source)
}

// formatCalls build an error or a string from their arguments.
var formatCalls = map[string]map[string]bool{
	"fmt":     {"Errorf": true, "Sprintf": true, "Sprint": true, "Sprintln": true},
	"errors":  {"New": true},
	"strings": {"Join": true},
}

// providerAssignedIDKeys are the log keys that carry an id a provider
// assigned (read from a provider response or webhook payload) in the
// provider-facing packages, taken from every id-named key logged in the
// worker tree.
var providerAssignedIDKeys = map[string]bool{
	"deployment_id": true, "event_id": true, "incident_id": true,
	"board_id": true, "team_id": true, "team_ids": true, "run": true,
}

// providerFacing reports whether a file belongs to the packages that read
// provider responses and webhooks; elsewhere a team_id is our own.
func providerFacing(file string) bool {
	return strings.HasPrefix(file, "internal/providersync/") || strings.HasPrefix(file, "internal/jobs/pagerduty/") ||
		strings.HasPrefix(file, "internal/providerfoundation/")
}

// logCalls are slog logger methods, package-level slog functions and slog
// attribute constructors: a provider-origin argument to one reaches a log
// line.
var logCalls = map[string]bool{
	"Debug": true, "Info": true, "Warn": true, "Error": true,
	"DebugContext": true, "InfoContext": true, "WarnContext": true, "ErrorContext": true,
	"Log": true, "LogAttrs": true, "With": true,
	"String": true, "Any": true, "Group": true,
	"Printf": true, "Print": true, "Println": true,
}

// scanProviderOriginSinks parses every non-test Go file of the given
// packages and reports each place provider-origin content enters an error
// or a string. Struct fields count only for types that implement error (a
// method named Error in any parsed file).
func scanProviderOriginSinks(t *testing.T, root string, files []string) ([]providerOriginSink, map[string]*ast.FuncDecl) {
	t.Helper()
	fileSet := token.NewFileSet()
	var parsed []*ast.File
	var names []string
	for _, path := range files {
		file, err := parser.ParseFile(fileSet, path, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		parsed = append(parsed, file)
		relative, _ := filepath.Rel(root, path)
		names = append(names, filepath.ToSlash(relative))
	}
	errorTypes := map[string]bool{}
	errorMethods := map[string]*ast.FuncDecl{}
	for _, file := range parsed {
		for _, declaration := range file.Decls {
			if method, ok := declaration.(*ast.FuncDecl); ok && method.Recv != nil && method.Name.Name == "Error" && len(method.Recv.List) == 1 {
				receiver := method.Recv.List[0].Type
				if star, ok := receiver.(*ast.StarExpr); ok {
					receiver = star.X
				}
				name := baseTypeName(receiver)
				errorTypes[name] = true
				errorMethods[name] = method
			}
		}
	}
	var sinks []providerOriginSink
	for index, file := range parsed {
		// An attribute named like a provider-assigned id is built by
		// logging.ProviderIDAttr / ProviderIDsAttr, never from a literal key.
		ast.Inspect(file, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			selector, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || !logCalls[selector.Sel.Name] {
				return true
			}
			for _, argument := range call.Args {
				literal, ok := argument.(*ast.BasicLit)
				if !ok || literal.Kind != token.STRING {
					continue
				}
				if key, _ := strconv.Unquote(literal.Value); providerAssignedIDKeys[key] && providerFacing(names[index]) {
					sinks = append(sinks, providerOriginSink{file: names[index], call: "log key " + key, source: "provider-assigned id", line: fileSet.Position(literal.Pos()).Line})
				}
			}
			return true
		})
		// The body accessor has no caller in the worker tree: no log path
		// calls it.
		ast.Inspect(file, func(node ast.Node) bool {
			if call, ok := node.(*ast.CallExpr); ok {
				if selector, ok := call.Fun.(*ast.SelectorExpr); ok && selector.Sel.Name == "ResponseBodySnippet" {
					sinks = append(sinks, providerOriginSink{file: names[index], call: "call ResponseBodySnippet", source: "response body", line: fileSet.Position(call.Pos()).Line})
				}
			}
			return true
		})
		for _, declaration := range file.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok || function.Body == nil {
				continue
			}
			for _, sink := range functionSinks(function, errorTypes, importNames(file)) {
				sink.file = names[index]
				sink.line = fileSet.Position(token.Pos(sink.line)).Line
				sinks = append(sinks, sink)
			}
		}
	}
	sort.Slice(sinks, func(i, j int) bool { return sinks[i].String() < sinks[j].String() })
	return sinks, errorMethods
}

// originOf names the provider-origin source an expression reads directly, or
// "".
func originOf(expression ast.Expr) string {
	found := ""
	ast.Inspect(expression, func(node ast.Node) bool {
		if found != "" {
			return false
		}
		switch typed := node.(type) {
		case *ast.CallExpr:
			if selector, ok := typed.Fun.(*ast.SelectorExpr); ok {
				name := selector.Sel.Name
				if receiver, ok := selector.X.(*ast.Ident); ok && receiver.Name == "io" && name == "ReadAll" {
					if len(typed.Args) > 0 && mentionsBody(typed.Args[0]) {
						found = "response body"
					}
				}
				if inner, ok := selector.X.(*ast.SelectorExpr); ok && inner.Sel.Name == "Header" && (name == "Get" || name == "Values") {
					found = "header value"
				}
				if inner, ok := selector.X.(*ast.SelectorExpr); ok && inner.Sel.Name == "URL" && (name == "String" || name == "Query" || name == "Redacted") {
					found = "request URL"
				}
			}
		case *ast.SelectorExpr:
			if typed.Sel.Name == "RawQuery" {
				found = "URL query"
			}
			if typed.Sel.Name == "Status" {
				if ident, ok := typed.X.(*ast.Ident); ok && (strings.HasPrefix(ident.Name, "resp") || ident.Name == "res") {
					found = "status line"
				}
			}
		}
		return true
	})
	return found
}

func mentionsBody(expression ast.Expr) bool {
	mentions := false
	ast.Inspect(expression, func(node ast.Node) bool {
		if selector, ok := node.(*ast.SelectorExpr); ok && selector.Sel.Name == "Body" {
			if ident, ok := selector.X.(*ast.Ident); ok && (strings.HasPrefix(ident.Name, "resp") || ident.Name == "res" || ident.Name == "httpResponse") {
				mentions = true
			}
		}
		if ident, ok := node.(*ast.Ident); ok && (ident.Name == "stream" || ident.Name == "limited" || ident.Name == "reader") {
			mentions = true
		}
		return !mentions
	})
	return mentions
}

// assignment is one write of a local name at a source position, with the
// provider-origin source it wrote ("" for a clean value).
type assignment struct {
	pos    token.Pos
	source string
}

// taintState is flow-ordered: a read of a name is tainted when the last write
// of that name before the read wrote provider-origin content.
type taintState map[string][]assignment

func (state taintState) at(name string, pos token.Pos) string {
	source := ""
	var latest token.Pos
	for _, write := range state[name] {
		if write.pos < pos && write.pos >= latest {
			latest = write.pos
			source = write.source
		}
	}
	return source
}

// functionSinks reports format calls, struct fields and returns in function
// that take provider-origin content.
func functionSinks(function *ast.FuncDecl, errorTypes map[string]bool, imports map[string]string) []providerOriginSink {
	state := taintState{}
	for {
		next := taintState{}
		ast.Inspect(function.Body, func(node ast.Node) bool {
			if call, ok := node.(*ast.CallExpr); ok {
				// json.Unmarshal(body, &decoded) and
				// json.NewDecoder(response.Body).Decode(&decoded) write
				// provider content into decoded.
				if target, source := decodeTarget(call, state); target != "" {
					next[target] = append(next[target], assignment{pos: call.End(), source: source})
				}
				return true
			}
			assign, ok := node.(*ast.AssignStmt)
			if !ok {
				return true
			}
			sources := make([]string, len(assign.Lhs))
			if len(assign.Rhs) == 1 && len(assign.Lhs) == 2 && transportCall(assign.Rhs[0]) {
				// response, err := client.Do(request): net/http's error
				// quotes the URL and, for a malformed response, the
				// provider's status line or header values.
				sources[1] = "transport error"
			}
			// An error returned by a call that read provider content
			// (json.Unmarshal(body, ...), strconv.Atoi(header), a decoder
			// over the body) can quote that content.
			if len(assign.Rhs) == 1 {
				if call, ok := assign.Rhs[0].(*ast.CallExpr); ok && !sanitizerCall(call) && callReadsContent(call, state, imports) {
					for index, target := range assign.Lhs {
						if ident, ok := target.(*ast.Ident); ok && errorName(ident.Name) && sources[index] == "" {
							sources[index] = "error derived from provider content"
						}
					}
				}
			}
			for index, right := range assign.Rhs {
				source := originOf(right)
				if source == "" {
					source = taintOfAt(right, state)
				}
				if source == "" {
					continue
				}
				if len(assign.Rhs) == len(assign.Lhs) {
					sources[index] = source
				} else if sources[0] == "" {
					// value, err := io.ReadAll(...): only the value is content.
					sources[0] = source
				}
			}
			for index, target := range assign.Lhs {
				if ident, ok := target.(*ast.Ident); ok && ident.Name != "_" {
					source := sources[index]
					// Content flows into an error only through the rules
					// above (a transport call, a call outside this module
					// that read content); a helper of this module that took
					// content returns its own error, scanned on its own.
					if errorName(ident.Name) && source != "transport error" && source != "error derived from provider content" {
						source = ""
					}
					next[ident.Name] = append(next[ident.Name], assignment{pos: assign.Pos(), source: source})
				}
			}
			return true
		})
		if fmt.Sprint(next) == fmt.Sprint(state) {
			break
		}
		state = next
	}
	sourceOf := func(expression ast.Expr) string {
		if source := originOf(expression); source != "" {
			return source
		}
		return taintOfAt(expression, state)
	}
	var sinks []providerOriginSink
	ast.Inspect(function.Body, func(node ast.Node) bool {
		if literal, ok := node.(*ast.CompositeLit); ok {
			if !errorTypes[baseTypeName(literal.Type)] {
				return true
			}
			for _, element := range literal.Elts {
				field, ok := element.(*ast.KeyValueExpr)
				if !ok {
					continue
				}
				if source := sourceOf(field.Value); source != "" && source != "header value" {
					sinks = append(sinks, providerOriginSink{function: function.Name.Name, call: "field " + typeName(literal.Type) + "." + render(field.Key), source: source, line: int(literal.Pos())})
				}
			}
			return true
		}
		if statement, ok := node.(*ast.ReturnStmt); ok {
			// return client.Do(request): the transport error returned as is.
			// A providerfoundation.HTTPDoer's own Do passes its delegate's
			// error to the provider clients, which replace any Doer error
			// with their own class (pinned by
			// TestHTTPClientsReplaceEveryDoerErrorWithTheirOwnClass).
			if len(statement.Results) == 1 && transportCall(statement.Results[0]) && !httpDoerMethod(function) {
				sinks = append(sinks, providerOriginSink{function: function.Name.Name, call: "return", source: "transport error", line: int(statement.Pos())})
			}
			for _, result := range statement.Results {
				if ident, ok := result.(*ast.Ident); ok && errorName(ident.Name) {
					if source := state.at(ident.Name, ident.Pos()); source != "" && !(source == "transport error" && httpDoerMethod(function)) {
						sinks = append(sinks, providerOriginSink{function: function.Name.Name, call: "return", source: source, line: int(statement.Pos())})
					}
				}
			}
			return true
		}
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		selector, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		if logCalls[selector.Sel.Name] {
			for _, argument := range call.Args {
				if source := sourceOf(argument); source != "" {
					sinks = append(sinks, providerOriginSink{function: function.Name.Name, call: "log " + selector.Sel.Name, source: source, line: int(call.Pos())})
					break
				}
			}
			return true
		}
		receiver, ok := selector.X.(*ast.Ident)
		if !ok || !formatCalls[receiver.Name][selector.Sel.Name] {
			return true
		}
		for _, argument := range call.Args {
			if source := sourceOf(argument); source != "" {
				sinks = append(sinks, providerOriginSink{function: function.Name.Name, call: receiver.Name + "." + selector.Sel.Name, source: source, line: int(call.Pos())})
				break
			}
		}
		return true
	})
	return sinks
}

// sanitizerCall reports the calls that end taint: they return our own text
// or a bounded identifier.
func sanitizerCall(call *ast.CallExpr) bool {
	selector, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	switch selector.Sel.Name {
	case "TransportFailure", "DecodeFailure", "ProviderAssignedID":
		return true
	}
	return false
}

// callReadsContent reports whether a call outside this module takes
// provider content (json.Unmarshal(body, ...), strconv.Atoi(header),
// url.Parse(location)), or is a method on a value that holds it
// (json.NewDecoder(response.Body).Decode). Functions of this module are
// scanned on their own, so their errors are not assumed to quote content.
func callReadsContent(call *ast.CallExpr, state taintState, imports map[string]string) bool {
	selector, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	if receiver, ok := selector.X.(*ast.Ident); ok && receiver.Obj == nil {
		path, imported := imports[receiver.Name]
		if !imported || strings.HasPrefix(path, modulePath) {
			return false
		}
		for _, argument := range call.Args {
			if originOf(argument) != "" || taintOfAt(argument, state) != "" || responseBody(argument) {
				return true
			}
		}
		return false
	}
	return originOf(selector.X) != "" || taintOfAt(selector.X, state) != "" || responseBody(selector.X)
}

// responseBody reports whether expression reads an HTTP response's Body
// (json.NewDecoder(response.Body)).
func responseBody(expression ast.Expr) bool {
	found := false
	ast.Inspect(expression, func(node ast.Node) bool {
		if selector, ok := node.(*ast.SelectorExpr); ok && selector.Sel.Name == "Body" {
			if ident, ok := selector.X.(*ast.Ident); ok && (strings.HasPrefix(ident.Name, "resp") || ident.Name == "res" || ident.Name == "httpResponse") {
				found = true
			}
		}
		return !found
	})
	return found
}

func errorName(name string) bool {
	return name == "err" || strings.HasSuffix(name, "Err") || strings.HasSuffix(name, "err")
}

// decodeTarget returns the local a decode call writes provider content into.
func decodeTarget(call *ast.CallExpr, state taintState) (string, string) {
	selector, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || len(call.Args) == 0 {
		return "", ""
	}
	var input ast.Expr
	var output ast.Expr
	switch selector.Sel.Name {
	case "Unmarshal":
		if len(call.Args) < 2 {
			return "", ""
		}
		input, output = call.Args[0], call.Args[1]
	case "Decode":
		input, output = selector.X, call.Args[0]
	default:
		return "", ""
	}
	source := originOf(input)
	if source == "" {
		source = taintOfAt(input, state)
	}
	if source == "" && responseBody(input) {
		source = "response body"
	}
	if source == "" {
		return "", ""
	}
	if unary, ok := output.(*ast.UnaryExpr); ok {
		output = unary.X
	}
	if ident, ok := output.(*ast.Ident); ok {
		return ident.Name, source
	}
	return "", ""
}

func taintOfAt(expression ast.Expr, state taintState) string {
	found := ""
	ast.Inspect(expression, func(node ast.Node) bool {
		if found != "" {
			return false
		}
		if ident, ok := node.(*ast.Ident); ok {
			if source := state.at(ident.Name, ident.Pos()); source != "" {
				found = source
			}
		}
		// Content passes through conversions and text helpers; a call that
		// parses or measures content (json.Unmarshal, len, strconv) returns
		// its own value or error, not the content.
		if call, ok := node.(*ast.CallExpr); ok {
			return passesContent(call)
		}
		return true
	})
	return found
}

// httpDoerMethod reports whether function implements
// providerfoundation.HTTPDoer: a method Do(request *http.Request).
func httpDoerMethod(function *ast.FuncDecl) bool {
	if function.Recv == nil || function.Name.Name != "Do" || len(function.Type.Params.List) != 1 {
		return false
	}
	star, ok := function.Type.Params.List[0].Type.(*ast.StarExpr)
	if !ok {
		return false
	}
	selector, ok := star.X.(*ast.SelectorExpr)
	return ok && selector.Sel.Name == "Request"
}

// transportCall reports whether expression is a net/http request call:
// X.Do(request) with one argument, or http.Get/Head/Post/PostForm.
func transportCall(expression ast.Expr) bool {
	call, ok := expression.(*ast.CallExpr)
	if !ok {
		return false
	}
	selector, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	if selector.Sel.Name == "Do" && len(call.Args) == 1 {
		return true
	}
	if receiver, ok := selector.X.(*ast.Ident); ok && receiver.Name == "http" {
		switch selector.Sel.Name {
		case "Get", "Head", "Post", "PostForm":
			return true
		}
	}
	return false
}

// baseTypeName is the unqualified name of a composite literal's type.
func baseTypeName(expression ast.Expr) string {
	switch typed := expression.(type) {
	case *ast.Ident:
		return typed.Name
	case *ast.SelectorExpr:
		return typed.Sel.Name
	}
	return ""
}

func typeName(expression ast.Expr) string {
	if expression == nil {
		return "?"
	}
	return render(expression)
}

// passesContent reports whether a call's result carries its arguments'
// content: a string/[]byte conversion, a strings/bytes/fmt/strconv.Quote
// helper, or a slice/append.
func passesContent(call *ast.CallExpr) bool {
	switch fun := call.Fun.(type) {
	case *ast.Ident:
		// A conversion, append, or a helper of the same package
		// (bestEffortID(body)) carries content through.
		return fun.Name != "len" && fun.Name != "cap"
	case *ast.ArrayType:
		return true
	case *ast.SelectorExpr:
		if receiver, ok := fun.X.(*ast.Ident); ok {
			switch receiver.Name {
			case "strings", "bytes", "fmt":
				return true
			case "strconv":
				return strings.HasPrefix(fun.Sel.Name, "Quote")
			}
		}
	}
	return false
}

// workerBinaries are the binaries the invariant covers.
var workerBinaries = []string{
	"dev-health-worker", "dev-health-stream-runner", "dev-health-scheduler",
	"dev-health-reconciler", "dev-health-workerctl", "dev-health-worker-migrate",
}

// accessorOnlyCarriers are error types that hold provider content in a field
// for a classifier to read; their Error() never formats that field, which
// the test checks on the method's source.
var accessorOnlyCarriers = map[string]string{
	"httpStatusError": "body",
	"ProviderError":   "Body",
}

// TestProviderOriginContentNeverEntersAnErrorAtItsSource enumerates every
// Go file the worker binaries link from this module and fails when an HTTP
// response body, a response header value, a status line, a request URL or
// query, or a net/http transport error (whose text embeds the URL) is
// formatted into an error or a string, returned raw, or stored in an error
// type's field -- except the accessor-only carriers above, whose Error() is
// checked not to read the field.
func TestProviderOriginContentNeverEntersAnErrorAtItsSource(t *testing.T) {
	t.Parallel()
	root := moduleRoot(t)
	arguments := []string{"list", "-deps", "-f", "{{if .Module}}{{if eq .Module.Path \"" + modulePath + "\"}}{{range .GoFiles}}{{$.Dir}}/{{.}}\n{{end}}{{end}}{{end}}"}
	for _, binary := range workerBinaries {
		arguments = append(arguments, "./cmd/"+binary)
	}
	command := exec.Command("go", arguments...)
	command.Dir = root
	output, err := command.Output()
	if err != nil {
		t.Fatalf("go list: %v", err)
	}
	seen := map[string]bool{}
	var files []string
	for _, line := range strings.Split(strings.TrimSpace(string(output)), "\n") {
		if line != "" && !seen[line] {
			seen[line] = true
			files = append(files, line)
		}
	}
	if len(files) < 500 {
		t.Fatalf("only %d files enumerated", len(files))
	}
	t.Logf("scanned %d files of the worker binaries", len(files))
	sinks, errorMethods := scanProviderOriginSinks(t, root, files)
	for _, sink := range sinks {
		if typeAndField := strings.SplitN(strings.TrimPrefix(sink.call, "field "), ".", 2); strings.HasPrefix(sink.call, "field ") && len(typeAndField) == 2 {
			if field, carrier := accessorOnlyCarriers[strings.TrimPrefix(typeAndField[0], "&")]; carrier && typeAndField[1] == field {
				continue
			}
		}
		t.Errorf("provider-origin content enters an error at %s:%d (%s %s <- %s)", sink.file, sink.line, sink.function, sink.call, sink.source)
	}
	if len(accessorOnlyCarriers) == 0 {
		t.Fatal("no carriers")
	}
	for carrier, field := range accessorOnlyCarriers {
		method, found := errorMethods[carrier]
		if !found {
			t.Errorf("carrier %s has no Error method in the worker tree", carrier)
			continue
		}
		ast.Inspect(method.Body, func(node ast.Node) bool {
			if selector, ok := node.(*ast.SelectorExpr); ok && selector.Sel.Name == field {
				t.Errorf("%s.Error() reads %s", carrier, field)
			}
			return true
		})
	}
}
