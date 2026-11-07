package server

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// requestIDExemptions are the raw request id reads that reach no log call. Each one is a response value, a
// principal request meta, or a guarded local that the log call then wraps; a new raw read needs a reason here.
var requestIDExemptions = map[string]string{
	"query_route.go#envelopeRequestID":                     "the one raw read of the header",
	"rest_scope.go#restScopeOrg":                           "local is checked, then wrapped by logSafeRequestID at the log",
	"rest_access.go#authenticateRESTRouted":                "principal.WithRequestMeta, not a log",
	"pdp_gate.go#writePDPDenied":                           "local is checked and stripped of CR and LF before the log",
	"explain_route.go#newExplainGetHandler":                "argument to writeExplainResponse, which wraps the id at its log",
	"explain_route.go#newExplainPostHandler":               "argument to writeExplainResponse, which wraps the id at its log",
	"internal_auth.go#authenticateInternalRequest":         "principal.WithRequestMeta, not a log",
	"rest_error_response.go#authenticateRESTRequest":       "principal.WithRequestMeta, not a log",
	"rest_error_response.go#echoRequestID":                 "response header, only for an acceptable id",
	"rest_error_response.go#bindRequestID":                 "response header and request context, only for an acceptable id",
	"graphql_error_presenter.go#safeGraphQLErrorPresenter": "local goes to the client as an extension; each log call wraps it",
}

func isRawRequestIDCall(call *ast.CallExpr) bool {
	switch fn := call.Fun.(type) {
	case *ast.Ident:
		return fn.Name == "envelopeRequestID" || fn.Name == "graphQLRequestID"
	case *ast.SelectorExpr:
		if fn.Sel.Name != "Get" || len(call.Args) != 1 {
			return false
		}
		lit, ok := call.Args[0].(*ast.BasicLit)
		if !ok || lit.Kind != token.STRING {
			return false
		}
		s, err := strconv.Unquote(lit.Value)
		return err == nil && strings.EqualFold(s, "X-Request-Id")
	}
	return false
}

var logSinkNames = map[string]bool{
	"Debug": true, "Info": true, "Warn": true, "Error": true,
	"DebugContext": true, "InfoContext": true, "WarnContext": true, "ErrorContext": true,
	"Log": true, "LogAttrs": true,
	"Printf": true, "Print": true, "Println": true,
	"Fatal": true, "Fatalf": true, "Fatalln": true, "Panic": true, "Panicf": true, "Panicln": true,
}

func isLogSinkCall(call *ast.CallExpr) bool {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	if id, ok := sel.X.(*ast.Ident); ok {
		switch id.Name {
		case "log":
			return true
		case "http", "policy":
			return false
		}
	}
	return logSinkNames[sel.Sel.Name]
}

// requestIDReads parses the files and returns the raw request id reads that sit inside a log call's arguments
// (log) and those that do not (other), as "file:line" and "file#func" entries. A read wrapped in logSafeRequestID
// is neither. A local assigned from a raw read, and a parameter named requestID, count as raw where a log call uses them.
func requestIDReads(t *testing.T, files map[string]string) (log, other []string) {
	t.Helper()
	fset := token.NewFileSet()
	for name, src := range files {
		f, err := parser.ParseFile(fset, name, src, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		rawVars := map[string]bool{}
		ast.Inspect(f, func(n ast.Node) bool {
			switch n := n.(type) {
			case *ast.AssignStmt:
				if len(n.Lhs) == 1 && len(n.Rhs) == 1 {
					if call, ok := n.Rhs[0].(*ast.CallExpr); ok && isRawRequestIDCall(call) {
						if id, ok := n.Lhs[0].(*ast.Ident); ok {
							rawVars[id.Name] = true
						}
					}
				}
			case *ast.FuncDecl:
				if n.Type.Params != nil {
					for _, p := range n.Type.Params.List {
						for _, id := range p.Names {
							if id.Name == "requestID" {
								rawVars[id.Name] = true
							}
						}
					}
				}
			}
			return true
		})
		for _, decl := range f.Decls {
			fd, ok := decl.(*ast.FuncDecl)
			if !ok || fd.Body == nil {
				continue
			}
			var visit func(n ast.Node, inLog bool)
			visit = func(n ast.Node, inLog bool) {
				ast.Inspect(n, func(n ast.Node) bool {
					switch n := n.(type) {
					case *ast.CallExpr:
						if id, ok := n.Fun.(*ast.Ident); ok && id.Name == "logSafeRequestID" {
							return false
						}
						if isRawRequestIDCall(n) {
							if inLog {
								log = append(log, fmt.Sprintf("%s:%d", name, fset.Position(n.Pos()).Line))
							} else {
								other = append(other, name+"#"+fd.Name.Name)
							}
							return false
						}
						if !inLog && isLogSinkCall(n) {
							for _, a := range n.Args {
								visit(a, true)
							}
							return false
						}
					case *ast.Ident:
						if inLog && rawVars[n.Name] {
							log = append(log, fmt.Sprintf("%s:%d", name, fset.Position(n.Pos()).Line))
						}
					}
					return true
				})
			}
			visit(fd.Body, false)
		}
	}
	sort.Strings(log)
	sort.Strings(other)
	return log, other
}

func TestRequestIDCensusRecognizesRawLogUses(t *testing.T) {
	src := `package p
import ("log"; "log/slog")
func a(r R) { log.Printf("x %s", envelopeRequestID(r)) }
func b(r R) { slog.Warn("x", slog.String("request_id", r.Header.Get("x-request-id"))) }
func c(r R) { id := envelopeRequestID(r); slog.Default().ErrorContext(ctx, "x", slog.String("request_id", id)) }
func d(requestID string) { log.Printf("x %s", requestID) }
func e(r R) { log.Printf("x %s", logSafeRequestID(envelopeRequestID(r))) }
func f(r R) { w.Header().Set("X-Request-Id", envelopeRequestID(r)) }
`
	logUses, other := requestIDReads(t, map[string]string{"x.go": src})
	want := []string{"x.go:3", "x.go:4", "x.go:5", "x.go:6"}
	if strings.Join(logUses, ",") != strings.Join(want, ",") {
		t.Fatalf("log uses = %v, want %v", logUses, want)
	}
	if strings.Join(other, ",") != "x.go#c,x.go#f" {
		t.Fatalf("other uses = %v, want [x.go#c x.go#f]", other)
	}
}

func TestEveryRequestIDLogUseIsLogSafe(t *testing.T) {
	paths, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	files := map[string]string{}
	for _, p := range paths {
		if strings.HasSuffix(p, "_test.go") {
			continue
		}
		b, err := readFileForCensus(p)
		if err != nil {
			t.Fatal(err)
		}
		files[p] = b
	}
	logUses, other := requestIDReads(t, files)
	if len(logUses) > 0 {
		t.Errorf("raw request id reaches a log call (wrap it in logSafeRequestID):\n  %s", strings.Join(logUses, "\n  "))
	}
	used := map[string]bool{}
	for _, o := range other {
		used[o] = true
		if _, ok := requestIDExemptions[o]; !ok {
			t.Errorf("raw request id read outside a log call has no exemption: %s", o)
		}
	}
	for k := range requestIDExemptions {
		if !used[k] {
			t.Errorf("stale request id exemption: %s", k)
		}
	}
}

func readFileForCensus(p string) (string, error) {
	b, err := os.ReadFile(p)
	return string(b), err
}

func TestLogSafeRequestIDStripsCRAndLF(t *testing.T) {
	for in, want := range map[string]string{"": "", "req-123_ok.~": "req-123_ok.~", "a\nb": "ab", "a\rb": "ab", "a\r\nb\n\rc": "abc"} {
		if got := logSafeRequestID(in); got != want {
			t.Errorf("logSafeRequestID(%q) = %q, want %q", in, got, want)
		}
	}
}
