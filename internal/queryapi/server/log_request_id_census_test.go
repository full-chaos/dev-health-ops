package server

import (
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

	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
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
	"graphql_edge_route.go#correlationID":                  "the id the edge middleware binds to the context and echoes; read as written",
	"graphql_error_presenter.go#graphQLRequestID":          "returns the bound id to its callers; each log call wraps it",
	"graphql_error_presenter.go#safeGraphQLErrorPresenter": "local goes to the client as an extension; each log call wraps it",
}

// importedConsts holds the header-name constants this package takes from other packages, keyed as spelled at the use.
var importedConsts = map[string]string{
	"httpapi.RequestIDHeader": httpapi.RequestIDHeader,
}

func isRequestIDName(v string) bool { return strings.EqualFold(v, "X-Request-Id") }

type censusCtx struct {
	consts map[string]string
	t      *testing.T
	fset   *token.FileSet
	bad    []string
}

func (c *censusCtx) resolve(e ast.Expr) (string, bool) {
	switch e := e.(type) {
	case *ast.BasicLit:
		if e.Kind == token.STRING {
			v, err := strconv.Unquote(e.Value)
			return v, err == nil
		}
	case *ast.ParenExpr:
		return c.resolve(e.X)
	case *ast.Ident:
		v, ok := c.consts[e.Name]
		return v, ok
	case *ast.SelectorExpr:
		if x, ok := e.X.(*ast.Ident); ok {
			v, ok := c.consts[x.Name+"."+e.Sel.Name]
			return v, ok
		}
	case *ast.BinaryExpr:
		if e.Op == token.ADD {
			l, ok1 := c.resolve(e.X)
			r, ok2 := c.resolve(e.Y)
			return l + r, ok1 && ok2
		}
	}
	return "", false
}

// nameIsRequestID resolves a header name by its constant value. A name that looks like a request id constant but
// does not resolve is reported, so an imported constant is added to importedConsts instead of slipping through.
func (c *censusCtx) nameIsRequestID(e ast.Expr) bool {
	if v, ok := c.resolve(e); ok {
		return isRequestIDName(v)
	}
	if strings.Contains(strings.ToLower(types.ExprString(e)), "requestid") {
		c.bad = append(c.bad, fmt.Sprintf("%s: header name %s does not resolve; add it to importedConsts", c.fset.Position(e.Pos()), types.ExprString(e)))
	}
	return false
}

// isRaw reports whether n reads the caller's request id: the helpers, a Get or Values of the header by its
// request id name, a map index by that name, or the request id the context carries.
func (c *censusCtx) isRaw(n ast.Node) bool {
	switch n := n.(type) {
	case *ast.CallExpr:
		switch fn := n.Fun.(type) {
		case *ast.Ident:
			return fn.Name == "envelopeRequestID" || fn.Name == "graphQLRequestID"
		case *ast.SelectorExpr:
			if fn.Sel.Name == "RequestIDFrom" {
				return true
			}
			if (fn.Sel.Name == "Get" || fn.Sel.Name == "Values") && len(n.Args) == 1 {
				return c.nameIsRequestID(n.Args[0])
			}
		}
	case *ast.IndexExpr:
		return c.nameIsRequestID(n.Index)
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

func isLogSafeCall(n ast.Node) bool {
	call, ok := n.(*ast.CallExpr)
	if !ok {
		return false
	}
	id, ok := call.Fun.(*ast.Ident)
	return ok && id.Name == "logSafeRequestID"
}

// tainted: e is, or is a slice, index or strings.* result of, a raw read or a tainted local.
func (c *censusCtx) tainted(e ast.Expr, vars map[string]bool) bool {
	switch e := e.(type) {
	case *ast.ParenExpr:
		return c.tainted(e.X, vars)
	case *ast.Ident:
		return vars[e.Name]
	case *ast.IndexExpr:
		return c.isRaw(e) || c.tainted(e.X, vars)
	case *ast.SliceExpr:
		return c.tainted(e.X, vars)
	case *ast.CallExpr:
		if isLogSafeCall(e) {
			return false
		}
		if c.isRaw(e) {
			return true
		}
		if sel, ok := e.Fun.(*ast.SelectorExpr); ok {
			if x, ok := sel.X.(*ast.Ident); ok && x.Name == "strings" {
				for _, a := range e.Args {
					if c.tainted(a, vars) {
						return true
					}
				}
			}
		}
	}
	return false
}

// requestIDReads parses the files and returns the raw request id reads that sit inside a log call's arguments
// (log) and those that do not (other), as "file:line" and "file#func" entries. A read wrapped in logSafeRequestID
// is neither. A local assigned from a raw read (also through an index, a slice or strings.*), and a parameter named
// requestID, count as raw where a log call uses them. consts is the header-name constants of the package files'
// own const decls plus importedConsts, resolved by value.
func requestIDReads(t *testing.T, files map[string]string) (logUses, other []string) {
	t.Helper()
	fset := token.NewFileSet()
	parsed := map[string]*ast.File{}
	c := &censusCtx{consts: map[string]string{}, t: t, fset: fset}
	for k, v := range importedConsts {
		c.consts[k] = v
	}
	for name, src := range files {
		f, err := parser.ParseFile(fset, name, src, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		parsed[name] = f
	}
	for pass := 0; pass < 3; pass++ {
		for _, f := range parsed {
			for _, d := range f.Decls {
				gd, ok := d.(*ast.GenDecl)
				if !ok || gd.Tok != token.CONST {
					continue
				}
				for _, sp := range gd.Specs {
					vs := sp.(*ast.ValueSpec)
					for i, id := range vs.Names {
						if i < len(vs.Values) {
							if v, ok := c.resolve(vs.Values[i]); ok {
								c.consts[id.Name] = v
							}
						}
					}
				}
			}
		}
	}
	for name, f := range parsed {
		for _, decl := range f.Decls {
			fd, ok := decl.(*ast.FuncDecl)
			if !ok || fd.Body == nil {
				continue
			}
			vars := map[string]bool{}
			if fd.Type.Params != nil {
				for _, p := range fd.Type.Params.List {
					for _, id := range p.Names {
						if id.Name == "requestID" {
							vars[id.Name] = true
						}
					}
				}
			}
			for changed := true; changed; {
				changed = false
				ast.Inspect(fd.Body, func(n ast.Node) bool {
					switch n := n.(type) {
					case *ast.AssignStmt:
						for i, l := range n.Lhs {
							id, ok := l.(*ast.Ident)
							if !ok || id.Name == "_" || vars[id.Name] {
								continue
							}
							var r ast.Expr
							if len(n.Rhs) == len(n.Lhs) {
								r = n.Rhs[i]
							} else if len(n.Rhs) == 1 && i == 0 {
								r = n.Rhs[0]
							}
							if r != nil && c.tainted(r, vars) {
								vars[id.Name] = true
								changed = true
							}
						}
					case *ast.ValueSpec:
						for i, id := range n.Names {
							if i < len(n.Values) && !vars[id.Name] && c.tainted(n.Values[i], vars) {
								vars[id.Name] = true
								changed = true
							}
						}
					}
					return true
				})
			}
			var visit func(n ast.Node, inLog bool)
			visit = func(root ast.Node, inLog bool) {
				ast.Inspect(root, func(n ast.Node) bool {
					if n == nil || isLogSafeCall(n) {
						return false
					}
					switch n := n.(type) {
					case *ast.CallExpr:
						if c.isRaw(n) {
							c.record(&logUses, &other, name, fd.Name.Name, n.Pos(), inLog)
							return false
						}
						if !inLog && isLogSinkCall(n) {
							for _, a := range n.Args {
								visit(a, true)
							}
							return false
						}
					case *ast.IndexExpr:
						if c.isRaw(n) {
							c.record(&logUses, &other, name, fd.Name.Name, n.Pos(), inLog)
							return false
						}
					case *ast.Ident:
						if inLog && vars[n.Name] {
							logUses = append(logUses, fmt.Sprintf("%s:%d", name, fset.Position(n.Pos()).Line))
						}
					}
					return true
				})
			}
			visit(fd.Body, false)
		}
	}
	for _, b := range c.bad {
		t.Error(b)
	}
	sort.Strings(logUses)
	sort.Strings(other)
	return logUses, other
}

func (c *censusCtx) record(logUses, other *[]string, file, fn string, pos token.Pos, inLog bool) {
	if inLog {
		*logUses = append(*logUses, fmt.Sprintf("%s:%d", file, c.fset.Position(pos).Line))
		return
	}
	*other = append(*other, file+"#"+fn)
}

func TestRequestIDCensusRecognizesRawLogUses(t *testing.T) {
	const pre = `package p
import ("log"; "log/slog")
const hdr = "X-Request-Id"
const hdr2 = "x-request-" + "id"
`
	for name, tc := range map[string]struct {
		body, other string
		logs        int
	}{
		"helper direct":             {`func a(r R) { log.Printf("x %s", envelopeRequestID(r)) }`, "", 1},
		"literal Get direct":        {`func a(r R) { slog.Warn("x", slog.String("request_id", r.Header.Get("x-request-id"))) }`, "", 1},
		"literal index direct":      {`func a(r R) { log.Print(r.Header["X-Request-Id"]) }`, "", 1},
		"const index direct":        {`func a(r R) { log.Print(r.Header[hdr]) }`, "", 1},
		"const Get direct":          {`func a(r R) { log.Print(r.Header.Get(hdr2)) }`, "", 1},
		"const index local":         {`func a(r R) { id := r.Header[hdr]; log.Print(id) }`, "p.go#a", 1},
		"const Get local":           {`func a(r R) { id := r.Header.Get(hdr); log.Print(id) }`, "p.go#a", 1},
		"const index outside":       {`func a(r R) { _ = r.Header[hdr] }`, "p.go#a", 0},
		"const Get outside":         {`func a(r R) { _ = r.Header.Get(hdr) }`, "p.go#a", 0},
		"imported const Get direct": {`func a(h H) { log.Print(h.Get(httpapi.RequestIDHeader)) }`, "", 1},
		"imported const Get local":  {`func a(h H) { id := h.Get(httpapi.RequestIDHeader); log.Print(id) }`, "p.go#a", 1},
		"imported const outside":    {`func a(h H) string { return h.Get(httpapi.RequestIDHeader) }`, "p.go#a", 0},
		"context id local":          {`func a(ctx C) { id := httpapi.RequestIDFrom(ctx); log.Print(id) }`, "p.go#a", 1},
		"values then index":         {`func a(r R) { v := r.Header.Values(hdr); log.Print(v[0]) }`, "p.go#a", 1},
		"strings passthrough":       {`func a(r R) { id := strings.ReplaceAll(envelopeRequestID(r), "x", "y"); log.Print(id) }`, "p.go#a", 1},
		"param named requestID":     {`func a(requestID string) { log.Print(requestID) }`, "", 1},
		"wrapped helper":            {`func a(r R) { log.Print(logSafeRequestID(envelopeRequestID(r))) }`, "", 0},
		"wrapped local":             {`func a(r R) { id := envelopeRequestID(r); log.Print(logSafeRequestID(id)) }`, "p.go#a", 0},
		"other header":              {`func a(r R) { log.Print(r.Header.Get("Accept"), r.Header["Accept"]) }`, "", 0},
		"response header set":       {`func a(r R) { w.Header().Set("X-Request-Id", envelopeRequestID(r)) }`, "p.go#a", 0},
	} {
		t.Run(name, func(t *testing.T) {
			logUses, other := requestIDReads(t, map[string]string{"p.go": pre + tc.body})
			if len(logUses) != tc.logs {
				t.Errorf("log uses = %v, want %d", logUses, tc.logs)
			}
			if got := strings.Join(other, ","); got != tc.other {
				t.Errorf("other uses = %q, want %q", got, tc.other)
			}
		})
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
