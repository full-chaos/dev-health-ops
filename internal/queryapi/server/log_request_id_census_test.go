package server

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/build"
	"go/constant"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/pdp"
)

// requestIDExemptions are the raw request id reads that reach no log call. Each one is a response value, a
// principal request meta, or a guarded local that the log call then wraps; a new raw read needs a reason here.
// requestIDExemptions: one row per raw read outside a log call, keyed file#function#ordinal (the read's order in
// that function). A further read needs its own row; a row no read uses fails.
var requestIDExemptions = map[string]string{
	"query_route.go#envelopeRequestID#1":                     "the one raw read of the header",
	"rest_scope.go#restScopeOrg#1":                           "local is checked, then wrapped by logSafeRequestID at the log",
	"rest_access.go#authenticateRESTRouted#1":                "principal.WithRequestMeta, not a log",
	"rest_access.go#authenticateRESTRouted#2":                "principal.WithRequestMeta, not a log",
	"pdp_gate.go#writePDPDenied#1":                           "local is checked and stripped of CR and LF before the log",
	"explain_route.go#newExplainGetHandler#1":                "argument to writeExplainResponse, which wraps the id at its log",
	"explain_route.go#newExplainPostHandler#1":               "argument to writeExplainResponse, which wraps the id at its log",
	"internal_auth.go#authenticateInternalRequest#1":         "principal.WithRequestMeta, not a log",
	"rest_error_response.go#authenticateRESTRequest#1":       "principal.WithRequestMeta, not a log",
	"rest_error_response.go#authenticateRESTRequest#2":       "principal.WithRequestMeta, not a log",
	"rest_error_response.go#echoRequestID#1":                 "response header, only for an acceptable id",
	"rest_error_response.go#bindRequestID#1":                 "response header and request context, only for an acceptable id",
	"rest_error_response.go#bindRequestID#2":                 "response header and request context, only for an acceptable id",
	"graphql_edge_route.go#correlationID#1":                  "the id the edge middleware binds to the context and echoes; read as written",
	"graphql_error_presenter.go#graphQLRequestID#1":          "returns the bound id to its callers; each log call wraps it",
	"graphql_error_presenter.go#graphQLRequestID#2":          "returns the bound id to its callers; each log call wraps it",
	"graphql_error_presenter.go#safeGraphQLErrorPresenter#1": "local goes to the client as an extension; each log call wraps it",
}

// requestIDForwardAllowed are the callees a raw request id may be passed to: none of them logs its argument.
var requestIDForwardAllowed = map[string]string{
	"github.com/full-chaos/dev-health-ops/internal/queryapi/principal.WithRequestMeta": "principal request meta; not a log",
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi.WithRequestID":         "request context value",
	"local.acceptableCorrelationID": "predicate",
	"local.writeExplainResponse":    "logs its id argument through logSafeRequestID",
	"local.withRequestID":           "client response extension",
	"net/http.Header.Set":           "response header",
	"net/http.Header.Add":           "response header",
	"builtin.len":                   "length only",
}

// isRequestIDName compares a header name by its canonical form: X-Request-Id and X-Request-ID are one name.
func isRequestIDName(v string) bool { return http.CanonicalHeaderKey(v) == "X-Request-Id" }

var (
	censusImporterOnce sync.Once
	censusSourceImp    types.Importer
)

// censusImporter type-checks the standard library and the httpapi package from source and gives every other
// import an empty package, so a constant that names another package's header name resolves by its value while
// the rest of the module need not compile here. A package it cannot load is recorded in errs: the census stops
// with that error instead of reading an unresolved name as "not a request id". With -trimpath the test binary
// has no built-in GOROOT, so it is taken from the environment or from `go env GOROOT`.
type censusImporter struct{ errs *[]string }

func (ci censusImporter) Import(path string) (*types.Package, error) {
	censusImporterOnce.Do(func() {
		if build.Default.GOROOT == "" {
			if r := os.Getenv("GOROOT"); r != "" {
				build.Default.GOROOT = r
			} else if out, err := exec.Command("go", "env", "GOROOT").Output(); err == nil {
				build.Default.GOROOT = strings.TrimSpace(string(out))
			}
		}
		censusSourceImp = importer.ForCompiler(token.NewFileSet(), "source", nil)
	})
	first, _, _ := strings.Cut(path, "/")
	if !strings.Contains(first, ".") || strings.HasSuffix(path, "/internal/auth/httpapi") {
		pkg, err := censusSourceImp.Import(path)
		if err != nil {
			*ci.errs = append(*ci.errs, fmt.Sprintf("%s: %v", path, err))
		}
		return pkg, err
	}
	pkg := types.NewPackage(path, filepath.Base(path))
	pkg.MarkComplete()
	return pkg, nil
}

type censusCtx struct {
	ordinal map[string]int
	info    *types.Info
	t       *testing.T
	fset    *token.FileSet
	bad     []string
}

// resolve returns the string constant value of e as the type checker computed it.
func (c *censusCtx) resolve(e ast.Expr) (string, bool) {
	tv, ok := c.info.Types[e]
	if !ok || tv.Value == nil || tv.Value.Kind() != constant.String {
		return "", false
	}
	return constant.StringVal(tv.Value), true
}

// nameIsRequestID resolves a header name by its constant value. A name that looks like a request id constant but
// does not resolve is reported, so an imported constant is added to importedConsts instead of slipping through.
func (c *censusCtx) nameIsRequestID(e ast.Expr) bool {
	if v, ok := c.resolve(e); ok {
		return isRequestIDName(v)
	}
	if strings.Contains(strings.ToLower(types.ExprString(e)), "requestid") {
		c.bad = append(c.bad, fmt.Sprintf("%s: header name %s is not a constant the type checker resolves", c.fset.Position(e.Pos()), types.ExprString(e)))
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
// requestID, count as raw where a log call uses them. A header name is a constant by its value, as go/types computes it.
func requestIDReads(t *testing.T, files map[string]string, allowed map[string]string) (logUses, other, forwards []string) {
	t.Helper()
	fset := token.NewFileSet()
	parsed := map[string]*ast.File{}
	var names []string
	for name := range files {
		names = append(names, name)
	}
	sort.Strings(names)
	var list []*ast.File
	for _, name := range names {
		f, err := parser.ParseFile(fset, name, files[name], 0)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		parsed[name] = f
		list = append(list, f)
	}
	info := &types.Info{Types: map[ast.Expr]types.TypeAndValue{}, Uses: map[*ast.Ident]types.Object{}, Selections: map[*ast.SelectorExpr]*types.Selection{}}
	var importErrs []string
	conf := types.Config{Importer: censusImporter{errs: &importErrs}, Error: func(error) {}, FakeImportC: true}
	conf.Check("p", fset, list, info)
	if len(importErrs) > 0 {
		t.Fatalf("the census cannot load a package it type-checks from source (GOROOT=%q): %s", build.Default.GOROOT, strings.Join(importErrs, "; "))
	}
	c := &censusCtx{info: info, t: t, fset: fset}
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
						if callee := c.calleeKey(n); callee != "" && !inLog && !strings.HasPrefix(callee, "strings.") {
							if _, ok := allowed[callee]; !ok {
								for _, a := range n.Args {
									if c.tainted(a, vars) {
										forwards = append(forwards, fmt.Sprintf("%s:%d %s", name, fset.Position(a.Pos()).Line, callee))
									}
								}
							}
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
	sort.Strings(forwards)
	return logUses, other, forwards
}

// calleeKey names the callee by what it resolves to, not by its spelling: "builtin.len", "local.name" (this
// package), "<import path>.Name" for a package function, "<receiver type>.Name" for a method, "?.Name" if it
// does not resolve. A same-name function in another package or on another type is a different key.
func (c *censusCtx) calleeKey(call *ast.CallExpr) string {
	switch fn := call.Fun.(type) {
	case *ast.Ident:
		switch o := c.info.Uses[fn].(type) {
		case *types.Builtin:
			return "builtin." + o.Name()
		case *types.Func:
			if o.Pkg() != nil && o.Pkg().Path() == "p" {
				return "local." + o.Name()
			}
			if o.Pkg() != nil {
				return o.Pkg().Path() + "." + o.Name()
			}
		case *types.TypeName:
			return "type." + o.Name()
		}
		return "?." + fn.Name
	case *ast.SelectorExpr:
		if x, ok := fn.X.(*ast.Ident); ok {
			if pn, ok := c.info.Uses[x].(*types.PkgName); ok {
				return pn.Imported().Path() + "." + fn.Sel.Name
			}
		}
		if sel := c.info.Selections[fn]; sel != nil {
			rt := sel.Recv()
			if ptr, ok := rt.(*types.Pointer); ok {
				rt = ptr.Elem()
			}
			return types.TypeString(rt, func(p *types.Package) string { return p.Path() }) + "." + fn.Sel.Name
		}
		return "?." + fn.Sel.Name
	}
	return ""
}

func (c *censusCtx) record(logUses, other *[]string, file, fn string, pos token.Pos, inLog bool) {
	if c.ordinal == nil {
		c.ordinal = map[string]int{}
	}
	if inLog {
		*logUses = append(*logUses, fmt.Sprintf("%s:%d", file, c.fset.Position(pos).Line))
		return
	}
	c.ordinal[file+"#"+fn]++
	*other = append(*other, fmt.Sprintf("%s#%s#%d", file, fn, c.ordinal[file+"#"+fn]))
}

func TestRequestIDCensusRecognizesRawLogUses(t *testing.T) {
	const pre = `package p
import (
	"context"
	"log"
	"log/slog"
	"net/http"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"example.com/other"
)
const hdr = "X-Request-Id"
const hdr2 = "x-request-" + "id"
const hdr3 = "X-Request-ID"
type R struct{ Header http.Header }
var w http.ResponseWriter
func envelopeRequestID(r R) string { return "" }
func graphQLRequestID(c context.Context) string { return "" }
func logSafeRequestID(s string) string { return s }
var _ = strings.ToLower
var _ = slog.Info
`
	for name, tc := range map[string]struct {
		body, other string
		logs        int
		forwards    int
	}{
		"helper direct":             {`func a(r R) { log.Printf("x %s", envelopeRequestID(r)) }`, "", 1, 0},
		"literal Get direct":        {`func a(r R) { slog.Warn("x", slog.String("request_id", r.Header.Get("x-request-id"))) }`, "", 1, 0},
		"literal index direct":      {`func a(r R) { log.Print(r.Header["X-Request-Id"]) }`, "", 1, 0},
		"const index direct":        {`func a(r R) { log.Print(r.Header[hdr]) }`, "", 1, 0},
		"const Get direct":          {`func a(r R) { log.Print(r.Header.Get(hdr2)) }`, "", 1, 0},
		"canonical-case const Get":  {`func a(r R) { log.Print(r.Header.Get(hdr3)) }`, "", 1, 0},
		"const index local":         {`func a(r R) { id := r.Header[hdr]; log.Print(id) }`, "p.go#a#1", 1, 0},
		"const Get local":           {`func a(r R) { id := r.Header.Get(hdr); log.Print(id) }`, "p.go#a#1", 1, 0},
		"const index outside":       {`func a(r R) { _ = r.Header[hdr] }`, "p.go#a#1", 0, 0},
		"const Get outside":         {`func a(r R) { _ = r.Header.Get(hdr) }`, "p.go#a#1", 0, 0},
		"imported const Get direct": {`func a(h http.Header) { log.Print(h.Get(httpapi.RequestIDHeader)) }`, "", 1, 0},
		"imported const Get local":  {`func a(h http.Header) { id := h.Get(httpapi.RequestIDHeader); log.Print(id) }`, "p.go#a#1", 1, 0},
		"imported const outside":    {`func a(h http.Header) string { return h.Get(httpapi.RequestIDHeader) }`, "p.go#a#1", 0, 0},
		"context id local":          {`func a(ctx context.Context) { id := httpapi.RequestIDFrom(ctx); log.Print(id) }`, "p.go#a#1", 1, 0},
		"values then index":         {`func a(r R) { v := r.Header.Values(hdr); log.Print(v[0]) }`, "p.go#a#1", 1, 0},
		"strings passthrough":       {`func a(r R) { id := strings.ReplaceAll(envelopeRequestID(r), "x", "y"); log.Print(id) }`, "p.go#a#1", 1, 0},
		"param named requestID":     {`func a(requestID string) { log.Print(requestID) }`, "", 1, 0},
		"wrapped helper":            {`func a(r R) { log.Print(logSafeRequestID(envelopeRequestID(r))) }`, "", 0, 0},
		"wrapped local":             {`func a(r R) { id := envelopeRequestID(r); log.Print(logSafeRequestID(id)) }`, "p.go#a#1", 0, 0},
		"other header":              {`func a(r R) { log.Print(r.Header.Get("Accept"), r.Header["Accept"]) }`, "", 0, 0},
		"response header set":       {`func a(r R) { w.Header().Set("X-Request-Id", envelopeRequestID(r)) }`, "p.go#a#1", 0, 0},
		"forward to a helper that logs": {`func a(r R) { helper(envelopeRequestID(r)) }
func helper(id string) { log.Print(id) }`, "p.go#a#1", 0, 1},
		"forward literal Get": {`func a(r R) { helper(r.Header.Get("X-Request-Id")) }
func helper(id string) { log.Print(id) }`, "p.go#a#1", 0, 1},
		"forward const index via local": {`func a(r R) { id := r.Header[hdr][0]; helper(id) }
func helper(id string) { log.Print(id) }`, "p.go#a#1", 0, 1},
		"forward through strings": {`func a(r R) { helper(strings.TrimSpace(envelopeRequestID(r))) }
func helper(id string) { log.Print(id) }`, "p.go#a#1", 0, 1},
		"forward wrapped is safe": {`func a(r R) { helper(logSafeRequestID(envelopeRequestID(r))) }
func helper(id string) { log.Print(id) }`, "", 0, 0},
		"forward to an allowed callee": {`func a(r R) { w.Header().Set("X-Request-Id", envelopeRequestID(r)) }`, "p.go#a#1", 0, 0},
		"forward to Set on a non-Header type": {`type L struct{}
func (L) Set(k, v string) { log.Print(v) }
func a(r R, l L) { l.Set("request_id", envelopeRequestID(r)) }`, "p.go#a#1", 0, 1},
		"forward to a same-name func in another package": {`func a(r R) { _ = other.WithRequestID(nil, envelopeRequestID(r)) }`, "p.go#a#1", 0, 1},
		"forward to a local func named like a listed one": {`func WithRequestID(c context.Context, id string) { log.Print(id) }
func a(r R) { WithRequestID(nil, envelopeRequestID(r)) }`, "p.go#a#1", 0, 1},
		"forward to http.Header Set and Add": {`func a(r R) { r.Header.Set("X-Request-Id", envelopeRequestID(r)); r.Header.Add("X", envelopeRequestID(r)) }`, "p.go#a#1,p.go#a#2", 0, 0},
		"second read in a function":          {`func a(r R) { _ = envelopeRequestID(r); _ = envelopeRequestID(r) }`, "p.go#a#1,p.go#a#2", 0, 0},
	} {
		t.Run(name, func(t *testing.T) {
			logUses, other, forwards := requestIDReads(t, map[string]string{"p.go": pre + tc.body}, requestIDForwardAllowed)
			if len(forwards) != tc.forwards {
				t.Errorf("forwards = %v, want %d", forwards, tc.forwards)
			}
			if len(logUses) != tc.logs {
				t.Errorf("log uses = %v, want %d", logUses, tc.logs)
			}
			if got := strings.Join(other, ","); got != tc.other {
				t.Errorf("other uses = %q, want %q", got, tc.other)
			}
		})
	}
}

// censusProblems is every way the files break the rule: a raw request id in a log call, a raw id passed to a
// callee that is not listed, a raw read outside a log call with no row, and a row no read uses.
func censusProblems(t *testing.T, files map[string]string, exemptions, allowed map[string]string) []string {
	t.Helper()
	logUses, other, forwards := requestIDReads(t, files, allowed)
	var out []string
	for _, l := range logUses {
		out = append(out, "raw request id reaches a log call: "+l)
	}
	for _, f := range forwards {
		out = append(out, "raw request id passed to a callee that may log it: "+f)
	}
	used := map[string]bool{}
	for _, o := range other {
		used[o] = true
		if _, ok := exemptions[o]; !ok {
			out = append(out, "raw request id read outside a log call has no row: "+o)
		}
	}
	for k := range exemptions {
		if !used[k] {
			out = append(out, "stale request id row: "+k)
		}
	}
	sort.Strings(out)
	return out
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
	for _, p := range censusProblems(t, files, requestIDExemptions, requestIDForwardAllowed) {
		t.Error(p)
	}
}

func TestRequestIDCensusRowsAndForwards(t *testing.T) {
	const pre = `package p
import (
	"log"
	"net/http"
)
type R struct{ Header http.Header }
func envelopeRequestID(r R) string { return "" }
func logSafeRequestID(s string) string { return s }
func helper(id string) { log.Print(id) }
`
	allowed := map[string]string{"local.setHeader": "response header write"}
	rows := map[string]string{"p.go#restScopeOrg#1": "guarded local"}
	for name, tc := range map[string]struct {
		body string
		want string
	}{
		"one read with its row": {`func restScopeOrg(r R) { _ = envelopeRequestID(r) }`, ""},
		"reviewer form: second read forwarded to a logging helper": {`func restScopeOrg(r R) { _ = envelopeRequestID(r); helper(r.Header.Get("X-Request-Id")) }`,
			"passed to a callee that may log it"},
		"second raw read has no row": {`func restScopeOrg(r R) { _ = envelopeRequestID(r); _ = r.Header.Get("X-Request-Id") }`,
			"has no row: p.go#restScopeOrg#2"},
		"forward through logSafeRequestID passes": {`func restScopeOrg(r R) { _ = envelopeRequestID(r); helper(logSafeRequestID(r.Header.Get("X-Request-Id"))) }`, ""},
		"forward to a listed non-log callee, own row": {`func restScopeOrg(r R) { _ = envelopeRequestID(r) }
func other(r R) { setHeader(envelopeRequestID(r)) }
func setHeader(string) {}`, "has no row: p.go#other#1"},
		"stale row": {`func restScopeOrg(r R) {}`, "stale request id row: p.go#restScopeOrg#1"},
	} {
		t.Run(name, func(t *testing.T) {
			got := strings.Join(censusProblems(t, map[string]string{"p.go": pre + tc.body}, rows, allowed), "\n")
			if tc.want == "" && got != "" || tc.want != "" && !strings.Contains(got, tc.want) {
				t.Errorf("problems = %q, want to contain %q", got, tc.want)
			}
		})
	}
	t.Run("forward to a listed non-log callee passes", func(t *testing.T) {
		src := pre + `func restScopeOrg(r R) { setHeader(envelopeRequestID(r)) }
func setHeader(string) {}`
		if got := censusProblems(t, map[string]string{"p.go": src}, rows, allowed); len(got) != 0 {
			t.Fatalf("problems = %v, want none", got)
		}
	})
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

func TestPDPRefusalLogsOneLineForARequestIDWithLineBreaks(t *testing.T) {
	var out bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&out, nil)))
	defer slog.SetDefault(prev)
	for _, id := range []string{"abc\r\nlevel=ERROR msg=forged", "abc\ndef", "abc\rdef"} {
		out.Reset()
		r := httptest.NewRequest(http.MethodGet, "/api/v1/x", nil)
		r.Header["X-Request-Id"] = []string{id}
		writePDPDenied(httptest.NewRecorder(), r, "rest", &pdp.DenyError{Reason: "role_lacks_action"})
		line := strings.TrimSuffix(out.String(), "\n")
		if !strings.Contains(line, "the PDP refused the request") || strings.ContainsAny(line, "\r\n") || strings.Contains(line, "forged") {
			t.Errorf("id %q: log output %q is not one line of the site without the id", id, out.String())
		}
	}
}

func TestCensusImporterRecordsAPackageItCannotLoad(t *testing.T) {
	var errs []string
	if _, err := (censusImporter{errs: &errs}).Import("nosuchstdpkg/inner"); err == nil || len(errs) != 1 {
		t.Fatalf("Import(nosuchstdpkg/inner) = %v, recorded %v; want an error and one record", err, errs)
	}
	if pkg, err := (censusImporter{errs: &errs}).Import("example.com/other"); err != nil || pkg == nil || len(errs) != 1 {
		t.Fatalf("module package: %v %v, recorded %v; want an empty package and no new record", pkg, err, errs)
	}
}
