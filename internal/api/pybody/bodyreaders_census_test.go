package pybody_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// goAPIBodyReaders is every place under internal/api and internal/apiservice that
// reads a request body, as file:function. A route body goes through
// pybody.ReadChecked (the lone-surrogate boundary) or is named here with its reason.
// A new reader fails the test: add it to the boundary, or name it with a reason.
var goAPIBodyReaders = map[string]string{
	"api/pybody/pybody.go:Read":                                  "the decode: ReadChecked wraps it",
	"api/policy/bodyfirst.go:Guard.bodyFirstOver":                "BOUNDARY: every Guard.Body* route",
	"api/teamsidentity/teamsidentity.go:handlers.decodeFirst":    "BOUNDARY: teams and identities routes",
	"api/producttelemetry/producttelemetry.go:handler.ServeHTTP": "BOUNDARY: product telemetry",
	"api/restcore/restcore.go:Response.Text":                     "OUTBOUND: a provider response body, not a request",
	"api/billing/webhook.go:handlers.stripeWebhook":              "EXEMPT: signed raw body (HMAC input), byte-exact",
	"api/webhookintake/github.go:Deps.handleGitHubWebhook":       "EXEMPT: signed raw body (HMAC input), byte-exact",
	"api/webhookintake/pagerduty.go:readBodyLimited":             "EXEMPT: signed raw body (HMAC input), byte-exact",
	"api/externalingest/handlers.go:Deps.handleAcceptBatch":      "EXEMPT: own envelope validator (jiter parity): a lone surrogate is its pinned legacy 500, nothing stored (ticket 13776)",
	"api/externalingest/handlers.go:Deps.handleValidate":         "EXEMPT: own envelope validator (jiter parity): a lone surrogate is its pinned legacy 500, nothing stored (ticket 13776)",
	"apiservice/customerpush/validate.go:readBodyLimited":        "EXEMPT: same envelope validator as externalingest (ticket 13776)",
	"apiservice/authproxy.go:authProxy.sessionRoute":             "PASS-THROUGH: replays the bytes to go-api's guard, or forwards them raw to auth-service",
}

func TestEveryGoAPIBodyReaderIsTheBoundaryOrNamed(t *testing.T) {
	found := map[string]bool{}
	for _, dir := range []string{"api", "apiservice"} {
		root := filepath.Join("..", "..", dir)
		err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			fset := token.NewFileSet()
			file, err := parser.ParseFile(fset, path, nil, 0)
			if err != nil {
				return err
			}
			rel, _ := filepath.Rel(filepath.Join("..", ".."), path)
			for _, decl := range file.Decls {
				fn, ok := decl.(*ast.FuncDecl)
				if !ok || fn.Body == nil {
					continue
				}
				name := fn.Name.Name
				if fn.Recv != nil && len(fn.Recv.List) == 1 {
					switch typed := fn.Recv.List[0].Type.(type) {
					case *ast.Ident:
						name = typed.Name + "." + name
					case *ast.StarExpr:
						if id, ok := typed.X.(*ast.Ident); ok {
							name = id.Name + "." + name
						}
					}
				}
				ast.Inspect(fn.Body, func(n ast.Node) bool {
					switch typed := n.(type) {
					case *ast.SelectorExpr:
						if id, ok := typed.X.(*ast.Ident); ok && typed.Sel.Name == "Body" && (id.Name == "r" || id.Name == "req" || id.Name == "request") {
							found[filepath.ToSlash(rel)+":"+name] = true
						}
						if id, ok := typed.X.(*ast.Ident); ok && id.Name == "pybody" && (typed.Sel.Name == "Read" || typed.Sel.Name == "ReadChecked") {
							found[filepath.ToSlash(rel)+":"+name] = true
							if typed.Sel.Name == "Read" {
								t.Errorf("%s:%s calls pybody.Read: a go-api body entry reads through pybody.ReadChecked", rel, name)
							}
						}
					}
					return true
				})
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	var unknown []string
	for key := range found {
		if _, ok := goAPIBodyReaders[key]; !ok {
			unknown = append(unknown, key)
		}
	}
	sort.Strings(unknown)
	for _, key := range unknown {
		t.Errorf("new request-body reader %s: route it through pybody.ReadChecked or name it in goAPIBodyReaders with a reason", key)
	}
	for key := range goAPIBodyReaders {
		if !found[key] {
			t.Errorf("goAPIBodyReaders names %s but no such reader exists: remove it", key)
		}
	}
}
