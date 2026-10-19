package providersync

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// responseReadAllowedFiles are the providerfoundation files that read a response body
// on purpose: the helpers themselves, the page decoders, the HTTP client's own error
// classification, and the credential and OAuth exchanges that carry no route template.
var responseReadAllowedFiles = map[string]bool{
	"response_read.go":                   true,
	"pagination.go":                      true,
	"http.go":                            true,
	"clients.go":                         true,
	"pagerduty_oauth.go":                 true,
	"pagerduty_oauth_callback.go":        true,
	"pagerduty_credential_validation.go": true,
}

// responseDrainAllowed lists the io.Copy drains of a response body that read no
// payload: the bytes are discarded and the error is ignored on purpose.
var responseDrainAllowed = map[string]string{
	"ownership_absence.go": "best-effort drain of a 204/200 membership probe; the verdict is the status, the error is ignored",
}

func responseBodyReadSites(t *testing.T, name string, source []byte) []string {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), name, source, 0)
	if err != nil {
		t.Fatal(err)
	}
	var sites []string
	ast.Inspect(file, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		selector, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		pkg, ok := selector.X.(*ast.Ident)
		if !ok || !((pkg.Name == "io" && selector.Sel.Name == "ReadAll") || (pkg.Name == "json" && selector.Sel.Name == "NewDecoder") || (pkg.Name == "io" && selector.Sel.Name == "Copy")) {
			return true
		}
		reads := false
		ast.Inspect(call, func(inner ast.Node) bool {
			if field, ok := inner.(*ast.SelectorExpr); ok && field.Sel.Name == "Body" {
				reads = true
			}
			return true
		})
		if reads {
			sites = append(sites, pkg.Name+"."+selector.Sel.Name)
		}
		return true
	})
	return sites
}

// A 2xx body read or decode outside the response helpers has no route template:
// its failure prints no route=. Every read goes through providerfoundation.ReadResponseBody
// (or a decodePage walker) so a failure is a ProviderError that names the route.
func TestNoProviderResponseBodyReadBypassesTheRouteHelpers(t *testing.T) {
	var offenders []string
	files := 0
	for _, dir := range []string{".", "../scheduler/sync", "../providerfoundation"} {
		names, err := filepath.Glob(filepath.Join(dir, "*.go"))
		if err != nil {
			t.Fatal(err)
		}
		for _, name := range names {
			if strings.HasSuffix(name, "_test.go") || (dir == "../providerfoundation" && responseReadAllowedFiles[filepath.Base(name)]) {
				continue
			}
			source, err := os.ReadFile(name)
			if err != nil {
				t.Fatal(err)
			}
			files++
			for _, site := range responseBodyReadSites(t, name, source) {
				if _, drain := responseDrainAllowed[filepath.Base(name)]; drain && dir == "." && site == "io.Copy" {
					continue
				}
				offenders = append(offenders, name+": "+site)
			}
		}
	}
	if files < 50 {
		t.Fatalf("scanned %d files; the census must see providersync, scheduler/sync and providerfoundation", files)
	}
	sort.Strings(offenders)
	if len(offenders) > 0 {
		t.Fatalf("response body read outside the route helpers:\n%s", strings.Join(offenders, "\n"))
	}
}

func TestResponseBodyReadDetectorFlagsRawReads(t *testing.T) {
	for source, want := range map[string]int{
		"io.ReadAll(io.LimitReader(response.Body, 10))":     1,
		"json.NewDecoder(response.Body).Decode(&v)":         1,
		"json.NewDecoder(io.LimitReader(resp.Body, 9))":     1,
		"io.Copy(io.Discard, response.Body)":                1,
		"io.Copy(io.Discard, reader)":                       0,
		"io.ReadAll(reader)":                                0,
		"json.NewDecoder(bytes.NewReader(raw)).Decode(&v)":  0,
		"providerfoundation.ReadResponseBody(response, 10)": 0,
	} {
		file := "package p\n\nfunc f() {\n\t_, _ = " + source + "\n}\n"
		if got := len(responseBodyReadSites(t, "fixture.go", []byte(file))); got != want {
			t.Errorf("%s: detector counts %d, want %d", source, got, want)
		}
	}
}
