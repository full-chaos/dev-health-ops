package providersync

import (
	"fmt"
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

// responseDrainAllowed names the one io.Copy that drains a response body without
// reading a payload. It is keyed by file:line, so a moved or second drain fails the census.
var responseDrainAllowed = map[string]string{
	"ownership_absence.go:68": "best-effort drain of a 204/200 team-repo probe; the verdict is the status and the error is ignored",
}

func responseBodyReadSites(t *testing.T, name string, source []byte) []string {
	t.Helper()
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, name, source, 0)
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
			sites = append(sites, fmt.Sprintf("%s.%s@%d", pkg.Name, selector.Sel.Name, fset.Position(call.Pos()).Line))
		}
		return true
	})
	return sites
}

// responseReadOffenders returns the raw body reads of one file that are not allowed, and the
// drain allowances the file used. Only an io.Copy at an allow-listed file:line passes.
func responseReadOffenders(t *testing.T, base string, source []byte, drainsApply bool) (offenders, used []string) {
	t.Helper()
	for _, site := range responseBodyReadSites(t, base, source) {
		key := base + ":" + site[strings.LastIndex(site, "@")+1:]
		if _, allowed := responseDrainAllowed[key]; allowed && drainsApply && strings.HasPrefix(site, "io.Copy@") {
			used = append(used, key)
			continue
		}
		offenders = append(offenders, site)
	}
	return offenders, used
}

// A 2xx body read or decode outside the response helpers has no route template:
// its failure prints no route=. Every read goes through providerfoundation.ReadResponseBody
// (or a decodePage walker) so a failure is a ProviderError that names the route.
func TestNoProviderResponseBodyReadBypassesTheRouteHelpers(t *testing.T) {
	var offenders []string
	usedDrains := map[string]bool{}
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
			bad, used := responseReadOffenders(t, filepath.Base(name), source, dir == ".")
			for _, offender := range bad {
				offenders = append(offenders, name+": "+offender)
			}
			for _, key := range used {
				usedDrains[key] = true
			}
		}
	}
	if files < 50 {
		t.Fatalf("scanned %d files; the census must see providersync, scheduler/sync and providerfoundation", files)
	}
	for key := range responseDrainAllowed {
		if !usedDrains[key] {
			offenders = append(offenders, "stale drain allowance: "+key)
		}
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
		"io.Copy(&buffer, response.Body)":                   1,
		"io.Copy(file, resp.Body)":                          1,
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

func TestDrainAllowanceCoversExactlyTheOneDrainSite(t *testing.T) {
	source, err := os.ReadFile("ownership_absence.go")
	if err != nil {
		t.Fatal(err)
	}
	if bad, used := responseReadOffenders(t, "ownership_absence.go", source, true); len(bad) != 0 || len(used) != 1 {
		t.Fatalf("real file: offenders=%v used=%v, want the single allow-listed drain", bad, used)
	}
	planted := append([]byte(nil), source...)
	planted = append(planted, []byte("\nfunc plantedRead(response *http.Response) {\n\t_, _ = io.Copy(&bytes.Buffer{}, response.Body)\n}\n")...)
	if bad, _ := responseReadOffenders(t, "ownership_absence.go", planted, true); len(bad) != 1 {
		t.Fatalf("a second io.Copy in the same file must be flagged, offenders=%v", bad)
	}
	moved := append([]byte("\n"), source...)
	if bad, _ := responseReadOffenders(t, "ownership_absence.go", moved, true); len(bad) != 1 {
		t.Fatalf("a moved drain must lose its allowance, offenders=%v", bad)
	}
}
