package providerfoundation

import (
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// A request sent without HTTPClient.Do or DoUnauthenticated is a wire request
// the unit's request ledger never counts. The census lists every Do call in the
// package's sources and what it is; a new one fails until it is listed.
func doSites(t *testing.T, sources map[string]string) []string {
	t.Helper()
	fset := token.NewFileSet()
	var sites []string
	for name, source := range sources {
		file, err := parser.ParseFile(fset, name, source, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(file, func(node ast.Node) bool {
			if call, ok := node.(*ast.CallExpr); ok {
				if selector, ok := call.Fun.(*ast.SelectorExpr); ok && selector.Sel.Name == "Do" {
					sites = append(sites, name+": "+types.ExprString(selector.X)+".Do")
				}
			}
			return true
		})
	}
	slices.Sort(sites)
	return slices.Compact(sites)
}

var listedDoSites = map[string]string{
	"http.go: guarded.Do":                              "HTTPClient.Do: counted per attempt, redirect hops counted by the per-call guard",
	"http.go: c.Doer.Do":                               "HTTPClient.DoUnauthenticated: counted",
	"http.go: c.Do":                                    "HTTPClient.DoRoute: goes through HTTPClient.Do",
	"pagerduty_pagination.go: client.Do":               "pagination through HTTPClient.Do",
	"budget.go: r.once.Do":                             "sync.Once, not a request",
	"budget.go: g.Client.Do":                           "Valkey command, not a provider request",
	"clients.go: httpguard.NoRedirectsDoer(a.doer).Do": "credential exchange (OAuth token / GitHub App installation token): an auth endpoint call outside the unit's route families",
	"pagerduty_oauth.go: pagerDutyFormClient(h.Doer, 45 * time.Second).Do":        "credential exchange (OAuth) outside the unit's route families",
	"pagerduty_oauth_callback.go: pagerDutyFormClient(doer, 10 * time.Second).Do": "credential exchange (OAuth) outside the unit's route families",
	"pagerduty_oauth_revoke.go: pagerDutyFormClient(doer, 10 * time.Second).Do":   "credential revoke (OAuth) outside the unit's route families",
	"pagerduty_credential_validation.go: client.Do":                               "credential validation read of a connection test, not a sync unit",
}

func TestEveryDoCallOfTheProviderFoundationIsListed(t *testing.T) {
	entries, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	sources := map[string]string{}
	for _, name := range entries {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		body, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		sources[name] = string(body)
	}
	seen := map[string]bool{}
	for _, site := range doSites(t, sources) {
		seen[site] = true
		if _, listed := listedDoSites[site]; !listed {
			t.Errorf("%s sends a request the unit's request ledger may not count; count it (HTTPClient.Do) or list it with the reason it is outside the unit", site)
		}
	}
	for site := range listedDoSites {
		if !seen[site] {
			t.Errorf("listed Do site %q no longer exists; remove it", site)
		}
	}
}

func TestTheDoCensusFindsARequestSentAroundTheClient(t *testing.T) {
	sites := doSites(t, map[string]string{"new_path.go": `package providerfoundation

func sendAround(c *HTTPClient) { _, _ = c.Doer.Do(nil); _, _ = c.Do(nil, "GET", "/x", nil) }
`})
	if len(sites) != 2 {
		t.Fatalf("sites = %v, want c.Doer.Do and c.Do", sites)
	}
	for _, site := range sites {
		if _, listed := listedDoSites[site]; listed {
			t.Fatalf("%q is listed in the census of another file", site)
		}
	}
}
