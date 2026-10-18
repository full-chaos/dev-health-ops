package providersync

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// A wire request that escapes the unit's request ledger is a request the
// estimate comparison never sees. It escapes when a route holds, builds or
// bypasses an HTTP client other than the unit's own: a second *HTTPClient on a
// handler, a client built with NewHTTPClient or as a literal, or a request sent
// through a client's Doer without HTTPClient.Do. The census lists every such
// place in the providersync sources; a new one fails until it is listed with
// the reason its requests reach the unit's ledger (or why it is not a unit).
func clientSites(fset *token.FileSet, file *ast.File) []string {
	typeName := func(e ast.Expr) string {
		var name func(ast.Expr) string
		name = func(e ast.Expr) string {
			switch x := e.(type) {
			case *ast.Ident:
				return x.Name
			case *ast.SelectorExpr:
				return name(x.X) + "." + x.Sel.Name
			case *ast.StarExpr:
				return name(x.X)
			}
			return ""
		}
		return name(e)
	}
	isClient := func(name string) bool { return name == "providerfoundation.HTTPClient" || name == "HTTPClient" }
	var sites []string
	ast.Inspect(file, func(node ast.Node) bool {
		switch x := node.(type) {
		case *ast.TypeSpec:
			if structType, ok := x.Type.(*ast.StructType); ok {
				for _, field := range structType.Fields.List {
					if isClient(typeName(field.Type)) {
						for _, name := range field.Names {
							sites = append(sites, "field "+x.Name.Name+"."+name.Name)
						}
					}
				}
			}
		case *ast.CompositeLit:
			if isClient(typeName(x.Type)) {
				sites = append(sites, "literal HTTPClient")
			}
		case *ast.CallExpr:
			if name := typeName(x.Fun); strings.HasSuffix(name, "NewHTTPClient") {
				sites = append(sites, "call NewHTTPClient")
			} else if strings.HasSuffix(name, ".Doer.Do") {
				sites = append(sites, "call "+name)
			}
		}
		return true
	})
	return sites
}

func censusOf(t *testing.T, sources map[string]string) []string {
	t.Helper()
	fset := token.NewFileSet()
	var found []string
	for name, source := range sources {
		file, err := parser.ParseFile(fset, name, source, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, site := range clientSites(fset, file) {
			found = append(found, name+": "+site)
		}
	}
	slices.Sort(found)
	return found
}

// Every place providersync holds, builds or bypasses an HTTP client, with the
// reason its requests are in the unit's ledger or why there is no unit.
var listedClientSites = map[string]string{
	"jira_atlassian_route.go: field JiraAtlassianRouteHandler.GraphQLClient":   "second client of a unit route: the route gives its copy the unit client's Ledger (TestJiraAtlassianRouteCountsTheGraphQLClientsRequestsInTheUnitLedger)",
	"github_team_catalog_route.go: field GitHubTeamCatalogRouteHandler.Client": "post-sync team import service, not a sync unit: no unit ledger exists",
	"linear_absence.go: field LinearAbsenceProver.Client":                      "absence lookup of the team import, not a sync unit: no unit ledger exists",
	"membership_absence.go: field githubMembershipAbsence.client":              "absence lookup of the team import, not a sync unit: no unit ledger exists",
	"membership_absence.go: field gitlabMembershipAbsence.client":              "absence lookup of the team import, not a sync unit: no unit ledger exists",
	"ownership_absence.go: field githubRepoGrantAbsence.client":                "absence lookup of the ownership derivation, not a sync unit: no unit ledger exists",
	"ownership_absence.go: field githubTeamAbsence.client":                     "absence lookup of the ownership derivation, not a sync unit: no unit ledger exists",
	"ownership_absence.go: field gitlabGroupProjectAbsence.client":             "absence lookup of the ownership derivation, not a sync unit: no unit ledger exists",
	"ownership_absence.go: field gitlabTeamAbsence.client":                     "absence lookup of the ownership derivation, not a sync unit: no unit ledger exists",
	"ownership_absence.go: field jiraProjectAbsence.client":                    "absence lookup of the ownership derivation, not a sync unit: no unit ledger exists",
}

func TestEveryClientAProvidersyncRouteHoldsBuildsOrBypassesIsListed(t *testing.T) {
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
	found := censusOf(t, sources)
	// de-duplicate: a type with several client fields lists each field once
	seen := map[string]bool{}
	for _, site := range found {
		seen[site] = true
	}
	for site := range seen {
		if _, listed := listedClientSites[site]; !listed {
			t.Errorf("%s holds, builds or bypasses an HTTP client the unit's ledger may not see; give it the unit client's Ledger (or prove it is not a unit) and list it with the reason", site)
		}
	}
	for site := range listedClientSites {
		if !seen[site] {
			t.Errorf("listed client site %q no longer exists; remove it", site)
		}
	}
}

// A second client added to a route without the ledger is a site the census
// reports: the scanner finds each shape, so the listed set above fails on it.
func TestTheCensusFindsASecondClientAClientBuiltByARouteAndADoerBypass(t *testing.T) {
	found := censusOf(t, map[string]string{"new_route.go": `package providersync

import "github.com/full-chaos/dev-health-ops/internal/providerfoundation"

type newRouteHandler struct {
	SecondaryClient *providerfoundation.HTTPClient
}

func (h newRouteHandler) Collect(client *providerfoundation.HTTPClient) {
	own, _ := providerfoundation.NewHTTPClient("x", "https://x.test", nil, nil, providerfoundation.RetryPolicy{}, nil)
	_ = &providerfoundation.HTTPClient{}
	_, _ = client.Doer.Do(nil)
	_ = own
}
`})
	want := []string{
		"new_route.go: call NewHTTPClient",
		"new_route.go: call client.Doer.Do",
		"new_route.go: field newRouteHandler.SecondaryClient",
		"new_route.go: literal HTTPClient",
	}
	if fmt.Sprint(found) != fmt.Sprint(want) {
		t.Fatalf("census = %v, want %v", found, want)
	}
	for _, site := range found {
		if _, listed := listedClientSites[site]; listed {
			t.Fatalf("%q is listed; a new site must fail the census until it is listed", site)
		}
	}
}
