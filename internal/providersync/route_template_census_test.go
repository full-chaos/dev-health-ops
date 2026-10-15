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

// CHAOS-9776 census: every provider page-walk option literal and every
// repository/project object fetch in this package names a static route
// template, so a status or transport failure says which route failed without
// printing a path that holds an owner, repository, project or member name.
// A file in routeTemplateNotCovered still builds its error without a route
// (the error then carries no path at all); moving a file out of the list is
// the way to cover it. A NEW file or a new un-templated call in a covered file
// fails here, so the gap is visible in review.
var routeTemplateNotCovered = map[string]string{
	"github_blame_route.go":             "file-path routes",
	"github_commit_stats_route.go":      "commit detail routes",
	"github_files_route.go":             "file-path routes",
	"github_pr_reviews_fetch.go":        "GraphQL POST, no name in the path",
	"github_repository_route.go":        "repository metadata",
	"github_security_route.go":          "security alert routes",
	"github_work_items_projects_v2.go":  "GraphQL POST, no name in the path",
	"github_work_items_social_fetch.go": "GraphQL POST, no name in the path",
	"gitlab_blame_route.go":             "file-path routes",
	"gitlab_commit_stats_route.go":      "commit detail routes",
	"gitlab_feature_flags_route.go":     "feature flag routes",
	"gitlab_files_route.go":             "file-path routes",
	"gitlab_incidents_route.go":         "incident routes",
	"gitlab_repository_route.go":        "repository metadata",
	"gitlab_security_route.go":          "security alert routes",
	"jira_atlassian_route.go":           "Jira routes",
	"jira_dev_status.go":                "Jira routes",
	"jira_incidents_route.go":           "Jira routes",
	"jira_story_points_field.go":        "Jira routes",
	"jira_work_items_route.go":          "Jira routes",
	"launchdarkly_route.go":             "LaunchDarkly routes",
	"linear_absence.go":                 "Linear GraphQL",
	"linear_reference_catalog_route.go": "Linear GraphQL",
	"ownership_absence.go":              "ownership absence provers (error swallowed)",
	"pagerduty_incidents_route.go":      "PagerDuty routes",
	"repo_listing.go":                   "repository listing",
}

func TestEveryPageWalkAndObjectFetchNamesARouteTemplate(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	var missing []string
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(token.NewFileSet(), name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		untemplated := 0
		ast.Inspect(file, func(node ast.Node) bool {
			switch value := node.(type) {
			case *ast.CompositeLit:
				selector, ok := value.Type.(*ast.SelectorExpr)
				if !ok || (selector.Sel.Name != "GitHubPageOptions" && selector.Sel.Name != "GitLabPageOptions") {
					return true
				}
				if len(value.Elts) == 0 {
					return true
				}
				hasRoute := false
				for _, element := range value.Elts {
					if pair, ok := element.(*ast.KeyValueExpr); ok {
						if key, ok := pair.Key.(*ast.Ident); ok && key.Name == "Route" {
							hasRoute = true
						}
					}
				}
				if !hasRoute {
					untemplated++
				}
			case *ast.CallExpr:
				if selector, ok := value.Fun.(*ast.SelectorExpr); ok && selector.Sel.Name == "Do" && len(value.Args) >= 3 {
					if receiver, ok := selector.X.(*ast.Ident); ok && receiver.Name == "client" {
						untemplated++
					}
				}
				if ident, ok := value.Fun.(*ast.Ident); ok && ident.Name == "fetchObject" && name != "native_rest.go" {
					untemplated++
				}
			}
			return true
		})
		if _, listed := routeTemplateNotCovered[name]; listed {
			continue
		}
		if untemplated > 0 {
			missing = append(missing, name)
		}
	}
	sort.Strings(missing)
	if len(missing) > 0 {
		t.Fatalf("page-walk options or fetchObject calls without a Route in %v: use Route / fetchObjectRoute with a static template, or list the file in routeTemplateNotCovered with the reason", missing)
	}
	for name := range routeTemplateNotCovered {
		if _, err := os.Stat(name); err != nil {
			t.Errorf("routeTemplateNotCovered lists %s, which does not exist: %v", name, err)
		}
	}
}
