package providersync

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// Every production GitLab route that keys rows by repository resolves the
// repository identity through the store (CHAOS-3383): a route that minted from
// the current path would split a renamed project's rows from the others'.
//
// Limits: the selection half reads the two production selectors only, and
// the source half reads the gitlab_*.go files of this package.
func TestEveryProductionGitLabRouteResolvesTheStoredRepositoryIdentity(t *testing.T) {
	lease := providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })
	selected := 0
	for _, capability := range Capabilities("gitlab") {
		var handler any
		if capability.Dataset == "work-items" {
			route, ok := SelectWorkItemsRouteHandler("gitlab", "work-items", WorkItemsRouteDeps{
				Conn: selectorConn{}, Lease: lease, StatusMapping: &StatusMapping{},
			})
			if !ok {
				t.Fatal("no gitlab work-items route")
			}
			handler = route
		} else if route, ok := SelectGitFamilyRoute("gitlab", capability.Dataset, GitFamilyDeps{Conn: selectorConn{}, Lease: lease}); ok {
			handler = route.Handler
		} else {
			continue
		}
		field := reflect.ValueOf(handler).FieldByName("RepoIdentity")
		if !field.IsValid() {
			if usesGitLabRepoIdentity(t, handler) {
				t.Errorf("gitlab %s: %T resolves a repository identity but has no RepoIdentity field", capability.Dataset, handler)
			}
			continue
		}
		reader, ok := field.Interface().(GitLabRepoIdentityClickHouseReader)
		if !ok || reader.Conn == nil || reader.Lease == nil {
			t.Errorf("gitlab %s: %T is selected without the ClickHouse repos reader (%#v)", capability.Dataset, handler, field.Interface())
		}
		selected++
	}
	if selected < 12 {
		t.Fatalf("%d gitlab datasets resolve the identity, want at least 12 (repository, commits, commit-stats, cicd, tests, incidents, deployments, files, blame, prs, pr-reviews, pr-comments, security, work-items)", selected)
	}

	files, err := filepath.Glob("gitlab_*.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		parsed, err := parser.ParseFile(token.NewFileSet(), file, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(parsed, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			if ident, ok := call.Fun.(*ast.Ident); ok && ident.Name == "repositoryIdentity" && file != "gitlab_repo_identity.go" {
				t.Errorf("%s mints a GitLab repository id from a path; use resolveGitLabRepoIdentity", file)
			}
			return true
		})
	}
}

// usesGitLabRepoIdentity reports whether handler's type is one whose routes
// key rows by repository: it names one in its Collect file.
func usesGitLabRepoIdentity(t *testing.T, handler any) bool {
	t.Helper()
	name := reflect.TypeOf(handler).Name()
	files, err := filepath.Glob("gitlab_*.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		parsed, err := parser.ParseFile(token.NewFileSet(), file, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		declares, resolves := false, false
		ast.Inspect(parsed, func(node ast.Node) bool {
			switch value := node.(type) {
			case *ast.TypeSpec:
				declares = declares || value.Name.Name == name
			case *ast.Ident:
				resolves = resolves || value.Name == "resolveGitLabRepoID" || value.Name == "resolveGitLabRepoIdentity" ||
					value.Name == "repositoryIdentity"
			}
			return true
		})
		if declares && resolves {
			return true
		}
	}
	return false
}
