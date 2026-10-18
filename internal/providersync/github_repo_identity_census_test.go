package providersync

import (
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"net/http"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// githubRepoIdentityField finds the RepoIdentity field of a production handler,
// looking one struct level down (the PR social handler wraps the PR handler;
// the work-items handler wraps the REST collector).
func githubRepoIdentityField(value reflect.Value) (reflect.Value, bool) {
	if field := value.FieldByName("RepoIdentity"); field.IsValid() {
		return field, true
	}
	for index := 0; index < value.NumField(); index++ {
		if value.Field(index).Kind() == reflect.Struct {
			if field := value.Field(index).FieldByName("RepoIdentity"); field.IsValid() {
				return field, true
			}
		}
	}
	return reflect.Value{}, false
}

func githubCensusHandlers(t *testing.T) map[string]CompleteRouteHandler {
	t.Helper()
	lease := providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })
	out := map[string]CompleteRouteHandler{}
	for _, capability := range Capabilities("github") {
		if capability.Dataset == "work-items" {
			if route, ok := SelectWorkItemsRouteHandler("github", "work-items", WorkItemsRouteDeps{
				Conn: selectorConn{}, Lease: lease, StatusMapping: &StatusMapping{},
			}); ok {
				out[capability.Dataset] = route
			}
		} else if route, ok := SelectGitFamilyRoute("github", capability.Dataset, GitFamilyDeps{Conn: selectorConn{}, Lease: lease}); ok {
			out[capability.Dataset] = route.Handler
		}
	}
	return out
}

// Every production GitHub route that keys rows by repository resolves the
// repository identity through the store (CHAOS-9879): a route that minted from
// the current name would split a renamed repository's rows from the others'.
func TestEveryProductionGitHubRouteResolvesTheStoredRepositoryIdentity(t *testing.T) {
	selected := 0
	for dataset, handler := range githubCensusHandlers(t) {
		value := reflect.New(reflect.TypeOf(handler)).Elem()
		value.Set(reflect.ValueOf(handler))
		field, ok := githubRepoIdentityField(value)
		if !ok {
			t.Errorf("github %s: %T has no RepoIdentity field", dataset, handler)
			continue
		}
		reader, ok := field.Interface().(GitLabRepoIdentityClickHouseReader)
		if !ok || reader.Conn == nil || reader.Lease == nil || reader.Provider != "github" {
			t.Errorf("github %s: %T is selected without the github ClickHouse repos reader (%#v)", dataset, handler, field.Interface())
		}
		selected++
	}
	if selected < 13 {
		t.Fatalf("%d github datasets resolve the identity, want at least 13", selected)
	}

	// The remaining name-derived ids are named here, each owed to the follow-up
	// on CHAOS-9879: Projects v2 membership subjects and project work items
	// name another repository by its current name only.
	allowed := map[string]bool{
		"github_repository_route.go":                 true, // the definition
		"github_repo_identity.go":                    true, // nil-reader mint
		"github_project_membership_snapshot_diff.go": true,
		"github_work_items_projects_v2.go":           true,
	}
	files, err := filepath.Glob("github_*.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") || allowed[file] {
			continue
		}
		parsed, err := parser.ParseFile(token.NewFileSet(), file, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(parsed, func(node ast.Node) bool {
			if call, ok := node.(*ast.CallExpr); ok {
				if ident, ok := call.Fun.(*ast.Ident); ok && ident.Name == "repositoryIdentity" {
					t.Errorf("%s mints a GitHub repository id from a name; use resolveGitHubRepoIdentity", file)
				}
			}
			return true
		})
	}
}

type refusingGitHubRepoIdentityReader struct{ calls *int }

var errCensusGitHubRepoIdentityRead = errors.New("census: the route read the stored repository identity")

func (reader refusingGitHubRepoIdentityReader) StoredRepoIdentities(context.Context, Claim, int64, string) ([]GitLabStoredRepoIdentity, error) {
	*reader.calls++
	return nil, errCensusGitHubRepoIdentityRead
}

func (reader refusingGitHubRepoIdentityReader) StoredRepoIdentitiesAtPaths(context.Context, Claim, []string) ([]GitLabStoredRepoIdentity, error) {
	*reader.calls++
	return nil, errCensusGitHubRepoIdentityRead
}

type censusGitHubRepoDoer struct{}

func (censusGitHubRepoDoer) Do(request *http.Request) (*http.Response, error) {
	body := `[]`
	if request.URL.Path == "/repos/acme/api" {
		body = `{"id":123,"name":"api","full_name":"acme/api","html_url":"https://github.com/acme/api","default_branch":"main"}`
	}
	return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"application/json"}},
		Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
}

// The behaviour half: every production-selected GitHub handler consults its
// reader on Collect. The reader refuses, so a route that resolved its id
// another way would return a batch instead of the refusal.
func TestEveryProductionGitHubRouteConsultsItsReaderOnCollect(t *testing.T) {
	checked := 0
	for dataset, handler := range githubCensusHandlers(t) {
		value := reflect.New(reflect.TypeOf(handler)).Elem()
		value.Set(reflect.ValueOf(handler))
		field, ok := githubRepoIdentityField(value)
		if !ok {
			continue
		}
		calls := 0
		field.Set(reflect.ValueOf(refusingGitHubRepoIdentityReader{calls: &calls}))
		claim := nativeTestClaim("github", dataset)
		batch, err := value.Interface().(CompleteRouteHandler).Collect(context.Background(), claim,
			providerfoundation.Credential{Provider: "github", ID: claim.CredentialID},
			gitHubRepositoryClient(t, censusGitHubRepoDoer{}, "https://api.github.com"),
			time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC))
		if !errors.Is(err, errCensusGitHubRepoIdentityRead) || calls == 0 {
			t.Errorf("github %s: %T did not consult its reader (calls=%d, err=%v, effects=%d)",
				dataset, handler, calls, err, len(batch.Effects))
		}
		checked++
	}
	if checked < 13 {
		t.Fatalf("%d github datasets checked, want at least 13", checked)
	}
}
